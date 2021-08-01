//FIXME
//! Redis cluster support.
//!
//! This module extends the library to be able to use cluster.
//! ClusterClient implements traits of ConnectionLike and Commands.
//!
//! Note that the cluster support currently does not provide pubsub
//! functionality.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::ClusterClient;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::open(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let _: () = connection.set("test", "test_data").unwrap();
//! let rv: String = connection.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::{cluster_pipe, ClusterClient};
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::open(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let key = "test";
//!
//! let _: () = cluster_pipe()
//!     .rpush(key, "123").ignore()
//!     .ltrim(key, -10, -1).ignore()
//!     .expire(key, 60).ignore()
//!     .query(&mut connection).unwrap();
//! ```
use std::collections::HashSet;
use std::ops::Div;
use std::thread;
/*
 # Step 1: connecting to the first Sentinel
   + 1. iterate over sentinenls and find working sentinel to connection (timeout: a few hundreds of milliseconds)
   + 2. first sentinel answering put in front of list
   + 3. all connections time out => Error out

 # Step 2: ask for master address and slaves
   + 1. `SENTINEL get-master-addr-by-name master-name`
   + 2. If an ip:port pair is received, this address should be used to connect to the Redis master.
   +- 3. if a null reply is received, the client should try the next Sentinel in the list.
    4. `SENTINEL replicas $master-name`

 # Step 3: call the ROLE command in the target instance
   + 1. call the ROLE command in order to verify the role of the instance is actually a master.
   +/- 2. If the instance is not a master as expected, the client should wait (timeout: a few hundreds of milliseconds) and should try again starting from Step 1


 # Handling reconnections
   1. client should resolve again the address using Sentinels restarting from Step
      - If the client reconnects after a timeout or socket error.
      - If the client reconnects because it was explicitly closed or reconnected by the user.
      - or any other case where the client lost the connection with the Redis server

 # Connection pools
   1. all the existing connections should be closed and connected to the new address.

 # Error reporting
   1. If **no Sentinel** can be contacted an error that clearly states that **Redis Sentinel is unreachable** should be returned.
   2. If all the Sentinels in the pool replied with **a null reply**, the user should be informed with an error that **Sentinels don't know this master name**.

 # Sentinels list automatic refresh (Optional)
   1. Obtain a list of other Sentinels for this master using the command `SENTINEL sentinels <master-name>`.
   2. Add every ip:port pair not already existing in our list at the end of the list

 # Subscribe to Sentinel events to improve responsiveness (TODO)
*/
use std::cell::RefCell;
use std::{collections::HashMap, time::Duration};

#[cfg(feature = "aio")]
use std::pin::Pin;

use crate::{
    connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo},
    types::{RedisResult, Value},
    ErrorKind, RedisError,
};

const SENTINEL_TIMEOUT: Duration = Duration::from_millis(400);

/// The sentinel client type.
#[derive(Debug, Clone)]
pub struct SentinelClient {
    master_group_name: String,
    sentinel_nodes: Vec<ConnectionInfo>,
    master_node: Option<ConnectionInfo>,
    replica_nodes: Vec<ConnectionInfo>,
    master_verification_timeout: RefCell<Option<Duration>>,
    connection_timeout: RefCell<Option<Duration>>,
}

/// The client acts as connector to the sentinel and redis servers.  By itself it does not
/// do much other than providing a convenient way to fetch a connection from it.
/// TODO In the future the plan is to provide a connection pool in the client.
///
/// When opening a client a URL in the following format should be used:
///
/// ```plain
/// redis://host:port/db
/// ```
///
/// Example usage::
///
/// ```rust,no_run
/// let client = redis::sentinel::SentinelClient::open(vec!["redis://127.0.0.1/"],"mymaster".to_string()).unwrap();
/// let con = client.get_write_connection().unwrap();
/// ```
impl SentinelClient {
    /// Connects to a redis server and returns a client.  This does not
    /// actually open a connection yet but it does perform some basic
    /// checks on the URL that might make the operation fail then updates
    /// master and slave node ConnectionInfos
    pub fn new<T: IntoConnectionInfo>(
        sentinel_nodes: Vec<T>,
        master_name: String,
    ) -> RedisResult<SentinelClient> {
        let sen: RedisResult<Vec<ConnectionInfo>> = sentinel_nodes
            .into_iter()
            .map(|x| x.into_connection_info())
            .collect();

        let mut client = SentinelClient {
            sentinel_nodes: sen?,
            master_group_name: master_name,
            master_node: None,
            replica_nodes: vec![],
            master_verification_timeout: RefCell::new(Some(SENTINEL_TIMEOUT)),
            connection_timeout: RefCell::new(Some(SENTINEL_TIMEOUT.div(4))),
        };

        client.update_nodes()?;

        Ok(client)
    }

    /// will add newly discoverd sentinel nodes to clients sentinel nods vector
    /// and will ignore duplicated discoverd nodes
    pub fn add_sentinel_nodes(&mut self, new_nodes: Vec<ConnectionInfo>) -> RedisResult<()> {
        let mut latest_nodes = self.sentinel_nodes.clone();
        latest_nodes.extend(new_nodes.into_iter());
        latest_nodes.dedup();
        self.sentinel_nodes = latest_nodes;

        Ok(())
    }

    /// will add newly discoverd replica nodes to clients replica nodes vector
    /// and will ignore duplicated discoverd nodes
    pub fn add_replica_nodes(&mut self, new_nodes: Vec<ConnectionInfo>) -> RedisResult<()> {
        let mut latest_nodes = self.replica_nodes.clone();
        latest_nodes.extend(new_nodes.into_iter());
        latest_nodes.dedup();
        self.replica_nodes = latest_nodes;

        Ok(())
    }
}

impl SentinelClient {
    fn update_nodes(&mut self) -> RedisResult<()> {
        for (sentinel_index, sen) in self.sentinel_nodes.iter().enumerate() {
            let dur = *self.connection_timeout.borrow();

            match connect(sen, dur) {
                Ok(mut conn) => {
                    if let Ok(master_conn_info) =
                        get_master_from_sentinel(&self.master_group_name, &mut conn)
                    {
                        //check if it's null continue with next sentinel
                        println!(
                            "[+] get_master_from_sentinel => conn_info : {:?}",
                            master_conn_info
                        );

                        //Get connecting to master and verify
                        if let Ok(_master_conn) = self.verify_master_node(&master_conn_info) {
                            //set new replica redis node connectioninfos
                            self.add_replica_nodes(cmd_other_replicas(
                                &self.master_group_name,
                                &mut conn,
                            )?)?;

                            //set new sentinel redis node connectioninfos
                            self.add_sentinel_nodes(cmd_other_sentinels(
                                &self.master_group_name,
                                &mut conn,
                            )?)?;

                            //set master connection info
                            self.master_node = Some(master_conn_info);

                            // move connected node to start to minimize retries on reconnection
                            let connected_node = self.sentinel_nodes.remove(sentinel_index);
                            self.sentinel_nodes.insert(0, connected_node);

                            return Ok(());
                        } else {
                            eprintln!(
                                "[-] continue on master verification fail: {:?}",
                                master_conn_info
                            );
                            // continue on master verification fail
                            continue;
                        }
                    } else {
                        // continue on master name not present in sentinel node
                        eprintln!(
                            "[-] continue on master name not present in sentinel node: {:?}",
                            sen
                        );
                        continue;
                    }
                }
                Err(e) => {
                    eprintln!("[-] continue on unreachable host: {:?}", e);
                    continue;
                }
            };
        }

        Err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            "Sentinel update error.",
            format!(
                "Couldn't find or connect sentinels with \"{}\" group",
                self.master_group_name
            ),
        )))
    }

    /// Verifies that a node is actually master node.
    /// if nodes fails it will retry 3 times after that will move on.
    ///
    /// see step 3 of: https://redis.io/topics/sentinel-clients
    fn verify_master_node(&self, master_conn: &ConnectionInfo) -> RedisResult<Connection> {
        let mut conn = connect(master_conn, None)?;

        let retries = 3;
        for _ in 0_i32..retries {
            let role: crate::Value = crate::cmd("ROLE").query(&mut conn)?;
            // ROLE returns a complex response, so we cannot use the usual type-casting
            if let crate::Value::Bulk(values) = role {

                if values
                    .get(0)
                    .unwrap()
                    .ne(&crate::Value::Data("master".into()))
                {
                    println!("[!] Retring to verify master on {:?}", values);
                    thread::sleep(
                        (*self.master_verification_timeout.borrow())
                            .unwrap_or_else(|| Duration::from_millis(1)),
                    );
                    continue;
                } else {
                    println!("[+] GOT MASTER {:?}, on {:?}", self.master_group_name, values);
                    return Ok(conn);
                }
            }
        }

        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "sentinel pointed to master node but the node is not master",
        )));
    }
}

/// get master for the group name from the sentinel connection
fn get_master_from_sentinel(
    name: &str,
    sentinel_conn: &mut Connection,
) -> RedisResult<ConnectionInfo> {
    let master_connection: (String, u16) = crate::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg(name)
        .query(sentinel_conn)?;

    Ok(master_connection.into_connection_info()?)
}

/// Queries a sentinel node for the address of the other Redis sentinels availible.
///
/// https://redis.io/topics/sentinel-clients#sentinels-list-automatic-refresh
fn cmd_other_sentinels(
    master_group_name: &str,
    sentinel_conn: &mut crate::Connection,
) -> crate::RedisResult<Vec<ConnectionInfo>> {
    let raw_sentinels: crate::Value = crate::cmd("SENTINEL")
        .arg("SENTINELS")
        .arg(master_group_name)
        .query(sentinel_conn)?;

    // let mut sentinels:Vec<HashMap<String,String>>=Vec::new();
    let mut sentinels = Vec::new();
    if let crate::Value::Bulk(ref values) = raw_sentinels {
        for v in values {
            let m: HashMap<String, String> = crate::from_redis_value(&v)?;
            sentinels.push(
                (
                    m.get("ip").unwrap().to_string(),
                    str::parse(m.get("port").unwrap()).unwrap(),
                )
                    .into_connection_info()?,
            )
        }
    }
    Ok(sentinels)
}

/// Queries a sentinel node for the address of the other Redis replica nodes availible.
///
/// https://redis.io/topics/sentinel-clients#sentinels-list-automatic-refresh
fn cmd_other_replicas(
    master_group_name: &str,
    sentinel_conn: &mut crate::Connection,
) -> crate::RedisResult<Vec<ConnectionInfo>> {
    let raw_replicas: crate::Value = crate::cmd("SENTINEL")
        .arg("REPLICAS")
        .arg(master_group_name)
        .query(sentinel_conn)?;

    // let mut sentinels:Vec<HashMap<String,String>>=Vec::new();
    let mut replicas = Vec::new();
    if let crate::Value::Bulk(ref values) = raw_replicas {
        for v in values {
            let m: HashMap<String, String> = crate::from_redis_value(&v)?;
            replicas.push(
                (
                    m.get("ip").unwrap().to_string(),
                    str::parse(m.get("port").unwrap()).unwrap(),
                )
                    .into_connection_info()?,
            )
        }
    }
    Ok(replicas)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sentinel_connection_test() {
        let client = SentinelClient::new(
            vec![
                ("10.1.0.69", 26379_u16).into_connection_info().unwrap(),
                "redis://localhost:26379".into_connection_info().unwrap(),
                "redis://localhost:26371".into_connection_info().unwrap(),
                "redis://localhost:26372".into_connection_info().unwrap(),
            ],
            "mymaster".to_string(),
        )
        .unwrap();

        println!("\n\ngot client with \n{:?}\n",client)
        // client.
        //   assert!(sentinel::new(("fe80::cafe:beef%eno1", 6379)).is_ok());
    }
}
