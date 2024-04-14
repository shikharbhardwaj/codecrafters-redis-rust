use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use bytes::Bytes;

use crate::ReplicationInfo;

pub type SharedRedisState = Arc<Mutex<RedisState>>;

pub struct RedisState {
    db: HashMap<String, (Bytes, Option<u128>)>,
    replication_info: ReplicationInfo,
}

impl RedisState {
    pub fn new(replicaof: Option<String>, listening_port: String) -> Self {
        Self {
            db: HashMap::new(),
            replication_info: ReplicationInfo::new(replicaof, listening_port),
        }
    }

    pub fn insert(&mut self, key: String, value: Bytes, expiry: Option<u128>) {
        self.db.insert(key, (value, expiry));
    }

    pub fn get(&self, key: &str) -> Option<&(Bytes, Option<u128>)> {
        self.db.get(key)
    }

    pub fn remove(&mut self, key: &str) {
        self.db.remove(key);
    }

    pub fn get_replication_info(&self) -> ReplicationInfo {
        self.replication_info.clone()
    }
    
    pub fn add_replica(&mut self, addr: String) {
        self.replication_info.add_replica(addr);
    }
    
    pub fn get_replicas(&self) -> Vec<String> {
        self.replication_info.get_replicas().clone()
    }
}