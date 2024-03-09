use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use bytes::Bytes;

pub type SharedState = Arc<Mutex<RedisState>>;

#[derive(Clone)]
pub struct ReplicationInfo {
    role: String,
    connected_slaves: u64,
    master_repl_offset: u64,
    master_replication_id: String,
    second_repl_offset: i64,
    repl_backlog_active: bool,
    repl_backlog_size: u64,
    repl_backlog_first_byte_offset: u64,
    repl_backlog_histlen: u64,
}

impl ReplicationInfo {
    pub fn new() -> Self {
        Self {
            role: "master".to_string(),
            connected_slaves: 0,
            master_repl_offset: 0,
            master_replication_id: "some-replication-id".to_string(),
            second_repl_offset: 0,
            repl_backlog_active: false,
            repl_backlog_size: 0,
            repl_backlog_first_byte_offset: 0,
            repl_backlog_histlen: 0,
        }
    }
    
    pub fn get_info_bytes(&self) -> Bytes {
        Bytes::from(format!(
            "# Replication\nrole:{}\nconnected_slaves:{}\nmaster_repl_offset:{}\nmaster_replication_id:{}\nsecond_repl_offset:{}\nrepl_backlog_active:{}\nrepl_backlog_size:{}\nrepl_backlog_first_byte_offset:{}\nrepl_backlog_histlen:{}\n",
            self.role,
            self.connected_slaves,
            self.master_repl_offset,
            self.master_replication_id,
            self.second_repl_offset,
            self.repl_backlog_active,
            self.repl_backlog_size,
            self.repl_backlog_first_byte_offset,
            self.repl_backlog_histlen
        ))
    }
}

pub struct RedisState {
    db: HashMap<String, (Bytes, Option<u128>)>,
    replication_info: ReplicationInfo,
}

impl RedisState {
    pub fn new() -> Self {
        Self {
            db: HashMap::new(),
            replication_info: ReplicationInfo::new(),
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
}