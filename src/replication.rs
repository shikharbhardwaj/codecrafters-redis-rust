use bytes::Bytes;

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
    pub fn new(replicaof: Option<&String>) -> Self {
        let role = match replicaof {
            Some(_) => "slave".to_string(),
            None => "master".to_string(),
        };

        // TODO: Generate random 40 character alphanumeric string for the replication id.
        let replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

        Self {
            role,
            connected_slaves: 0,
            master_repl_offset: 0,
            master_replication_id: replication_id.to_string(),
            second_repl_offset: 0,
            repl_backlog_active: false,
            repl_backlog_size: 0,
            repl_backlog_first_byte_offset: 0,
            repl_backlog_histlen: 0,
        }
    }
    
    pub fn get_info_bytes(&self) -> Bytes {
        Bytes::from(format!(
            "# Replication\nrole:{}\nconnected_slaves:{}\nmaster_repl_offset:{}\nmaster_replid:{}\nsecond_repl_offset:{}\nrepl_backlog_active:{}\nrepl_backlog_size:{}\nrepl_backlog_first_byte_offset:{}\nrepl_backlog_histlen:{}\n",
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