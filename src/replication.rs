use bytes::Bytes;
use tokio::net::TcpStream;

use crate::{debug, info, Command, Connection, Frame, SharedRedisState};

pub const EMPTY_RDB_FILE_BYTES: &[u8] = &[
    0x52,0x45,0x44,0x49,0x53,0x30,0x30,0x31,0x31,0xfa,0x09,0x72,0x65,0x64,0x69,0x73,
    0x2d,0x76,0x65,0x72,0x05,0x37,0x2e,0x32,0x2e,0x30,0xfa,0x0a,0x72,0x65,0x64,0x69,
    0x73,0x2d,0x62,0x69,0x74,0x73,0xc0,0x40,0xfa,0x05,0x63,0x74,0x69,0x6d,0x65,0xc2,
    0x6d,0x08,0xbc,0x65,0xfa,0x08,0x75,0x73,0x65,0x64,0x2d,0x6d,0x65,0x6d,0xc2,0xb0,
    0xc4,0x10,0x00,0xfa,0x08,0x61,0x6f,0x66,0x2d,0x62,0x61,0x73,0x65,0xc0,0x00,0xff,
    0xf0,0x6e,0x3b,0xfe,0xc0,0xff,0x5a,0xa2,
];


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
    reaplicaof_addr: Option<String>,
    listening_port: String,
    replicas: Vec<String>,
}

impl ReplicationInfo {
    pub fn new(replicaof: Option<String>, listening_port: String) -> Self {
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
            reaplicaof_addr: replicaof,
            listening_port: listening_port,
            replicas: vec![],
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

    pub fn get_replication_id(&self) -> String {
        self.master_replication_id.clone()
    }

    pub fn get_replication_offset(&self) -> u64 {
        self.master_repl_offset
    }

    pub fn add_replica(&mut self, addr: String) {
        assert!(self.role == "master");
        self.replicas.push(addr);
        self.connected_slaves += 1;
    }

    pub fn get_replicas(&self) -> Vec<String> {
        self.replicas.clone()
    }
}

// ReplicationWorker is responsible for managing the replication behaviour of the server.
pub struct ReplicationWorker {
    replication_info: ReplicationInfo,
    db: SharedRedisState,
    connection: Option<Connection>,
}

impl ReplicationWorker {
    pub fn new(replication_info: ReplicationInfo, db: SharedRedisState) -> Self {
        Self { replication_info, db, connection: None }
    }

    // Start the replication worker as a background tokio task.
    pub async fn start(&mut self) -> crate::Result<()> {
        info!("Starting replication worker");
        self.connection = Some(self.connect().await?);

        self.handshake().await?;

        let conn = self.connection.as_mut().unwrap();

        debug!("Start waiting for frames");
        while let Some(frame) = conn.read_frame(false).await? {
            debug!("Got frame: {:?}", frame);

            match Command::from_frame(frame) {
                Ok(Command::Set(cmd)) => cmd.apply_replica(self.db.clone()).await?,
                _ => {
                    debug!("Encountered error while replaying replicated command")
                }, // TODO: Error handling?
            }
        }

        Ok(())
    }

    async fn connect(&mut self) -> crate::Result<Connection> {
        let stream = TcpStream::connect(self.replication_info.reaplicaof_addr.as_ref().unwrap()).await?;
        return Ok(Connection::new(stream));
    }

    async fn handshake(&mut self) -> crate::Result<()> {
        let conn = self.connection.as_mut().unwrap();

        // Send the first ping.
        conn.write_frame(&Frame::Array(vec![
            Frame::Bulk(Some(Bytes::from("PING"))),
        ])).await?;

        if let Some(pong) = conn.read_frame(false).await? {
            if let Frame::Simple(pong) = pong {
                assert!(pong.to_lowercase() == "pong");
                info!("Received response: {}", pong);
            } else {
                return Err("Did not get PONG response from master".into());
            }
        }

        conn.write_frame(&Frame::Array(vec![
            Frame::Bulk(Some(Bytes::from("REPLCONF"))),
            Frame::Bulk(Some(Bytes::from("listening-port"))),
            Frame::Bulk(Some(Bytes::from(self.replication_info.listening_port.clone()))),
        ])).await?;

        if let Some(ok) = conn.read_frame(false).await? {
            if let Frame::Simple(ok) = ok {
                assert!(ok.to_lowercase() == "ok");
                info!("Received response: {}", ok);
            } else {
                return Err("Did not get OK response from master".into());
            }
        }

        conn.write_frame(&Frame::Array(vec![
            Frame::Bulk(Some(Bytes::from("REPLCONF"))),
            Frame::Bulk(Some(Bytes::from("capa"))),
            Frame::Bulk(Some(Bytes::from("psync2"))),
        ])).await?;

        if let Some(ok) = conn.read_frame(false).await? {
            if let Frame::Simple(ok) = ok {
                assert!(ok.to_lowercase() == "ok");
                info!("Received response: {}", ok);
            } else {
                return Err("Did not get OK response from master".into());
            }
        }

        conn.write_frame(&Frame::Array(vec![
            Frame::Bulk(Some(Bytes::from("PSYNC"))),
            Frame::Bulk(Some(Bytes::from("?"))),
            Frame::Bulk(Some(Bytes::from("-1"))),
        ])).await?;

        if let Some(resync) = conn.read_frame(false).await? {
            if let Frame::Simple(resync) = resync {
                info!("Received response: {}", resync);
            } else {
                return Err("Did not get OK response from master".into());
            }
        }

        if let Some(rdb) = conn.read_frame(true).await? {
            if let Frame::File(rdb) = rdb {
                info!("Received RDB file of size: {:?}", rdb.len());
            } else {
                return Err("Did not get RDB file from master".into());
            }
        }

        Ok(())
    }
}