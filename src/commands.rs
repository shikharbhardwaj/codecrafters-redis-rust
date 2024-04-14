use bytes::Bytes;

use crate::{debug, get_unix_ts_millis, warn, Connection, ConnectionManager, Frame, SharedRedisState};

#[derive(Debug)]
pub struct Ping {}

impl Ping {
    pub fn new() -> Ping {
        Ping {}
    }

    pub async fn apply(self, dst_addr: String, _db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        conn_manager.write_frame(dst_addr, &Frame::Simple("PONG".to_string())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Unknown {}

impl Unknown {
    pub fn new() -> Unknown {
        Unknown {}
    }

    pub async fn apply(self, _dst_addr: String, _db: SharedRedisState, _conn_manager: ConnectionManager) -> crate::Result<()> {
        // ...
        warn!("Not implemented!");
        Err("Command not supported".into())
    }
}

#[derive(Debug)]
pub struct CommandList {}

impl CommandList {
    pub fn new() -> CommandList {
        CommandList {}
    }

    pub async fn apply(self, dst_addr: String, _db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        conn_manager.write_frame(dst_addr, &Frame::Array(vec![])).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Echo {
    arg: Bytes,
}

impl Echo {
    pub fn new(arg: Bytes) -> Echo {
        Echo { arg }
    }

    pub async fn apply(self, dst_addr: String, _db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        conn_manager.write_frame(dst_addr, &Frame::Bulk(Some(self.arg))).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Set {
    key: String,
    val: Bytes,
    expiry_duration_millis: Option<u128>,
}

impl Set {
    pub fn new(key: String, val: Bytes, expiry_duration_millis: Option<u128>) -> Set {
        Set {
            key,
            val,
            expiry_duration_millis,
        }
    }

    pub async fn apply(self, dst_addr: String, db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        let mut db = db.lock().await;

        if let Some(duration) = self.expiry_duration_millis {
            let ts = get_unix_ts_millis() + duration;

            db.insert(self.key.clone(), self.val.clone(), Some(ts));
        } else {
            db.insert(self.key.clone(), self.val.clone(), None);
        }

        debug!("Replicating SET command");
        let replicas = db.get_replicas();
        self.replicate(replicas, &conn_manager).await?;
        debug!("Done replicating SET command");

        conn_manager.write_frame(dst_addr, &Frame::Simple("OK".to_string())).await?;

        Ok(())
    }

    pub async fn apply_replica(self, db: SharedRedisState) -> crate::Result<()> {
        let mut db = db.lock().await;

        if let Some(duration) = self.expiry_duration_millis {
            let ts = get_unix_ts_millis() + duration;

            db.insert(self.key.clone(), self.val.clone(), Some(ts));
        } else {
            db.insert(self.key.clone(), self.val.clone(), None);
        }

        Ok(())
    }

    async fn replicate(self, replicas: Vec<String>, conn_manager: &ConnectionManager) -> crate::Result<()> {
        for replica in replicas {
            debug!("Replicating to replica: {}", replica);
            conn_manager.write_frame(replica, &Frame::Array(vec![
                Frame::Bulk(Some(Bytes::from("SET"))),
                Frame::Bulk(Some(Bytes::from(self.key.clone()))),
                Frame::Bulk(Some(self.val.clone())),
            ])).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Get {
        Get { key }
    }

    pub async fn apply(self, dst_addr: String, db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        let mut db = db.lock().await;

        let mut valid = false;

        if let Some((val, epxiry)) = db.get(&self.key) {
            valid = true;

            if let Some(ts) = epxiry {
                valid = ts > &get_unix_ts_millis();
            }

            if valid {
                conn_manager.write_frame(dst_addr.clone(), &Frame::Bulk(Some(val.clone()))).await?;
            } else {
                db.remove(&self.key);
            }
        }

        if !valid {
            conn_manager.write_frame(dst_addr, &Frame::Bulk(None)).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct Info {
    section: Option<String>,
}

impl Info {
    pub fn new(section: Option<String>) -> Info {
        Info { section }
    }

    pub async fn apply(self, dst_addr: String, db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        if let Some(section) = self.section {
            match section.as_str() {
                "replication" => {
                    let db = db.lock().await;
                    conn_manager.write_frame(dst_addr, &Frame::Bulk(Some(db.get_replication_info().get_info_bytes()))).await?;
                }
                _ => {
                    conn_manager.write_frame(dst_addr, &Frame::Error("ERR: Invalid section".to_string())).await?;
                } // Handle all other possible values of section
            }
        } else {
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum ReplConfOption {
    ListeningPort(String),
    Capabilities(Vec<String>),
    GetAck(String),
}

#[derive(Debug)]
pub struct ReplConf {
    pub option: ReplConfOption,
}


impl ReplConf {
    pub fn new(option: ReplConfOption) -> ReplConf {
        ReplConf { option }
    }

    pub async fn apply(self, dst_addr: String, _db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        conn_manager.write_frame(dst_addr, &Frame::Simple("OK".to_string())).await?;

        Ok(())
    }

    pub async fn apply_replica(self, dst: & mut Connection, _db: SharedRedisState) -> crate::Result<()> {
        match self.option {
            ReplConfOption::GetAck(_) => {
                dst.write_frame(&Frame::Array(vec![
                    Frame::Bulk(Some(Bytes::from("REPLCONF"))),
                    Frame::Bulk(Some(Bytes::from("ACK"))),
                    Frame::Bulk(Some(Bytes::from("0"))),
                ])).await?;

                Ok(())
            },
            _ => { Err(format!("ERR: Invalid REPLCONF option passed to replica.").into()) }
        }
    }
}


#[derive(Debug)]
pub struct Psync {
    replication_id: String,
    _replication_offset: i64,
}

impl Psync {
    pub fn new(replication_id: String, _replication_offset: i64) -> Psync {
        Psync {
            replication_id,
            _replication_offset,
        }
    }

    pub async fn apply(self, dst_addr: String, db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        let mut db = db.lock().await;

        let repl_info = db.get_replication_info();

        if repl_info.get_replication_id() != self.replication_id {
            // Full resync
            conn_manager.write_frame(dst_addr.clone(), 
                &Frame::Simple(format!(
                    "FULLRESYNC {} {}",
                    repl_info.get_replication_id(),
                    repl_info.get_replication_offset()))).await?;
            
            // TODO: Send the actual RDB snapshot.
            conn_manager.write_frame(dst_addr.clone(), &Frame::File(Bytes::from(crate::EMPTY_RDB_FILE_BYTES))).await?;
            db.add_replica(dst_addr.clone());
        } else {
            // Partial sync
            // ...
        }

        Ok(())
    }
}


#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    CommandList(CommandList),
    Echo(Echo),
    Unknown(Unknown),
    Set(Set),
    Get(Get),
    Info(Info),
    ReplConf(ReplConf),
    Psync(Psync),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("Need a RESP array as command, got {:?}", frame).into()),
        };

        let command_name = match &array[0] {
            Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?.to_lowercase(),
            frame => return Err(format!("Need a RESP array as command, got {:?}", frame).into()),
        };

        match command_name.as_str() {
            "ping" => Ok(Command::Ping(Ping::new())),
            "command" => Ok(Command::CommandList(CommandList::new())),
            "echo" => {
                if array.len() != 2 {
                    return Err(format!("ERR: Wrong number of arguments for ECHO").into());
                }

                let arg = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => {
                        return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into())
                    }
                };

                Ok(Command::Echo(Echo::new(arg.clone())))
            }
            "get" => {
                if array.len() != 2 {
                    return Err(format!("ERR: Wrong number of arguments for GET").into());
                }

                let arg = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => {
                        return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into())
                    }
                };

                Ok(Command::Get(Get::new(String::from_utf8(arg.to_vec())?)))
            }
            "set" => {
                if array.len() != 3 && array.len() != 5 {
                    return Err(format!("ERR: Wrong number of arguments for SET").into());
                }

                let key = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => {
                        return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into())
                    }
                };

                let val = match &array[2] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => {
                        return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into())
                    }
                };

                let mut expiry_duration_millis = None;

                if array.len() == 5 {
                    let command = match &array[3] {
                        Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?,
                        Frame::Simple(val) => val.to_string(),
                        frame => {
                            return Err(
                                format!("ERR: Wrong expiry command frame, got {:?}", frame).into()
                            )
                        }
                    };

                    let multiplier = match command.to_uppercase().as_str() {
                        "EX" => 1000,
                        "PX" => 1,
                        cmd => {
                            return Err(format!("ERR: Wrong expiry command, got {:?}", cmd).into())
                        }
                    };

                    let duration = match &array[4] {
                        Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?,
                        Frame::Simple(val) => val.to_string(),
                        frame => {
                            return Err(format!(
                                "ERR: Wrong expiry duration frame, got {:?}",
                                frame
                            )
                            .into())
                        }
                    };

                    expiry_duration_millis = Some(duration.parse::<u128>().unwrap() * multiplier);
                }

                Ok(Command::Set(Set::new(
                    String::from_utf8(key.to_vec())?,
                    val.clone(),
                    expiry_duration_millis,
                )))
            },
            "info" => {
                if array.len() != 2 {
                    return Err(format!("ERR: Wrong number of arguments for INFO").into());
                }

                let arg = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => {
                        return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into())
                    }
                };

                Ok(Command::Info(Info::new(Some(String::from_utf8(arg.to_vec())?))))
            },
            "replconf" => {
                if array.len() < 3 {
                    return Err(format!("ERR: Wrong number of arguments for REPLCONF").into());
                }

                let arg = match array.get(1).unwrap() {
                    Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?,
                    frame => return Err(format!("ERR: Wrong argument for REPLCONF, got {:?}", frame).into())
                };

                if arg == "listening-port" {
                    let arg = match &array[2] {
                        Frame::Bulk(Some(bytes)) => bytes,
                        frame => return Err(format!("ERR: Wrong argument for REPLCONF, got {:?}", frame).into())
                    };
                    let listening_port = String::from_utf8(arg.to_vec())?;
                    Ok(Command::ReplConf(ReplConf::new(ReplConfOption::ListeningPort(listening_port))))
                } else if arg == "capa" {
                    let mut capabilities = Vec::new();
                    for i in 2..array.len() {
                        let arg = match &array[i] {
                            Frame::Bulk(Some(bytes)) => bytes,
                            frame => {
                                return Err(format!("ERR: Wrong argument for REPLCONF, got {:?}", frame).into())
                            }
                        };
                        capabilities.push(String::from_utf8(arg.to_vec())?);
                    }
                    Ok(Command::ReplConf(ReplConf::new(ReplConfOption::Capabilities(capabilities))))
                } else if arg == "getack" {
                    let arg = match &array[2] {
                        Frame::Bulk(Some(bytes)) => bytes,
                        frame => return Err(format!("ERR: Wrong argument for REPLCONF, got {:?}", frame).into())
                    };
                    Ok(Command::ReplConf(ReplConf::new(ReplConfOption::GetAck(String::from_utf8(arg.to_vec())?))))
                } else {
                    Err(format!("ERR: Wrong argument for REPLCONF").into())
                }
            },
            "psync" => {
                if array.len() != 3 {
                    return Err(format!("ERR: Wrong number of arguments for PSYNC").into());
                }

                let replication_id = match &array[1] {
                    Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?,
                    frame => return Err(format!("ERR: Wrong argument for PSYNC, got {:?}", frame).into())
                };

                let replication_offset = match &array[2] {
                    Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?.parse::<i64>()?,
                    frame => return Err(format!("ERR: Wrong argument for PSYNC, got {:?}", frame).into())
                };

                Ok(Command::Psync(Psync::new(replication_id, replication_offset)))
            },
            _ => Ok(Command::Unknown(Unknown::new())),
        }
    }

    pub async fn apply(self, dst_addr: String, db: SharedRedisState, conn_manager: ConnectionManager) -> crate::Result<()> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            CommandList(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            Echo(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            Unknown(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            Set(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            Get(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            Info(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            ReplConf(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
            Psync(cmd) => cmd.apply(dst_addr, db, conn_manager).await,
        }
    }
}
