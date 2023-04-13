use bytes::Bytes;

use crate::{warn, Connection, Frame, SharedDb, get_unix_ts_millis};

#[derive(Debug)]
pub struct Ping {}

impl Ping {
    pub fn new() -> Ping {
        Ping {}
    }

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
        dst.write_frame(&Frame::Simple("PONG".to_string())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Unknown {}

impl Unknown {
    pub fn new() -> Unknown {
        Unknown {}
    }

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
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

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
        dst.write_frame(&Frame::Array(vec![])).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Echo {
    arg: Bytes
}

impl Echo {
    pub fn new(arg: Bytes) -> Echo {
        Echo {
            arg
        }
    }

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
        dst.write_frame(&Frame::Bulk(Some(self.arg))).await?;

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
            key, val, expiry_duration_millis
        }
    }

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
        let mut db = db.lock().await;

        if let Some(duration) = self.expiry_duration_millis {
            let ts = get_unix_ts_millis() + duration;

            db.insert(self.key.clone(), (self.val.clone(), Some(ts)));
        } else {
            db.insert(self.key.clone(), (self.val.clone(), None));
        }


        dst.write_frame(&Frame::Simple("OK".to_string())).await?;

        Ok(())
    }
}


#[derive(Debug)]
pub struct Get {
    key: String
}

impl Get {
    pub fn new(key: String) -> Get {
        Get {
            key
        }
    }

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
        let mut db = db.lock().await;

        let mut valid = false;

        if let Some((val , epxiry)) = db.get(&self.key) {
            valid = true;

            if let Some(ts) = epxiry {
                valid = ts > &get_unix_ts_millis();
            }

            if valid {
                dst.write_frame(&Frame::Bulk(Some(val.clone()))).await?;
            } else {
                db.remove(&self.key);
            }
        } 

        if !valid {
            dst.write_frame(&Frame::Bulk(None)).await?;
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
                    return Err(format!("ERR: Wrong number of arguments for ECHO").into())
                }

                let arg = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into()),
                };

                Ok(Command::Echo(Echo::new(arg.clone())))
            },
            "get" => {
                if array.len() != 2 {
                    return Err(format!("ERR: Wrong number of arguments for GET").into())
                }

                let arg = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into()),
                };

                Ok(Command::Get(Get::new(String::from_utf8(arg.to_vec())? )))
            },
            "set" => {
                if array.len() != 3 && array.len() != 5 {
                    return Err(format!("ERR: Wrong number of arguments for SET").into())
                }

                let key = match &array[1] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into()),
                };

                let val= match &array[2] {
                    Frame::Bulk(Some(bytes)) => bytes,
                    frame => return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into()),
                };

                let mut expiry_duration_millis= None;

                if array.len() == 5 {
                    let command = match &array[3] {
                        Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?,
                        Frame::Simple(val) => val.to_string(),
                        frame => return Err(format!("ERR: Wrong expiry command frame, got {:?}", frame).into()),
                    };

                    let multiplier = match command.to_uppercase().as_str() {
                        "EX" => 1000,
                        "PX" => 1,
                        cmd => return Err(format!("ERR: Wrong expiry command, got {:?}", cmd).into()),
                    };

                    let duration = match &array[4] {
                        Frame::Bulk(Some(bytes)) => String::from_utf8(bytes.to_vec())?,
                        Frame::Simple(val) => val.to_string(),
                        frame => return Err(format!("ERR: Wrong expiry duration frame, got {:?}", frame).into()),
                    };

                    expiry_duration_millis = Some(duration.parse::<u128>().unwrap() * multiplier);
                }

                Ok(Command::Set(Set::new(String::from_utf8(key.to_vec())?, val.clone(), expiry_duration_millis)))
            },
            _ => Ok(Command::Unknown(Unknown::new())),
        }
    }

    pub async fn apply(self, dst: &mut Connection, db: SharedDb) -> crate::Result<()> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst, db).await,
            CommandList(cmd) => cmd.apply(dst, db).await,
            Echo(cmd) => cmd.apply(dst, db).await,
            Unknown(cmd) => cmd.apply(dst, db).await,
            Set(cmd) => cmd.apply(dst, db).await,
            Get(cmd) => cmd.apply(dst, db).await,
        }
    }
}
