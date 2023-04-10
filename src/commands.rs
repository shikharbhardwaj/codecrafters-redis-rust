use bytes::Bytes;

use crate::{warn, Connection, Frame};

#[derive(Debug)]
pub struct Ping {}

impl Ping {
    pub fn new() -> Ping {
        Ping {}
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
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

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        // ...
        warn!("Not implemented!");
        Ok(())
    }
}

#[derive(Debug)]
pub struct CommandList {}

impl CommandList {
    pub fn new() -> CommandList {
        CommandList {}
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
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

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Bulk(self.arg)).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    CommandList(CommandList),
    Echo(Echo),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("Need a RESP array as command, got {:?}", frame).into()),
        };

        let command_name = match &array[0] {
            Frame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?.to_lowercase(),
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
                    Frame::Bulk(bytes) => bytes,
                    frame => return Err(format!("ERR: Wrong argument for ECHO, got {:?}", frame).into()),
                };

                Ok(Command::Echo(Echo::new(arg.clone())))
            }
            _ => Ok(Command::Unknown(Unknown::new())),
        }
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
            CommandList(cmd) => cmd.apply(dst).await,
            Echo(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
