use bytes::Bytes;

use crate::{Connection, Frame, warn};

#[derive(Debug)]
pub struct Ping {
}

impl Ping {
    pub fn new() -> Ping {
        Ping{}
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let reply = Bytes::from_static(b"PONG");

        dst.write_frame(&Frame::Bulk(reply)).await?;
        Ok(())
    }
}


#[derive(Debug)]
pub struct Unknown {
}

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
pub enum Command {
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("Need a RESP array as command, got {:?}", frame).into())
        };

        let command_name = match &array[0] {
            Frame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?.to_lowercase(),
            frame => return Err(format!("Need a RESP array as command, got {:?}", frame).into())
        };

        match command_name.as_str() {
            "ping" => Ok(Command::Ping(Ping::new())),
            _ => Ok(Command::Unknown(Unknown::new()))
        }
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()>{
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
