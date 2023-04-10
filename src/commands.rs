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
        dst.write_frame(&Frame::Simple("PONG".to_string())).await?;
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
pub struct CommandList {
}

impl CommandList {
    pub fn new() -> CommandList {
        CommandList {}
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let commands = Frame::Array(vec![
            Frame::Array(
                vec![
                    Frame::Integer(-1),
                    Frame::Array(vec![
                        Frame::Simple("stale".to_string()),
                        Frame::Simple("fast".to_string()),
                    ]),
                    Frame::Integer(0),
                    Frame::Integer(0),
                    Frame::Integer(0),
                ]
            )
        ]);

        dst.write_frame(&commands).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    CommandList(CommandList),
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
            "command" => Ok(Command::CommandList(CommandList::new())),
            _ => Ok(Command::Unknown(Unknown::new()))
        }
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()>{
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
            CommandList(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
