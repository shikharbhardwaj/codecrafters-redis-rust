mod log;

mod connection;
use std::time::{SystemTime, UNIX_EPOCH};

pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod commands;
pub use commands::Command;

mod db;
pub use db::SharedState;
pub use db::RedisState;

mod replication;
pub use replication::ReplicationInfo;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

pub const DELIM: &[u8; 2] = b"\r\n";

pub const PIPELINE_MAX_COMMANDS: usize = 500;

pub fn get_unix_ts_millis() -> u128 {
    let start = SystemTime::now();

    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards").as_millis()
}