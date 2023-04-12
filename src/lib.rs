mod log;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod commands;
pub use commands::Command;

mod db;
pub use db::SharedDb;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

pub const DELIM: &[u8; 2] = b"\r\n";

pub const PIPELINE_MAX_COMMANDS: usize = 500;