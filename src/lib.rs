mod log;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod commands;
pub use commands::Command;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;

pub const DELIM: &[u8; 2] = b"\r\n";