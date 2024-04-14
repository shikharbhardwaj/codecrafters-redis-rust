use std::collections::HashMap;
use std::io::{self, Cursor};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::{debug, DELIM};
use crate::frame::{self, Frame};

pub struct ReadConnection {
    stream: OwnedReadHalf,
    buffer: BytesMut,
}

impl ReadConnection {
    pub fn new(stream: OwnedReadHalf) -> ReadConnection {
        ReadConnection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Read a frame from the connection.
    /// 
    /// Returns `None` if EOF is read.
    pub async fn read_frame(&mut self, expect_file: bool) -> crate::Result<Option<Frame>> {
        loop {
            debug!("read_frame(): Start");

            // Try to see if we can parse a frame from the current buffer.
            if let Some(frame) = self.parse_frame(expect_file)? {
                debug!("read_frame(): Parsing OK");
                return Ok(Some(frame));
            }

            // We don't have enough data to parse a frame.
            // Attempt to read more data from the socket to the buffer.

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // No more data was read from the buffer, meaning the remote end
                // closed the connection. For this to have been a clean
                // shutdown, there should be no data in the buffer, otherwise
                // the peer closed the connection while sending a frame.
                if self.buffer.is_empty() {
                    debug!("read_frame(): Exit from empty");
                    return Ok(None);
                } else {
                    return Err("Connection reset by peer".into());
                }
            }
            debug!("read_frame(): Continuing loop");
        }
    }

    /// Parse a frame to the connection.
    fn parse_frame(&mut self, expect_file: bool) -> crate::Result<Option<Frame>> {
        debug!("parse_frame(): Start");
        use frame::Error::Incomplete;

        let mut buf = Cursor::new(&self.buffer[..]);

        debug!("parse_frame(): match");

        match Frame::check(&mut buf, expect_file) {
            Ok(_) => {
                // Get the current position in the buffer.
                let len = buf.position() as usize;

                // Reset to the beginning, to allow parsing.
                buf.set_position(0);

                // Parse the frame out of the buffer.
                let frame = Frame::parse(&mut buf, expect_file)?;

                // Advance the buffer past this frame.
                self.buffer.advance(len);

                Ok(Some(frame))
            },
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

pub struct WriteConnection {
    stream: OwnedWriteHalf,
}

impl WriteConnection {
    pub fn new(stream: OwnedWriteHalf) -> WriteConnection {
        WriteConnection {
            stream
        }
    }

    /// Write a frame to the connection.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;

                self.write_decimal(val.len() as u64).await?;

                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?
        }

        Ok(())
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Bulk(bytes) => {
                if let Some(content) = bytes {
                    let len = content.len();

                    self.stream.write_u8(b'$').await?;
                    self.write_decimal(len as u64).await?;

                    self.stream.write_all(content).await?;
                    self.stream.write_all(DELIM).await?;
                } else {
                    self.stream.write_u8(b'$').await?;
                    self.stream.write_u8(b'-').await?;
                    self.stream.write_u8(b'1').await?;
                    self.stream.write_all(DELIM).await?;
                }
            },
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;

                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(DELIM).await?;
            },
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;

                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(DELIM).await?;
            },
            Frame::File(contents) => {
                let len = contents.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;

                self.stream.write_all(contents).await?;
            },
            _ => {}
        }

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(DELIM).await?;

        Ok(())
    }
}

pub struct Connection {
    w_conn: WriteConnection,
    r_conn: ReadConnection,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        let (r, w) = stream.into_split();

        Connection {
            w_conn: WriteConnection::new(w),
            r_conn: ReadConnection::new(r),
        }
    }

    pub async fn read_frame(&mut self, expect_file: bool) -> crate::Result<Option<Frame>> {
        self.r_conn.read_frame(expect_file).await
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.w_conn.write_frame(frame).await
    }
}

pub struct ConnectionManager {
    read_connections: Arc<Mutex<HashMap<String, Arc<Mutex<ReadConnection>>>>>,
    write_connections: Arc<Mutex<HashMap<String, Arc<Mutex<WriteConnection>>>>>
}

impl ConnectionManager {
    pub fn new() -> Self {
        ConnectionManager {
            read_connections: Arc::new(Mutex::new(HashMap::new())),
            write_connections: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    async fn get_read_conn(&self, addr: String) -> Option<Arc<Mutex<ReadConnection>>> {
        let connections = self.read_connections.lock().await;

        if let Some(conn) = connections.get(&addr) {
            return Some(conn.clone());
        }

        None
    }

    async fn get_write_conn(&self, addr: String) -> Option<Arc<Mutex<WriteConnection>>> {
        let connections = self.write_connections.lock().await;

        if let Some(conn) = connections.get(&addr) {
            return Some(conn.clone());
        }

        None
    }

    pub async fn add(&self, addr: String, stream: TcpStream) {
        let (rconn, wconn) = stream.into_split();

        let mut read_connections = self.read_connections.lock().await;
        let rconn = Arc::new(Mutex::new(ReadConnection::new(rconn)));
        read_connections.insert(addr.clone(), rconn.clone());

        let mut write_connections = self.write_connections.lock().await;
        let wconn = Arc::new(Mutex::new(WriteConnection::new(wconn)));
        write_connections.insert(addr, wconn.clone());
    }

    pub async fn read_frame(&self, addr: String, expect_file: bool) -> crate::Result<Option<Frame>> {
        let conn = self.get_read_conn(addr).await;

        if let Some(conn) = conn {
            debug!("Getting conn lock");
            let mut conn = conn.lock().await;
            debug!("Got conn lock");
            conn.read_frame(expect_file).await
        } else {
            Err("Connection not found".into())
        }
    }

    pub async fn write_frame(&self, addr: String, frame: &Frame) -> io::Result<()> {
        debug!("Writing to addr: {}", addr);
        let conn = self.get_write_conn(addr).await;
        debug!("Got conn");

        if let Some(conn) = conn {
            debug!("Getting conn lock");
            let mut conn = conn.lock().await;
            debug!("Got conn lock");
            conn.write_frame(frame).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Connection not found"))
        }
    }

    pub fn clone(&self) -> Self {
        ConnectionManager {
            read_connections: self.read_connections.clone(),
            write_connections: self.write_connections.clone()
        }
    }
}
