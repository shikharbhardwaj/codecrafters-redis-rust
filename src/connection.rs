use std::io::{Cursor, self};
use std::sync::Arc;
use std::task::{Context, Wake};
use std::thread::{self, Thread};

use bytes::{Buf, BytesMut};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{debug, info, DELIM};
use crate::frame::{self, Frame};

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

/// A waker that wakes up the current thread when called.
struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Read a frame from the connection.
    /// 
    /// Returns `None` if EOF is read.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            debug!("read_frame(): Start");

            // Try to see if we can parse a frame from the current buffer.
            if let Some(frame) = self.parse_frame()? {
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
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        debug!("parse_frame(): Start");
        use frame::Error::Incomplete;

        let mut buf = Cursor::new(&self.buffer[..]);

        debug!("parse_frame(): match");

        match Frame::check(&mut buf) {
            Ok(_) => {
                // Get the current position in the buffer.
                let len = buf.position() as usize;

                // Reset to the beginning, to allow parsing.
                buf.set_position(0);

                // Parse the frame out of the buffer.
                let frame = Frame::parse(&mut buf)?;

                // Advance the buffer past this frame.
                self.buffer.advance(len);

                Ok(Some(frame))
            },
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
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
                let len = bytes.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;

                self.stream.write_all(bytes).await?;
                self.stream.write_all(DELIM).await?;
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

    pub async fn is_read_ready(&self) -> bool {
        let t = thread::current();
        let waker = Arc::new(ThreadWaker(t)).into();
        let mut cx = Context::from_waker(&waker);

        let res = self.stream.poll_read_ready(& mut cx);

        res.is_ready()
    }

    pub fn get_buf(&mut self) -> String {
        String::from_utf8(self.buffer.to_vec()).unwrap()
    }
}