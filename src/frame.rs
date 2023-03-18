use bytes::{Buf, Bytes};

use std::fmt;
use std::io::{Cursor, Read};
use std::string::FromUtf8Error;

use std::convert::TryInto;
use std::num::TryFromIntError;

use crate::{debug, warn};

#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message format
    Other(crate::Error),
}

impl Frame {
    /// Checks if the buffer has enough data to decode a frame.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'$' => { // RESP string.
                let len: usize = get_decimal(src)?.try_into()?;

                skip(src, len + 2)
            }
            b'*' => { // RESP array.
                let len: usize = get_decimal(src)?.try_into()?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// Parses the buffer into a Frame.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'$' => { // RESP string.
                let len: usize = get_decimal(src)?.try_into()?;

                debug!("Parsing decimal string with length: {}", len);

                let n = len + 2;

                if src.remaining() < n {
                    debug!("Had {} remaining elements, needed {}", src.remaining(), n);
                    return Err(Error::Incomplete);
                }

                let mut buffer = vec![0; len];
                std::io::Read::take(&mut src.by_ref(), len as u64).read_exact(&mut buffer).unwrap();

                // Skip the delimiter.
                skip(src, 2)?;

                Ok(Frame::Bulk(buffer.into()))
            }
            b'*' => { // RESP array.
                let len = get_decimal(src)?.try_into()?;

                let mut result = Vec::with_capacity(len);
                
                for i in 0..len {
                    debug!("Parsing array element: {}", i);
                    let part = Frame::parse(src)?;
                    result.push(part);
                }

                Ok(Frame::Array(result))
            }
            _ => {
                warn!("Woohoo!");

                Ok(Frame::Null)
            },
        }
    }
}

/// Skip the given number of bytes, return an error if not possible.
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the \n
            src.set_position((i + 2) as u64);

            // Return the line
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let line = get_line(src)?;

    debug!("Got line: {}", String::from_utf8(line.to_vec())?);

    let mut result = 0u64;

    for &b in line.iter() {
        if  !b.is_ascii_digit() {
            return Err(Error::Other("Invalid decimal string".into())); 
        }
        result = result * 10 + (b - b'0') as u64;
    }

    Ok(result)
}

/// Read a u8
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}