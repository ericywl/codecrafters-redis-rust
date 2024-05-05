use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::debug;

use crate::util;

use super::{
    cmd::{Command, CommandError},
    resp::{DecodeError, EncodeError, Value},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request(Value);

impl Request {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self(Value::decode(buf)?))
    }

    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        encode_value(&self.0)
    }

    pub fn as_command(&self) -> Result<Command, CommandError> {
        Command::try_from(self.0.clone())
    }
}

impl From<Value> for Request {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

impl Into<Value> for Request {
    fn into(self) -> Value {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response(Value);

impl Response {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self(Value::decode(buf)?))
    }

    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        encode_value(&self.0)
    }
}

impl From<Value> for Response {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

impl Into<Value> for Response {
    fn into(self) -> Value {
        self.0
    }
}

fn encode_value(val: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut write_buf = Vec::new();
    let mut buf = util::Buffer::new(&mut write_buf);
    val.encode(&mut buf)?;

    let count = buf.count;
    Ok(buf.inner[..count].to_vec())
}

#[async_trait]
pub trait Responder {
    async fn respond(&self, req: Request) -> Result<Response, SessionError>;
}

#[derive(Debug)]
pub struct Session {
    stream: TcpStream,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error(transparent)]
    Encode(#[from] EncodeError),

    #[error(transparent)]
    Decode(#[from] DecodeError),

    #[error(transparent)]
    TokioIo(#[from] tokio::io::Error),
}

impl Session {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn receive_request(&mut self) -> Result<Option<Request>, SessionError> {
        let mut buf = [0u8; 512];
        let bytes_read = self.stream.read(&mut buf).await?;
        if bytes_read == 0 {
            return Ok(None);
        }

        debug!("Received {:?}", &buf[..bytes_read]);
        Ok(Some(Request::decode(&buf[..bytes_read])?))
    }

    pub async fn send_response(&mut self, resp: Response) -> Result<(), SessionError> {
        let buf = resp.encode()?;
        self.stream.write(&buf).await?;

        Ok(())
    }

    pub async fn send_request_and_wait_reply(
        &mut self,
        req: Request,
    ) -> Result<Option<Response>, SessionError> {
        let buf = req.encode()?;
        self.stream.write(&buf).await?;

        let mut buf = [0u8; 512];
        let bytes_read = self.stream.read(&mut buf).await?;
        if bytes_read == 0 {
            return Ok(None);
        }

        Ok(Some(Response::decode(&buf[..bytes_read])?))
    }
}

pub fn response_is_simple_string(resp: Response, expected: &str) -> bool {
    response_is(resp, Value::SimpleString(expected.into()))
}

pub fn response_is(resp: Response, expected: Value) -> bool {
    let value: Value = resp.into();
    value == expected
}
