use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::debug;

use super::{
    cmd::{Command, ParseCommandError},
    resp::{Array, BulkString, DecodeError, EncodeError, Value},
    util,
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

    pub fn as_command(&self) -> Result<Command, ParseCommandError> {
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

    pub fn is(&self, expected: Value) -> bool {
        self.0 == expected
    }

    pub fn is_simple_string(&self, expected: &str) -> bool {
        self.is(Value::SimpleString(expected.into()))
    }

    pub fn is_bulk_string(&self, expected: &[u8]) -> bool {
        self.is(Value::BulkString(expected.to_vec().into()))
    }

    pub fn is_bulk_string_array(&self, bulk_strings: Vec<BulkString>) -> bool {
        let values = bulk_strings
            .iter()
            .map(|bs| Value::BulkString(bs.clone()))
            .collect();

        self.is(Value::Array(Array::new(values)))
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
    async fn respond(&mut self, req: Request) -> Result<Response, SessionError>;
}

#[derive(Debug)]
pub struct Session {
    stream: TcpStream,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("No response from session")]
    NoResponse,

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
    ) -> Result<Response, SessionError> {
        let buf = req.encode()?;
        self.stream.write(&buf).await?;

        let mut buf = [0u8; 512];
        let bytes_read = self.stream.read(&mut buf).await?;
        if bytes_read == 0 {
            return Err(SessionError::NoResponse);
        }

        Ok(Response::decode(&buf[..bytes_read])?)
    }
}

#[async_trait]
impl Responder for Session {
    async fn respond(&mut self, req: Request) -> Result<Response, SessionError> {
        Ok(self.send_request_and_wait_reply(req).await?)
    }
}

#[cfg(test)]
pub struct MockResponder {
    pub expected_req: Request,
    pub returned_resp: Response,
}

#[cfg(test)]
#[async_trait]
impl Responder for MockResponder {
    async fn respond(&mut self, req: Request) -> Result<Response, SessionError> {
        assert_eq!(req, self.expected_req);
        Ok(self.returned_resp.clone())
    }
}
