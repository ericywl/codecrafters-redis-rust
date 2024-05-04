use rand::distributions::DistString;

use crate::redis::{
    cmd::{Command, CommandError},
    resp::{DecodeError, EncodeError, Value},
};

/// Buffer is a wrapper for io::Write.
struct Buffer<W> {
    inner: W,
    count: usize,
}

impl<W> Buffer<W>
where
    W: std::io::Write,
{
    fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W> std::io::Write for Buffer<W>
where
    W: std::io::Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.count += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Debug)]
pub struct Request(Value);

impl Request {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self(Value::decode(buf)?))
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

#[derive(Debug)]
pub struct Response(Value);

impl Response {
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        encode_value(&self.0)
    }
}

impl From<Value> for Response {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

pub fn encode_value(val: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut write_buf = Vec::new();
    let mut buf = Buffer::new(&mut write_buf);
    val.encode(&mut buf)?;

    let count = buf.count;
    Ok(buf.inner[..count].to_vec())
}

pub fn generate_random_alphanumeric_string(len: usize) -> String {
    rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), len)
}
