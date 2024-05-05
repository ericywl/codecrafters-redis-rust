pub mod echo;
pub use echo::*;
pub mod ping;
pub use ping::*;
pub mod set;
pub use set::*;

use std::time::Duration;

use thiserror::Error;

use super::resp::{Array, BulkString, DecodeError, Value};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InfoArg {
    section: InfoSection,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum InfoSection {
    Default,
    Replication,
}

impl InfoArg {
    pub fn new(section: InfoSection) -> Self {
        Self { section }
    }

    fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 0, 1)?;
        let section = Self::parse_info_section(args.get(0))?;

        Ok(Self::new(section))
    }

    fn parse_info_section(opt_bs: Option<&BulkString>) -> Result<InfoSection, ParseCommandError> {
        let section_str = match opt_bs {
            Some(bs) => {
                bs.as_str()
                    .ok_or(ParseCommandError::InvalidArgument(Value::BulkString(
                        bs.clone(),
                    )))?
            }
            None => "".to_string(),
        };

        Ok(match section_str.to_lowercase().as_str() {
            "replication" => InfoSection::Replication,
            _ => InfoSection::Default,
        })
    }

    pub fn section(&self) -> &InfoSection {
        &self.section
    }
}

#[derive(Debug, Clone)]
pub struct GetArg {
    key: BulkString,
}

impl GetArg {
    pub fn new(key: BulkString) -> Self {
        Self { key }
    }

    pub fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 1, 0)?;
        let key = args.get(0).unwrap().clone();

        Ok(Self::new(key))
    }

    pub fn key(&self) -> &BulkString {
        &self.key
    }
}

fn bulk_string_to_uint64(bs: &BulkString) -> Result<u64, ParseCommandError> {
    let s = bulk_string_to_string(bs)?;
    Ok(s.parse::<u64>().map_err(|e| DecodeError::ParseInt(e))?)
}

fn bulk_string_to_string(bs: &BulkString) -> Result<String, ParseCommandError> {
    bs.as_str()
        .ok_or(ParseCommandError::InvalidArgument(Value::BulkString(
            bs.clone(),
        )))
}

fn value_to_bulk_string(val: &Value) -> Result<BulkString, ParseCommandError> {
    Ok(val
        .bulk_string()
        .ok_or(ParseCommandError::InvalidArgument(val.clone()))?
        .clone())
}

fn consume_args_from_iter(
    iter: &mut std::slice::Iter<'_, Value>,
    necessary: usize,
    optional: usize,
) -> Result<Vec<BulkString>, ParseCommandError> {
    let mut args = Vec::with_capacity(necessary);
    // Get all necessary args
    for _ in 0..necessary {
        let val = iter.next().ok_or(ParseCommandError::WrongNumArgs)?;
        args.push(value_to_bulk_string(val)?);
    }

    // Get all optional args
    for _ in 0..optional {
        if let Some(val) = iter.next() {
            args.push(value_to_bulk_string(val)?);
        }
    }

    // If there are still any args outside of necessary and optional, return error.
    // Else return result.
    if iter.next().is_some() {
        Err(ParseCommandError::WrongNumArgs)
    } else {
        Ok(args)
    }
}

/// Available commands for Redis.
#[derive(Debug, Clone)]
pub enum Command {
    /// PING [msg]
    Ping(PingArg),

    /// ECHO msg
    Echo(EchoArg),

    /// INFO
    Info(InfoArg),

    /// SET key value [PX milliseconds]
    Set(SetArg),

    /// GET key
    Get(GetArg),
}

pub trait CommandArgParser {
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError>
    where
        Self: Sized;
}

#[derive(Debug, Clone, Error)]
pub enum ParseCommandError {
    #[error("Invalid command")]
    InvalidCommand,

    #[error("Wrong number of arguments")]
    WrongNumArgs,

    #[error("Invalid argument for command {0:?}")]
    InvalidArgument(Value),

    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl Command {
    pub fn parse(buf: &[u8]) -> Result<Self, ParseCommandError> {
        let value = Value::decode(buf)?;
        Self::try_from(value)
    }

    fn get_command_str_from_iter(
        iter: &mut std::slice::Iter<'_, Value>,
    ) -> Result<String, ParseCommandError> {
        // Get first value, which should be a BulkString
        let first_val = iter.next().ok_or(ParseCommandError::InvalidCommand)?;
        let bulk_string = first_val
            .bulk_string()
            .ok_or(ParseCommandError::InvalidCommand)?;

        bulk_string
            .as_str()
            .ok_or(ParseCommandError::InvalidCommand)
    }
}

impl TryFrom<Value> for Command {
    type Error = ParseCommandError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let arr = match value {
            Value::Array(a) => a,
            _ => return Err(ParseCommandError::InvalidCommand),
        };

        let values = match arr.values() {
            Some(v) => v,
            None => return Err(ParseCommandError::InvalidCommand),
        };

        let mut iter: std::slice::Iter<'_, Value> = values.iter();
        let cmd = Self::get_command_str_from_iter(&mut iter)?;

        match cmd.to_lowercase().as_str() {
            "ping" => Ok(Self::Ping(PingArg::parse_arg(&mut iter)?)),
            "echo" => Ok(Self::Echo(EchoArg::parse_arg(&mut iter)?)),
            "set" => Ok(Self::Set(SetArg::parse_arg(&mut iter)?)),
            "get" => Ok(Self::Get(GetArg::parse(&mut iter)?)),
            "info" => Ok(Self::Info(InfoArg::parse(&mut iter)?)),
            _ => Err(ParseCommandError::InvalidCommand),
        }
    }
}

impl Into<Value> for Command {
    fn into(self) -> Value {
        match self {
            Command::Ping(arg) => {
                let mut parts = vec![Value::BulkString("PING".into())];
                if arg.msg.is_some() {
                    parts.push(Value::BulkString(arg.msg.unwrap()));
                }
                Value::Array(Array::new(parts))
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_ping() {
        let cmd = Command::parse(b"*1\r\n$4\r\nPING\r\n").expect("Parse command unexpected error");
        match cmd {
            Command::Ping(arg) => assert_eq!(arg, PingArg { msg: None }),
            _ => panic!("Wrong command for ping"),
        }
    }

    #[test]
    fn parse_ping_optional() {
        let cmd = Command::parse(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n")
            .expect("Parse command unexpected error");
        match cmd {
            Command::Ping(arg) => assert_eq!(
                arg,
                PingArg {
                    msg: Some(BulkString::from("hello"))
                }
            ),
            _ => panic!("Wrong command for ping"),
        }
    }

    #[test]
    fn parse_echo() {
        let cmd = Command::parse(b"*2\r\n$4\r\nECHO\r\n$4\r\nYEET\r\n")
            .expect("Parse command unexpected error");
        match cmd {
            Command::Echo(arg) => assert_eq!(
                arg,
                EchoArg {
                    msg: BulkString::from("YEET")
                }
            ),
            _ => panic!("Wrong command for echo"),
        }
    }
}
