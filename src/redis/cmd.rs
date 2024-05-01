use std::time::Duration;

use thiserror::Error;

use super::resp::{BulkString, DecodeError, Value};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PingArg {
    msg: Option<BulkString>,
}

impl PingArg {
    fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let args = consume_args_from_iter(iter, 0, 1)?;
        let msg = args.get(0).map(|bs| bs.clone());

        Ok(PingArg { msg })
    }

    pub fn msg(&self) -> Option<&BulkString> {
        self.msg.as_ref()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EchoArg {
    msg: BulkString,
}

impl EchoArg {
    pub fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let args = consume_args_from_iter(iter, 1, 0)?;
        let msg = args.get(0).unwrap().clone();

        Ok(Self { msg })
    }

    pub fn msg(&self) -> &BulkString {
        &self.msg
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetArg {
    key: BulkString,
    value: BulkString,
    expiry: Option<Duration>,
}

impl SetArg {
    pub fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let args = consume_args_from_iter(iter, 2, 2)?;
        let key = args.get(0).unwrap().clone();
        let value = args.get(1).unwrap().clone();

        let expiry = match args.get(2) {
            Some(arg) => {
                if bulk_string_to_string(arg)?.eq_ignore_ascii_case("px") {
                    Some(Duration::from_millis(bulk_string_to_uint64(
                        args.get(3).ok_or(CommandError::WrongNumArgs)?,
                    )?))
                } else {
                    return Err(CommandError::InvalidArgument(Value::BulkString(
                        arg.clone(),
                    )));
                }
            }
            None => None,
        };

        Ok(Self { key, value, expiry })
    }

    pub fn key(&self) -> &BulkString {
        &self.key
    }

    pub fn value(&self) -> &BulkString {
        &self.value
    }

    pub fn expiry(&self) -> Option<&Duration> {
        self.expiry.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct GetArg {
    key: BulkString,
}

impl GetArg {
    pub fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let args = consume_args_from_iter(iter, 1, 0)?;
        let key = args.get(0).unwrap().clone();

        Ok(Self { key })
    }

    pub fn key(&self) -> &BulkString {
        &self.key
    }
}

fn bulk_string_to_uint64(bs: &BulkString) -> Result<u64, CommandError> {
    let s = bulk_string_to_string(bs)?;
    Ok(s.parse::<u64>().map_err(|e| DecodeError::ParseInt(e))?)
}

fn bulk_string_to_string(bs: &BulkString) -> Result<String, CommandError> {
    bs.as_str()
        .ok_or(CommandError::InvalidArgument(Value::BulkString(bs.clone())))
}

fn value_to_bulk_string(val: &Value) -> Result<BulkString, CommandError> {
    Ok(val
        .bulk_string()
        .ok_or(CommandError::InvalidArgument(val.clone()))?
        .clone())
}

fn consume_args_from_iter(
    iter: &mut std::slice::Iter<'_, Value>,
    necessary: usize,
    optional: usize,
) -> Result<Vec<BulkString>, CommandError> {
    let mut args = Vec::with_capacity(necessary);
    // Get all necessary args
    for _ in 0..necessary {
        let val = iter.next().ok_or(CommandError::WrongNumArgs)?;
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
        Err(CommandError::WrongNumArgs)
    } else {
        Ok(args)
    }
}

/// Available commands for Redis.
#[derive(Debug, Clone)]
pub enum Command {
    /// Ping expects either 0 or 1 BulkString argument.
    Ping(PingArg),

    /// Echo expects 1 BulkString argument.
    Echo(EchoArg),

    /// Set expects 2 or 3 BulkString argument.
    Set(SetArg),

    /// Get expects 1 BulkString argument.
    Get(GetArg),
}

#[derive(Debug, Clone, Error)]
pub enum CommandError {
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
    pub fn parse(buf: &[u8]) -> Result<Self, CommandError> {
        let arr = match Value::decode(buf)? {
            Value::Array(a) => a,
            _ => return Err(CommandError::InvalidCommand),
        };

        let values = match arr.values() {
            Some(v) => v,
            None => return Err(CommandError::InvalidCommand),
        };

        let mut iter: std::slice::Iter<'_, Value> = values.iter();
        let cmd = Self::get_command_str_from_iter(&mut iter)?;

        match cmd.to_lowercase().as_str() {
            "ping" => Ok(Self::Ping(PingArg::parse(&mut iter)?)),
            "echo" => Ok(Self::Echo(EchoArg::parse(&mut iter)?)),
            "set" => Ok(Self::Set(SetArg::parse(&mut iter)?)),
            "get" => Ok(Self::Get(GetArg::parse(&mut iter)?)),
            _ => Err(CommandError::InvalidCommand),
        }
    }

    fn get_command_str_from_iter(
        iter: &mut std::slice::Iter<'_, Value>,
    ) -> Result<String, CommandError> {
        // Get first value, which should be a BulkString
        let first_val = iter.next().ok_or(CommandError::InvalidCommand)?;
        let bulk_string = first_val
            .bulk_string()
            .ok_or(CommandError::InvalidCommand)?;

        bulk_string.as_str().ok_or(CommandError::InvalidCommand)
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
