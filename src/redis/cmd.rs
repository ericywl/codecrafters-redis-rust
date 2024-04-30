use thiserror::Error;

use super::resp::{BulkString, DecodeError, Value};

#[derive(Debug, Clone)]
pub struct PingArg {
    msg: Option<BulkString>,
}

impl PingArg {
    fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let msg = match iter.next() {
            Some(val) => Some(
                val.bulk_string()
                    .ok_or(CommandError::InvalidArgument(val.clone()))?
                    .clone(),
            ),
            None => None,
        };
        if iter.next().is_some() {
            return Err(CommandError::WrongNumArgs);
        }

        Ok(PingArg { msg })
    }

    pub fn msg(&self) -> Option<&BulkString> {
        self.msg.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct EchoArg {
    msg: BulkString,
}

impl EchoArg {
    pub fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let val = iter.next().ok_or(CommandError::WrongNumArgs)?;
        if iter.next().is_some() {
            return Err(CommandError::WrongNumArgs);
        }

        Ok(Self {
            msg: val
                .bulk_string()
                .ok_or(CommandError::InvalidArgument(val.clone()))?
                .clone(),
        })
    }

    pub fn msg(&self) -> &BulkString {
        &self.msg
    }
}

/// Available commands for Redis.
#[derive(Debug, Clone)]
pub enum Command {
    /// Ping expects either 0 or 1 BulkString argument.
    Ping(PingArg),

    /// Echo expects 1 BulkString argument.
    Echo(EchoArg),
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
