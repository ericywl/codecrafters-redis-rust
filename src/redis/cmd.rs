use thiserror::Error;

use super::resp::{Array, BulkString, DecodeError, Value};

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(BulkString),
}

#[derive(Debug, Clone, Error)]
pub enum CommandError {
    #[error("Invalid command")]
    InvalidCommand,

    #[error("Missing arguments")]
    MissingArgs,

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

        if arr.is_null() {
            return Err(CommandError::InvalidCommand);
        }

        let mut iter: std::slice::Iter<'_, Value> = arr.values().iter();
        let cmd = Self::get_command_str_from_iter(&mut iter)?;

        match cmd.to_lowercase().as_str() {
            "ping" => Ok(Self::Ping),
            "echo" => {
                let val = iter.next().ok_or(CommandError::MissingArgs)?;
                Ok(Self::Echo(
                    val.bulk_string()
                        .ok_or(CommandError::InvalidArgument(val.clone()))?
                        .clone(),
                ))
            }
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
