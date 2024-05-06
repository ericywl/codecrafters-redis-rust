use std::fmt::Display;

use super::super::client::ClientError;
use super::super::resp::{Array, BulkString, SimpleString, Value};
use super::super::session::{Request, Responder, Response};
use super::{consume_args_from_iter, CommandArgParser, ParseCommandError};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ReplConfArgConfig {
    ListeningPort(u16),
    Capabilities(String),
}

impl ReplConfArgConfig {
    fn to_bulk_strings(&self) -> Vec<BulkString> {
        match self {
            Self::ListeningPort(port) => vec![
                BulkString::from("listening-port"),
                BulkString::from(port.to_string()),
            ],
            Self::Capabilities(s) => vec![BulkString::from("capa"), BulkString::from(s.clone())],
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ReplConfArg {
    pub config: ReplConfArgConfig,
}

impl CommandArgParser for ReplConfArg {
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 2, 0)?;
        let first = args.get(0).unwrap();
        let second = args.get(1).unwrap();

        let key = first
            .as_str()
            .ok_or(ParseCommandError::InvalidArgument(Value::BulkString(
                first.clone(),
            )))?;
        let value =
            second
                .as_str()
                .ok_or(ParseCommandError::InvalidArgument(Value::BulkString(
                    second.clone(),
                )))?;

        match key.to_lowercase().as_str() {
            "listening-port" => {
                // Parse port
                let port = value.parse::<u16>().map_err(|_| {
                    ParseCommandError::InvalidArgument(Value::BulkString(second.clone()))
                })?;
                Ok(Self {
                    config: ReplConfArgConfig::ListeningPort(port),
                })
            }
            "capa" => Ok(Self {
                config: ReplConfArgConfig::Capabilities(value),
            }),
            _ => Err(ParseCommandError::InvalidArgument(Value::BulkString(
                first.clone(),
            ))),
        }
    }
}

pub struct ReplConf;

impl ReplConf {
    /// Returns an instance of REPLCONF client.
    pub fn client<'a, T>(responder: &'a mut T) -> ReplConfClient<'a, T>
    where
        T: Responder,
    {
        ReplConfClient { responder }
    }

    /// Returns an instance of REPLCONF command handler.
    pub fn handler() -> ReplConfHandler {
        ReplConfHandler
    }

    /// Returns REPLCONF as a Command in the form of Value.
    pub fn command_value(arg: ReplConfArg) -> Value {
        let mut v = vec![Value::BulkString("REPLCONF".into())];
        let mut configs = arg
            .config
            .to_bulk_strings()
            .iter()
            .map(|bs| Value::BulkString(bs.clone()))
            .collect();

        v.append(&mut configs);

        Value::Array(v.into())
    }
}

pub struct ReplConfClient<'a, T: Responder> {
    responder: &'a mut T,
}

impl<'a, T> ReplConfClient<'a, T>
where
    T: Responder,
{
    /// Sends REPLCONF command to the responder.
    /// Expects responder to reply with just `OK` as SimpleString.
    pub async fn replconf(&mut self, arg: ReplConfArg) -> Result<Response, ClientError> {
        let request: Request = ReplConf::command_value(arg.clone()).into();
        let response = self.responder.respond(request).await?;

        if !response.is_simple_string("OK") {
            return Err(ClientError::InvalidResponse);
        }

        Ok(response)
    }
}

pub struct ReplConfHandler;
