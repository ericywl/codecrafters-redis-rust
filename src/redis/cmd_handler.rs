use std::collections::HashMap;

use thiserror::Error;
use tracing::info;

use super::{
    cmd::{Command, EchoArg, PingArg},
    resp::{Array, BulkString, SimpleString, Value},
};

#[derive(Debug, Error)]
pub enum HandleCommandError {}

#[derive(Debug)]
struct PingHandler;

impl PingHandler {
    fn new() -> Self {
        Self
    }

    fn handle(&self, arg: PingArg) -> Result<Value, HandleCommandError> {
        if let Some(msg) = arg.msg() {
            Ok(Value::Array(Array::new(vec![
                Value::BulkString(BulkString::new(b"PONG".to_vec())),
                Value::BulkString(msg.clone()),
            ])))
        } else {
            Ok(Value::SimpleString(SimpleString::new("PONG".to_owned())))
        }
    }
}

#[derive(Debug)]
struct EchoHandler;

impl EchoHandler {
    fn new() -> Self {
        Self
    }

    fn handle(&self, arg: EchoArg) -> Result<Value, HandleCommandError> {
        Ok(Value::BulkString(arg.msg().clone()))
    }
}

#[derive(Debug)]
pub struct CommandHandler<'a> {
    map: &'a HashMap<Value, Value>,
}

impl<'a> CommandHandler<'a> {
    pub fn new(map: &'a HashMap<Value, Value>) -> Self {
        Self { map }
    }

    pub fn handle(&self, cmd: Command) -> Result<Value, HandleCommandError> {
        info!("Handling command {cmd:?}");
        match cmd {
            Command::Ping(arg) => PingHandler::new().handle(arg),
            Command::Echo(arg) => EchoHandler::new().handle(arg),
        }
    }
}
