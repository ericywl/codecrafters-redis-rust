use std::collections::{hash_map::Entry, HashMap};

use thiserror::Error;
use tracing::info;

use super::{
    cmd::{Command, EchoArg, PingArg, SetArg},
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
            Ok(Value::SimpleString(SimpleString::new("PONG".into())))
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
struct SetHandler<'a> {
    map: &'a mut HashMap<BulkString, BulkString>,
}

impl<'a> SetHandler<'a> {
    fn new(map: &'a mut HashMap<BulkString, BulkString>) -> Self {
        Self { map }
    }

    fn handle(&mut self, arg: SetArg) -> Result<Value, HandleCommandError> {
        match self.map.entry(arg.key().clone()) {
            Entry::Occupied(mut e) => *e.get_mut() = arg.value().clone(),
            Entry::Vacant(e) => {
                e.insert(arg.value().clone());
            }
        };

        Ok(Value::SimpleString(SimpleString::new("OK".into())))
    }
}

#[derive(Debug)]
pub struct CommandHandler<'a> {
    map: &'a mut HashMap<BulkString, BulkString>,
}

impl<'a> CommandHandler<'a> {
    pub fn new(map: &'a mut HashMap<BulkString, BulkString>) -> Self {
        Self { map }
    }

    pub fn handle(&mut self, cmd: Command) -> Result<Value, HandleCommandError> {
        info!("Handling command {cmd:?}");
        match cmd {
            Command::Ping(arg) => PingHandler::new().handle(arg),
            Command::Echo(arg) => EchoHandler::new().handle(arg),
            Command::Set(arg) => SetHandler::new(self.map).handle(arg),
        }
    }
}
