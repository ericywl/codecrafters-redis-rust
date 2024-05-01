use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use thiserror::Error;
use tracing::info;

use super::{
    cmd::{Command, EchoArg, GetArg, PingArg, SetArg},
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
struct SetHandler {
    map: Arc<RwLock<HashMap<BulkString, Data>>>,
}

impl SetHandler {
    fn new(map: Arc<RwLock<HashMap<BulkString, Data>>>) -> Self {
        Self { map }
    }

    fn handle(&mut self, arg: SetArg) -> Result<Value, HandleCommandError> {
        let deadline = match arg.expiry() {
            Some(expiry) => SystemTime::now().checked_add(*expiry),
            None => None,
        };

        let data = Data {
            value: arg.value().clone(),
            deadline,
        };

        let mut map = self.map.write().expect("RwLock poisoned");
        match map.entry(arg.key().clone()) {
            Entry::Occupied(mut e) => *e.get_mut() = data,
            Entry::Vacant(e) => {
                e.insert(data);
            }
        };

        Ok(Value::SimpleString(SimpleString::new("OK".into())))
    }
}

#[derive(Debug)]
struct GetHandler {
    map: Arc<RwLock<HashMap<BulkString, Data>>>,
}

impl GetHandler {
    fn new(map: Arc<RwLock<HashMap<BulkString, Data>>>) -> Self {
        Self { map }
    }

    fn handle(&mut self, arg: GetArg) -> Result<Value, HandleCommandError> {
        let read_map = self.map.read().expect("RwLock poisoned");
        let data = match read_map.get(arg.key()) {
            Some(data) => data.clone(),
            None => return Ok(Value::BulkString(BulkString::null())),
        };

        drop(read_map);
        // No deadline or deadline haven't reached yet
        if data.deadline.is_none() || data.deadline.unwrap().gt(&SystemTime::now()) {
            return Ok(Value::BulkString(data.value));
        }

        // Deadline passed, we should clear the entry
        let mut write_map = self.map.write().expect("RwLock poisonsed");
        match write_map.entry(arg.key().clone()) {
            Entry::Occupied(e) => {
                if e.get().deadline.is_some() && SystemTime::now().gt(&e.get().deadline.unwrap()) {
                    e.remove();
                }
            }
            Entry::Vacant(_) => (),
        };

        Ok(Value::BulkString(BulkString::null()))
    }
}

#[derive(Debug, Clone)]
pub struct Data {
    value: BulkString,
    deadline: Option<SystemTime>,
}

#[derive(Debug)]
pub struct CommandHandler {
    map: Arc<RwLock<HashMap<BulkString, Data>>>,
}

impl CommandHandler {
    pub fn new(map: Arc<RwLock<HashMap<BulkString, Data>>>) -> Self {
        Self { map }
    }

    pub fn handle(&mut self, cmd: Command) -> Result<Value, HandleCommandError> {
        info!("Handling command {cmd:?}");
        // Clone Arc to increment reference count.
        match cmd {
            Command::Ping(arg) => PingHandler::new().handle(arg),
            Command::Echo(arg) => EchoHandler::new().handle(arg),
            Command::Set(arg) => SetHandler::new(self.map.clone()).handle(arg),
            Command::Get(arg) => GetHandler::new(self.map.clone()).handle(arg),
        }
    }
}
