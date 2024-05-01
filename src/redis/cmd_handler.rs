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

    /// Returns PONG if no argument is provided.
    /// Otherwise returns a copy of the argument.
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

    /// Returns message.
    fn handle(&self, arg: EchoArg) -> Result<Value, HandleCommandError> {
        Ok(Value::BulkString(arg.msg().clone()))
    }
}

#[derive(Debug)]
struct SetHandler {
    map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
}

impl SetHandler {
    fn new(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> Self {
        Self { map }
    }

    /// Set key to hold the value.
    /// If key already holds a value, it is overwritten.
    /// Any previous time to live associated with the key is discarded on successful SET operation.
    fn handle(&mut self, arg: SetArg) -> Result<Value, HandleCommandError> {
        // Calculate deadline from expiry
        let deadline = match arg.expiry() {
            Some(expiry) => SystemTime::now().checked_add(*expiry),
            None => None,
        };
        let data = StoredData {
            value: arg.value().clone(),
            deadline,
        };

        // Write lock and insert data
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
    map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
}

impl GetHandler {
    fn new(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> Self {
        Self { map }
    }

    /// Get the value of key.
    /// If the key does not exist the special value nil is returned.
    ///
    /// On getting a key, if the value stored in the key has expired, it will be removed.
    /// TODO: Implement active expiry on-top of this passive one.
    fn handle(&mut self, arg: GetArg) -> Result<Value, HandleCommandError> {
        // Read lock to access data.
        let read_map = self.map.read().expect("RwLock poisoned");
        // Clone the data.
        let data = match read_map.get(arg.key()) {
            Some(data) => data.clone(),
            None => return Ok(Value::BulkString(BulkString::null())),
        };

        // Unlock, since we already have the cloned data.
        drop(read_map);

        // No deadline or deadline haven't reached yet.
        if !data.has_expired() {
            return Ok(Value::BulkString(data.value));
        }

        // Deadline passed, we should clear the entry.
        // Write lock and test that entry is still expired. We need to test it again since
        // the entry could have been overwritten by the time we acquire write lock.
        let mut write_map = self.map.write().expect("RwLock poisonsed");
        match write_map.entry(arg.key().clone()) {
            Entry::Occupied(e) => {
                if e.get().has_expired() {
                    e.remove();
                }
            }
            Entry::Vacant(_) => (),
        };

        Ok(Value::BulkString(BulkString::null()))
    }
}

#[derive(Debug, Clone)]
pub struct StoredData {
    value: BulkString,
    deadline: Option<SystemTime>,
}

impl StoredData {
    /// Returns true if there is a deadline and current time is greater than deadline.
    fn has_expired(&self) -> bool {
        return self.deadline.is_some() && SystemTime::now().gt(&self.deadline.unwrap());
    }
}

#[derive(Debug)]
pub struct CommandHandler {
    map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
}

impl CommandHandler {
    pub fn new(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> Self {
        Self { map }
    }

    pub fn handle(&mut self, cmd: Command) -> Result<Value, HandleCommandError> {
        info!("Handling command {cmd:?}");
        match cmd {
            Command::Ping(arg) => PingHandler::new().handle(arg),
            Command::Echo(arg) => EchoHandler::new().handle(arg),
            // Clone Arc to increment reference count.
            Command::Set(arg) => SetHandler::new(self.map.clone()).handle(arg),
            Command::Get(arg) => GetHandler::new(self.map.clone()).handle(arg),
        }
    }
}
