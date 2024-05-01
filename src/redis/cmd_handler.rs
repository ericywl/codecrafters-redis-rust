use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use thiserror::Error;
use tracing::info;

use super::{
    cmd::{Command, EchoArg, GetArg, InfoArg, InfoSection, PingArg, SetArg},
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
struct InfoHandler;

impl InfoHandler {
    fn new() -> Self {
        Self
    }

    /// Returns info based on section provided.
    fn handle(&self, arg: InfoArg) -> Result<Value, HandleCommandError> {
        let resp = match arg.section().to_owned() {
            InfoSection::Replication => Value::BulkString(BulkString::from("role:master")),
            InfoSection::Default => todo!(),
        };

        Ok(resp)
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
            Command::Info(arg) => InfoHandler::new().handle(arg),
            // Clone Arc to increment reference count.
            Command::Set(arg) => SetHandler::new(self.map.clone()).handle(arg),
            Command::Get(arg) => GetHandler::new(self.map.clone()).handle(arg),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use super::*;

    fn new_hash_map() -> Arc<RwLock<HashMap<BulkString, StoredData>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    #[test]
    fn ping() {
        let mut handler = CommandHandler::new(new_hash_map());
        let resp = handler
            .handle(Command::Ping(PingArg::new(None)))
            .expect("Handle ping unexpected error");

        assert_eq!(resp, Value::SimpleString("PONG".into()));
    }

    #[test]
    fn ping_with_msg() {
        let mut handler = CommandHandler::new(new_hash_map());
        let resp = handler
            .handle(Command::Ping(PingArg::new(Some(BulkString::from("WOOP")))))
            .expect("Handle ping unexpected error");

        assert_eq!(
            resp,
            Value::Array(Array::new(vec![
                Value::BulkString("PONG".into()),
                Value::BulkString("WOOP".into())
            ]))
        );
    }

    #[test]
    fn echo() {
        let mut handler = CommandHandler::new(new_hash_map());
        let resp = handler
            .handle(Command::Echo(EchoArg::new(BulkString::from("Hello World"))))
            .expect("Handle echo unexpected error");

        assert_eq!(resp, Value::BulkString("Hello World".into()))
    }

    fn simple_set(handler: &mut CommandHandler, k: &str, v: &str, expiry: Option<Duration>) {
        let key = BulkString::from(k);
        let value = BulkString::from(v);

        let resp = handler
            .handle(Command::Set(SetArg::new(key, value, expiry.clone())))
            .expect("Handle set unexpected error");
        assert_eq!(resp, Value::SimpleString(SimpleString::from("OK")));
    }

    fn simple_get(handler: &mut CommandHandler, k: &str) -> Value {
        let key = BulkString::from(k);

        handler
            .handle(Command::Get(GetArg::new(key)))
            .expect("Handle get unexpected error")
    }

    #[test]
    fn set_and_get() {
        let mut handler = CommandHandler::new(new_hash_map());

        let key = "My Key";
        let value = "My Value";

        // Set entry
        simple_set(&mut handler, key, value, None);

        // Entry exists
        let resp = simple_get(&mut handler, key);
        assert_eq!(
            resp.bulk_string().unwrap().as_str(),
            Some(value.to_string())
        );
    }

    #[test]
    fn set_expiry_and_get() {
        let mut handler = CommandHandler::new(new_hash_map());

        let key = "My Key";
        let value = "My Value";
        let expiry = Duration::from_millis(200);

        // Set entry with expiry
        simple_set(&mut handler, key, value, Some(expiry));

        // Entry still exists
        thread::sleep(Duration::from_millis(100));
        let resp = simple_get(&mut handler, key);
        assert_eq!(
            resp.bulk_string().unwrap().as_str(),
            Some(value.to_string())
        );

        // Entry expired
        thread::sleep(Duration::from_millis(200));
        let resp = simple_get(&mut handler, key);
        assert_eq!(resp.bulk_string().unwrap().as_str(), None);
    }
}
