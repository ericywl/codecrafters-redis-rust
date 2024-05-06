use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::super::client::ClientError;
use super::super::handler::StoredData;
use super::super::resp::{Array, BulkString, SimpleString, Value};
use super::super::session::{Request, Responder, Response};
use super::{consume_args_from_iter, CommandArgParser, ParseCommandError};

#[derive(Debug, Clone)]
pub struct GetArg {
    pub key: BulkString,
}

impl CommandArgParser for GetArg {
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 1, 0)?;
        let key = args.get(0).unwrap().clone();

        Ok(Self { key })
    }
}

pub struct Get;

impl Get {
    /// Returns an instance of GET client.
    pub fn client() -> GetClient {
        GetClient {}
    }

    /// Returns an instance of GET command handler.
    pub fn handler(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> GetHandler {
        GetHandler { map }
    }

    /// Returns GET as a Command in the form of Value.
    pub fn command_value(arg: GetArg) -> Value {
        let v = vec![Value::BulkString("GET".into()), Value::BulkString(arg.key)];
        Value::Array(v.into())
    }
}

pub struct GetClient;

pub struct GetHandler {
    map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
}

impl GetHandler {
    /// Get the value of key.
    /// If the key does not exist the special value nil is returned.
    ///
    /// On getting a key, if the value stored in the key has expired, it will be removed.
    /// TODO: Implement active expiry on-top of this passive one.
    pub fn handle(&mut self, arg: GetArg) -> Value {
        // Read lock to access data.
        let read_map = self.map.read().expect("RwLock poisoned");
        // Clone the data.
        let data = match read_map.get(&arg.key) {
            Some(data) => data.clone(),
            None => return Value::BulkString(BulkString::null()),
        };

        // Unlock, since we already have the cloned data.
        drop(read_map);

        // No deadline or deadline haven't reached yet.
        if !data.has_expired() {
            return Value::BulkString(data.value);
        }

        // Deadline passed, we should clear the entry.
        // Write lock and test that entry is still expired. We need to test it again since
        // the entry could have been overwritten by the time we acquire write lock.
        let mut write_map = self.map.write().expect("RwLock poisonsed");
        match write_map.entry(arg.key.clone()) {
            Entry::Occupied(e) => {
                if e.get().has_expired() {
                    e.remove();
                }
            }
            Entry::Vacant(_) => (),
        };

        Value::BulkString(BulkString::null())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn command() {
        let val = Get::command_value(GetArg { key: "key".into() });

        assert_eq!(
            val.array().unwrap().values().unwrap().to_vec(),
            vec![
                Value::BulkString("GET".into()),
                Value::BulkString("key".into()),
            ]
        )
    }
}

#[cfg(test)]
mod handler_test {
    use super::*;

    fn new_get_handler(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> GetHandler {
        Get::handler(map)
    }

    fn simple_get(handler: &mut GetHandler, k: &str) -> Value {
        let key = BulkString::from(k);

        handler.handle(GetArg { key })
    }

    #[test]
    fn handle_get() {
        let key = "My Key";
        let value = "My Value";

        let mut map = HashMap::new();
        map.insert(
            BulkString::from(key),
            StoredData {
                value: BulkString::from(value),
                deadline: None,
            },
        );

        let map = Arc::new(RwLock::new(map));
        let mut handler = new_get_handler(map.clone());

        let get_value = simple_get(&mut handler, key);
        assert_eq!(get_value, Value::BulkString(value.into()));
    }
}
