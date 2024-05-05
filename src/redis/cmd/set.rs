use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use super::super::client::ClientError;
use super::super::handler::StoredData;
use super::super::resp::{Array, BulkString, SimpleString, Value};
use super::super::session::{Request, Responder, Response};
use super::{
    bulk_string_to_string, bulk_string_to_uint64, consume_args_from_iter, CommandArgParser,
    ParseCommandError,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SetArg {
    pub key: BulkString,
    pub value: BulkString,
    pub expiry: Option<Duration>,
}

impl CommandArgParser for SetArg {
    /// SET key value [px milliseconds]
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 2, 2)?;
        let key = args.get(0).unwrap().clone();
        let value = args.get(1).unwrap().clone();

        let expiry = match args.get(2) {
            Some(arg) => {
                if bulk_string_to_string(arg)?.eq_ignore_ascii_case("px") {
                    // Has expiry defined as `px milliseconds`
                    Some(Duration::from_millis(bulk_string_to_uint64(
                        args.get(3).ok_or(ParseCommandError::WrongNumArgs)?,
                    )?))
                } else {
                    return Err(ParseCommandError::InvalidArgument(Value::BulkString(
                        arg.clone(),
                    )));
                }
            }
            None => None,
        };

        Ok(Self { key, value, expiry })
    }
}

pub struct Set;

impl Set {
    pub fn client() -> SetClient {
        todo!()
    }

    pub fn handler(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> SetHandler {
        SetHandler { map }
    }
}

pub struct SetClient;

#[derive(Debug)]
pub struct SetHandler {
    map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
}

impl SetHandler {
    /// Set key to hold the value.
    /// If key already holds a value, it is overwritten.
    /// Any previous time to live associated with the key is discarded on successful SET operation.
    pub fn handle(&mut self, arg: SetArg) -> Value {
        // Calculate deadline from expiry
        let deadline = match arg.expiry {
            Some(expiry) => SystemTime::now().checked_add(expiry),
            None => None,
        };
        let data = StoredData {
            value: arg.value.clone(),
            deadline,
        };

        // Write lock and insert data
        let mut map = self.map.write().expect("RwLock poisoned");
        match map.entry(arg.key.clone()) {
            Entry::Occupied(mut e) => *e.get_mut() = data,
            Entry::Vacant(e) => {
                e.insert(data);
            }
        };

        Value::SimpleString(SimpleString::new("OK".into()))
    }
}

#[cfg(test)]
mod handler_test {
    use super::*;

    fn new_set_handler(map: Arc<RwLock<HashMap<BulkString, StoredData>>>) -> SetHandler {
        Set::handler(map)
    }

    fn simple_set(handler: &mut SetHandler, key: &str, value: &str, expiry: Option<Duration>) {
        let key = BulkString::from(key);
        let value = BulkString::from(value);

        let resp = handler.handle(SetArg {
            key,
            value,
            expiry: expiry.clone(),
        });
        assert_eq!(resp, Value::SimpleString(SimpleString::from("OK")));
    }

    #[test]
    fn handle_set() {
        let map = Arc::new(RwLock::new(HashMap::new()));
        let mut handler = new_set_handler(map.clone());

        let key = "My Key";
        let value = "My Value";

        simple_set(&mut handler, key, value, None);
        let read_map = map.read().expect("RwLock poisoned");
        let data = read_map.get(&BulkString::from(key)).unwrap();

        assert_eq!(
            data,
            &StoredData {
                value: BulkString::from(value),
                deadline: None
            }
        )
    }
}
