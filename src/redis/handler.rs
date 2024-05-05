use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use thiserror::Error;
use tracing::info;

use super::{
    cmd::{Command, Echo, GetArg, InfoArg, InfoSection, Ping, Set},
    resp::{BulkString, SimpleString, Value},
};

#[derive(Debug, Error)]
pub enum HandleCommandError {}

#[derive(Debug)]
struct InfoHandler {
    is_replica: bool,
    master_repl_id_and_offset: Option<(String, u64)>,
}

impl InfoHandler {
    fn new(is_replica: bool, master_repl_id_and_offset: Option<(String, u64)>) -> Self {
        Self {
            is_replica,
            master_repl_id_and_offset,
        }
    }

    /// Returns information and statistics about the server in a format that is simple to parse by computers and easy to read by humans.
    fn handle(&self, arg: InfoArg) -> Result<Value, HandleCommandError> {
        match arg.section().to_owned() {
            InfoSection::Replication => self.handle_replication(),
            InfoSection::Default => todo!(),
        }
    }

    fn handle_replication(&self) -> Result<Value, HandleCommandError> {
        if self.is_replica {
            Ok(Value::BulkString(BulkString::from("role:slave")))
        } else {
            let mut info = vec!["role:master".to_string()];
            if self.master_repl_id_and_offset.is_some() {
                let m = self.master_repl_id_and_offset.clone().unwrap();
                info.push(format!("master_replid:{}", m.0,));
                info.push(format!("master_repl_offset:{}", m.1,));
            }

            Ok(Value::BulkString(BulkString::from(
                info.join("\n").as_ref(),
            )))
        }
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StoredData {
    pub value: BulkString,
    pub deadline: Option<SystemTime>,
}

impl StoredData {
    /// Returns true if there is a deadline and current time is greater than deadline.
    pub fn has_expired(&self) -> bool {
        return self.deadline.is_some() && SystemTime::now().gt(&self.deadline.unwrap());
    }
}

#[derive(Debug)]
pub struct CommandHandler {
    map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
    config: CommandHandlerConfig,
}

#[derive(Debug)]
pub struct CommandHandlerConfig {
    pub is_replica: bool,
    pub master_repl_id_and_offset: Option<(String, u64)>,
}

impl CommandHandler {
    pub fn new(
        map: Arc<RwLock<HashMap<BulkString, StoredData>>>,
        config: CommandHandlerConfig,
    ) -> Self {
        Self { map, config }
    }

    pub fn handle(&mut self, cmd: Command) -> Result<Value, HandleCommandError> {
        info!("Handling command {cmd:?}");
        match cmd {
            Command::Ping(arg) => Ok(Ping::handler().handle(arg)),
            Command::Echo(arg) => Ok(Echo::handler().handle(arg)),
            Command::Info(arg) => InfoHandler::new(
                self.config.is_replica,
                self.config.master_repl_id_and_offset.clone(),
            )
            .handle(arg),
            // Clone Arc to increment reference count.
            Command::Set(arg) => Ok(Set::handler(self.map.clone()).handle(arg)),
            Command::Get(arg) => GetHandler::new(self.map.clone()).handle(arg),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use super::super::cmd::SetArg;
    use super::*;

    fn new_hash_map() -> Arc<RwLock<HashMap<BulkString, StoredData>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn new_cmd_handler() -> CommandHandler {
        CommandHandler::new(
            new_hash_map(),
            CommandHandlerConfig {
                is_replica: false,
                master_repl_id_and_offset: None,
            },
        )
    }

    fn simple_set(handler: &mut CommandHandler, k: &str, v: &str, expiry: Option<Duration>) {
        let key = BulkString::from(k);
        let value = BulkString::from(v);

        let resp = handler
            .handle(Command::Set(SetArg {
                key,
                value,
                expiry: expiry.clone(),
            }))
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
        let mut handler = new_cmd_handler();

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
        let mut handler = new_cmd_handler();

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
