use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use thiserror::Error;
use tracing::info;

use super::{
    cmd::{Command, Echo, Get, Info, Ping, Set},
    resp::{BulkString, Value},
};

#[derive(Debug, Error)]
pub enum HandleCommandError {}

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
            Command::Info(arg) => Ok(Info::handler(
                self.config.is_replica,
                self.config.master_repl_id_and_offset.clone(),
            )
            .handle(arg)),
            // Clone Arc to increment reference count.
            Command::Set(arg) => Ok(Set::handler(self.map.clone()).handle(arg)),
            Command::Get(arg) => Ok(Get::handler(self.map.clone()).handle(arg)),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use super::super::cmd::{GetArg, SetArg};
    use super::super::resp::SimpleString;
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
            .handle(Command::Get(GetArg { key }))
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
