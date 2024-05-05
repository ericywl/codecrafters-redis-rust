use super::super::client::ClientError;
use super::super::resp::{Array, BulkString, SimpleString, Value};
use super::super::session::{Request, Responder, Response};
use super::{consume_args_from_iter, CommandArgParser, ParseCommandError};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InfoArg {
    pub section: InfoSection,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum InfoSection {
    Default,
    Replication,
}

impl CommandArgParser for InfoArg {
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 0, 1)?;
        let section = Self::parse_info_section(args.get(0))?;

        Ok(Self { section })
    }
}

impl InfoArg {
    fn parse_info_section(opt_bs: Option<&BulkString>) -> Result<InfoSection, ParseCommandError> {
        let section_str = match opt_bs {
            Some(bs) => {
                bs.as_str()
                    .ok_or(ParseCommandError::InvalidArgument(Value::BulkString(
                        bs.clone(),
                    )))?
            }
            None => "".to_string(),
        };

        Ok(match section_str.to_lowercase().as_str() {
            "replication" => InfoSection::Replication,
            _ => InfoSection::Default,
        })
    }
}

pub struct Info;

impl Info {
    /// Returns an instance of INFO client.
    pub fn client() -> InfoClient {
        InfoClient {}
    }

    /// Returns an instance of INFO command handler.
    pub fn handler(
        is_replica: bool,
        master_repl_id_and_offset: Option<(String, u64)>,
    ) -> InfoHandler {
        InfoHandler::new(is_replica, master_repl_id_and_offset)
    }

    /// Returns INFO as a Command in the form of Value.
    pub fn command_value(arg: InfoArg) -> Value {
        todo!()
    }
}

pub struct InfoClient;

#[derive(Debug)]
pub struct InfoHandler {
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
    pub fn handle(&self, arg: InfoArg) -> Value {
        match arg.section.to_owned() {
            InfoSection::Replication => self.handle_replication(),
            InfoSection::Default => todo!(),
        }
    }

    fn handle_replication(&self) -> Value {
        if self.is_replica {
            Value::BulkString(BulkString::from("role:slave"))
        } else {
            let mut info = vec!["role:master".to_string()];
            if self.master_repl_id_and_offset.is_some() {
                let m = self.master_repl_id_and_offset.clone().unwrap();
                info.push(format!("master_replid:{}", m.0,));
                info.push(format!("master_repl_offset:{}", m.1,));
            }

            Value::BulkString(BulkString::from(info.join("\n").as_ref()))
        }
    }
}
