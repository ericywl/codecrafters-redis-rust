use std::cell::RefCell;
use std::rc::Rc;

use super::super::client::ClientError;
use super::super::resp::{Array, BulkString, SimpleString, Value};
use super::super::session::{Request, Responder, Response};
use super::{consume_args_from_iter, CommandArgParser, ParseCommandError};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PingArg {
    pub msg: Option<BulkString>,
}

impl CommandArgParser for PingArg {
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 0, 1)?;
        let msg = args.get(0).map(|bs| bs.clone());

        Ok(PingArg { msg })
    }
}

pub struct Ping;

impl Ping {
    /// Returns an instance of PING client.
    pub fn client<'a, T>(responder: &'a mut T) -> PingClient<'a, T>
    where
        T: Responder,
    {
        PingClient { responder }
    }

    /// Returns an instance of PING command handler.
    pub fn handler() -> PingHandler {
        PingHandler {}
    }

    /// Returns PING as a Command in the form of Value.
    pub fn command_value(arg: PingArg) -> Value {
        let mut parts = vec![Value::BulkString("PING".into())];
        if arg.msg.is_some() {
            parts.push(Value::BulkString(arg.msg.unwrap()));
        }
        Value::Array(Array::new(parts))
    }
}

pub struct PingClient<'a, T: Responder> {
    responder: &'a mut T,
}

impl<'a, T> PingClient<'a, T>
where
    T: Responder,
{
    /// Sends PING command to the responder.
    /// If `arg` contains a message, it will expect responder to reply with `PONG msg` as array of BulkStrings.
    /// Otherwise, it will expect responder to reply with just `PONG` as SimpleString.
    pub async fn ping(&mut self, arg: PingArg) -> Result<Response, ClientError> {
        let request: Request = Ping::command_value(arg.clone()).into();
        let response = self.responder.respond(request).await?;

        match arg.msg {
            Some(msg) => {
                if !response.is_bulk_string_array(vec!["PONG".into(), msg.clone()]) {
                    return Err(ClientError::InvalidResponse);
                }
            }
            None => {
                if !response.is_simple_string("PONG") {
                    return Err(ClientError::InvalidResponse);
                }
            }
        }

        Ok(response)
    }
}

pub struct PingHandler;

impl PingHandler {
    /// Returns SimpleString PONG if no message.
    /// Otherwise returns Array with 2 BulkStrings PONG and the message.
    pub fn handle(&self, arg: PingArg) -> Value {
        if let Some(msg) = arg.msg {
            Value::Array(Array::new(vec![
                Value::BulkString(BulkString::new(b"PONG".to_vec())),
                Value::BulkString(msg.clone()),
            ]))
        } else {
            Value::SimpleString(SimpleString::new("PONG".into()))
        }
    }
}

#[cfg(test)]
mod client_test {
    use super::super::super::session::MockResponder;
    use super::*;

    fn new_ping_responder(
        expected_msg: Option<BulkString>,
        returned_value: Value,
    ) -> MockResponder {
        let mut values = vec![Value::BulkString("PING".into())];
        if expected_msg.is_some() {
            values.push(Value::BulkString(expected_msg.unwrap()))
        }
        let expected_req = Request::new(Value::Array(Array::new(values)));

        MockResponder {
            expected_req,
            returned_resp: returned_value.into(),
        }
    }

    #[tokio::test]
    async fn ping() {
        let mut responder = new_ping_responder(None, Value::SimpleString("PONG".into()));
        let mut client = Ping::client(&mut responder);

        client
            .ping(PingArg { msg: None })
            .await
            .expect("Unexpected ping error");
    }

    #[tokio::test]
    async fn ping_with_msg() {
        let msg: BulkString = "Hello".into();

        let mut responder = new_ping_responder(
            Some(msg.clone()),
            Value::Array(
                vec![
                    Value::BulkString("PONG".into()),
                    Value::BulkString(msg.clone()),
                ]
                .into(),
            ),
        );
        let mut client = Ping::client(&mut responder);

        client
            .ping(PingArg { msg: Some(msg) })
            .await
            .expect("Unexpected ping error");
    }

    #[tokio::test]
    async fn ping_wrong_response() {
        let mut responder = new_ping_responder(None, Value::SimpleString("Hello".into()));
        let mut client = Ping::client(&mut responder);

        match client.ping(PingArg { msg: None }).await {
            Ok(_) => panic!("Should have error"),
            Err(e) => assert!(matches!(e, ClientError::InvalidResponse)),
        }
    }
}

#[cfg(test)]
mod handler_test {
    use super::*;

    fn new_ping_handler() -> PingHandler {
        Ping::handler()
    }

    #[test]
    fn handle_ping() {
        let handler = new_ping_handler();
        let resp = handler.handle(PingArg { msg: None });

        assert_eq!(resp, Value::SimpleString("PONG".into()));
    }

    #[test]
    fn handle_ping_with_msg() {
        let handler = new_ping_handler();
        let resp = handler.handle(PingArg {
            msg: Some(BulkString::from("WOOP")),
        });

        assert_eq!(
            resp,
            Value::Array(Array::new(vec![
                Value::BulkString("PONG".into()),
                Value::BulkString("WOOP".into())
            ]))
        );
    }
}
