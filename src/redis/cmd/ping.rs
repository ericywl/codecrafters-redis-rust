use thiserror::Error;

use crate::redis::session::response_is;

use super::super::resp::{Array, BulkString, SimpleString, Value};
use super::super::session::{
    response_is_simple_string, Request, Responder, Response, SessionError,
};
use super::{consume_args_from_iter, CommandError};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PingArg {
    pub msg: Option<BulkString>,
}

impl PingArg {
    pub fn parse(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, CommandError> {
        let args = consume_args_from_iter(iter, 0, 1)?;
        let msg = args.get(0).map(|bs| bs.clone());

        Ok(PingArg { msg })
    }
}

pub struct Ping;

impl Ping {
    pub fn client(responder: Box<dyn Responder>) -> PingClient {
        PingClient { responder }
    }

    pub fn handler() -> PingHandler {
        PingHandler {}
    }

    pub fn command_value(arg: PingArg) -> Value {
        let mut parts = vec![Value::BulkString("PING".into())];
        if arg.msg.is_some() {
            parts.push(Value::BulkString(arg.msg.unwrap()));
        }
        Value::Array(Array::new(parts))
    }
}

#[derive(Debug, Error)]
pub enum PingClientError {
    #[error("PONG not returned from server")]
    PongNotReturned,

    #[error(transparent)]
    Session(#[from] SessionError),
}

pub struct PingClient {
    responder: Box<dyn Responder>,
}

impl PingClient {
    pub async fn ping(&self, arg: PingArg) -> Result<(), PingClientError> {
        let request: Request = Ping::command_value(arg.clone()).into();
        let response = self.responder.respond(request).await?;

        match arg.msg {
            Some(msg) => {
                if !response_is(
                    response,
                    Value::Array(Array::new(vec![
                        Value::BulkString("PONG".into()),
                        Value::BulkString(msg.clone()),
                    ])),
                ) {
                    return Err(PingClientError::PongNotReturned);
                }
            }
            None => {
                if !response_is_simple_string(response, "PONG") {
                    return Err(PingClientError::PongNotReturned);
                }
            }
        }

        Ok(())
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
    use async_trait::async_trait;

    use super::*;

    struct MockResponder {
        expected_req: Request,
        returned_resp: Response,
    }

    #[async_trait]
    impl Responder for MockResponder {
        async fn respond(&self, req: Request) -> Result<Response, SessionError> {
            assert_eq!(req, self.expected_req);
            Ok(self.returned_resp.clone())
        }
    }

    fn new_ping_client(expected_req: Request, returned_resp: Response) -> PingClient {
        let responder = MockResponder {
            expected_req,
            returned_resp,
        };
        Ping::client(Box::new(responder))
    }

    #[tokio::test]
    async fn ping() {
        let client = new_ping_client(
            Request::new(Value::Array(Array::new(vec![Value::BulkString(
                "PING".into(),
            )]))),
            Response::new(Value::SimpleString("PONG".into())),
        );

        client
            .ping(PingArg { msg: None })
            .await
            .expect("Unexpected ping error");
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
