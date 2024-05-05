use super::super::client::ClientError;
use super::super::resp::{Array, BulkString, Value};
use super::super::session::{Request, Responder, Response};
use super::{consume_args_from_iter, CommandArgParser, ParseCommandError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoArg {
    pub msg: BulkString,
}

impl CommandArgParser for EchoArg {
    /// ECHO msg
    fn parse_arg(iter: &mut std::slice::Iter<'_, Value>) -> Result<Self, ParseCommandError> {
        let args = consume_args_from_iter(iter, 1, 0)?;
        let msg = args.get(0).unwrap().clone();

        Ok(Self { msg })
    }
}

pub struct Echo;

impl Echo {
    /// Returns an instance of ECHO client.
    pub fn client<'a, T>(responder: &'a mut T) -> EchoClient<'a, T>
    where
        T: Responder,
    {
        EchoClient { responder }
    }

    /// Returns an instance of ECHO command handler.
    pub fn handler() -> EchoHandler {
        EchoHandler {}
    }

    /// Returns ECHO as a Command in the form of Value.
    pub fn command_value(arg: EchoArg) -> Value {
        let parts = vec![Value::BulkString("ECHO".into()), Value::BulkString(arg.msg)];
        Value::Array(Array::new(parts))
    }
}

pub struct EchoClient<'a, T: Responder> {
    responder: &'a mut T,
}

impl<'a, T> EchoClient<'a, T>
where
    T: Responder,
{
    /// Sends ECHO command to the responder with message.
    /// Expects responder to reply with the sent message.
    pub async fn echo(&mut self, arg: EchoArg) -> Result<Response, ClientError> {
        let msg = arg.msg.clone();
        if msg.as_bytes().is_none() {
            return Err(ClientError::InvalidArg);
        }

        let request: Request = Echo::command_value(arg.clone()).into();
        let response = self.responder.respond(request).await?;

        if !response.is_bulk_string(msg.as_bytes().unwrap()) {
            return Err(ClientError::InvalidResponse);
        }

        Ok(response)
    }
}

#[derive(Debug)]
pub struct EchoHandler;

impl EchoHandler {
    /// Returns message.
    pub fn handle(&self, arg: EchoArg) -> Value {
        Value::BulkString(arg.msg.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn command() {
        let val = Echo::command_value(EchoArg {
            msg: "hello".into(),
        });

        assert_eq!(
            val.array().unwrap().values().unwrap().to_vec(),
            vec![
                Value::BulkString("ECHO".into()),
                Value::BulkString("hello".into())
            ]
        )
    }
}

#[cfg(test)]
mod client_test {
    use super::super::super::session::MockResponder;
    use super::*;

    fn new_echo_responder(expected_msg: BulkString, returned_bs: BulkString) -> MockResponder {
        let expected_req = Request::new(Value::Array(Array::new(vec![
            Value::BulkString("ECHO".into()),
            Value::BulkString(expected_msg),
        ])));

        MockResponder {
            expected_req,
            returned_resp: Value::BulkString(returned_bs).into(),
        }
    }

    #[tokio::test]
    async fn echo() {
        let msg: BulkString = "Hello".into();
        let mut responder = new_echo_responder(msg.clone(), msg.clone());
        let mut client = Echo::client(&mut responder);

        client
            .echo(EchoArg { msg })
            .await
            .expect("Unexpected echo error");
    }

    #[tokio::test]
    async fn echo_wrong_response() {
        let msg: BulkString = "Hello".into();
        let mut responder = new_echo_responder(msg.clone(), "Wrong".into());
        let mut client = Echo::client(&mut responder);

        match client.echo(EchoArg { msg }).await {
            Ok(_) => panic!("Should have error"),
            Err(e) => assert!(matches!(e, ClientError::InvalidResponse)),
        }
    }
}

#[cfg(test)]
mod handler_test {
    use super::*;

    fn new_echo_handler() -> EchoHandler {
        Echo::handler()
    }

    #[test]
    fn handle_echo() {
        let handler = new_echo_handler();
        let resp = handler.handle(EchoArg {
            msg: "What's up".into(),
        });

        assert_eq!(resp, Value::BulkString("What's up".into()));
    }
}
