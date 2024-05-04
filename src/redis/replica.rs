use thiserror::Error;
use tokio::net::TcpStream;

use super::{
    resp::{Array, Value},
    session::{Request, Response, Session, SessionError},
};

pub struct Replication {}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("Unable to connect to master")]
    CannotConnectMaster,

    #[error(transparent)]
    Session(#[from] SessionError),

    #[error(transparent)]
    TokioIo(#[from] tokio::io::Error),
}

impl Replication {
    pub async fn init(master_addr: String) -> Result<Self, ReplicationError> {
        Self::connect_to_master(master_addr).await?;

        Ok(Self {})
    }

    async fn connect_to_master(master_addr: String) -> Result<(), ReplicationError> {
        let stream = TcpStream::connect(master_addr).await?;
        let mut session = Session::new(stream);

        // First handshake
        let request: Request =
            Value::Array(Array::new(vec![Value::BulkString("PING".into())])).into();
        let response = session
            .send_request_and_wait_reply(request)
            .await?
            .ok_or(ReplicationError::CannotConnectMaster)?;

        // if !response_is_ok(response) {
        //     return Err(ReplicationError::CannotConnectMaster);
        // }

        // Second handshake

        Ok(())
    }
}

fn response_is_ok(resp: Response) -> bool {
    let value: Value = resp.into();
    match value.simple_string() {
        Some(s) => s.as_str() == "OK",
        None => false,
    }
}
