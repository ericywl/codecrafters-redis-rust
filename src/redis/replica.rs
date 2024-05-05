use thiserror::Error;
use tokio::net::TcpStream;

use super::{
    cmd::{ping::PingArg, Command},
    resp::Value,
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
        // PING
        let cmd_value: Value = Command::Ping(PingArg { msg: None }).into();
        let request: Request = cmd_value.into();
        let response = session
            .send_request_and_wait_reply(request)
            .await?
            .ok_or(ReplicationError::CannotConnectMaster)?;

        if !response_is(response, "PONG") {
            return Err(ReplicationError::CannotConnectMaster);
        }

        // Second handshake
        // REPLCONF listening-port <PORT>

        Ok(())
    }
}

fn response_is(resp: Response, expected: &str) -> bool {
    let value: Value = resp.into();
    match value.simple_string() {
        Some(s) => s.as_str() == expected,
        None => false,
    }
}
