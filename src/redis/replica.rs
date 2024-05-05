use std::rc::Rc;

use thiserror::Error;
use tokio::net::TcpStream;

use super::{
    client::ClientError,
    cmd::{ping::PingArg, Command, Echo, EchoArg, Ping},
    resp::Value,
    session::{Request, Response, Session, SessionError},
};

pub struct Replication {}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("Unable to connect to master")]
    CannotConnectMaster,

    #[error(transparent)]
    Client(#[from] ClientError),

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
        let _ = Ping::client(&mut session)
            .ping(PingArg { msg: None })
            .await?;

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
