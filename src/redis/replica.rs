use std::net::SocketAddr;

use thiserror::Error;
use tokio::net::TcpStream;

use super::{
    client::ClientError,
    cmd::{ping::PingArg, Ping, ReplConf, ReplConfArg, ReplConfArgConfig},
    session::Session,
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
    pub async fn init(
        master_addr: SocketAddr,
        listening_port: u16,
    ) -> Result<Self, ReplicationError> {
        Self::connect_to_master(master_addr, listening_port).await?;

        Ok(Self {})
    }

    async fn connect_to_master(
        master_addr: SocketAddr,
        listening_port: u16,
    ) -> Result<(), ReplicationError> {
        let stream = TcpStream::connect(master_addr).await?;
        let mut session = Session::new(stream);

        // First handshake
        // PING
        let _ = Ping::client(&mut session)
            .ping(PingArg { msg: None })
            .await?;

        // Second handshake
        // REPLCONF listening-port <PORT>
        let mut replconf_client = ReplConf::client(&mut session);
        let _ = replconf_client
            .replconf(ReplConfArg {
                config: ReplConfArgConfig::ListeningPort(listening_port),
            })
            .await?;

        // REPLCONF capa psync2
        let _ = replconf_client
            .replconf(ReplConfArg {
                config: ReplConfArgConfig::Capabilities("psync2".into()),
            })
            .await?;

        Ok(())
    }
}
