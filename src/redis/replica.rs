use thiserror::Error;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::util;

use super::resp::{Array, EncodeError, Value};

pub struct Replication {}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error(transparent)]
    Encode(#[from] EncodeError),

    #[error(transparent)]
    TokioIo(#[from] tokio::io::Error),
}

impl Replication {
    pub async fn init(master_addr: String) -> Result<Self, ReplicationError> {
        Self::connect_to_master(master_addr).await?;

        Ok(Self {})
    }

    async fn connect_to_master(master_addr: String) -> Result<(), ReplicationError> {
        let mut stream = TcpStream::connect(master_addr).await?;

        // First handshake
        let request = Value::Array(Array::new(vec![Value::BulkString("PING".into())]));
        let buf = util::encode_value(&request)?;
        stream.write(&buf).await?;

        // Second handshake

        Ok(())
    }
}
