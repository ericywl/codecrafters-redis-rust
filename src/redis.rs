pub mod cmd;
pub mod cmd_handler;
pub mod replica;
pub mod resp;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use super::util;

use self::cmd::CommandError;
use self::cmd_handler::HandleCommandError;
use self::cmd_handler::{CommandHandler, CommandHandlerConfig};
use self::replica::{Replication, ReplicationError};
use self::resp::{DecodeError, EncodeError};

struct RequestChannel {
    req: util::Request,
    tx: oneshot::Sender<util::Response>,
}

impl RequestChannel {
    fn new(req: util::Request) -> (Self, oneshot::Receiver<util::Response>) {
        let (tx, rx) = oneshot::channel();
        (Self { req, tx }, rx)
    }
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error(transparent)]
    Encode(#[from] EncodeError),

    #[error(transparent)]
    Decode(#[from] DecodeError),

    #[error(transparent)]
    Command(#[from] CommandError),

    #[error(transparent)]
    HandleCommand(#[from] HandleCommandError),

    #[error(transparent)]
    Replication(#[from] ReplicationError),

    #[error(transparent)]
    TokioIo(#[from] tokio::io::Error),
}

pub struct Redis {
    /// Listen to client connections.
    listener: tokio::net::TcpListener,

    /// Handles commands from client requests.
    handler: CommandHandler,

    /// Handles replication.
    replication: Option<Replication>,
}

#[derive(Debug)]
pub struct RedisConfig {
    pub master_addr: Option<String>,
}

impl Redis {
    pub async fn init(addr: String, config: RedisConfig) -> Result<Self, RedisError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let is_replica = config.master_addr.is_some();
        let master_repl_id_and_offset = if is_replica {
            None
        } else {
            Some((util::generate_random_alphanumeric_string(40), 0))
        };
        let replication = if is_replica {
            Some(Replication::init(config.master_addr.unwrap().clone()).await?)
        } else {
            None
        };

        Ok(Self {
            listener,
            handler: CommandHandler::new(
                Arc::new(RwLock::new(HashMap::new())),
                CommandHandlerConfig {
                    is_replica,
                    master_repl_id_and_offset,
                },
            ),
            replication,
        })
    }

    pub async fn start(mut self) -> Result<(), RedisError> {
        let (reqs_tx, mut reqs_rx) = mpsc::channel(128);

        loop {
            tokio::select! {
                // Handle connection
                conn = self.listener.accept() => {
                    let (stream, addr) = conn?;
                    info!("Accepted new connection from {addr:?}");
                    let reqs_tx = reqs_tx.clone();
                    let _ = tokio::spawn(async move {
                        match Self::handle_connection(stream, reqs_tx).await {
                            Ok(_) => (),
                            Err(e) => error!("Error handling connection: {e}"),
                        }
                    });
                }

                // Handle request from connection
                Some(req) = reqs_rx.recv() => {
                    match self.handle_request(req).await {
                        Ok(_) => (),
                        Err(e) => error!("Error handling request: {e}"),
                    }
                }
            }
        }
    }

    async fn handle_connection(
        mut stream: TcpStream,
        reqs_tx: mpsc::Sender<RequestChannel>,
    ) -> Result<(), RedisError> {
        let mut buf = [0u8; 512];
        loop {
            let bytes = stream.read(&mut buf).await?;
            if bytes == 0 {
                break;
            }

            debug!("Received {:?}", &buf[..bytes]);
            // Send request to the request handler
            let (req_ch, resp_rx) = RequestChannel::new(util::Request::decode(&buf[..bytes])?);
            let _ = reqs_tx.send(req_ch).await;

            // Wair for response from the request handler, encode the response
            let resp = resp_rx.await.unwrap();
            let buf = resp.encode()?;

            // Write encoded response to stream
            debug!("Sending {:?}", &buf);
            stream.write(&buf).await?;
        }

        Ok(())
    }

    async fn handle_request(&mut self, req_ch: RequestChannel) -> Result<(), RedisError> {
        // Handle request and send back response via channel
        let RequestChannel { req, tx } = req_ch;
        let cmd = req.as_command()?;
        let resp: util::Response = self.handler.handle(cmd)?.into();
        let _ = tx.send(resp);

        Ok(())
    }
}
