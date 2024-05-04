pub mod cmd;
pub mod cmd_handler;
pub mod replica;
pub mod resp;
pub mod session;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use super::util;

use self::cmd::CommandError;
use self::cmd_handler::HandleCommandError;
use self::cmd_handler::{CommandHandler, CommandHandlerConfig};
use self::replica::{Replication, ReplicationError};
use self::session::{Request, Response, Session, SessionError};

struct RequestChannel {
    req: Request,
    tx: oneshot::Sender<Response>,
}

impl RequestChannel {
    fn new(req: Request) -> (Self, oneshot::Receiver<Response>) {
        let (tx, rx) = oneshot::channel();
        (Self { req, tx }, rx)
    }
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error(transparent)]
    Session(#[from] SessionError),

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
        let (reqs_ch_tx, mut reqs_ch_rx) = mpsc::channel(128);

        loop {
            tokio::select! {
                // Handle connection
                conn = self.listener.accept() => {
                    let (stream, addr) = conn?;
                    info!("Accepted new connection from {addr:?}");
                    let reqs_ch_tx = reqs_ch_tx.clone();
                    let session = Session::new(stream);
                    let _ = tokio::spawn(async move {
                        match Self::handle_connection(session, reqs_ch_tx).await {
                            Ok(_) => (),
                            Err(e) => error!("Error handling connection: {e}"),
                        }
                    });
                }

                // Handle request from connection
                Some(req) = reqs_ch_rx.recv() => {
                    match self.handle_request(req).await {
                        Ok(_) => (),
                        Err(e) => error!("Error handling request: {e}"),
                    }
                }
            }
        }
    }

    async fn handle_connection(
        mut session: Session,
        reqs_ch_tx: mpsc::Sender<RequestChannel>,
    ) -> Result<(), RedisError> {
        loop {
            let req = session.receive_request().await?;
            if req.is_none() {
                break;
            }

            // Send request to the request handler
            let (req_ch, resp_rx) = RequestChannel::new(req.unwrap());
            let _ = reqs_ch_tx.send(req_ch).await;

            // Wait for response from the request handler and send it
            let resp = resp_rx.await.unwrap();
            session.send_response(resp).await?;
        }

        Ok(())
    }

    async fn handle_request(&mut self, req_ch: RequestChannel) -> Result<(), RedisError> {
        // Handle request and send back response via channel
        let RequestChannel { req, tx } = req_ch;
        let cmd = req.as_command()?;
        let resp: Response = self.handler.handle(cmd)?.into();
        let _ = tx.send(resp);

        Ok(())
    }
}
