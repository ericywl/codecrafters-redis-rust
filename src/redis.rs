pub mod cmd;
pub mod cmd_handler;
pub mod resp;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use self::cmd::{Command, CommandError};
use self::cmd_handler::HandleCommandError;
use self::cmd_handler::{CommandHandler, CommandHandlerConfig};
use self::resp::{EncodeError, Value};

#[derive(Debug)]
struct Request {
    cmd: Command,
    tx: oneshot::Sender<Response>,
}

impl Request {
    fn new(cmd: Command) -> (Self, oneshot::Receiver<Response>) {
        let (tx, rx) = oneshot::channel();
        (Self { cmd, tx }, rx)
    }

    fn decode(buf: &[u8]) -> Result<(Self, oneshot::Receiver<Response>), CommandError> {
        Ok(Self::new(Command::parse(buf)?))
    }
}

#[derive(Debug)]
struct Response(Value);

impl Response {
    fn encode(&self, buf: &mut impl std::io::Write) -> Result<(), EncodeError> {
        self.0.encode(buf)
    }
}

impl From<Value> for Response {
    fn from(value: Value) -> Self {
        Self(value)
    }
}

#[derive(Debug, Error)]
pub enum RedisError {
    #[error(transparent)]
    Encode(#[from] EncodeError),

    #[error(transparent)]
    Command(#[from] CommandError),

    #[error(transparent)]
    HandleCommand(#[from] HandleCommandError),

    #[error(transparent)]
    TokioIo(#[from] tokio::io::Error),
}

pub struct Redis {
    listener: tokio::net::TcpListener,
    handler: CommandHandler,
}

#[derive(Debug)]
pub struct RedisConfig {
    pub replica_addr: Option<String>,
}

impl Redis {
    pub async fn new(addr: String, config: RedisConfig) -> Result<Self, RedisError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            handler: CommandHandler::new(
                Arc::new(RwLock::new(HashMap::new())),
                CommandHandlerConfig {
                    is_replica: config.replica_addr.is_some(),
                },
            ),
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
        reqs_tx: mpsc::Sender<Request>,
    ) -> Result<(), RedisError> {
        let mut buf = [0u8; 512];
        loop {
            let bytes = stream.read(&mut buf).await?;
            if bytes == 0 {
                break;
            }

            debug!("Received {:?}", &buf[..bytes]);
            // Send request to the request handler
            let (req, resp_rx) = Request::decode(&buf[..bytes])?;
            let _ = reqs_tx.send(req).await;

            // Wair for response from the request handler, encode the response
            let resp = resp_rx.await.unwrap();
            let mut write_buf = Vec::new();
            let mut buf = Buffer::new(&mut write_buf);
            resp.encode(&mut buf)?;

            let count = buf.count;
            let buf = &buf.inner[..count];

            // Write encoded response to stream
            debug!("Sending {:?}", buf);
            stream.write(buf).await?;
        }

        Ok(())
    }

    async fn handle_request(&mut self, req: Request) -> Result<(), RedisError> {
        // Handle request and send back response via channel
        let Request { cmd, tx } = req;
        let resp: Response = self.handler.handle(cmd)?.into();
        let _ = tx.send(resp);

        Ok(())
    }
}

/// Buffer is a wrapper for io::Write.
struct Buffer<W> {
    inner: W,
    count: usize,
}

impl<W> Buffer<W>
where
    W: std::io::Write,
{
    fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W> std::io::Write for Buffer<W>
where
    W: std::io::Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.count += len;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
