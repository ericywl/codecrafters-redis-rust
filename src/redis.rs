use tokio::net::ToSocketAddrs;

pub mod cmd;
pub mod resp;
pub mod session;

pub struct Redis {
    listener: tokio::net::TcpListener,
    sessions: Vec<tokio::task::JoinHandle<Result<(), ()>>>,
}

impl Redis {
    pub async fn new(addr: impl ToSocketAddrs) -> Result<Self, tokio::io::Error> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            sessions: Vec::new(),
        })
    }
}
