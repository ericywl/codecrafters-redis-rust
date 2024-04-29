// use tokio::{
//     io::{AsyncReadExt, AsyncWriteExt},
//     sync::mpsc,
// };
// use tracing::{debug, error, info};

use std::io;

pub struct Buffer<W> {
    inner: W,
    count: usize,
}

impl<W> Buffer<W>
where
    W: io::Write,
{
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W> io::Write for Buffer<W>
where
    W: io::Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.count += len;
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// pub struct Session {
//     conn: tokio::net::TcpStream,
//     buf: Vec<u8>,
//     reqs_tx: mpsc::Sender<Request>,
// }

// impl Session {
//     async fn handle_command(&mut self, cmd: Command) -> Result<()> {
//         info!("Handling command {cmd:?}");
//         let resp = match cmd {
//             Command::Ping(msg) => {
//                 if let Some(msg) = msg {
//                     Value::Array(vec![
//                         Value::Str(StringValue::Bulk("PONG".to_owned())),
//                         Value::Str(StringValue::Bulk(msg)),
//                     ])
//                     .into()
//                 } else {
//                     Value::Str(StringValue::Simple("PONG".to_owned())).into()
//                 }
//             }
//             Command::Echo(msg) => Value::Str(StringValue::Bulk(msg)).into(),
//             cmd => {
//                 let (req, rx) = Request::new(cmd);
//                 let _ = self.reqs_tx.send(req).await;
//                 // TODO: properly handle channel closing
//                 let resp = rx.await.unwrap();
//                 resp
//             }
//         };

//         self.buf.clear();
//         let mut buf = Buffer::new(&mut self.buf);
//         resp.encode(&mut buf)?;

//         let count = buf.count;
//         let buf = &buf.inner[..count];
//         debug!("sending {:?}", buf);
//         self.conn.write(buf).await?;
//         Ok(())
//     }
// }
