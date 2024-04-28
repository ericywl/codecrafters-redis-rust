use redis_starter_rust::resp::{BulkString, Decoder, Encoder, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("Accepted new connection.");
                tokio::spawn(async move {
                    match handle_connection(stream).await {
                        Ok(_) => (),
                        Err(e) => eprintln!("Handle connection error: {e}"),
                    }
                });
            }
            Err(e) => {
                eprintln!("Tcp stream error: {e}");
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).await?;
        if read_count == 0 {
            return Ok(());
        }

        let value = Decoder::decode(&buf[..]).map_err(|e| anyhow::anyhow!("Decode error: {e}"))?;
        let (command, args) = parse_request(value)?;
        let response = match command.to_ascii_lowercase().as_str() {
            "ping" => Value::SimpleString("PONG".to_string().into()),
            "echo" => Value::BulkString(args.first().unwrap().clone()),
            c => panic!("Cannot handle command {}", c),
        };

        stream.write(&Encoder::encode(response)).await?;
    }
}

fn parse_request(value: Value) -> anyhow::Result<(String, Vec<BulkString>)> {
    match value {
        Value::Array(a) => {
            let first_value = a
                .values()
                .first()
                .ok_or(anyhow::anyhow!("Missing command"))?;
            let bs = first_value
                .bulk_string()
                .ok_or(anyhow::anyhow!("Command not BulkString"))?;
            let cmd: String = bs
                .try_into()
                .map_err(|_| anyhow::anyhow!("Command is not string"))?;

            let mut args = Vec::new();
            for v in a.values().iter().skip(1) {
                let arg = match v.bulk_string() {
                    Some(bs) => bs.clone(),
                    None => return Err(anyhow::anyhow!("Args not BulkString")),
                };
                args.push(arg)
            }

            Ok((cmd, args))
        }
        _ => Err(anyhow::anyhow!("Unexpected request format")),
    }
}
