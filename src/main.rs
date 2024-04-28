use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
};

use redis_starter_rust::ThreadPool;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let pool = ThreadPool::build(4).unwrap();

    for _stream in listener.incoming() {
        match _stream {
            Ok(stream) => {
                println!("Accepted new connection");
                pool.execute(|| {
                    handle_connection(stream)
                        .unwrap_or_else(|e| eprintln!("Handle connection error: {e}"));
                })
                .unwrap_or_else(|e| eprintln!("Pool execute error: {e}"));
            }
            Err(e) => {
                eprintln!("Tcp stream error: {e}");
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<(), io::Error> {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf)?;
        if read_count == 0 {
            return Ok(());
        }
        stream.write(b"+PONG\r\n")?;
    }
}
