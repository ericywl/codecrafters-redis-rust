use std::{
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for _stream in listener.incoming() {
        match _stream {
            Ok(stream) => {
                println!("Accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                eprintln!("Tcp stream error: {e}");
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).unwrap();
        if read_count == 0 {
            break;
        }
        stream.write(b"+PONG\r\n").unwrap();
    }
}
