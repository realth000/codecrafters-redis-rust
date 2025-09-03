use std::{
    io::{Read, Write},
    net::TcpListener,
};

mod threading;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => loop {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).expect("failed to recv");
                if n == 0 {
                    println!("connection closed");
                    break;
                }
                stream.write(b"+PONG\r\n").expect("failed to respond");
                println!("accepted new connection");
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
