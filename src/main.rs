// Uncomment this block to pass the first stage
use std::error::Error;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn ping(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let _ = stream.write(b"+PONG\r\n").unwrap();
    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => ping(stream)?,
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
