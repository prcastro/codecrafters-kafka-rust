#![allow(unused_imports)]
use std::io::Write;
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                _stream.write_all(&[0, 0, 0, 5, 0, 0, 0, 7]).unwrap();
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
