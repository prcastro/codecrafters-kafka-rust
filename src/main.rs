#![allow(unused_imports)]
use std::io::{Read, Write};
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
                // Find correlation_id
                let mut input: [u8; 12] = [0; 12];
                let _input_size = _stream.read(&mut input).unwrap();
                let correlation_id = &input[8..12];

                // Write message_size
                _stream.write_all(&[0, 0, 0, 4]).unwrap();

                // Write correlation_id
                _stream.write_all(correlation_id).unwrap();

                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
