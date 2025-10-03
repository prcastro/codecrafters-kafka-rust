mod api_version;
mod cluser_metadata;
mod describe_topic;
mod varint;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn handle_connection(mut stream: TcpStream) {
    let mut input: [u8; 512] = [0; 512];

    loop {
        let result = stream.read(&mut input);
        match result {
            Ok(0) => break,
            Ok(_) => {
                // Parse data
                let api_key = i16::from_be_bytes(input[4..6].try_into().unwrap());

                let result = match api_key {
                    18 => api_version::handle_request(&input),
                    75 => describe_topic::handle_request(&input),
                    _ => {
                        println!("Error processing unknown API Key");
                        break;
                    }
                };

                match stream.write_all(&result) {
                    Err(e) => {
                        println!("Error writing to stream: {}", e);
                        break;
                    }
                    Ok(_) => {}
                }
            }
            Err(e) => {
                println!("Error reading from connection: {}", e);
            }
        }
    }
}

fn main() {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted new connection");
                thread::spawn(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
}
