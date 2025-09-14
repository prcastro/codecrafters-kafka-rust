#![allow(unused_imports)]
use std::io::{self, Read, Write};
use std::net::TcpListener;

fn main() -> io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092")?;
    //
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                // Find correlation_id
                let mut input: [u8; 512] = [0; 512];
                let _ = stream.read(&mut input)?;
                let correlation_id = &input[8..12];

                // Write message_size
                stream.write_all(&[0, 0, 0, 4])?;

                // Write correlation_id
                stream.write_all(correlation_id)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
