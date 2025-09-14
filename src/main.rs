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

                let mut input: [u8; 512] = [0; 512];
                let _ = stream.read(&mut input)?;

                // Parse data
                let api_version = i16::from_be_bytes(input[6..8].try_into().unwrap());
                let correlation_id = &input[8..12];

                // Logic
                let error_code: i16 = if api_version != 4 { 32 } else { 0 };

                // Write result
                stream.write_all(&[0, 0, 0, 6])?;
                stream.write_all(correlation_id)?;
                stream.write_all(&error_code.to_be_bytes())?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
