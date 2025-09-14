#![allow(unused_imports)]
use std::io::{self, Read, Write};
use std::net::TcpListener;

struct ApiKeyVerInfo {
    pub id: i16,
    pub min: i16,
    pub max: i16,
}

const API_VERSIONS: &[ApiKeyVerInfo] = &[ApiKeyVerInfo {
    id: 18,
    min: 0,
    max: 4,
}];

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
                let mut header = vec![];
                let mut body = vec![];

                let mut input: [u8; 512] = [0; 512];
                let _ = stream.read(&mut input)?;

                // Parse data
                let api_version = i16::from_be_bytes(input[6..8].try_into().unwrap());
                let correlation_id = &input[8..12];

                // Logic
                let error_code: i16 = if api_version != 4 { 35 } else { 0 };
                let array_length: u8 = API_VERSIONS.len() as u8 + 1;
                let tag_buffer: u8 = 0;
                let throttle_time: i32 = 0;

                // Header
                header.extend_from_slice(correlation_id);

                // Body
                body.extend_from_slice(&error_code.to_be_bytes());
                body.extend_from_slice(&array_length.to_be_bytes());

                for api_version in API_VERSIONS {
                    body.extend_from_slice(&api_version.id.to_be_bytes());
                    body.extend_from_slice(&api_version.min.to_be_bytes());
                    body.extend_from_slice(&api_version.max.to_be_bytes());
                    body.extend_from_slice(&tag_buffer.to_be_bytes());
                }

                body.extend_from_slice(&throttle_time.to_be_bytes());
                body.extend_from_slice(&tag_buffer.to_be_bytes());

                // Write to stream
                let message_size: i32 = header.len() as i32 + body.len() as i32 + 1;
                stream.write_all(&message_size.to_be_bytes())?;
                stream.write_all(&header)?;
                stream.write_all(&body)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
