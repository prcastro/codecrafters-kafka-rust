use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

struct ApiKeyVerInfo {
    pub id: i16,
    pub min: i16,
    pub max: i16,
}

const API_VERSIONS: &[ApiKeyVerInfo] = &[
    // APIVersions
    ApiKeyVerInfo {
        id: 18,
        min: 0,
        max: 4,
    },
    // DescribeTopicPartitions
    ApiKeyVerInfo {
        id: 75,
        min: 0,
        max: 0,
    },
];

fn handle_api_version(input: &[u8]) -> Vec<u8> {
    let api_version = i16::from_be_bytes(input[6..8].try_into().unwrap());
    let correlation_id = &input[8..12];
    let error_code: i16 = if api_version != 4 { 35 } else { 0 };
    let array_length: u8 = API_VERSIONS.len() as u8 + 1;
    let tag_buffer: u8 = 0;
    let throttle_time: i32 = 0;

    // Header
    let mut header = vec![];
    header.extend_from_slice(correlation_id);

    // Body
    let mut body = vec![];
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
    let mut result = vec![];
    let message_size: i32 = header.len() as i32 + body.len() as i32;
    result.extend_from_slice(&message_size.to_be_bytes());
    result.extend_from_slice(&header);
    result.extend_from_slice(&body);
    return result;
}

fn handle_describe_topic(input: &[u8]) -> Vec<u8> {
    let mut idx = 6;

    let _api_version = i16::from_be_bytes(input[idx..idx + 2].try_into().unwrap());
    idx += 2;

    let correlation_id = &input[idx..idx + 4];
    idx += 4;

    let client_id_length = i16::from_be_bytes(input[idx..idx + 2].try_into().unwrap()) as usize;
    idx += 2;

    let _client_id = &input[idx..idx + client_id_length];
    idx += client_id_length;

    idx += 1; // Tag buffer

    let topic_array_length = input[idx] - 1;
    idx += 1;

    let mut topics = vec![];
    for _ in 0..topic_array_length {
        let topic_name_length = input[idx] as usize;
        idx += 1;

        topics.push(String::from_utf8_lossy(&input[idx..idx + topic_name_length]).to_string());
        idx += topic_name_length;

        idx += 1; // Tag buffer
    }

    // Header
    let mut header = vec![];
    header.extend_from_slice(correlation_id);
    header.push(0); // Tag buffer

    // Body
    let mut body = vec![];
    let throttle_time: u32 = 0;
    body.extend_from_slice(&throttle_time.to_be_bytes());
    body.extend_from_slice(&(topic_array_length + 1).to_be_bytes());
    for topic_idx in 0..topic_array_length {
        let error_code: i16 = 3;
        body.extend_from_slice(&error_code.to_be_bytes());

        let topic_name = &topics[topic_idx as usize];
        let topic_name_utf8 = topic_name.as_bytes();
        let topic_name_length = topic_name_utf8.len() as u8;

        body.push(topic_name_length);
        body.extend_from_slice(topic_name_utf8);

        let topic_id: [u8; 16] = [0; 16];
        body.extend_from_slice(&topic_id);

        let is_internal: u8 = 0;
        body.push(is_internal);

        // TODO: Handle partitions here
        let partition_length: u8 = 0;
        body.push(partition_length + 1);

        let authorized_operations: i32 = 0;
        body.extend_from_slice(&authorized_operations.to_be_bytes());

        body.push(0); // Tag buffer
    }

    let next_cursor: u8 = 0xff;
    body.push(next_cursor);
    body.push(0); // Tag buffer

    // Write result
    let mut result = vec![];
    let message_size: i32 = header.len() as i32 + body.len() as i32;
    result.extend_from_slice(&message_size.to_be_bytes());
    result.extend_from_slice(&header);
    result.extend_from_slice(&body);
    return result;
}

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
                    18 => handle_api_version(&input),
                    75 => handle_describe_topic(&input),
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
                println!("accepted new connection");
                thread::spawn(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
