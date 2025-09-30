mod cluser_metadata;
mod varint;

use cluser_metadata::{ClusterMetadata, RecordValue};

use bytes::{Buf, BufMut};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::{fs, thread};

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

#[derive(Debug)]
struct Partition {
    error_code: i16,
    partition_id: i32,
    leader_id: i32,
    leader_epoch: i32,
    replicas_length: u8,
    replicas: Vec<i32>,
    insync_replicas_length: u8,
    insync_replicas: Vec<i32>,
    eligible_leader_length: u8,
    eligible_leader: Vec<i32>,
    last_known_elr_length: u8,
    last_known_elr: Vec<i32>,
    offline_replica_length: u8,
    offline_replica: Vec<i32>,
    tag_buffer: u8,
}

#[derive(Debug)]
struct TopicDescription {
    name: String,
    error_code: i16,
    topic_id: [u8; 16],
    is_internal: bool,
    partition_length: u8,
    partition_info: Vec<Partition>,
    authorized_operations: u32,
}

struct DescribeTopicResult {
    correlation_id: u32,
    throttle_time: u32,
    topic_descriptions: Vec<TopicDescription>,
    next_cursor: u8,
}

fn topic_id_from_name(cluster_metadata: &ClusterMetadata, topic_name: &str) -> Option<[u8; 16]> {
    for record_batch in &cluster_metadata.record_bacthes {
        for record in &record_batch.records {
            match &record.value {
                Some(RecordValue::Topic(topic_info)) => {
                    if topic_info.name == topic_name {
                        return Some(topic_info.topic_id);
                    }
                }
                _ => continue,
            }
        }
    }
    return None;
}

fn partition_info_from_topic(
    cluster_metadata: &ClusterMetadata,
    topic_id: [u8; 16],
) -> Vec<Partition> {
    let mut partitions = vec![];

    for record_batch in &cluster_metadata.record_bacthes {
        for record in &record_batch.records {
            match &record.value {
                Some(RecordValue::Partition(partition_record)) => {
                    if partition_record.topic_id == topic_id {
                        partitions.push(Partition {
                            error_code: 0,
                            partition_id: partition_record.partition_id,
                            leader_id: partition_record.leader_id,
                            leader_epoch: partition_record.leader_epoch,
                            replicas_length: partition_record.replicas_length,
                            replicas: partition_record.replicas.clone(),
                            insync_replicas_length: partition_record.insync_replicas_length,
                            insync_replicas: partition_record.insync_replicas.clone(),
                            eligible_leader_length: 0,
                            eligible_leader: vec![],
                            last_known_elr_length: 0,
                            last_known_elr: vec![],
                            offline_replica_length: 0,
                            offline_replica: vec![],
                            tag_buffer: 0,
                        });
                    }
                }
                _ => {}
            }
        }
    }

    partitions
}

// TODO:
// 3. Find the Partition Records, extract the right partition information given a topic name
// 4. Add topic_id and partition information to the TopicDescription struct
fn describe_topics(correlation_id: u32, topics: Vec<String>) -> DescribeTopicResult {
    let mut sorted_topics = topics.clone();
    sorted_topics.sort();
    let customer_metadata_raw =
        fs::read("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log").unwrap();

    let cluster_metadata = ClusterMetadata::parse(&customer_metadata_raw);

    let mut topic_descriptions = vec![];
    for topic_name in sorted_topics {
        let topic_id = match topic_id_from_name(&cluster_metadata, &topic_name) {
            Some(id) => id,
            None => {
                topic_descriptions.push(TopicDescription {
                    name: topic_name,
                    error_code: 3,
                    topic_id: [0; 16],
                    is_internal: false,
                    partition_length: 0,
                    partition_info: vec![],
                    authorized_operations: 0,
                });
                continue;
            }
        };

        let partition_info = partition_info_from_topic(&cluster_metadata, topic_id);
        topic_descriptions.push(TopicDescription {
            name: topic_name,
            error_code: 0,
            topic_id: topic_id,
            is_internal: false,
            partition_length: partition_info.len() as u8,
            partition_info: partition_info,
            authorized_operations: 0,
        });
    }

    DescribeTopicResult {
        correlation_id: correlation_id,
        throttle_time: 0,
        topic_descriptions: topic_descriptions,
        next_cursor: 0xff,
    }
}

fn handle_describe_topic(mut input: &[u8]) -> Vec<u8> {
    // Deserialize input
    let _message_size = input.get_u32();
    let _api_key = input.get_u16();
    let _api_version = input.get_u16();
    let correlation_id = input.get_u32();
    let client_id_length = input.get_u16() as usize;
    let _client_id: Vec<u8> = (0..client_id_length).map(|_| input.get_u8()).collect();
    input.get_u8(); // Tag Buffer
    let topic_array_length = input.get_u8() - 1;

    let mut topic_names: Vec<String> = vec![];
    for _ in 0..topic_array_length {
        let topic_name_length = input.get_u8() - 1;
        let topic_name_utf8: Vec<u8> = (0..topic_name_length).map(|_| input.get_u8()).collect();
        let topic_name = String::from_utf8_lossy(&topic_name_utf8).to_string();
        topic_names.push(topic_name);
        input.get_u8(); // Tag Buffer
    }

    let result = describe_topics(correlation_id, topic_names);

    // Serialize result
    let mut header = vec![];
    header.put_u32(result.correlation_id);
    header.put_u8(0); // Tag buffer

    let mut body = vec![];
    body.put_u32(result.throttle_time);
    body.put_u8(result.topic_descriptions.len() as u8 + 1);
    for topic_description in result.topic_descriptions {
        body.put_i16(topic_description.error_code);
        let topic_name_utf8 = topic_description.name.as_bytes();
        body.put_u8(topic_name_utf8.len() as u8 + 1);
        body.extend_from_slice(topic_name_utf8);
        body.extend_from_slice(&topic_description.topic_id);
        body.put_u8(topic_description.is_internal as u8);
        body.put_u8(topic_description.partition_length + 1);
        for partition in topic_description.partition_info {
            body.put_i16(partition.error_code);
            body.put_i32(partition.partition_id);
            body.put_i32(partition.leader_id);
            body.put_i32(partition.leader_epoch);
            body.put_u8(partition.replicas_length + 1);
            for replica in partition.replicas {
                body.put_i32(replica);
            }
            body.put_u8(partition.insync_replicas_length + 1);
            for replica in partition.insync_replicas {
                body.put_i32(replica);
            }
            body.put_u8(partition.eligible_leader_length + 1);
            for replica in partition.eligible_leader {
                body.put_i32(replica);
            }
            body.put_u8(partition.last_known_elr_length + 1);
            for replica in partition.last_known_elr {
                body.put_i32(replica);
            }
            body.put_u8(partition.offline_replica_length + 1);
            for replica in partition.offline_replica {
                body.put_i32(replica);
            }
            body.put_u8(partition.tag_buffer);
        }
        body.put_u32(topic_description.authorized_operations);
        body.put_u8(0); // Tag buffer
    }
    body.put_u8(result.next_cursor);
    body.put_u8(0); // Tag buffer

    // Write response buffer
    let mut response = vec![];
    let message_size: u32 = header.len() as u32 + body.len() as u32;
    response.put_u32(message_size);
    response.extend_from_slice(&header);
    response.extend_from_slice(&body);
    return response;
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
