use crate::cluser_metadata::{ClusterMetadata, PartitionRecord};

use bytes::{Buf, BufMut};
use std::fs;

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

fn partition_record_to_partition(partition_record: PartitionRecord) -> Partition {
    Partition {
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
    }
}

fn describe_topics(correlation_id: u32, topics: Vec<String>) -> DescribeTopicResult {
    let mut sorted_topics = topics.clone();
    sorted_topics.sort();
    let customer_metadata_raw =
        fs::read("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log").unwrap();

    let cluster_metadata = ClusterMetadata::parse(&customer_metadata_raw);

    let mut topic_descriptions = vec![];
    for topic_name in sorted_topics {
        let topic_id = match cluster_metadata.topic_id(&topic_name) {
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

        let partition_info: Vec<Partition> = cluster_metadata
            .partitions(topic_id)
            .into_iter()
            .map(partition_record_to_partition)
            .collect();

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

pub fn handle_request(mut input: &[u8]) -> Vec<u8> {
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
