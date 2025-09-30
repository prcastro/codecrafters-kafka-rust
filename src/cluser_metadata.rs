use crate::varint::Varint;
use bytes::Buf;

#[derive(Debug)]
pub struct FeatureLevelRecord {
    _frame_version: u8,
    _record_type: u8,
    _version: u8,
    _name_length: u8,
    _name: String,
    _feature_level: i16,
    _tagged_field_length: u8,
    _tagged_fields: Vec<u8>,
}

impl FeatureLevelRecord {
    fn parse(cursor: &mut &[u8], frame_version: u8, record_type: u8) -> FeatureLevelRecord {
        let version = cursor.get_u8();
        let name_length = cursor.get_u8() - 1;
        let name_raw: Vec<u8> = (0..name_length).map(|_| cursor.get_u8()).collect();
        let name = String::from_utf8_lossy(&name_raw).to_string();
        let feature_level = cursor.get_i16();
        let tagged_field_length = cursor.get_u8();
        let tagged_fields = (0..tagged_field_length).map(|_| cursor.get_u8()).collect();
        FeatureLevelRecord {
            _frame_version: frame_version,
            _record_type: record_type,
            _version: version,
            _name_length: name_length,
            _name: name,
            _feature_level: feature_level,
            _tagged_field_length: tagged_field_length,
            _tagged_fields: tagged_fields,
        }
    }
}

#[derive(Debug)]
pub struct TopicRecord {
    _frame_version: u8,
    _record_type: u8,
    _version: u8,
    _name_length: u8,
    pub name: String,
    pub topic_id: [u8; 16],
    _tagged_field_length: u8,
    _tagged_fields: Vec<u8>,
}

impl TopicRecord {
    fn parse(cursor: &mut &[u8], frame_version: u8, record_type: u8) -> TopicRecord {
        let version = cursor.get_u8();
        let name_length = cursor.get_u8() - 1;
        let name_raw: Vec<u8> = (0..name_length).map(|_| cursor.get_u8()).collect();
        let name = String::from_utf8_lossy(&name_raw).to_string();
        let topic_id: [u8; 16] = cursor[..16].try_into().unwrap();
        cursor.advance(16);
        let tagged_field_length = cursor.get_u8();
        let tagged_fields = (0..tagged_field_length).map(|_| cursor.get_u8()).collect();

        TopicRecord {
            _frame_version: frame_version,
            _record_type: record_type,
            _version: version,
            _name_length: name_length,
            name,
            topic_id,
            _tagged_field_length: tagged_field_length,
            _tagged_fields: tagged_fields,
        }
    }
}

#[derive(Debug)]
pub struct PartitionRecord {
    _frame_version: u8,
    _record_type: u8,
    _version: u8,
    pub partition_id: i32,
    pub topic_id: [u8; 16],
    pub replicas_length: u8,
    pub replicas: Vec<i32>,
    pub insync_replicas_length: u8,
    pub insync_replicas: Vec<i32>,
    _removing_replicas_length: u8,
    _removing_replicas: Vec<i32>,
    _adding_replicas_length: u8,
    _adding_replicas: Vec<i32>,
    pub leader_id: i32,
    pub leader_epoch: i32,
    _partition_epoch: i32,
    _directories_length: u8,
    _directories: Vec<u32>,
    _tagged_field_length: u8,
    _tagged_fields: Vec<u8>,
}
impl PartitionRecord {
    fn parse(cursor: &mut &[u8], frame_version: u8, record_type: u8) -> PartitionRecord {
        let version = cursor.get_u8();
        let partition_id = cursor.get_i32();
        let topic_id = cursor[..16].try_into().unwrap();
        cursor.advance(16);
        let replicas_length = cursor.get_u8() - 1;
        let replicas = (0..replicas_length).map(|_| cursor.get_i32()).collect();
        let insync_replicas_length = cursor.get_u8() - 1;
        let insync_replicas = (0..insync_replicas_length)
            .map(|_| cursor.get_i32())
            .collect();
        let removing_replicas_length = cursor.get_u8() - 1;
        let removing_replicas = (0..removing_replicas_length)
            .map(|_| cursor.get_i32())
            .collect();
        let adding_replicas_length = cursor.get_u8() - 1;
        let adding_replicas = (0..adding_replicas_length)
            .map(|_| cursor.get_i32())
            .collect();
        let leader_id = cursor.get_i32();
        let leader_epoch = cursor.get_i32();
        let partition_epoch = cursor.get_i32();
        let directories_length = cursor.get_u8() - 1;
        let directories = (0..directories_length).map(|_| cursor.get_u32()).collect();
        let tagged_field_length = cursor.get_u8();
        let tagged_fields = (0..tagged_field_length).map(|_| cursor.get_u8()).collect();

        PartitionRecord {
            _frame_version: frame_version,
            _record_type: record_type,
            _version: version,
            partition_id,
            topic_id,
            replicas_length,
            replicas,
            insync_replicas_length,
            insync_replicas,
            _removing_replicas_length: removing_replicas_length,
            _removing_replicas: removing_replicas,
            _adding_replicas_length: adding_replicas_length,
            _adding_replicas: adding_replicas,
            leader_id,
            leader_epoch,
            _partition_epoch: partition_epoch,
            _directories_length: directories_length,
            _directories: directories,
            _tagged_field_length: tagged_field_length,
            _tagged_fields: tagged_fields,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum RecordValue {
    FeatureLevel(FeatureLevelRecord),
    Topic(TopicRecord),
    Partition(PartitionRecord),
}

impl RecordValue {
    fn parse(record_raw: &[u8]) -> RecordValue {
        let mut cursor = record_raw;
        let frame_version = cursor.get_u8();
        let record_type = cursor.get_u8();

        match record_type {
            12 => RecordValue::FeatureLevel(FeatureLevelRecord::parse(
                &mut cursor,
                frame_version,
                record_type,
            )),
            2 => RecordValue::Topic(TopicRecord::parse(&mut cursor, frame_version, record_type)),
            3 => RecordValue::Partition(PartitionRecord::parse(
                &mut cursor,
                frame_version,
                record_type,
            )),
            _ => panic!("Unknown record type"),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    _length: i64,
    _attributes: u8,
    _timestamp_delta: i8,
    _offset_delta: i8,
    _key_length: i64,
    _key: Option<Vec<u8>>,
    _value_length: i64,
    pub value: Option<RecordValue>,
    _header_array_count: u8,
}

#[derive(Debug)]
pub struct RecordBatch {
    _base_offset: i64,
    _batch_length: i32,
    _partition_leader_epoch: i32,
    _magic_byte: u8,
    _crc: i32,
    _attributes: u16,
    _last_offset_delta: i32,
    _base_timestamp: i64,
    _max_timestamp: i64,
    _producer_id: i64,
    _producer_epoch: i16,
    _base_sequence: i32,
    _records_length: u32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    fn parse(input: &[u8]) -> (RecordBatch, &[u8]) {
        let mut cursor = input;
        let base_offset = cursor.get_i64();
        let batch_length = cursor.get_i32();
        let partition_leader_epoch = cursor.get_i32();
        let magic_byte = cursor.get_u8();
        let crc = cursor.get_i32();
        let attributes = cursor.get_u16();
        let last_offset_delta = cursor.get_i32();
        let base_timestamp = cursor.get_i64();
        let max_timestamp = cursor.get_i64();
        let producer_id = cursor.get_i64();
        let producer_epoch = cursor.get_i16();
        let base_sequence = cursor.get_i32();
        let records_length = cursor.get_u32();

        let mut records = Vec::with_capacity(records_length as usize);

        for _ in 0..records_length {
            let length = cursor.get_signed_varint();
            let attributes = cursor.get_u8();
            let timestamp_delta = cursor.get_i8();
            let offset_delta = cursor.get_i8();

            let key_length = cursor.get_signed_varint();
            let key: Option<Vec<u8>> = if key_length >= 0 {
                Some((0..key_length).map(|_| cursor.get_u8()).collect())
            } else {
                None
            };

            let value_length = cursor.get_signed_varint();
            let value: Option<RecordValue> = if value_length >= 0 {
                let value_raw: Vec<u8> = (0..value_length).map(|_| cursor.get_u8()).collect();
                Some(RecordValue::parse(&value_raw))
            } else {
                None
            };

            let header_array_count = cursor.get_u8();

            let record = Record {
                _length: length,
                _attributes: attributes,
                _timestamp_delta: timestamp_delta,
                _offset_delta: offset_delta,
                _key_length: key_length,
                _key: key,
                _value_length: value_length,
                value: value,
                _header_array_count: header_array_count,
            };

            records.push(record);
        }

        let record_batch = RecordBatch {
            _base_offset: base_offset,
            _batch_length: batch_length,
            _partition_leader_epoch: partition_leader_epoch,
            _magic_byte: magic_byte,
            _crc: crc,
            _attributes: attributes,
            _last_offset_delta: last_offset_delta,
            _base_timestamp: base_timestamp,
            _max_timestamp: max_timestamp,
            _producer_id: producer_id,
            _producer_epoch: producer_epoch,
            _base_sequence: base_sequence,
            _records_length: records_length,
            records: records,
        };

        return (record_batch, cursor);
    }
}

#[derive(Debug)]
pub struct ClusterMetadata {
    pub record_bacthes: Vec<RecordBatch>,
}

impl ClusterMetadata {
    pub fn parse(input: &[u8]) -> ClusterMetadata {
        let mut cursor: &[u8] = &input;

        let mut record_batches = vec![];
        while cursor.len() != 0 {
            let (record_batch, new_cursor) = RecordBatch::parse(cursor);
            record_batches.push(record_batch);
            cursor = new_cursor;
        }

        ClusterMetadata {
            record_bacthes: record_batches,
        }
    }
}
