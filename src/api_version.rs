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

pub fn handle_request(input: &[u8]) -> Vec<u8> {
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
