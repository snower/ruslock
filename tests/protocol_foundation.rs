use std::time::Duration;

use ruslock::protocol::codec::decode_response_header;
use ruslock::protocol::command::{Command, InitCommand, LockCommand, PingCommand};
use ruslock::protocol::constants::*;
use ruslock::{ClientOptions, Id16, Key16, LockData, LockResultData, PackedTime, SlockError};

#[test]
fn key16_right_aligns_short_keys_and_hashes_long_keys() {
    let key = Key16::new(b"abc");
    let mut expected = [0u8; 16];
    expected[13..].copy_from_slice(b"abc");
    assert_eq!(key.as_bytes(), &expected);

    let long = Key16::new(b"0123456789abcdef-more");
    let digest = {
        use md5::{Digest, Md5};
        let mut hasher = Md5::new();
        hasher.update(b"0123456789abcdef-more");
        hasher.finalize()
    };
    assert_eq!(long.as_bytes(), digest.as_slice());
}

#[test]
fn packed_time_keeps_value_and_flags_in_java_layout() {
    let time = PackedTime::with_flags(0x1234, 0xabcd);
    assert_eq!(time.value(), 0x1234);
    assert_eq!(time.flags(), 0xabcd);
    assert_eq!(time.bits(), 0xabcd1234);
    assert_eq!(time.merge_flags(0x0001).flags(), 0xabcd | 0x0001);
}

#[test]
fn client_options_match_java_defaults() {
    let options = ClientOptions::default();
    assert_eq!(options.connect_timeout, Duration::from_secs(5));
    assert_eq!(options.reconnect_interval, Duration::from_secs(2));
    assert_eq!(options.command_timeout_grace, Duration::from_secs(120));
    assert!(options.auto_reconnect);
    assert!(options.tcp_nodelay);
    assert!(options.tcp_keepalive);
}

#[test]
fn generated_ids_are_16_bytes_and_unique() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..1000 {
        let id = Id16::new();
        assert_eq!(id.as_bytes().len(), 16);
        assert!(seen.insert(*id.as_bytes()));
    }
}

#[test]
fn init_ping_lock_commands_encode_to_64_byte_headers() {
    let init = Command::Init(InitCommand::new(
        Id16::from_bytes([1; 16]),
        Id16::from_bytes([2; 16]),
    ));
    let ping = Command::Ping(PingCommand::new(Id16::from_bytes([3; 16])));
    let lock = Command::Lock(LockCommand::new(
        COMMAND_TYPE_LOCK,
        0,
        9,
        Id16::from_bytes([4; 16]),
        Key16::from_bytes([5; 16]),
        Id16::from_bytes([6; 16]),
        PackedTime::with_flags(0x1111, 0x2222),
        PackedTime::with_flags(0x3333, 0x4444),
        0x5555,
        0x66,
        None,
    ));

    assert_eq!(init.encode().unwrap().header.len(), 64);
    assert_eq!(ping.encode().unwrap().header.len(), 64);
    assert_eq!(lock.encode().unwrap().header.len(), 64);
}

#[test]
fn lock_command_fields_land_at_java_offsets() {
    let request_id = Id16::from_bytes([0x11; 16]);
    let lock_id = Id16::from_bytes([0x22; 16]);
    let lock_key = Key16::from_bytes([0x33; 16]);
    let command = Command::Lock(LockCommand::new(
        COMMAND_TYPE_LOCK,
        0x44,
        0x55,
        request_id,
        lock_key,
        lock_id,
        PackedTime::from_bits(0x11223344),
        PackedTime::from_bits(0x55667788),
        0x99aa,
        0xbb,
        None,
    ));
    let encoded = command.encode().unwrap();
    assert_eq!(encoded.header[0], MAGIC);
    assert_eq!(encoded.header[1], VERSION);
    assert_eq!(encoded.header[2], COMMAND_TYPE_LOCK);
    assert_eq!(&encoded.header[3..19], request_id.as_bytes());
    assert_eq!(encoded.header[19], 0x44);
    assert_eq!(encoded.header[20], 0x55);
    assert_eq!(&encoded.header[21..37], lock_id.as_bytes());
    assert_eq!(&encoded.header[37..53], lock_key.as_bytes());
    assert_eq!(&encoded.header[53..57], &0x11223344u32.to_le_bytes());
    assert_eq!(&encoded.header[57..61], &0x55667788u32.to_le_bytes());
    assert_eq!(&encoded.header[61..63], &0x99aau16.to_le_bytes());
    assert_eq!(encoded.header[63], 0xbb);
}

#[test]
fn invalid_magic_or_version_is_protocol_error() {
    let mut header = [0u8; 64];
    header[0] = 0;
    header[1] = VERSION;
    header[2] = COMMAND_TYPE_PING;
    let err = decode_response_header(&header).unwrap_err();
    assert!(matches!(err, SlockError::Protocol(_)));

    header[0] = MAGIC;
    header[1] = 0;
    let err = decode_response_header(&header).unwrap_err();
    assert!(matches!(err, SlockError::Protocol(_)));
}

#[test]
fn lock_data_encodes_java_frame_and_pipeline_lengths() {
    let set = LockData::set("aaa");
    assert_eq!(
        set.encode().unwrap(),
        vec![5, 0, 0, 0, LOCK_DATA_COMMAND_TYPE_SET, 0, b'a', b'a', b'a']
    );

    let incr = LockData::incr(-3);
    let incr_bytes = incr.encode().unwrap();
    assert_eq!(&incr_bytes[0..4], &10u32.to_le_bytes());
    assert_eq!(incr_bytes[4], LOCK_DATA_COMMAND_TYPE_INCR);
    assert_eq!(incr_bytes[5], LOCK_DATA_FLAG_VALUE_TYPE_NUMBER);
    assert_eq!(&incr_bytes[6..14], &(-3i64).to_le_bytes());

    let pipeline = LockData::pipeline(vec![LockData::set("a"), LockData::append("b")]);
    let encoded = pipeline.encode().unwrap();
    assert_eq!(&encoded[0..4], &16u32.to_le_bytes());
    assert_eq!(encoded[4], LOCK_DATA_COMMAND_TYPE_PIPELINE);
    assert_eq!(encoded.len(), 20);
}

#[test]
fn lock_data_encodes_all_java_command_variants() {
    assert_lock_data_frame(
        LockData::unset(),
        LOCK_DATA_STAGE_CURRENT,
        LOCK_DATA_COMMAND_TYPE_UNSET,
        0,
        &[],
    );
    assert_lock_data_frame(
        LockData::append("bbb"),
        LOCK_DATA_STAGE_CURRENT,
        LOCK_DATA_COMMAND_TYPE_APPEND,
        0,
        b"bbb",
    );
    assert_lock_data_frame(
        LockData::shift(7),
        LOCK_DATA_STAGE_CURRENT,
        LOCK_DATA_COMMAND_TYPE_SHIFT,
        LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
        &7u32.to_le_bytes(),
    );
    assert_lock_data_frame(
        LockData::push("ccc"),
        LOCK_DATA_STAGE_CURRENT,
        LOCK_DATA_COMMAND_TYPE_PUSH,
        0,
        b"ccc",
    );
    assert_lock_data_frame(
        LockData::pop(2),
        LOCK_DATA_STAGE_CURRENT,
        LOCK_DATA_COMMAND_TYPE_POP,
        LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
        &2u32.to_le_bytes(),
    );

    let nested = LockCommand::new(
        COMMAND_TYPE_LOCK,
        0,
        1,
        Id16::from_bytes([0x11; 16]),
        Key16::new("execute-key"),
        Id16::from_bytes([0x22; 16]),
        PackedTime::new(3),
        PackedTime::new(4),
        5,
        6,
        None,
    );
    let encoded = LockData::execute(&nested).unwrap().encode().unwrap();
    assert_eq!(&encoded[0..4], &66u32.to_le_bytes());
    assert_eq!(encoded[4], LOCK_DATA_COMMAND_TYPE_EXECUTE);
    assert_eq!(encoded[5], 0);
    assert_eq!(encoded.len(), 70);
    assert_eq!(encoded[6], MAGIC);
    assert_eq!(encoded[7], VERSION);
    assert_eq!(encoded[8], COMMAND_TYPE_LOCK);
}

#[test]
fn lock_result_data_skips_java_property_block_before_value() {
    let data = LockResultData::new(vec![
        0,
        0,
        0,
        0,
        LOCK_DATA_COMMAND_TYPE_SET,
        LOCK_DATA_FLAG_CONTAINS_PROPERTY,
        3,
        0,
        b'k',
        b'e',
        b'y',
        b'a',
        b'a',
        b'a',
    ]);

    assert_eq!(data.command_type(), Some(LOCK_DATA_COMMAND_TYPE_SET));
    assert_eq!(data.flags(), Some(LOCK_DATA_FLAG_CONTAINS_PROPERTY));
    assert_eq!(data.as_bytes(), Some(&b"aaa"[..]));
    assert_eq!(data.as_string().unwrap(), "aaa");
}

#[test]
fn lock_result_data_parses_scalar_list_and_map_values() {
    let string_data = LockResultData::new(vec![
        5,
        0,
        0,
        0,
        LOCK_DATA_COMMAND_TYPE_SET,
        0,
        b'a',
        b'b',
        b'c',
    ]);
    assert_eq!(string_data.as_bytes(), Some(&b"abc"[..]));
    assert_eq!(string_data.as_string().unwrap(), "abc");

    let long_data = LockResultData::new({
        let mut raw = vec![
            10,
            0,
            0,
            0,
            LOCK_DATA_COMMAND_TYPE_INCR,
            LOCK_DATA_FLAG_VALUE_TYPE_NUMBER,
        ];
        raw.extend_from_slice(&42i64.to_le_bytes());
        raw
    });
    assert_eq!(long_data.as_i64(), 42);

    let list_data = LockResultData::new({
        let mut raw = vec![
            0,
            0,
            0,
            0,
            LOCK_DATA_COMMAND_TYPE_PUSH,
            LOCK_DATA_FLAG_VALUE_TYPE_ARRAY,
        ];
        raw.extend_from_slice(&3u32.to_le_bytes());
        raw.extend_from_slice(b"one");
        raw.extend_from_slice(&3u32.to_le_bytes());
        raw.extend_from_slice(b"two");
        raw
    });
    assert_eq!(list_data.as_string_list().unwrap(), vec!["one", "two"]);

    let map_data = LockResultData::new({
        let mut raw = vec![
            0,
            0,
            0,
            0,
            LOCK_DATA_COMMAND_TYPE_SET,
            LOCK_DATA_FLAG_VALUE_TYPE_KV,
        ];
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(b"k");
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(b"v");
        raw
    });
    assert_eq!(map_data.as_string_map().unwrap().get("k").unwrap(), "v");
}

fn assert_lock_data_frame(
    data: LockData,
    stage: u8,
    command_type: u8,
    command_flag: u8,
    value: &[u8],
) {
    let encoded = data.encode().unwrap();
    assert_eq!(
        &encoded[0..4],
        &u32::try_from(value.len() + 2).unwrap().to_le_bytes()
    );
    assert_eq!(encoded[4] >> 6, stage);
    assert_eq!(encoded[4] & 0x3f, command_type);
    assert_eq!(encoded[5], command_flag);
    assert_eq!(&encoded[6..], value);
}
