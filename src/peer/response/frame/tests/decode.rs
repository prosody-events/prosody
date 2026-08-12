use super::decode_frame;
use super::{CountingCodec, header};
use crate::codec::Codec;
use crate::error::ErrorCategory;
use crate::peer::response::frame::decode::FrameDecodeError;
use crate::peer::response::frame::encode::{stage_error, stage_success};
use crate::peer::response::frame::{
    DELIVER_RESULT_HANDLER_ERROR_TAG, DELIVER_RESULT_REQUEST_ID_TAG, DELIVER_RESULT_SUBSYSTEM_TAG,
    DELIVER_RESULT_SUCCESS_TAG, DELIVER_RESULT_TARGET_PEER_TAG, HANDLER_ERROR_CATEGORY_TAG,
    RESPONSE_SUCCESS_FORMAT_TAG, RESPONSE_SUCCESS_PAYLOAD_TAG,
};
use crate::peer::response::{
    RequestId,
    frame::{FrameResult, HandlerError, ResponseSuccess},
};
use crate::peer::router::{Framed, PeerId};
use bytes::BytesMut;
use color_eyre::Result;
use color_eyre::eyre::bail;
use prost::Message;
use prost::encoding::{WireType, encode_key, encode_varint};
use prost_types::FileDescriptorSet;
use prost_types::field_descriptor_proto::Type;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

const DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/peer_descriptor.bin"));

#[quickcheck]
fn both_response_arms_round_trip(
    target: u128,
    request: u128,
    relay: Option<u128>,
    category: u8,
    mut payload: Vec<u8>,
    failure: bool,
) -> TestResult {
    payload.truncate(2048);
    let Ok(mut header) = header(
        "billing",
        relay.map(|id| PeerId::from_bytes(id.to_le_bytes())),
    ) else {
        return TestResult::error("the fixed subsystem must be valid");
    };
    header.target = PeerId::from_bytes(target.to_le_bytes());
    header.request = RequestId::from_bytes(request.to_le_bytes());
    let expected_category = match category % 3 {
        0 => ErrorCategory::Transient,
        1 => ErrorCategory::Permanent,
        _ => ErrorCategory::Terminal,
    };
    let staged = if failure {
        stage_error(
            &header,
            expected_category,
            String::from_utf8_lossy(&payload).into_owned(),
        )
    } else {
        match stage_success::<CountingCodec>(&header, &payload) {
            Ok(staged) => staged,
            Err(error) => return TestResult::error(error.to_string()),
        }
    };
    let mut wire = BytesMut::with_capacity(staged.bytes());
    staged.write(&mut wire);
    let decoded = match decode_frame(&mut wire) {
        Ok(frame) => frame,
        Err(error) => return TestResult::error(error.to_string()),
    };
    assert_eq!(decoded.header, header);
    match decoded.result {
        FrameResult::Success(ResponseSuccess {
            format,
            payload: decoded,
        }) => {
            assert!(!failure);
            assert_eq!(format.as_bytes(), CountingCodec::FORMAT_ID.as_bytes());
            assert_eq!(decoded, payload);
        }
        FrameResult::HandlerError(HandlerError { category, message }) => {
            assert!(failure);
            assert_eq!(category, expected_category);
            assert_eq!(message, String::from_utf8_lossy(&payload).as_bytes());
        }
    }
    TestResult::passed()
}

#[test]
fn a_frame_requires_exactly_one_result_arm() {
    let mut wire = raw_header();
    assert_eq!(
        decode_frame(&mut wire),
        Err(FrameDecodeError::MissingField("result"))
    );
}

#[test]
fn protobuf_merge_semantics_are_preserved() -> Result<()> {
    let header = header("billing", None)?;
    let staged = stage_success::<CountingCodec>(&header, &b"hi".to_vec())?;
    let mut valid = BytesMut::with_capacity(staged.bytes());
    staged.write(&mut valid);

    let mut repeated = valid.clone();
    let mut handler = BytesMut::new();
    raw_varint(
        HANDLER_ERROR_CATEGORY_TAG,
        i32::from(ErrorCategory::Permanent) as u64,
        &mut handler,
    );
    raw_bytes(DELIVER_RESULT_HANDLER_ERROR_TAG, &handler, &mut repeated);
    raw_bytes(DELIVER_RESULT_TARGET_PEER_TAG, &[0x33; 16], &mut repeated);
    let decoded = decode_frame(&mut repeated)?;
    assert_eq!(decoded.header.target, PeerId::from_bytes([0x33; 16]));
    assert!(matches!(
        decoded.result,
        FrameResult::HandlerError(HandlerError {
            category: ErrorCategory::Permanent,
            ..
        })
    ));

    let mut merged = raw_header();
    let mut format = BytesMut::new();
    raw_bytes(
        RESPONSE_SUCCESS_FORMAT_TAG,
        CountingCodec::FORMAT_ID.as_bytes(),
        &mut format,
    );
    raw_bytes(DELIVER_RESULT_SUCCESS_TAG, &format, &mut merged);
    let mut payload = BytesMut::new();
    raw_bytes(RESPONSE_SUCCESS_PAYLOAD_TAG, b"merged", &mut payload);
    raw_bytes(DELIVER_RESULT_SUCCESS_TAG, &payload, &mut merged);
    assert!(matches!(
        decode_frame(&mut merged)?.result,
        FrameResult::Success(ResponseSuccess { payload, .. }) if payload == b"merged"[..]
    ));

    Ok(())
}

#[test]
fn invalid_domain_fields_are_refused() {
    let mut invalid_text = raw_header();
    let mut success = BytesMut::new();
    raw_bytes(RESPONSE_SUCCESS_FORMAT_TAG, &[0xff], &mut success);
    raw_bytes(DELIVER_RESULT_SUCCESS_TAG, &success, &mut invalid_text);
    assert!(matches!(
        decode_frame(&mut invalid_text),
        Err(FrameDecodeError::InvalidText(_))
    ));

    let mut unknown_category = raw_header();
    let mut handler = BytesMut::new();
    raw_varint(HANDLER_ERROR_CATEGORY_TAG, 0, &mut handler);
    raw_bytes(
        DELIVER_RESULT_HANDLER_ERROR_TAG,
        &handler,
        &mut unknown_category,
    );
    assert!(matches!(
        decode_frame(&mut unknown_category),
        Err(FrameDecodeError::UnknownCategory(_))
    ));
}

fn raw_header() -> BytesMut {
    let mut wire = BytesMut::new();
    raw_bytes(DELIVER_RESULT_TARGET_PEER_TAG, &[0x11; 16], &mut wire);
    raw_bytes(DELIVER_RESULT_REQUEST_ID_TAG, &[0x22; 16], &mut wire);
    raw_bytes(DELIVER_RESULT_SUBSYSTEM_TAG, b"billing", &mut wire);
    wire
}

fn raw_varint(tag: u32, value: u64, wire: &mut BytesMut) {
    encode_key(tag, WireType::Varint, wire);
    encode_varint(value, wire);
}

fn raw_bytes(tag: u32, value: &[u8], wire: &mut BytesMut) {
    encode_key(tag, WireType::LengthDelimited, wire);
    encode_varint(value.len() as u64, wire);
    wire.extend_from_slice(value);
}

#[test]
fn the_schema_matches_the_frame_contract() -> Result<()> {
    let set = FileDescriptorSet::decode(DESCRIPTOR)?;
    let Some(file) = set
        .file
        .iter()
        .find(|file| file.name() == "prosody/peer/v1/peer.proto")
    else {
        bail!("the descriptor set must contain the peer schema");
    };
    let Some(message) = file
        .message_type
        .iter()
        .find(|message| message.name() == "DeliverResultRequest")
    else {
        bail!("the peer schema must define DeliverResultRequest");
    };
    let routing_fields = [
        ("target_peer", 1_i32),
        ("request_id", 2_i32),
        ("subsystem", 3_i32),
        ("relay_peer", 6_i32),
    ];
    for (name, number) in routing_fields {
        let Some(field) = message.field.iter().find(|field| field.name() == name) else {
            bail!("DeliverResultRequest must define {name}");
        };
        assert_eq!(field.number(), number);
        assert_eq!(field.r#type(), Type::Bytes);
    }
    let Some(oneof) = message.oneof_decl.first() else {
        bail!("DeliverResultRequest must define its result oneof");
    };
    assert_eq!(oneof.name(), "result");
    let arms = [
        ("success", 4_i32, Type::Message),
        ("handler_error", 5_i32, Type::Message),
    ];
    for (name, number, kind) in arms {
        let Some(field) = message.field.iter().find(|field| field.name() == name) else {
            bail!("the result must define {name}");
        };
        assert_eq!(field.number(), number);
        assert_eq!(field.r#type(), kind);
        assert_eq!(field.oneof_index(), 0_i32);
    }
    let expected_messages = [
        (
            "ResponseSuccess",
            [
                ("format", 1_i32, Type::Bytes),
                ("payload", 2_i32, Type::Bytes),
            ],
        ),
        (
            "HandlerError",
            [
                ("category", 1_i32, Type::Enum),
                ("message", 2_i32, Type::Bytes),
            ],
        ),
    ];
    for (name, fields) in expected_messages {
        let Some(message) = file
            .message_type
            .iter()
            .find(|message| message.name() == name)
        else {
            bail!("the peer schema must define {name}");
        };
        for (field_name, number, kind) in fields {
            let Some(field) = message
                .field
                .iter()
                .find(|field| field.name() == field_name)
            else {
                bail!("{name} must define {field_name}");
            };
            assert_eq!(field.number(), number);
            assert_eq!(field.r#type(), kind);
        }
    }
    Ok(())
}
