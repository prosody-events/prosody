use super::{CountingCodec, header};
use crate::codec::Codec;
use crate::error::ErrorCategory;
use crate::response::frame::decode::{FrameDecodeError, decode_frame};
use crate::response::frame::encode::{stage_error, stage_success};
use crate::response::frame::{
    FIELD_ERROR_CATEGORY, FIELD_HANDLER_ERROR, FIELD_REQUEST_ID, FIELD_SUBSYSTEM, FIELD_SUCCESS,
    FIELD_SUCCESS_FORMAT, FIELD_TARGET_PEER,
};
use crate::response::{
    RequestId,
    frame::{FrameResult, HandlerError, ResponseSuccess},
};
use crate::router::{Framed, PeerId};
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
fn malformed_result_fields_are_refused() -> Result<()> {
    let header = header("billing", None)?;
    let staged = stage_success::<CountingCodec>(&header, &b"hi".to_vec())?;
    let mut valid = BytesMut::with_capacity(staged.bytes());
    staged.write(&mut valid);

    let mut repeated = valid.clone();
    let mut handler = BytesMut::new();
    raw_varint(
        FIELD_ERROR_CATEGORY,
        i32::from(ErrorCategory::Permanent) as u64,
        &mut handler,
    );
    raw_bytes(FIELD_HANDLER_ERROR, &handler, &mut repeated);
    assert!(matches!(
        decode_frame(&mut repeated),
        Err(FrameDecodeError::RepeatedField(_))
    ));

    let mut truncated = valid;
    truncated.truncate(truncated.len() - 1);
    assert!(matches!(
        decode_frame(&mut truncated),
        Err(FrameDecodeError::Truncated { .. } | FrameDecodeError::Protobuf(_))
    ));

    let mut invalid_text = raw_header();
    let mut success = BytesMut::new();
    raw_bytes(FIELD_SUCCESS_FORMAT, &[0xff], &mut success);
    raw_bytes(FIELD_SUCCESS, &success, &mut invalid_text);
    assert!(matches!(
        decode_frame(&mut invalid_text),
        Err(FrameDecodeError::InvalidText(_))
    ));

    let mut unknown_category = raw_header();
    let mut handler = BytesMut::new();
    raw_varint(FIELD_ERROR_CATEGORY, 0, &mut handler);
    raw_bytes(FIELD_HANDLER_ERROR, &handler, &mut unknown_category);
    assert!(matches!(
        decode_frame(&mut unknown_category),
        Err(FrameDecodeError::UnknownCategory(_))
    ));
    Ok(())
}

fn raw_header() -> BytesMut {
    let mut wire = BytesMut::new();
    raw_bytes(FIELD_TARGET_PEER, &[0x11; 16], &mut wire);
    raw_bytes(FIELD_REQUEST_ID, &[0x22; 16], &mut wire);
    raw_bytes(FIELD_SUBSYSTEM, b"billing", &mut wire);
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
fn the_schema_models_results_as_oneof() -> Result<()> {
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
        .find(|message| message.name() == "DeliverResponseRequest")
    else {
        bail!("the peer schema must define DeliverResponseRequest");
    };
    let Some(oneof) = message.oneof_decl.first() else {
        bail!("DeliverResponseRequest must define its result oneof");
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
        ("ResponseSuccess", [("format", 1_i32), ("payload", 2_i32)]),
        ("HandlerError", [("category", 1_i32), ("message", 2_i32)]),
    ];
    for (name, fields) in expected_messages {
        let Some(message) = file
            .message_type
            .iter()
            .find(|message| message.name() == name)
        else {
            bail!("the peer schema must define {name}");
        };
        for (field_name, number) in fields {
            let Some(field) = message
                .field
                .iter()
                .find(|field| field.name() == field_name)
            else {
                bail!("{name} must define {field_name}");
            };
            assert_eq!(field.number(), number);
        }
    }
    Ok(())
}
