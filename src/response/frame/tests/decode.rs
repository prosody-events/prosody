use super::{CountingCodec, RAW_ID, RawFrame, raw_bytes_field, raw_varint_field};
use crate::codec::Codec;
use crate::error::{ErrorCategory, UnknownErrorCategory};
use crate::response::frame::decode::{FrameDecodeError, decode_frame};
use crate::response::frame::encode::stage;
use crate::response::frame::{
    FIELD_FORMAT, FIELD_PAYLOAD, FIELD_RELAY_NODE, FIELD_REQUEST_ID, FIELD_STATUS, FIELD_SUBSYSTEM,
    FIELD_TARGET_NODE, FrameHeader,
};
use crate::response::{RequestId, ResponseStatus};
use crate::router::{Framed, NodeId};
use crate::subsystem::{SubsystemName, SubsystemNameError};
use bytes::BytesMut;
use color_eyre::Result;
use color_eyre::eyre::bail;
use prost::Message;
use prost_types::FileDescriptorSet;
use prost_types::field_descriptor_proto::Type;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

/// The descriptor set `build.rs` writes beside the generated code.
const DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/peer_descriptor.bin"));

/// Subsystem names are drawn by index from a fixed vocabulary, so a generated
/// name is always legal and the bound is always among the values tried.
const SUBSYSTEMS: [&str; 6] = [
    "a",
    "billing",
    "orders.v2",
    "a-fairly-long-subsystem-name-that-still-fits",
    "0123456789012345678901234567890123456789012345678901234567890123",
    "ünïcodé",
];

/// A 16-byte identifier is the only length a frame may carry.
const SHORT_ID: [u8; 15] = [0x22; 15];

/// One byte past the longest subsystem name a frame may carry.
const LONG_SUBSYSTEM: [u8; 65] = [b'x'; 65];

/// A payload large enough to catch accidental tiny transport assumptions.
const BIG_PAYLOAD: [u8; 512] = [0x5a; 512];

/// Whatever a responder frames, the far end reads back unchanged.
#[quickcheck]
fn a_framed_response_round_trips(
    target: u128,
    request: u128,
    subsystem: usize,
    status: u8,
    relay: Option<u128>,
    mut payload: Vec<u8>,
) -> TestResult {
    let Ok(subsystem) = SubsystemName::try_new(SUBSYSTEMS[subsystem % SUBSYSTEMS.len()]) else {
        return TestResult::error("every name in the vocabulary is legal");
    };
    let header = FrameHeader {
        target: node(target),
        request: RequestId::from_bytes(request.to_le_bytes()),
        subsystem,
        status: match status % 4 {
            0 => ResponseStatus::Error(ErrorCategory::Transient),
            1 => ResponseStatus::Error(ErrorCategory::Permanent),
            2 => ResponseStatus::Error(ErrorCategory::Terminal),
            _ => ResponseStatus::Success,
        },
        relay: relay.map(node),
    };
    payload.truncate(2048);

    let mut wire = BytesMut::new();
    match stage::<CountingCodec>(&header, &payload) {
        Ok(staged) => staged.write(&mut wire),
        Err(error) => return TestResult::error(format!("staging failed: {error}")),
    }
    let allocation = wire.as_ptr_range();
    let decoded = match decode_frame(&mut wire) {
        Ok(decoded) => decoded,
        Err(error) => return TestResult::error(format!("decoding failed: {error}")),
    };

    assert_eq!(
        decoded.header, header,
        "the header must survive the round trip"
    );
    assert_eq!(
        decoded.format.to_str(),
        CountingCodec::FORMAT_ID,
        "the frame names the codec that wrote it"
    );
    assert_eq!(
        &decoded.payload[..],
        &payload[..],
        "the payload must survive the round trip"
    );
    if !decoded.payload.is_empty() {
        assert!(
            allocation.contains(&decoded.payload.as_ptr()),
            "the payload must share the transport allocation"
        );
    }
    TestResult::passed()
}

fn node(value: u128) -> NodeId {
    NodeId::from_bytes(value.to_le_bytes())
}

/// The fixture itself must decode, or every rejection below could pass for the
/// wrong reason.
#[test]
fn the_raw_fixture_is_a_well_formed_frame() -> Result<()> {
    let decoded = decode_frame(&mut RawFrame::default().encode())?;
    assert_eq!(
        decoded.header.subsystem,
        SubsystemName::try_new("billing")?,
        "the fixture carries its subsystem"
    );
    assert_eq!(
        decoded.header.status,
        ResponseStatus::Error(ErrorCategory::Permanent),
        "the fixture carries its status"
    );
    assert_eq!(
        &decoded.payload[..],
        b"hi",
        "the fixture carries its payload"
    );
    Ok(())
}

/// A frame with nothing to say and nowhere it has been is still a legal frame:
/// a codec may serialize to zero bytes, and a frame no relay touched carries no
/// relay node, so a peer that omits protobuf defaults must be understood.
#[test]
fn omitted_payload_and_relay_decode_as_absent() -> Result<()> {
    let raw = RawFrame {
        payload: None,
        relay: None,
        ..RawFrame::default()
    };
    let decoded = decode_frame(&mut raw.encode())?;
    assert!(
        decoded.payload.is_empty(),
        "an omitted payload decodes as empty"
    );
    assert_eq!(
        decoded.header.relay, None,
        "an omitted relay decodes as absent"
    );
    Ok(())
}

#[test]
fn a_malformed_frame_is_refused_by_the_field_that_broke_it() -> Result<()> {
    let cases = [
        (
            RawFrame {
                target: Some(&SHORT_ID),
                ..RawFrame::default()
            },
            FrameDecodeError::MalformedId {
                field: "target_node",
                bytes: 15,
            },
        ),
        (
            RawFrame {
                subsystem: Some(&LONG_SUBSYSTEM),
                ..RawFrame::default()
            },
            FrameDecodeError::StringTooLong {
                field: "subsystem",
                bytes: 65,
                limit: 64,
            },
        ),
        (
            RawFrame {
                status: Some(0),
                ..RawFrame::default()
            },
            FrameDecodeError::Status(UnknownErrorCategory(0)),
        ),
        // A varint wider than the field's `int32`. Narrowing it would fold this
        // one onto `Success`, which is the status that decides what a requester
        // believes happened to its request.
        (
            RawFrame {
                status: Some((1_u64 << 32_u32) | 4),
                ..RawFrame::default()
            },
            FrameDecodeError::StatusTooWide(4_294_967_300),
        ),
        (
            RawFrame {
                subsystem: None,
                ..RawFrame::default()
            },
            FrameDecodeError::MissingField("subsystem"),
        ),
        // An explicitly empty string is protobuf's own spelling of an omitted
        // one, so a decoder that refuses the omission must refuse this too.
        (
            RawFrame {
                subsystem: Some(b""),
                ..RawFrame::default()
            },
            FrameDecodeError::MissingField("subsystem"),
        ),
        // Not empty on the wire, but blank once trimmed. The length bound
        // cannot catch this one, so the name's own constructor does.
        (
            RawFrame {
                subsystem: Some(b"   "),
                ..RawFrame::default()
            },
            FrameDecodeError::Subsystem(SubsystemNameError::Blank),
        ),
        (
            RawFrame {
                format: Some(b""),
                ..RawFrame::default()
            },
            FrameDecodeError::MissingField("format"),
        ),
    ];
    for (raw, expected) in cases {
        match decode_frame(&mut raw.encode()) {
            Err(actual) => assert_eq!(actual, expected, "the frame was refused for another reason"),
            Ok(_) => bail!("the frame must be refused as {expected}"),
        }
    }
    Ok(())
}

/// Protobuf permits a repeated singular field and takes the last; this decoder
/// refuses it instead — for `payload` a repeat would otherwise buy an
/// allocate-and-discard per occurrence, and for the rest a contradiction the
/// frame cannot resolve. An empty occurrence counts: it is protobuf's spelling
/// of the field, not its absence.
#[test]
fn a_repeated_field_is_refused() -> Result<()> {
    let cases = [
        (FIELD_TARGET_NODE, "target_node"),
        (FIELD_REQUEST_ID, "request_id"),
        (FIELD_SUBSYSTEM, "subsystem"),
        (FIELD_FORMAT, "format"),
        (FIELD_STATUS, "status"),
        (FIELD_PAYLOAD, "payload"),
        (FIELD_RELAY_NODE, "relay_node"),
    ];
    for (tag, field) in cases {
        let mut wire = RawFrame {
            relay: Some(&RAW_ID),
            ..RawFrame::default()
        }
        .encode();
        // Empty for every field, so the repeat is refused for being a second
        // occurrence rather than for anything the occurrence carries.
        if tag == FIELD_STATUS {
            raw_varint_field(tag, 1, &mut wire);
        } else {
            raw_bytes_field(tag, b"", &mut wire);
        }
        match decode_frame(&mut wire) {
            Err(actual) => assert_eq!(
                actual,
                FrameDecodeError::RepeatedField(field),
                "a repeated {field} was refused for another reason"
            ),
            Ok(_) => bail!("a frame repeating {field} must be refused"),
        }
    }
    Ok(())
}

/// A frame cut short is refused rather than read past its end: every
/// length-delimited field reports the cut itself rather than trusting the
/// length it claimed, and no cut smuggles bytes into a field the frame does not
/// carry.
#[test]
fn a_frame_cut_short_is_refused_by_the_field_that_ran_out() {
    let wire = RawFrame::default().encode();
    let mut cut = Vec::new();
    for length in 0..wire.len() {
        match decode_frame(&mut &wire[..length]) {
            Err(FrameDecodeError::Truncated { field, .. }) => cut.push(field),
            Err(_) => {}
            // The payload is the fixture's last field and the one field whose
            // default is legal, so a cut landing exactly on its key still
            // decodes — as a frame that carries no payload at all.
            Ok(frame) => assert!(
                frame.payload.is_empty(),
                "a frame cut at {length} bytes decoded a payload it does not carry"
            ),
        }
    }
    for field in [
        "target_node",
        "request_id",
        "subsystem",
        "format",
        "payload",
    ] {
        assert!(
            cut.contains(&field),
            "a cut inside {field} must be reported there"
        );
    }
}

/// The frame codec leaves message-size policy to gRPC.
#[test]
fn a_large_payload_decodes() -> Result<()> {
    let raw = RawFrame {
        payload: Some(&BIG_PAYLOAD),
        ..RawFrame::default()
    };
    let mut wire = raw.encode();
    assert_eq!(decode_frame(&mut wire)?.payload, &BIG_PAYLOAD[..]);
    Ok(())
}

/// The hand-written codec and the peer Protobuf schema are one contract. The
/// descriptor set is regenerated from the `.proto`, so a field number changed
/// on only one side shows up here.
#[test]
fn the_frame_fields_match_the_proto() -> Result<()> {
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

    let expected = [
        ("target_node", FIELD_TARGET_NODE, Type::Bytes),
        ("request_id", FIELD_REQUEST_ID, Type::Bytes),
        ("subsystem", FIELD_SUBSYSTEM, Type::String),
        ("format", FIELD_FORMAT, Type::String),
        ("status", FIELD_STATUS, Type::Enum),
        ("payload", FIELD_PAYLOAD, Type::Bytes),
        ("relay_node", FIELD_RELAY_NODE, Type::Bytes),
    ];
    assert_eq!(
        message.field.len(),
        expected.len(),
        "every request field must be accounted for here"
    );
    for (name, tag, kind) in expected {
        let Some(field) = message.field.iter().find(|field| field.name() == name) else {
            bail!("DeliverResponseRequest must define {name}");
        };
        assert_eq!(
            field.number(),
            tag as i32,
            "{name} must keep the number the encoder writes"
        );
        assert_eq!(field.r#type(), kind, "{name} must keep its wire type");
    }

    let Some(status) = file
        .enum_type
        .iter()
        .find(|item| item.name() == "ResponseStatus")
    else {
        bail!("the peer schema must define ResponseStatus");
    };
    let expected = [
        ("RESPONSE_STATUS_UNSPECIFIED", 0_i32),
        ("RESPONSE_STATUS_TRANSIENT_ERROR", 1_i32),
        ("RESPONSE_STATUS_PERMANENT_ERROR", 2_i32),
        ("RESPONSE_STATUS_TERMINAL_ERROR", 3_i32),
        ("RESPONSE_STATUS_SUCCESS", 4_i32),
    ];
    assert_eq!(status.value.len(), expected.len());
    for (value, (name, number)) in status.value.iter().zip(expected) {
        assert_eq!(value.name(), name);
        assert_eq!(value.number(), number);
    }

    let Some(service) = file.service.first() else {
        bail!("the peer schema must define a service");
    };
    assert_eq!(service.name(), "PeerService");
    let Some(method) = service.method.first() else {
        bail!("PeerService must define a method");
    };
    assert_eq!(
        method.name(),
        "DeliverResponse",
        "the method names what it delivers"
    );
    assert_eq!(
        method.input_type(),
        ".prosody.peer.v1.DeliverResponseRequest",
        "the method takes a delivery request"
    );
    assert_eq!(
        method.output_type(),
        ".prosody.peer.v1.DeliverResponseResponse",
        "the method has no response body"
    );
    Ok(())
}
