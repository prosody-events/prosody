use super::{CountingCodec, RAW_ID, RawFrame, UNKNOWN_TAG, raw_bytes_field, raw_varint_field};
use crate::codec::Codec;
use crate::error::{ErrorCategory, UnknownErrorCategory};
use crate::response::frame::decode::{FrameDecodeError, decode_frame};
use crate::response::frame::encode::FrameEncoder;
use crate::response::frame::{
    FIELD_FORMAT, FIELD_PAYLOAD, FIELD_PROTOCOL_VERSION, FIELD_RELAY_NODE, FIELD_REQUEST_ID,
    FIELD_STATUS, FIELD_SUBSYSTEM, FIELD_TARGET_NODE, FrameCap, FrameHeader, PayloadError,
};
use crate::response::{FormatToken, RequestId, ResponseStatus};
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

/// A payload that pushes a frame past the smallest supported cap.
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
    let Ok(cap) = FrameCap::new(4096) else {
        return TestResult::error("4 KiB is a supported cap");
    };
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
    payload.truncate(cap.bytes() / 2);

    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let mut wire = BytesMut::with_capacity(cap.bytes());
    match encoder.stage(&header, payload.clone()) {
        Ok(staged) => staged.write(&mut wire),
        Err(error) => return TestResult::error(format!("staging failed: {error}")),
    }
    let decoded = match decode_frame(&mut wire, cap) {
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
    TestResult::passed()
}

fn node(value: u128) -> NodeId {
    NodeId::from_bytes(value.to_le_bytes())
}

/// The fixture itself must decode, or every rejection below could pass for the
/// wrong reason.
#[test]
fn the_raw_fixture_is_a_well_formed_frame() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let decoded = decode_frame(&mut RawFrame::default().encode(), cap)?;
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
    let cap = FrameCap::new(1024)?;
    let raw = RawFrame {
        payload: None,
        relay: None,
        ..RawFrame::default()
    };
    let decoded = decode_frame(&mut raw.encode(), cap)?;
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

/// A field this release does not know is skipped, so a later protocol version
/// can add one.
#[test]
fn an_unknown_field_is_skipped() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let raw = RawFrame {
        unknown: Some(7),
        ..RawFrame::default()
    };
    let decoded = decode_frame(&mut raw.encode(), cap)?;
    assert_eq!(&decoded.payload[..], b"hi", "the known fields still decode");
    Ok(())
}

#[test]
fn a_malformed_frame_is_refused_by_the_field_that_broke_it() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let cases = [
        (
            RawFrame {
                version: Some(2),
                ..RawFrame::default()
            },
            FrameDecodeError::UnsupportedVersion(2),
        ),
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
        match decode_frame(&mut raw.encode(), cap) {
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
    let cap = FrameCap::new(1024)?;
    let cases = [
        (FIELD_PROTOCOL_VERSION, "protocol_version"),
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
        if tag == FIELD_PROTOCOL_VERSION || tag == FIELD_STATUS {
            raw_varint_field(tag, 1, &mut wire);
        } else {
            raw_bytes_field(tag, b"", &mut wire);
        }
        match decode_frame(&mut wire, cap) {
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
fn a_frame_cut_short_is_refused_by_the_field_that_ran_out() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let wire = RawFrame::default().encode();
    let mut cut = Vec::new();
    for length in 0..wire.len() {
        match decode_frame(&mut &wire[..length], cap) {
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
    Ok(())
}

/// The whole encoded frame is bounded before any per-field work, so nothing is
/// allocated on behalf of a length a peer merely claimed.
#[test]
fn an_over_cap_frame_is_refused_whole() -> Result<()> {
    let raw = RawFrame {
        payload: Some(&BIG_PAYLOAD),
        ..RawFrame::default()
    };
    let mut wire = raw.encode();
    let length = wire.len();

    let cap = FrameCap::new(FrameCap::MIN_BYTES)?;
    let Err(FrameDecodeError::FrameTooLarge { bytes, limit }) = decode_frame(&mut wire, cap) else {
        bail!("a frame over the cap must be refused before it is read");
    };
    assert_eq!(
        (bytes, limit),
        (length, cap.bytes()),
        "the refusal must name the whole frame"
    );
    Ok(())
}

/// A format token the reading codec does not speak stops the frame before a
/// payload byte is parsed.
#[test]
fn a_mismatched_format_is_refused_without_decoding() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let mut frame = decode_frame(&mut RawFrame::default().encode(), cap)?;
    let codec = CountingCodec::default();
    let mut reader = codec.clone();

    frame.format = FormatToken::make("other-format");
    assert!(
        matches!(
            frame.decode_with(&mut reader),
            Err(PayloadError::FormatMismatch { .. })
        ),
        "a frame in another format must be refused"
    );
    assert_eq!(
        codec.deserializes(),
        0,
        "a mismatched frame must never reach the codec"
    );

    frame.format = FormatToken::make(CountingCodec::FORMAT_ID);
    assert_eq!(
        frame.decode_with(&mut reader)?,
        b"hi".to_vec(),
        "the codec's own format decodes"
    );
    assert_eq!(
        codec.deserializes(),
        1,
        "a matching frame reaches the codec exactly once"
    );
    Ok(())
}

/// The hand-written codec and `proto/peer.proto` are one contract. The
/// descriptor set is regenerated from the `.proto`, so a field number changed
/// on only one side shows up here.
#[test]
fn the_frame_fields_match_the_proto() -> Result<()> {
    let set = FileDescriptorSet::decode(DESCRIPTOR)?;
    let Some(file) = set.file.iter().find(|file| file.name() == "peer.proto") else {
        bail!("the descriptor set must contain peer.proto");
    };
    let Some(message) = file
        .message_type
        .iter()
        .find(|message| message.name() == "ResponseFrame")
    else {
        bail!("peer.proto must define ResponseFrame");
    };

    let expected = [
        ("protocol_version", FIELD_PROTOCOL_VERSION, Type::Uint32),
        ("target_node", FIELD_TARGET_NODE, Type::Bytes),
        ("request_id", FIELD_REQUEST_ID, Type::Bytes),
        ("subsystem", FIELD_SUBSYSTEM, Type::String),
        ("format", FIELD_FORMAT, Type::String),
        ("status", FIELD_STATUS, Type::Int32),
        ("payload", FIELD_PAYLOAD, Type::Bytes),
        ("relay_node", FIELD_RELAY_NODE, Type::Bytes),
    ];
    assert_eq!(
        message.field.len(),
        expected.len(),
        "every field of ResponseFrame must be accounted for here"
    );
    assert!(
        !expected.iter().any(|(_, tag, _)| *tag == UNKNOWN_TAG),
        "the skipped-field test's tag must stay unused by the protocol"
    );
    for (name, tag, kind) in expected {
        let Some(field) = message.field.iter().find(|field| field.name() == name) else {
            bail!("ResponseFrame must define {name}");
        };
        assert_eq!(
            field.number(),
            tag as i32,
            "{name} must keep the number the encoder writes"
        );
        assert_eq!(field.r#type(), kind, "{name} must keep its wire type");
    }

    let Some(service) = file.service.first() else {
        bail!("peer.proto must define a service");
    };
    assert_eq!(service.name(), "Peer", "the service is named for the peers");
    let Some(method) = service.method.first() else {
        bail!("Peer must define a method");
    };
    assert_eq!(
        method.name(),
        "DeliverResponse",
        "the method names what it delivers"
    );
    assert_eq!(
        method.input_type(),
        ".prosody.peer.v1.ResponseFrame",
        "the method takes a response frame"
    );
    assert_eq!(
        method.output_type(),
        ".google.protobuf.Empty",
        "the method has no response body"
    );
    Ok(())
}
