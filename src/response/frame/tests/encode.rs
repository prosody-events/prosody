use super::{CountingCodec, cache_uses, header, serialize_capacity, success};
use crate::codec::SerializeBufGuard;
use crate::error::ErrorCategory;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::encode::{Forwarded, stage_error, stage_success};
use crate::response::frame::{FrameResult, HandlerError};
use crate::router::{Framed, NodeId};
use bytes::BytesMut;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

const RELAY: NodeId = NodeId::from_bytes([0x77; 16]);

#[test]
fn success_encoding_uses_standard_codec_resources() -> Result<()> {
    const SEEDED: usize = 4096;
    let mut shared = SerializeBufGuard::acquire();
    shared.reserve(SEEDED);
    drop(shared);
    let before = cache_uses();
    drop(stage_success::<CountingCodec>(
        &header("billing", None)?,
        &b"hi".to_vec(),
    )?);
    assert_eq!(cache_uses(), before + 1);
    assert!(serialize_capacity() >= SEEDED);
    Ok(())
}

#[test]
fn grpc_owns_response_size_policy() -> Result<()> {
    let staged = stage_success::<CountingCodec>(&header("billing", None)?, &vec![0_u8; 64 * 1024])?;
    assert!(staged.bytes() > 64 * 1024);
    Ok(())
}

#[test]
fn a_forwarded_frame_replaces_the_previous_relay() -> Result<()> {
    let earlier = NodeId::from_bytes([0x33; 16]);
    let staged =
        stage_success::<CountingCodec>(&header("billing", Some(earlier))?, &b"hi".to_vec())?;
    let mut wire = BytesMut::with_capacity(staged.bytes());
    staged.write(&mut wire);
    let forwarded = Forwarded::new(decode_frame(&mut wire)?, RELAY);
    let mut sent = BytesMut::with_capacity(forwarded.bytes());
    forwarded.write(&mut sent);
    assert_eq!(decode_frame(&mut sent)?.header.relay, Some(RELAY));
    Ok(())
}

#[test]
fn both_result_arms_have_stable_wire_bytes() -> Result<()> {
    let header = header("billing", None)?;
    let success_frame = stage_success::<CountingCodec>(&header, &b"hi".to_vec())?;
    let error_frame = stage_error(&header, ErrorCategory::Permanent, "rejected".to_owned());
    let mut success_wire = BytesMut::with_capacity(success_frame.bytes());
    let mut error_wire = BytesMut::with_capacity(error_frame.bytes());
    success_frame.write(&mut success_wire);
    error_frame.write(&mut error_wire);
    let expected_success = [
        0x0a, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x12, 0x10, 16, 17, 18,
        19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 0x1a, 0x07, b'b', b'i', b'l', b'l',
        b'i', b'n', b'g', 0x22, 0x10, 0x0a, 0x0a, b't', b'e', b's', b't', b'-', b'b', b'y', b't',
        b'e', b's', 0x12, 0x02, b'h', b'i',
    ];
    let expected_error = [
        0x0a, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x12, 0x10, 16, 17, 18,
        19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 0x1a, 0x07, b'b', b'i', b'l', b'l',
        b'i', b'n', b'g', 0x2a, 0x0c, 0x08, 0x02, 0x12, 0x08, b'r', b'e', b'j', b'e', b'c', b't',
        b'e', b'd',
    ];
    assert_eq!(success_wire.as_ref(), expected_success);
    assert_eq!(error_wire.as_ref(), expected_error);
    assert_eq!(
        success(&decode_frame(&mut success_wire)?.result),
        Some((b"test-bytes".as_slice(), b"hi".as_slice()))
    );
    assert_eq!(
        decode_frame(&mut error_wire)?.result,
        FrameResult::HandlerError(HandlerError {
            category: ErrorCategory::Permanent,
            message: "rejected".into(),
        })
    );
    Ok(())
}

#[quickcheck]
fn every_staged_frame_fits_its_forwarded_form(mut payload: Vec<u8>, failure: bool) -> TestResult {
    payload.truncate(u16::MAX as usize);
    let Ok(header) = header("billing", None) else {
        return TestResult::error("the fixed subsystem must be valid");
    };
    let staged = if failure {
        stage_error(
            &header,
            ErrorCategory::Transient,
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
    let frame = match decode_frame(&mut wire) {
        Ok(frame) => frame,
        Err(error) => return TestResult::error(error.to_string()),
    };
    let forwarded = Forwarded::new(frame, RELAY);
    let mut sent = BytesMut::with_capacity(forwarded.bytes());
    forwarded.write(&mut sent);
    assert_eq!(sent.len(), forwarded.bytes());
    TestResult::passed()
}
