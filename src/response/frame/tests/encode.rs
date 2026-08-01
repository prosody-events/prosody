use super::{CountingCodec, expected_frame_len, header};
use crate::error::ErrorCategory;
use crate::response::frame::FrameCap;
use crate::response::frame::encode::{EncodeError, FrameEncoder};
use bytes::BytesMut;
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

/// The exact bytes one deterministic response frames to.
///
/// A round-trip cannot catch a wire break, because the encoder and the decoder
/// move together. Field *order* is this encoder's choice rather than a protobuf
/// requirement, but the bytes a peer of another release reads are not.
const FROZEN: [u8; 65] = [
    0x08, 0x01, // protocol_version = 1
    0x12, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // target_node
    0x1a, 0x10, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // request_id
    0x22, 0x07, b'b', b'i', b'l', b'l', b'i', b'n', b'g', // subsystem
    0x2a, 0x0a, b't', b'e', b's', b't', b'-', b'b', b'y', b't', b'e', b's', // format
    0x30, 0x02, // category = Permanent
    0x3a, 0x02, b'h', b'i', // payload
];

/// The steady state allocates nothing: over a run of encodes up to the
/// configured maximum, neither the encoder's scratch nor a destination buffer
/// sized at the cap ever grows, the payload is serialized exactly once per
/// response, and the frame is exactly as long as the staging said it would be.
#[quickcheck]
fn steady_state_encodes_never_reallocate(lengths: Vec<u16>) -> TestResult {
    let Ok(cap) = FrameCap::new(64 * 1024) else {
        return TestResult::error("64 KiB is a supported cap");
    };
    let codec = CountingCodec::default();
    let mut encoder = FrameEncoder::new(codec.clone(), cap);
    let mut dst = BytesMut::with_capacity(cap.bytes());
    let scratch_capacity = encoder.scratch_capacity();
    let dst_capacity = dst.capacity();
    let subsystem = "billing";
    let header = header(subsystem, ErrorCategory::Transient, None);

    // The boundary lengths lead every run, so the largest frame the cap admits
    // — and the first one it refuses — are exercised whatever quickcheck
    // generated. The largest is found through the model rather than restated as
    // a literal, so a change to the header cannot silently move it.
    let Some(largest) = (0..=cap.bytes())
        .rev()
        .find(|&length| expected_frame_len(subsystem, length, false) <= cap.bytes())
    else {
        return TestResult::error("the cap admits some payload");
    };
    let boundaries = [cap.bytes(), largest + 1, largest, largest - 1];
    let generated = lengths.into_iter().map(usize::from);
    for length in boundaries.into_iter().chain(generated) {
        let payload: Vec<u8> = (0..length).map(|index| index as u8).collect();
        let serializes = codec.serializes();
        dst.clear();

        match encoder.stage(&header, payload.clone()) {
            Err(EncodeError::TooLarge { bytes, limit }) => {
                assert!(
                    bytes > limit as u64,
                    "a refusal must name a length over the cap, got {bytes} against {limit}"
                );
            }
            Err(other) => return TestResult::error(format!("unexpected refusal: {other}")),
            Ok(staged) => {
                staged.write(&mut dst);
                assert_eq!(
                    dst.len(),
                    staged.bytes(),
                    "the staged length must be exactly what the frame writes"
                );
                assert_eq!(
                    dst.len(),
                    expected_frame_len(subsystem, length, false),
                    "the frame must cost exactly the fields it carries"
                );
                assert!(
                    dst.len() <= cap.bytes(),
                    "an accepted frame must fit the cap: {} over {}",
                    dst.len(),
                    cap.bytes()
                );
                assert_eq!(
                    &dst[dst.len() - length..],
                    &payload[..],
                    "the payload must be framed verbatim"
                );
            }
        }

        assert_eq!(
            codec.serializes(),
            serializes + 1,
            "each response must be serialized exactly once"
        );
        assert_eq!(
            encoder.scratch_capacity(),
            scratch_capacity,
            "the scratch must never reallocate"
        );
        assert_eq!(
            dst.capacity(),
            dst_capacity,
            "a destination sized at the cap must never reallocate"
        );
    }
    TestResult::passed()
}

/// The cap covers the complete frame, and it is checked before anything is
/// framed. The two lengths are the exact boundary for this header: a 960-byte
/// payload frames to precisely 1024 bytes, and one more byte costs two.
#[test]
fn an_over_cap_response_is_refused_before_it_is_framed() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let header = header("billing", ErrorCategory::Permanent, None);

    let staged = encoder.stage(&header, vec![0u8; 960])?;
    assert_eq!(
        staged.bytes(),
        1024,
        "the largest accepted response must frame to exactly the cap"
    );

    let Err(EncodeError::TooLarge { bytes, limit }) = encoder.stage(&header, vec![0u8; 961]) else {
        bail!("a response one byte past the boundary must be refused");
    };
    assert_eq!(
        (bytes, limit),
        (1025, 1024),
        "the refusal must name the complete frame's length"
    );
    Ok(())
}

/// A refused response does not leave the encoder holding an allocation sized
/// for something the cap will refuse again: the next response finds the scratch
/// back at exactly the cap.
#[test]
fn a_refused_response_returns_the_scratch_to_the_cap() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let header = header("billing", ErrorCategory::Transient, None);

    let Err(EncodeError::TooLarge { .. }) = encoder.stage(&header, vec![0u8; cap.bytes() * 4])
    else {
        bail!("a response four times the cap must be refused");
    };
    let staged = encoder.stage(&header, vec![0u8; 8])?;
    assert!(staged.bytes() < cap.bytes(), "a small response still fits");
    assert_eq!(
        encoder.scratch_capacity(),
        cap.bytes(),
        "the scratch must be back at the cap"
    );
    Ok(())
}

/// A subsystem name no decoder would accept is refused rather than framed into
/// a message the far end must throw away.
#[test]
fn an_over_long_subsystem_is_refused() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let header = header(&"x".repeat(65), ErrorCategory::Transient, None);

    let Err(EncodeError::SubsystemTooLong { bytes, limit }) = encoder.stage(&header, Vec::new())
    else {
        bail!("a 65-byte subsystem name must be refused");
    };
    assert_eq!(
        (bytes, limit),
        (65, 64),
        "the refusal must name the length and the limit"
    );
    Ok(())
}

#[test]
fn one_response_frames_to_known_bytes() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let header = header("billing", ErrorCategory::Permanent, None);
    let mut dst = BytesMut::with_capacity(cap.bytes());

    encoder.stage(&header, b"hi".to_vec())?.write(&mut dst);
    assert_eq!(&dst[..], &FROZEN[..], "the frame's bytes are frozen");
    Ok(())
}
