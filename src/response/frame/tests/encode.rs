use super::{CountingCodec, RAW_ID, RELAY_FIELD_BYTES, expected_frame_len, header};
use crate::error::ErrorCategory;
use crate::response::frame::FrameCap;
use crate::response::frame::encode::{EncodeError, FrameEncoder};
use crate::router::{Framed, NodeId};
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

/// The encoder never reserves: over a run of encodes up to the configured
/// maximum, a destination buffer sized at the cap never grows, the payload is
/// serialized exactly once per response, and the frame is exactly as long as
/// the staging said it would be. After every accepted response the encoder is
/// left holding a scratch the size of the one it started with — even after a
/// response big enough to have grown it — or, for a codec that moves its own
/// buffer into the empty scratch, exactly that buffer, at whatever capacity it
/// came with.
///
/// Both codec shapes run and every payload is handed over with slack past the
/// cap, because the copy shape alone — or a buffer whose capacity is only its
/// length — leaves half of [`Codec::serialize`]'s contract untested.
#[quickcheck]
fn steady_state_encodes_never_reallocate(lengths: Vec<u16>, relay: bool) -> TestResult {
    let lengths: Vec<usize> = lengths.into_iter().map(usize::from).collect();
    for codec in [CountingCodec::default(), CountingCodec::moving()] {
        let outcome = encodes_without_reserving(&codec, &lengths, relay);
        if outcome.is_failure() || outcome.is_error() {
            return outcome;
        }
    }
    TestResult::passed()
}

fn encodes_without_reserving(codec: &CountingCodec, lengths: &[usize], relay: bool) -> TestResult {
    let Ok(cap) = FrameCap::new(64 * 1024) else {
        return TestResult::error("64 KiB is a supported cap");
    };
    let mut encoder = FrameEncoder::new(codec.clone(), cap);
    let mut dst = BytesMut::with_capacity(cap.bytes());
    let scratch_capacity = encoder.scratch_capacity();
    let dst_capacity = dst.capacity();
    let subsystem = "billing";
    let Ok(header) = header(
        subsystem,
        ErrorCategory::Transient,
        relay.then(|| NodeId::from_bytes(RAW_ID)),
    ) else {
        return TestResult::error("the fixture subsystem is a legal name");
    };

    // The boundary lengths lead every run, so the largest frame the cap admits
    // — and the first one it refuses — are exercised whatever quickcheck
    // generated. The first is large enough to grow the scratch, so the run
    // covers giving that memory back. The largest is found through the model
    // rather than restated as a literal, so a change to the header cannot
    // silently move it.
    let Some(largest) = (0..=cap.bytes())
        .rev()
        .find(|&length| expected_frame_len(subsystem, length, relay) <= cap.bytes())
    else {
        return TestResult::error("the cap admits some payload");
    };
    let boundaries = [
        cap.bytes() * 2,
        cap.bytes(),
        largest + 1,
        largest,
        largest - 1,
    ];
    for length in boundaries.into_iter().chain(lengths.iter().copied()) {
        let payload: Vec<u8> = (0..length).map(|index| index as u8).collect();
        // Slack past the cap, because a payload buffer's capacity is the
        // application's business and only its length faces the cap: what a
        // moving codec hands the encoder can be far larger than any frame.
        let mut handed = Vec::with_capacity(length + cap.bytes());
        handed.extend_from_slice(&payload);
        let expected_scratch = codec.expected_scratch(handed.capacity(), scratch_capacity);
        let serializes = codec.serializes();
        dst.clear();

        match encoder.stage(&header, handed) {
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
                    expected_frame_len(subsystem, length, relay),
                    "the frame must cost exactly the fields it carries"
                );
                assert!(
                    dst.len() <= cap.bytes(),
                    "an accepted frame must fit the cap: {} over {}",
                    dst.len(),
                    cap.bytes()
                );
                let tail = if relay { RELAY_FIELD_BYTES } else { 0 };
                assert_eq!(
                    &dst[dst.len() - tail - length..dst.len() - tail],
                    &payload[..],
                    "the payload must be framed verbatim"
                );
                // A refused response grows the scratch and a moving codec
                // replaces it outright, but `stage` shrinks it back toward the
                // cap before the next response, so the encoder is left holding
                // exactly the buffer its codec's shape implies and never one it
                // had to reserve.
                assert_eq!(
                    encoder.scratch_capacity(),
                    expected_scratch,
                    "the encoder reserved a scratch instead of reusing one"
                );
            }
        }

        assert_eq!(
            codec.serializes(),
            serializes + 1,
            "each response must be serialized exactly once"
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
    let header = header("billing", ErrorCategory::Permanent, None)?;

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

/// A released encoder holds nothing of the response before it: the scratch is
/// emptied and given back to the cap, whatever the codec left there.
///
/// What an encoder holds between responses is what the process holds, because a
/// destination can go quiet for as long as it likes. The moving codec is the
/// case that matters — it hands its own buffer over, at a capacity the cap
/// never bounded.
#[test]
fn a_released_encoder_holds_nothing_of_the_response_before_it() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let header = header("billing", ErrorCategory::Permanent, None)?;
    for codec in [CountingCodec::default(), CountingCodec::moving()] {
        let mut encoder = FrameEncoder::new(codec, cap);
        let mut dst = BytesMut::with_capacity(cap.bytes());
        // Far more capacity than the cap, so a scratch left as the codec handed
        // it over is unmistakable.
        let mut payload = Vec::with_capacity(cap.bytes() * 4);
        payload.extend_from_slice(b"hi");

        encoder.stage(&header, payload)?.write(&mut dst);
        encoder.release();
        assert!(
            encoder.scratch_capacity() <= cap.bytes(),
            "a released encoder holds {} bytes, over the {}-byte cap",
            encoder.scratch_capacity(),
            cap.bytes()
        );
    }
    Ok(())
}

#[test]
fn one_response_frames_to_known_bytes() -> Result<()> {
    let cap = FrameCap::new(1024)?;
    let mut encoder = FrameEncoder::new(CountingCodec::default(), cap);
    let header = header("billing", ErrorCategory::Permanent, None)?;
    let mut dst = BytesMut::with_capacity(cap.bytes());

    encoder.stage(&header, b"hi".to_vec())?.write(&mut dst);
    assert_eq!(&dst[..], &FROZEN[..], "the frame's bytes are frozen");
    Ok(())
}
