use super::{
    CountingCodec, RELAY_FIELD_BYTES, cache_uses_on_this_thread, header,
    serialize_capacity_on_this_thread,
};
use crate::Codec;
use crate::codec::SerializeBufGuard;
use crate::error::ErrorCategory;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::encode::{Forwarded, stage};
use crate::response::{FORMAT_MAX_BYTES, ResponseStatus};
use crate::router::{Framed, NodeId};
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use color_eyre::Result;
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::convert::Infallible;

/// The id a relay writes into a frame it sends on.
const RELAY_ID: [u8; 16] = [0x77; 16];

/// The id a frame already carried before it reached that relay.
const EARLIER_RELAY_ID: [u8; 16] = [0x33; 16];

/// Eight visible 16-byte blocks make the protocol's 128-byte format limit.
const WIDEST_FORMAT: &str = concat!(
    "0000000000000000",
    "1111111111111111",
    "2222222222222222",
    "3333333333333333",
    "4444444444444444",
    "5555555555555555",
    "6666666666666666",
    "7777777777777777",
);

#[derive(Default)]
struct WidestHeaderCodec;

impl Codec for WidestHeaderCodec {
    type Error = Infallible;
    type Payload = ();

    const FORMAT_ID: &'static str = WIDEST_FORMAT;

    fn deserialize(&mut self, _buf: &mut [u8]) -> Result<(), Infallible> {
        Ok(())
    }

    fn deserialize_owned(&mut self, _buf: BytesMut) -> Result<(), Infallible> {
        Ok(())
    }

    fn serialize(&mut self, (): (), _buf: &mut Vec<u8>) -> Result<(), Infallible> {
        Ok(())
    }

    fn serialize_ref(&mut self, (): &(), _buf: &mut Vec<u8>) -> Result<(), Infallible> {
        Ok(())
    }
}

/// The widest legal header encodes.
#[test]
fn the_widest_legal_header_encodes() -> Result<()> {
    assert_eq!(WidestHeaderCodec::FORMAT_ID.len(), FORMAT_MAX_BYTES);
    let subsystem = "s".repeat(SubsystemName::MAX_BYTES);
    let header = header(&subsystem, ResponseStatus::Success, None)?;
    let staged = stage::<WidestHeaderCodec>(&header, &())?;
    assert!(staged.bytes() > RELAY_FIELD_BYTES);
    Ok(())
}

/// The exact bytes one deterministic response frames to.
///
/// A round-trip cannot catch a wire break, because the encoder and the decoder
/// move together. Field *order* is this encoder's choice rather than a protobuf
/// requirement, but the bytes a peer of another release reads are not.
const FROZEN: [u8; 63] = [
    0x0a, 0x10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, // target_node
    0x12, 0x10, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, // request_id
    0x1a, 0x07, b'b', b'i', b'l', b'l', b'i', b'n', b'g', // subsystem
    0x22, 0x0a, b't', b'e', b's', b't', b'-', b'b', b'y', b't', b'e', b's', // format
    0x28, 0x02, // status = Permanent
    0x32, 0x02, b'h', b'i', // payload
];

/// Where the status value sits in [`FROZEN`]. The assertion in the frozen-bytes
/// test holds this to the key that precedes it.
const FROZEN_STATUS: usize = 58;

/// Response encoding uses the codec cache and the shared serialize buffer.
#[test]
fn response_encoding_uses_standard_codec_resources() -> Result<()> {
    const SEEDED: usize = 4096;
    let mut shared = SerializeBufGuard::acquire();
    shared.reserve(SEEDED);
    drop(shared);

    let before = cache_uses_on_this_thread();
    let payload = b"hi".to_vec();
    drop(stage::<CountingCodec>(
        &header("billing", ResponseStatus::Success, None)?,
        &payload,
    )?);

    assert_eq!(cache_uses_on_this_thread(), before + 1);
    assert!(serialize_capacity_on_this_thread() >= SEEDED);
    Ok(())
}

/// Response framing leaves message-size policy to gRPC.
#[test]
fn a_large_response_is_framed() -> Result<()> {
    let header = header(
        "billing",
        ResponseStatus::Error(ErrorCategory::Permanent),
        None,
    )?;

    assert!(stage::<CountingCodec>(&header, &vec![0_u8; 64 * 1024])?.bytes() > 64 * 1024);
    Ok(())
}

#[test]
fn known_responses_frame_to_known_bytes() -> Result<()> {
    assert_eq!(
        FROZEN[FROZEN_STATUS - 1],
        0x28,
        "the status byte must follow its own field key",
    );
    // The error row is the frame a peer of another release already reads. The
    // success row is the same frame with the one byte a success changes.
    for (status, byte) in [
        (ResponseStatus::Error(ErrorCategory::Permanent), 0x02),
        (ResponseStatus::Success, 0x04),
    ] {
        let header = header("billing", status, None)?;
        let mut dst = BytesMut::with_capacity(FROZEN.len());
        let mut expected = FROZEN;
        expected[FROZEN_STATUS] = byte;

        stage::<CountingCodec>(&header, &b"hi".to_vec())?.write(&mut dst);
        assert_eq!(&dst[..], &expected[..], "the frame's bytes are frozen");
    }
    Ok(())
}

/// Every frame this build emits can be sent on with a relay identifier.
#[quickcheck]
fn a_staged_frame_always_fits_its_forwarded_form(lengths: Vec<u16>) -> TestResult {
    match staged_frames_all_forward(lengths) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

fn staged_frames_all_forward(lengths: Vec<u16>) -> Result<()> {
    let header = header(
        "billing",
        ResponseStatus::Error(ErrorCategory::Permanent),
        None,
    )?;
    for length in lengths.into_iter().map(usize::from) {
        let staged = stage::<CountingCodec>(&header, &vec![0_u8; length])?;
        let mut bytes = BytesMut::with_capacity(staged.bytes());
        staged.write(&mut bytes);
        let frame = decode_frame(&mut bytes)?;
        drop(Forwarded::new(frame, NodeId::from_bytes(RELAY_ID)));
    }
    Ok(())
}

/// A relay writes its own id over whatever the frame already carried.
///
/// The constructor is unconditional, so no caller has to remember to clear the
/// field. That is what the loop stop rests on: a frame this process sends on
/// always names this process.
#[test]
fn a_forwarded_frame_never_keeps_the_relay_id_it_arrived_with() -> Result<()> {
    let earlier = NodeId::from_bytes(EARLIER_RELAY_ID);
    let relay = NodeId::from_bytes(RELAY_ID);
    let header = header("billing", ResponseStatus::Success, Some(earlier))?;

    let staged = stage::<CountingCodec>(&header, &b"hi".to_vec())?;
    let mut bytes = BytesMut::with_capacity(staged.bytes());
    staged.write(&mut bytes);
    let arrived = decode_frame(&mut bytes)?;
    assert_eq!(
        arrived.header.relay,
        Some(earlier),
        "the fixture must arrive already naming another relay"
    );

    let forwarded = Forwarded::new(arrived, relay);
    let mut sent = BytesMut::with_capacity(forwarded.bytes());
    forwarded.write(&mut sent);
    assert_eq!(
        decode_frame(&mut sent)?.header.relay,
        Some(relay),
        "the frame must name {relay}, the relay that sent it on, not {earlier}"
    );
    Ok(())
}
