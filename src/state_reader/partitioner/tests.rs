use color_eyre::Result;

use super::{EmptyKeyError, PartitionCount, partition_for_key};

/// Frozen `(key, count) → partition` table for the `consistent_random`
/// partitioner.
///
/// The `b"123456789"` rows are pinned to the published CRC-32/ISO-HDLC check
/// value `crc32(b"123456789") == 0xCBF43926` (`3_421_780_262`). That ties the
/// expected partitions to a value computed outside this crate, not to the
/// code under test. The remaining rows cover other fixed keys.
///
/// If the partitioner's key-to-partition mapping ever changes, one of these
/// rows will fail. That is the point of a golden test: it catches
/// accidental drift in the mapping.
const GOLDEN: &[(&[u8], i32, i32)] = &[
    (b"123456789", 31, 14),
    (b"123456789", 7, 5),
    (b"123456789", 1, 0),
    (b"123456789", i32::MAX, 1_274_296_615),
    (b"a", 31, 23),
    (b"a", 7, 4),
    (b"a", 1000, 907),
    (b"user-42", 31, 3),
    (b"user-42", 64, 51),
    (b"user-42", 997, 147),
];

#[test]
fn golden_vectors_are_frozen() -> Result<()> {
    for &(key, count, expected) in GOLDEN {
        let count = PartitionCount::try_from(count)?;
        assert_eq!(
            partition_for_key(key, count)?,
            expected,
            "partition_for_key({key:?}, {count:?}) drifted from frozen contract",
        );
    }
    Ok(())
}

#[test]
fn empty_key_is_rejected() -> Result<()> {
    let count = PartitionCount::try_from(31_i32)?;
    assert!(
        matches!(partition_for_key(b"", count), Err(EmptyKeyError)),
        "empty key must be rejected: librdkafka randomizes empty/NULL keys",
    );
    Ok(())
}

#[test]
fn partition_count_rejects_non_positive_and_round_trips() -> Result<()> {
    assert!(PartitionCount::try_from(0_i32).is_err(), "zero rejected");
    assert!(
        PartitionCount::try_from(-1_i32).is_err(),
        "negative rejected"
    );
    assert!(
        PartitionCount::try_from(i32::MIN).is_err(),
        "i32::MIN rejected",
    );
    assert_eq!(
        i32::from(PartitionCount::try_from(31_i32)?),
        31_i32,
        "positive count round-trips through i32",
    );
    // The maximum representable count is accepted and round-trips.
    assert_eq!(
        i32::from(PartitionCount::try_from(i32::MAX)?),
        i32::MAX,
        "i32::MAX count round-trips",
    );
    Ok(())
}
