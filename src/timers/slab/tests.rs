use super::*;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;

#[test]
fn test_slab_new() {
    let slab_id = 42;
    let size = CompactDuration::new(60); // 60 seconds

    let slab = Slab::new(slab_id, size);

    assert_eq!(slab.id(), slab_id);
    assert_eq!(slab.size(), size);
}

#[test]
fn test_slab_from_time() {
    let size = CompactDuration::new(60); // 60 seconds
    let time = CompactDateTime::from(123_i32); // 123 seconds since epoch

    let slab = Slab::from_time(size, time);

    assert_eq!(slab.size(), size);
    assert_eq!(slab.id(), 2); // 123 / 60 = 2
}

#[test]
fn test_slab_from_time_zero_size() {
    let size = CompactDuration::new(0); // Zero duration
    let time = CompactDateTime::from(123_i32); // 123 seconds since epoch

    let slab = Slab::from_time(size, time);

    assert_eq!(slab.size(), size);
    assert_eq!(slab.id(), 0); // Slab ID should default to 0 for zero size
}

#[test]
fn test_slab_range() {
    let slab_id = 3;
    let size = CompactDuration::new(60); // 60 seconds

    let slab = Slab::new(slab_id, size);
    let range = slab.range();

    assert_eq!(range.start.epoch_seconds(), 180); // 3 * 60 = 180
    assert_eq!(range.end.epoch_seconds(), 240); // 180 + 60 = 240
}

#[test]
fn test_slab_range_zero_size() {
    let slab_id = 3;
    let size = CompactDuration::new(0); // Zero duration

    let slab = Slab::new(slab_id, size);
    let range = slab.range();

    assert_eq!(range.start.epoch_seconds(), 0); // Start should be 0
    assert_eq!(range.end.epoch_seconds(), 0); // End should also be 0
}

#[test]
fn test_slab_debug() {
    let slab_id = 42;
    let size = CompactDuration::new(60); // 60 seconds

    let slab = Slab::new(slab_id, size);
    let debug_str = format!("{slab:?}");

    assert_eq!(debug_str, format!("Slab({slab_id})"));
}

#[test]
fn test_slab_display() {
    let slab_id = 3;
    let size = CompactDuration::new(60); // 60 seconds

    let slab = Slab::new(slab_id, size);
    let display_str = format!("{slab}");

    assert_eq!(
        display_str,
        format!(
            "{slab_id}[{}—{}]",
            CompactDateTime::from(180_i32), // Start of range
            CompactDateTime::from(240_i32)  // End of range
        )
    );
}

#[test]
fn test_slab_equality() {
    let size = CompactDuration::new(60); // 60 seconds

    let slab1 = Slab::new(1, size);
    let slab2 = Slab::new(1, size);
    let slab3 = Slab::new(2, size);

    assert_eq!(slab1, slab2);
    assert_ne!(slab1, slab3);
}

#[test]
fn test_slab_ordering() {
    let size = CompactDuration::new(60); // 60 seconds

    let slab1 = Slab::new(1, size);
    let slab2 = Slab::new(2, size);

    assert!(slab1 < slab2);
    assert!(slab2 > slab1);
}
