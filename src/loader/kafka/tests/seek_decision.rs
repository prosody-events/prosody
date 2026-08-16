/// Helper to compute `should_seek` using the same logic as
/// `seek_to_first_active_offset`. This mirrors the production logic for
/// testability.
fn should_seek(current_position: Option<i64>, min_offset: i64, discard_threshold: i64) -> bool {
    match current_position {
        None => true,
        Some(position) => {
            let past_target = position > min_offset;
            let too_far_behind = position + discard_threshold < min_offset;
            past_target || too_far_behind
        }
    }
}

#[test]
fn invalid_position_always_seeks() {
    // After incremental_assign() but before first poll(), position() returns
    // Invalid (None). assign_if_needed only assigns on the first request;
    // concurrent lower-offset requests skip re-assignment, so the consumer may
    // be anchored above min_offset. Always seek when Invalid.
    assert!(should_seek(None, 70, 5));
    assert!(should_seek(None, 0, 10));
    assert!(should_seek(None, 1000, 100));
}

#[test]
fn position_past_target_seeks() {
    // Current position (60) > target (50) - need to seek backward
    assert!(should_seek(Some(60), 50, 5));
    assert!(should_seek(Some(100), 50, 10));
}

#[test]
fn position_too_far_behind_seeks() {
    // Position (50) + threshold (5) < target (70) → too far behind
    // 50 + 5 = 55 < 70 → seek
    assert!(should_seek(Some(50), 70, 5));
    // 50 + 10 = 60 < 100 → seek
    assert!(should_seek(Some(50), 100, 10));
}

#[test]
fn position_within_threshold_does_not_seek() {
    // Position (50) + threshold (5) >= target (55) → within range, read
    // sequentially 50 + 5 = 55 >= 55 → don't seek
    assert!(!should_seek(Some(50), 55, 5));
    // 50 + 5 = 55 >= 54 → don't seek
    assert!(!should_seek(Some(50), 54, 5));
    // 50 + 10 = 60 >= 55 → don't seek
    assert!(!should_seek(Some(50), 55, 10));
}

#[test]
fn position_at_target_does_not_seek() {
    // Position equals target - already there
    assert!(!should_seek(Some(50), 50, 5));
}

#[test]
fn position_past_target_always_seeks() {
    // Position (52) > target (50) → past_target is true → must seek backward
    assert!(should_seek(Some(52), 50, 5));
}

#[test]
fn threshold_boundary_exact() {
    // position + threshold == min_offset → NOT too far behind → don't seek
    // 50 + 5 = 55, min = 55 → exactly at boundary → don't seek
    assert!(!should_seek(Some(50), 55, 5));
    // 50 + 5 = 55, min = 56 → one past boundary → seek
    assert!(should_seek(Some(50), 56, 5));
}

#[test]
fn zero_threshold_seeks_any_forward_gap() {
    // threshold=0: position + 0 < min_offset whenever position < min_offset
    assert!(should_seek(Some(49), 50, 0));
    assert!(should_seek(Some(0), 1, 0));
    // position == min_offset: not past, not behind → don't seek
    assert!(!should_seek(Some(50), 50, 0));
}
