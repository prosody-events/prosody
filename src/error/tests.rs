use super::{ErrorCategory, RetryAlert, UnknownErrorCategory};
use strum::VariantArray;

/// The numbers a peer of another release reads off the wire. A round trip
/// cannot pin them — both conversions sit beside each other and move together —
/// so each is frozen here, along with the zero no category may claim.
#[test]
fn each_category_keeps_its_wire_discriminant() {
    let frozen: [(ErrorCategory, i32); 3] = [
        (ErrorCategory::Transient, 1),
        (ErrorCategory::Permanent, 2),
        (ErrorCategory::Terminal, 3),
    ];
    let unknown: [i32; 4] = [0, -1, 4, i32::MAX];

    for (category, discriminant) in frozen {
        assert_eq!(
            i32::from(category),
            discriminant,
            "{category:?} must keep the number peers read it as"
        );
        assert_eq!(
            ErrorCategory::try_from(discriminant),
            Ok(category),
            "{discriminant} must read back as {category:?}"
        );
    }
    for value in unknown {
        assert_eq!(
            ErrorCategory::try_from(value),
            Err(UnknownErrorCategory(value)),
            "{value} names no category"
        );
    }
}

/// A retry loop alerts at its first failure that is not `Transient`, and never
/// again. Every three-failure sequence is checked.
#[test]
fn retry_alert_fires_once_at_the_first_failure_retry_cannot_clear() {
    for &first in ErrorCategory::VARIANTS {
        for &second in ErrorCategory::VARIANTS {
            for &third in ErrorCategory::VARIANTS {
                let sequence = [first, second, third];
                let mut alert = RetryAlert::default();
                let alerts = sequence.map(|category| alert.alerts(category));

                let alerting = sequence
                    .iter()
                    .position(|&category| category != ErrorCategory::Transient);
                let expected = [0, 1, 2].map(|index| Some(index) == alerting);
                assert_eq!(alerts, expected, "{sequence:?}");
            }
        }
    }
}
