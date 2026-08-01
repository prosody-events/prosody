use super::{SubsystemName, SubsystemNameError};
use crate::error::{ClassifyError, ErrorCategory};
use quickcheck::{QuickCheck, TestResult};

/// A name is accepted exactly when the trimmed form is non-blank and fits
/// [`SubsystemName::MAX_BYTES`], and is stored in that trimmed form. Every
/// other name is refused, with the reason naming which rule it broke.
///
/// The bound is what keeps a configured name addressable: a longer name could
/// never match a `response-awaited` header and could never be framed back, so a
/// consumer holding one would be silently unreachable.
#[test]
fn prop_subsystem_name_trims_and_bounds() {
    fn prop(name: String) -> TestResult {
        let trimmed = name.trim().to_owned();
        let parsed = name.parse::<SubsystemName>();
        if parsed != SubsystemName::try_new(name) {
            return TestResult::error("constructor and environment parsing must agree");
        }
        let expected = expected_error(&trimmed);
        match (parsed, expected) {
            (Ok(subsystem), None) if subsystem.as_str() == trimmed => TestResult::passed(),
            (Ok(subsystem), None) => TestResult::error(format!(
                "expected {trimmed:?}, got {:?}",
                subsystem.as_str()
            )),
            (Ok(_), Some(error)) => TestResult::error(format!("{trimmed:?} must be {error:?}")),
            (Err(error), Some(expected)) if error == expected => TestResult::passed(),
            (Err(error), expected) => {
                TestResult::error(format!("{trimmed:?} gave {error:?}, expected {expected:?}"))
            }
        }
    }
    QuickCheck::new().quickcheck(prop as fn(String) -> TestResult);
}

/// The rule a trimmed name breaks, derived from the name alone.
fn expected_error(trimmed: &str) -> Option<SubsystemNameError> {
    if trimmed.is_empty() {
        Some(SubsystemNameError::Blank)
    } else if trimmed.len() > SubsystemName::MAX_BYTES {
        Some(SubsystemNameError::TooLong {
            bytes: trimmed.len(),
        })
    } else {
        None
    }
}

/// Covers the boundary cases quickcheck's random generator rarely produces
/// exactly: a blank name, a name on the bound, and one byte past it.
#[test]
fn subsystem_name_boundaries() -> color_eyre::Result<()> {
    let at_bound = "a".repeat(SubsystemName::MAX_BYTES);
    let past_bound = "a".repeat(SubsystemName::MAX_BYTES + 1);

    for refused in ["", "   ", "\t\n", &past_bound, &format!("  {past_bound}  ")] {
        assert!(
            matches!(
                SubsystemName::try_new(refused),
                Err(e) if e.classify_error() == ErrorCategory::Permanent
            ),
            "name of {} bytes must be refused as Permanent",
            refused.len(),
        );
    }

    assert_eq!(SubsystemName::try_new(&at_bound)?.as_str(), at_bound);
    Ok(())
}

/// A name at the bound is stored inline, so no name ever allocates. The frame
/// decoder builds one per received frame, and the reserved-header parser
/// compares one per record, so an allocating name would be a per-message cost.
///
/// The inline capacity must exceed the bound, because `Flexstr` spills to the
/// heap once the text reaches its capacity. One byte short and every
/// maximum-length name would move to the heap with nothing to say so.
#[test]
fn a_name_at_the_bound_is_stored_inline() -> color_eyre::Result<()> {
    let at_bound = SubsystemName::try_new("a".repeat(SubsystemName::MAX_BYTES))?;

    assert!(
        at_bound.0.is_fixed(),
        "a name at the bound must not reach the heap"
    );
    Ok(())
}
