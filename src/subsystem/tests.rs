use super::SubsystemName;
use quickcheck::{QuickCheck, TestResult};

/// A trimmed-non-empty name is accepted and round-trips as its trimmed form; a
/// blank (empty or all-whitespace) name is rejected.
#[test]
fn prop_subsystem_name_trims_and_rejects_blank() {
    fn prop(name: String) -> TestResult {
        let trimmed = name.trim().to_owned();
        match SubsystemName::try_new(name) {
            Ok(_) if trimmed.is_empty() => TestResult::error("blank name must be rejected"),
            Ok(subsystem) if subsystem.as_str() == trimmed => TestResult::passed(),
            Ok(subsystem) => TestResult::error(format!(
                "expected {trimmed:?}, got {:?}",
                subsystem.as_str()
            )),
            Err(_) if trimmed.is_empty() => TestResult::passed(),
            Err(_) => TestResult::error("non-blank name must be accepted"),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(String) -> TestResult);
}

/// Explicit blank boundaries the generator rarely hits verbatim.
#[test]
fn blank_subsystem_names_rejected() {
    for blank in ["", "   ", "\t\n"] {
        assert!(
            SubsystemName::try_new(blank).is_err(),
            "blank name {blank:?} must be rejected",
        );
    }
}
