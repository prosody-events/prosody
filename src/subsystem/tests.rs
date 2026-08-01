use super::SubsystemName;
use crate::error::{ClassifyError, ErrorCategory};
use quickcheck::{QuickCheck, TestResult};

/// A name that is non-blank after trimming is accepted and stored in its
/// trimmed form. A blank name (empty or all-whitespace) is rejected.
#[test]
fn prop_subsystem_name_trims_and_rejects_blank() {
    fn prop(name: String) -> TestResult {
        let trimmed = name.trim().to_owned();
        let parsed = name.parse::<SubsystemName>();
        if parsed != SubsystemName::try_new(name) {
            return TestResult::error("constructor and environment parsing must agree");
        }
        match parsed {
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

/// Covers blank-name boundary cases that quickcheck's random generator rarely
/// produces exactly.
#[test]
fn blank_subsystem_names_rejected() {
    for blank in ["", "   ", "\t\n"] {
        assert!(
            matches!(
                SubsystemName::try_new(blank),
                Err(e) if e.classify_error() == ErrorCategory::Permanent
            ),
            "blank name {blank:?} must be rejected as Permanent",
        );
    }
}
