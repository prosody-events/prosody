use super::{SubsystemName, SubsystemNameError};
use crate::error::{ClassifyError, ErrorCategory};
use quickcheck::{QuickCheck, TestResult};

/// A name is accepted exactly when its trimmed form is not blank. It is stored
/// in that trimmed form.
#[test]
fn prop_subsystem_name_trims_and_rejects_blank() {
    fn prop(name: String) -> TestResult {
        let trimmed = name.trim().to_owned();
        let parsed = name.parse::<SubsystemName>();
        if parsed != SubsystemName::try_new(name) {
            return TestResult::error("constructor and environment parsing must agree");
        }
        match parsed {
            Ok(subsystem) if subsystem.as_str() == trimmed => TestResult::passed(),
            Ok(subsystem) => TestResult::error(format!(
                "expected {trimmed:?}, got {:?}",
                subsystem.as_str()
            )),
            Err(SubsystemNameError) if trimmed.is_empty() => TestResult::passed(),
            Err(error) => TestResult::error(format!("{trimmed:?} gave {error:?}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(String) -> TestResult);
}

/// Blank names are invalid. Long names remain valid.
#[test]
fn subsystem_name_validity() -> color_eyre::Result<()> {
    for refused in ["", "   ", "\t\n"] {
        assert!(
            matches!(
                SubsystemName::try_new(refused),
                Err(e) if e.classify_error() == ErrorCategory::Permanent
            ),
            "name of {} bytes must be refused as Permanent",
            refused.len(),
        );
    }

    let long = "a".repeat(256);
    assert_eq!(SubsystemName::try_new(&long)?.as_str(), long);
    Ok(())
}

/// Existing subsystem names stay inline.
#[test]
fn common_names_are_stored_inline() -> color_eyre::Result<()> {
    for name in ["crm", "sites", "listing-accounts"] {
        assert!(SubsystemName::try_new(name)?.0.is_fixed());
    }
    Ok(())
}
