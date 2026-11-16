use super::option_env_or_file;
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use std::fs;
use tempfile::TempDir;

const VAR: &str = "PROSODY_TEST";
const FILE_VAR: &str = "PROSODY_TEST_FILE";

/// A value as an operator writes it. It can be blank or `none`, and it can
/// have the padding that a writer leaves behind, such as a trailing newline.
#[derive(Clone, Debug)]
struct Written(String);

impl Arbitrary for Written {
    fn arbitrary(g: &mut Gen) -> Self {
        let words = ["", " ", " \t\n", "none", "NONE", "rack-a"];
        let word = |g: &mut Gen| g.choose(&words).copied().unwrap_or_default();
        let middle = if bool::arbitrary(g) {
            String::arbitrary(g)
        } else {
            word(g).to_owned()
        };
        Self(format!("{}{middle}{}", word(g), word(g)))
    }
}

/// The variable has priority over the file, and a blank variable falls
/// through to the file. Both sources trim the value, and a blank value or
/// `none` is unset. A named file that does not exist is an error that names
/// its variable.
#[quickcheck]
fn value_comes_from_the_variable_then_the_file(
    variable: Option<Written>,
    file: Option<Written>,
    file_exists: bool,
) -> Result<()> {
    let variable = variable.map(|Written(value)| value);
    let file = file.map(|Written(content)| content);
    let dir = TempDir::new()?;
    let path = dir.path().join("value");
    if let (Some(content), true) = (&file, file_exists) {
        fs::write(&path, content)?;
    }
    let lookup = |name: &str| match name {
        VAR => variable.clone(),
        FILE_VAR => file.as_ref().map(|_| path.display().to_string()),
        _ => None,
    };

    let given = |value: &str| {
        let value = value.trim();
        (!value.is_empty() && !value.eq_ignore_ascii_case("none")).then(|| value.to_owned())
    };
    let set_variable = variable.as_deref().filter(|value| !value.trim().is_empty());
    let expected = match (set_variable, &file) {
        (Some(value), _) => Some(given(value)),
        (None, None) => Some(None),
        (None, Some(content)) => file_exists.then(|| given(content)),
    };

    match (option_env_or_file::<String>(VAR, lookup), expected) {
        (Ok(actual), Some(expected)) => assert_eq!(actual, expected),
        (Err(error), None) => assert!(error.contains(FILE_VAR), "names the variable: {error}"),
        (actual, expected) => bail!("got {actual:?}, expected {expected:?}"),
    }
    Ok(())
}

/// A file value that does not parse is an error that names the variable.
#[test]
fn unparsable_file_value_is_an_error() -> Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("value");
    fs::write(&path, "not a number\n")?;
    let lookup = |name: &str| (name == FILE_VAR).then(|| path.display().to_string());

    let Err(error) = option_env_or_file::<u32>(VAR, lookup) else {
        bail!("an unparsable value must be an error");
    };
    assert!(error.contains(FILE_VAR), "names the variable: {error}");
    Ok(())
}
