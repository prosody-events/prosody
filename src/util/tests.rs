use super::from_option_file;
use color_eyre::Result;
use color_eyre::eyre::{Error, bail};
use quickcheck_macros::quickcheck;
use std::fs;
use tempfile::TempDir;

const FILE_VAR: &str = "PROSODY_TEST_FILE";

/// A file value is its trimmed content, and a blank file counts as unset.
/// The generator pads arbitrary content with the whitespace a writer
/// leaves behind, such as a trailing newline.
#[quickcheck]
fn file_value_is_trimmed_content(content: String, padding: (u8, u8)) -> Result<()> {
    let pad = |count: u8| " \n\t".repeat(usize::from(count % 3));
    let expected = Some(content.trim().to_owned()).filter(|value| !value.is_empty());
    let mut written = content;
    written.insert_str(0, &pad(padding.0));
    written.push_str(&pad(padding.1));

    let dir = TempDir::new()?;
    let path = dir.path().join("value");
    fs::write(&path, &written)?;

    let actual = from_option_file::<String>(FILE_VAR, &path).map_err(Error::msg)?;
    assert_eq!(actual, expected);
    Ok(())
}

/// A named file that cannot be read is an error, never an unset value.
#[test]
fn missing_file_is_an_error() -> Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("missing");

    let Err(error) = from_option_file::<String>(FILE_VAR, &path) else {
        bail!("a missing file must be an error");
    };
    assert!(
        error.contains(FILE_VAR),
        "the error names the variable: {error}"
    );
    Ok(())
}

/// A value that does not parse is an error that names the variable.
#[test]
fn unparsable_file_value_is_an_error() -> Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("value");
    fs::write(&path, "not a number\n")?;

    let Err(error) = from_option_file::<u32>(FILE_VAR, &path) else {
        bail!("an unparsable value must be an error");
    };
    assert!(
        error.contains(FILE_VAR),
        "the error names the variable: {error}"
    );
    Ok(())
}
