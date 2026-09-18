//! A failed set read cannot establish that the set is empty.

use super::support::{FaultPoint, GROUP_A, ScriptedEnv, topic};
use crate::Key;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::set_state;
use crate::state::order_codec::Utf8KeyCodec;
use crate::state_reader::StateReaderError;
use color_eyre::eyre::{Result, bail, ensure};

#[tokio::test]
async fn set_is_empty_preserves_transient_store_failure() -> Result<()> {
    let env = ScriptedEnv::new(set_state::<Utf8KeyCodec>("failed-set-read"))?;
    let key = Key::from("key");
    let source = topic("source");
    env.publish(GROUP_A, source).await;
    env.fault(GROUP_A, source, &key, FaultPoint::AtOpen)?;

    let reader = env.reader_eager()?;
    let Err(error) = reader.is_empty(key).await else {
        bail!("a failed read reported whether the set was empty");
    };
    ensure!(
        error.classify_error() == ErrorCategory::Transient,
        "expected a transient error, got {error:?}"
    );
    ensure!(
        matches!(&error, StateReaderError::Store { message, .. } if message.contains("scripted cell-source fault")),
        "the reader lost the store error: {error:?}"
    );
    Ok(())
}
