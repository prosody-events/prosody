use super::{flush_telemetry, shutdown_telemetry};
use color_eyre::Result;

/// FFI clients call flush and shutdown unconditionally on dispose or process
/// exit, even when [`super::initialize_tracing`] was never called. Both must
/// succeed as no-ops in that case.
///
/// This test only holds while no other test in this binary calls
/// `initialize_tracing`. The global tracing subscriber can be set once per
/// process.
#[test]
fn flush_and_shutdown_are_noops_when_uninitialized() -> Result<()> {
    flush_telemetry()?;
    shutdown_telemetry()?;
    Ok(())
}
