use super::{flush_telemetry, shutdown_telemetry};
use color_eyre::Result;

/// FFI clients call the flush/shutdown surface unconditionally at dispose or
/// process exit, so both must succeed as no-ops when
/// [`super::initialize_tracing`] never ran. Holds only while no test in this
/// binary initializes tracing — `initialize_tracing`'s global subscriber can
/// be set once per process.
#[test]
fn flush_and_shutdown_are_noops_when_uninitialized() -> Result<()> {
    flush_telemetry()?;
    shutdown_telemetry()?;
    Ok(())
}
