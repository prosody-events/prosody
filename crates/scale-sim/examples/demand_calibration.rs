//! Generates repeated demand calibration figures.

use std::env;
use std::fs;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;

use prosody_scale_sim::{
    CalibrationError, PlotError, PrincipalRegime, ReportError, run_demand_calibration,
    write_demand_calibration_figures, write_demand_calibration_report_pdf,
};
use thiserror::Error;

fn main() -> Result<(), DemandCalibrationGenerationError> {
    let trial_count = env::args()
        .nth(1)
        .map_or(Ok(16_u64), |value| value.parse())?;
    if trial_count == 0 {
        return Err(DemandCalibrationGenerationError::ZeroTrials);
    }
    let regimes = [
        PrincipalRegime::ApplicationLimited,
        PrincipalRegime::SeasonalWaves,
        PrincipalRegime::SnapshotFaults,
        PrincipalRegime::MissingReporter,
        PrincipalRegime::HistoricalMatch,
        PrincipalRegime::HistoricalExceeded,
        PrincipalRegime::HistoricalUnder,
        PrincipalRegime::HistoricalMissing,
    ];
    let seeds = (1_u64..=trial_count).collect::<Vec<_>>();
    let calibration = run_demand_calibration(&regimes, &seeds)?;
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("reports")
        .join("demand-calibration");
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    write_demand_calibration_figures(&directory, &calibration)?;
    write_demand_calibration_report_pdf(&directory.join("report.pdf"), &calibration)?;
    Ok(())
}

#[derive(Debug, Error)]
enum DemandCalibrationGenerationError {
    #[error(transparent)]
    Calibration(#[from] CalibrationError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Parse(#[from] ParseIntError),
    #[error(transparent)]
    Plot(#[from] PlotError),
    #[error(transparent)]
    Report(#[from] ReportError),
    #[error("the calibration trial count must be positive")]
    ZeroTrials,
}
