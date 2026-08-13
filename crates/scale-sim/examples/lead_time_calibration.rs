//! Generates repeated actuation lead-time calibration data.

use std::env;
use std::fs;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;

use prosody_scale_sim::{
    CalibrationError, PlotError, PrincipalRegime, ReportError, run_lead_time_calibration,
    write_lead_time_calibration_data, write_lead_time_calibration_report_pdf,
};
use thiserror::Error;

fn main() -> Result<(), LeadTimeCalibrationGenerationError> {
    let trial_count = env::args()
        .nth(1)
        .map_or(Ok(16_u64), |value| value.parse())?;
    if trial_count == 0 {
        return Err(LeadTimeCalibrationGenerationError::ZeroTrials);
    }
    let regimes = [
        PrincipalRegime::LinearThroughput,
        PrincipalRegime::FlatPostKnee,
        PrincipalRegime::DecliningPostKnee,
        PrincipalRegime::SeasonalWaves,
    ];
    let seeds = (1_u64..=trial_count).collect::<Vec<_>>();
    let calibration = run_lead_time_calibration(&regimes, &seeds)?;
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("reports")
        .join("lead-time-calibration");
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    write_lead_time_calibration_data(&directory, &calibration)?;
    write_lead_time_calibration_report_pdf(&directory.join("report.pdf"), &calibration)?;
    Ok(())
}

#[derive(Debug, Error)]
enum LeadTimeCalibrationGenerationError {
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
