//! Generates repeated capacity calibration figures.

use std::env;
use std::fs;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;

use prosody_scale_sim::{
    CalibrationError, PlotError, PrincipalRegime, ReportError, run_capacity_calibration,
    run_capacity_sensitivity, write_capacity_calibration_figures,
    write_capacity_calibration_report_pdf, write_capacity_sensitivity_figures,
};
use thiserror::Error;

fn main() -> Result<(), CalibrationGenerationError> {
    let trial_count = env::args()
        .nth(1)
        .map_or(Ok(4_u64), |value| value.parse())?;
    if trial_count == 0 {
        return Err(CalibrationGenerationError::ZeroTrials);
    }
    let requested = match env::args().nth(2) {
        Some(value) => value,
        None => "declining-post-knee".to_owned(),
    };
    let sensitivity_trial_count = env::args()
        .nth(3)
        .map_or(Ok(trial_count.min(16)), |value| value.parse())?;
    if sensitivity_trial_count == 0 {
        return Err(CalibrationGenerationError::ZeroTrials);
    }
    let regimes = if requested == "all" {
        vec![
            PrincipalRegime::LinearThroughput,
            PrincipalRegime::FlatPostKnee,
            PrincipalRegime::DecliningPostKnee,
        ]
    } else {
        vec![
            PrincipalRegime::ALL
                .into_iter()
                .find(|regime| regime.name() == requested)
                .ok_or(CalibrationGenerationError::UnknownRegime(requested))?,
        ]
    };
    let seeds = (1_u64..=trial_count).collect::<Vec<_>>();
    let sensitivity_seeds = (1_u64..=sensitivity_trial_count).collect::<Vec<_>>();
    let calibration = run_capacity_calibration(&regimes, &seeds)?;
    let sensitivity = run_capacity_sensitivity(&regimes, &sensitivity_seeds)?;
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("reports")
        .join("calibration");
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    write_capacity_calibration_figures(&directory, &calibration)?;
    write_capacity_sensitivity_figures(&directory, &sensitivity)?;
    write_capacity_calibration_report_pdf(
        &directory.join("report.pdf"),
        &calibration,
        &sensitivity,
    )?;
    Ok(())
}

#[derive(Debug, Error)]
enum CalibrationGenerationError {
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
    #[error("unknown capacity regime: {0}")]
    UnknownRegime(String),
}
