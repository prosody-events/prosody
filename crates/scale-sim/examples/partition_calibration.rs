//! Generates repeated partition-shape calibration data.

use std::env;
use std::fs;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;

use prosody_scale_sim::{
    CalibrationError, PlotError, PrincipalRegime, ReportError, run_partition_calibration,
    write_partition_calibration_data, write_partition_calibration_report_pdf,
};
use thiserror::Error;

fn main() -> Result<(), PartitionCalibrationGenerationError> {
    let trial_count = env::args()
        .nth(1)
        .map_or(Ok(16_u64), |value| value.parse())?;
    if trial_count == 0 {
        return Err(PartitionCalibrationGenerationError::ZeroTrials);
    }
    let regimes = [
        PrincipalRegime::ApplicationLimited,
        PrincipalRegime::SeasonalWaves,
        PrincipalRegime::HotPartition,
        PrincipalRegime::HotSerializedKey,
    ];
    let seeds = (1_u64..=trial_count).collect::<Vec<_>>();
    let calibration = run_partition_calibration(&regimes, &seeds)?;
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("reports")
        .join("partition-calibration");
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    write_partition_calibration_data(&directory, &calibration)?;
    write_partition_calibration_report_pdf(&directory.join("report.pdf"), &calibration)?;
    Ok(())
}

#[derive(Debug, Error)]
enum PartitionCalibrationGenerationError {
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
