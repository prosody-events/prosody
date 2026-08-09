//! Generates deterministic principal-regime reports.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use prosody_scale_sim::{
    BatchSloError, ExperimentReport, PlantError, PlotError, PrincipalRegime, PrincipalRunError,
    RegimeExperiment, RegimeReport, RegimeStory, RegimeValidationError, ReportError, run_batch_slo,
    run_capacity_evidence_regime, run_principal_regime, validate_principal_regime,
    write_batch_actuation_svg, write_batch_report_pdf, write_batch_slo_svg,
    write_capacity_belief_svg, write_model_belief_figures, write_model_belief_snapshot_figures,
    write_regime_report_pdf, write_regime_story_figures,
};
use rayon::prelude::*;
use thiserror::Error;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> Result<(), PlotGenerationError> {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .finish()
        .try_init()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let report_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("reports");
    fs::create_dir_all(&report_directory)?;
    let requested = env::args().nth(1);
    if let Some(name) = requested {
        if name == "batch-backlog" {
            clear_directory(&report_directory.join(&name))?;
            return generate_batch(&report_directory);
        }
        let regime = PrincipalRegime::ALL
            .into_iter()
            .find(|regime| regime.name() == name)
            .ok_or(PlotGenerationError::UnknownRegime(name))?;
        let include_capacity_evidence = match env::args().nth(2).as_deref() {
            None => true,
            Some("closed-loop") => false,
            Some(experiment) => {
                return Err(PlotGenerationError::UnknownExperiment(
                    experiment.to_owned(),
                ));
            }
        };
        clear_directory(&report_directory.join(regime.name()))?;
        return generate_regime(&report_directory, regime, include_capacity_evidence);
    }
    clear_plot_files(&report_directory)?;
    let results = PrincipalRegime::ALL
        .par_iter()
        .copied()
        .map(|regime| {
            let result = generate_regime(&report_directory, regime, true);
            if let Err(error) = &result {
                tracing::error!(regime = regime.name(), %error, "regime report failed");
            }
            (regime, result)
        })
        .collect::<Vec<_>>();
    let mut failures = Vec::new();
    for (regime, result) in results {
        if let Err(error) = result {
            failures.push(format!("{}: {error}", regime.name()));
        }
    }
    if let Err(error) = generate_batch(&report_directory) {
        tracing::error!(regime = "batch-backlog", %error, "regime report failed");
        failures.push(format!("batch-backlog: {error}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(PlotGenerationError::ValidationSummary { failures })
    }
}

fn generate_batch(report_directory: &Path) -> Result<(), PlotGenerationError> {
    let batch_directory = report_directory.join("batch-backlog");
    fs::create_dir_all(&batch_directory)?;
    let mut batch_summaries = Vec::with_capacity(4);
    for budget_hours in [6_u64, 12, 24, 48] {
        batch_summaries.push(run_batch_slo(budget_hours * 60 * 60 * 1_000_000, 0.05)?);
    }
    write_batch_slo_svg(&batch_directory.join("slo-sweep.svg"), &batch_summaries)?;
    write_batch_actuation_svg(&batch_directory.join("actuation.svg"), &batch_summaries)?;
    write_batch_report_pdf(&batch_directory.join("report.pdf"), &batch_summaries)?;
    Ok(())
}

fn generate_regime(
    report_directory: &Path,
    regime: PrincipalRegime,
    include_capacity_evidence: bool,
) -> Result<(), PlotGenerationError> {
    let regime_directory = report_directory.join(regime.name());
    fs::create_dir_all(&regime_directory)?;
    let result = run_principal_regime(regime)?;
    let closed_loop_validation =
        validate_principal_regime(regime, RegimeExperiment::ClosedLoop, &result);
    let trace = result.metric_trace(result.metric_window_micros(), regime.budget_micros())?;
    write_experiment_figures(
        &regime_directory.join("closed-loop"),
        regime,
        &result,
        &trace,
    )?;
    let capacity_evidence = if include_capacity_evidence
        && matches!(
            regime,
            PrincipalRegime::LinearThroughput
                | PrincipalRegime::FlatPostKnee
                | PrincipalRegime::DecliningPostKnee
        ) {
        let capacity_evidence_result = run_capacity_evidence_regime(regime)?;
        let validation = validate_principal_regime(
            regime,
            RegimeExperiment::CapacityEvidence,
            &capacity_evidence_result,
        );
        let capacity_evidence_trace = capacity_evidence_result.metric_trace(
            capacity_evidence_result.metric_window_micros(),
            regime.budget_micros(),
        )?;
        write_experiment_figures(
            &regime_directory.join("capacity-evidence"),
            regime,
            &capacity_evidence_result,
            &capacity_evidence_trace,
        )?;
        Some((
            capacity_evidence_result,
            capacity_evidence_trace,
            validation,
        ))
    } else {
        None
    };
    // Write all artifacts before validation so failed regimes keep their reports.
    write_regime_report_pdf(
        &regime_directory.join("report.pdf"),
        &RegimeReport {
            regime,
            closed_loop: ExperimentReport {
                trace: &trace,
                controller: result.controller(),
                stop: result.stop(),
            },
            capacity_evidence: capacity_evidence.as_ref().map(|evidence| ExperimentReport {
                trace: &evidence.1,
                controller: evidence.0.controller(),
                stop: evidence.0.stop(),
            }),
        },
    )?;
    closed_loop_validation?;
    if let Some(evidence) = capacity_evidence {
        evidence.2?;
    }
    Ok(())
}

fn write_experiment_figures(
    directory: &Path,
    regime: PrincipalRegime,
    result: &prosody_scale_sim::PrincipalRun,
    trace: &prosody_scale_sim::MetricTrace,
) -> Result<(), PlotGenerationError> {
    fs::create_dir_all(directory)?;
    write_regime_story_figures(
        &directory.join("story"),
        &RegimeStory {
            trace,
            controller: result.controller(),
            inputs: result.inputs(),
            stop: result.stop(),
            budget_micros: regime.budget_micros(),
            allowed_miss_fraction: 0.01,
        },
    )?;
    write_capacity_belief_svg(
        &directory.join("capacity-belief.svg"),
        &format!("{} capacity belief", regime.name()),
        result.controller(),
    )?;
    write_model_belief_figures(&directory.join("beliefs"), result.controller())?;
    write_model_belief_snapshot_figures(&directory.join("snapshots"), result.controller())?;
    Ok(())
}

fn clear_plot_files(directory: &PathBuf) -> Result<(), io::Error> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_file() {
            fs::remove_file(path)?;
        } else if path.is_dir() {
            fs::remove_dir_all(path)?;
        }
    }
    Ok(())
}

fn clear_directory(directory: &Path) -> Result<(), io::Error> {
    if directory.exists() {
        fs::remove_dir_all(directory)?;
    }
    fs::create_dir_all(directory)
}

#[derive(Debug, Error)]
enum PlotGenerationError {
    #[error(transparent)]
    Batch(#[from] BatchSloError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Plant(#[from] PlantError),
    #[error(transparent)]
    Principal(#[from] PrincipalRunError),
    #[error(transparent)]
    Plot(#[from] PlotError),
    #[error(transparent)]
    Report(#[from] ReportError),
    #[error(transparent)]
    Validation(#[from] RegimeValidationError),
    #[error("report generation failed: {failures:?}")]
    ValidationSummary { failures: Vec<String> },
    #[error("unknown regime: {0}")]
    UnknownRegime(String),
    #[error("unknown experiment: {0}")]
    UnknownExperiment(String),
}
