use std::cmp::Ordering;
use std::fmt::{self, Write};
use std::fs;
use std::io;
use std::path::Path;

use thiserror::Error;
use typst_as_lib::{TypstEngine, typst_kit_options::TypstKitFontOptions};
use typst_pdf::PdfOptions;

use crate::{
    BatchSloSummary, CapacityCalibration, CapacityEvidenceKind, CapacityEvidenceSample,
    CapacitySensitivity, CapacitySensitivityCalibration, CapacityWindowSample, ControllerTrace,
    DemandCalibration, DocumentManifest, ImageManifestEntry, LeadTimeCalibration, MetricTrace,
    PanelContent, PartitionCalibration, PrincipalRegime, PriorArtifactKind, ReportCheckError,
    ReportSection, RunStop, RunStopReason, check_document, check_images,
    predictive_coverage_levels,
};
use prosody_scale_core::{PosteriorQuery, TransitionDirection};

const STORY_FIGURES: [FlowFigure; 19] = [
    FlowFigure::new(
        "Demand and history",
        "01-demand.svg",
        "Current messages and timers show realized demand. Historical messages show the supplied \
         forecast.",
    ),
    FlowFigure::new(
        "Backlog response",
        "02-backlog.svg",
        "Backlog and completions show whether plant capacity kept pace with demand.",
    ),
    FlowFigure::new(
        "Replica demand and supply",
        "03-scale.svg",
        "This figure separates historical replicas, external interventions, controller targets, \
         and actual replicas.",
    ),
    FlowFigure::new(
        "Saturation limit",
        "04-saturation-cap.svg",
        "The cap limits controller targets when accepted capacity evidence identifies saturation.",
    ),
    FlowFigure::new(
        "Latency outcome",
        "05-latency.svg",
        "Observed latency quantiles show whether completed events met the declared SLO.",
    ),
    FlowFigure::new(
        "Inferred SLO risk",
        "06-risk.svg",
        "Expected loss and saturation probability connect model belief to realized misses.",
    ),
    FlowFigure::new(
        "Accepted capacity evidence",
        "07-capacity-evidence.svg",
        "Passive resource windows show the evidence that updated capacity belief.",
    ),
    FlowFigure::new(
        "Capacity posterior",
        "08-capacity-posterior.svg",
        "The heatmap shows capacity probability mass through time. The line marks the posterior \
         median.",
    ),
    FlowFigure::new(
        "Arrival posterior predictive check",
        "09-arrival-predictive.svg",
        "Realized arrivals should behave like draws from the one-step predictive distribution.",
    ),
    FlowFigure::new(
        "Service inputs",
        "10-service-inputs.svg",
        "Base service times and observed latency show the queue response to offered load.",
    ),
    FlowFigure::new(
        "Shared resource response",
        "11-shared-resource.svg",
        "Nominal resource capacity and completed attempts show whether overload limits service.",
    ),
    FlowFigure::new(
        "Actuation delay",
        "12-actuation.svg",
        "Configured launch delay and inferred lead times show the delay between a target and \
         ready replicas.",
    ),
    FlowFigure::new(
        "Reporter coverage",
        "13-reporter-coverage.svg",
        "Missing reporter count shows when direct evidence is unavailable.",
    ),
    FlowFigure::new(
        "Snapshot age",
        "14-snapshot-age.svg",
        "Snapshot age shows how long the controller has lacked fresh reporter evidence.",
    ),
    FlowFigure::new(
        "Reliability evidence",
        "15-reliability-evidence.svg",
        "Final and retry-producing outcome counts show evidence for the reliability posterior.",
    ),
    FlowFigure::new(
        "Deadline satisfaction by replica candidate",
        "16-decision-pass.svg",
        "Light cells have high posterior deadline-satisfaction probability. The lines show the \
         selected target and actual replicas.",
    ),
    FlowFigure::new(
        "Decision loss by replica candidate",
        "17-decision-loss.svg",
        "Dark cells have low expected cost. Light cells have high expected cost. The lines show \
         the selected target, saturation cap, and actual replicas.",
    ),
    FlowFigure::new(
        "Certified capacity event trace",
        "18-capacity-trace.svg",
        "The panel shows the W6 busy-slot path. Exposed states summarize E_n. The trace retains \
         D_n and equal-clock transition groups for audit.",
    ),
    FlowFigure::new(
        "Capacity predictive coverage",
        "19-capacity-coverage.svg",
        "Each point records whether the accepted outcome entered its stated 80% interval. The \
         cumulative line shows empirical prequential coverage for this experiment.",
    ),
];

const MODEL_FACTORS: [ModelFactor; 12] = [
    ModelFactor::new(
        "Arrival rate",
        "arrival-rate",
        "The heatmap shows how live demand evidence changes the arrival-rate posterior.",
    ),
    ModelFactor::new(
        "Partition share",
        "partition-share",
        "The heatmap shows uncertainty about load concentration across partitions.",
    ),
    ModelFactor::new(
        "Peak capacity",
        "peak-capacity",
        "The heatmap shows how accepted throughput evidence changes peak capacity belief.",
    ),
    ModelFactor::new(
        "Service time",
        "service-time",
        "The heatmap shows service-time probability mass after accepted observations.",
    ),
    ModelFactor::new(
        "Collapse strength",
        "collapse-strength",
        "The heatmap shows belief about throughput loss beyond the knee.",
    ),
    ModelFactor::new(
        "Knee concurrency",
        "knee-concurrency",
        "The heatmap shows the concurrency range where saturation begins.",
    ),
    ModelFactor::new(
        "Normal retry probability",
        "normal-retry-probability",
        "The heatmap shows retry probability after a normal attempt.",
    ),
    ModelFactor::new(
        "Failure retry probability",
        "failure-retry-probability",
        "The heatmap shows retry probability after a failure attempt.",
    ),
    ModelFactor::new(
        "Scale-up lead time",
        "scale-up-lead-time",
        "The heatmap shows the launch-delay posterior for scale-up actions.",
    ),
    ModelFactor::new(
        "Scale-down lead time",
        "scale-down-lead-time",
        "The heatmap shows the completion-delay posterior for scale-down actions.",
    ),
    ModelFactor::new(
        "Scale-up rebalance time",
        "scale-up-rebalance-time",
        "The heatmap shows the partition-pause posterior for scale-up actions.",
    ),
    ModelFactor::new(
        "Scale-down rebalance time",
        "scale-down-rebalance-time",
        "The heatmap shows the partition-pause posterior for scale-down actions.",
    ),
];

#[derive(Clone, Copy)]
struct FlowFigure {
    heading: &'static str,
    file: &'static str,
    caption: &'static str,
}

impl FlowFigure {
    const fn new(heading: &'static str, file: &'static str, caption: &'static str) -> Self {
        Self {
            heading,
            file,
            caption,
        }
    }
}

#[derive(Clone, Copy)]
struct ModelFactor {
    heading: &'static str,
    file: &'static str,
    belief_caption: &'static str,
}

impl ModelFactor {
    const fn new(heading: &'static str, file: &'static str, belief_caption: &'static str) -> Self {
        Self {
            heading,
            file,
            belief_caption,
        }
    }
}

/// Inputs for one generated regime report.
pub struct RegimeReport<'a> {
    /// The tested operating regime.
    pub regime: PrincipalRegime,
    /// Closed-loop run evidence.
    pub closed_loop: ExperimentReport<'a>,
    /// Capacity-evidence experiment, when applicable.
    pub capacity_evidence: Option<ExperimentReport<'a>>,
}

/// Evidence from one complete regime experiment.
#[derive(Clone, Copy)]
pub struct ExperimentReport<'a> {
    /// Reproducible source and artifact identity.
    pub metadata: crate::ReportMetadata,
    /// Plant and controller metrics.
    pub trace: &'a MetricTrace,
    /// Exact controller evidence and decisions.
    pub controller: &'a ControllerTrace,
    /// Exact duration and stop reason.
    pub stop: RunStop,
    /// Images produced for this experiment.
    pub images: &'a [ImageManifestEntry],
}

/// One owned row in the four-case historical comparison.
#[derive(Clone, Copy)]
pub struct HistoricalComparisonRow {
    regime: PrincipalRegime,
    final_no_knee_probability: f64,
    maximum_target: u32,
    peak_backlog: u64,
    miss_fraction: f64,
    final_cost: f64,
}

impl HistoricalComparisonRow {
    /// Builds one aligned comparison row from a completed experiment.
    #[must_use]
    pub fn from_experiment(regime: PrincipalRegime, experiment: ExperimentReport<'_>) -> Self {
        let summary = ReportSummary::from_trace(experiment.trace, experiment.controller);
        let final_cost = experiment
            .controller
            .len()
            .checked_sub(1)
            .and_then(|index| experiment.controller.sample(index))
            .map_or(0.0_f64, |sample| sample.selected_cost);
        Self {
            regime,
            final_no_knee_probability: summary.final_no_knee_probability,
            maximum_target: summary.maximum_target,
            peak_backlog: summary.peak_backlog,
            miss_fraction: summary.total_miss_fraction,
            final_cost,
        }
    }
}

/// Writes the shared four-case historical comparison document.
///
/// # Errors
///
/// Returns an error when the four cases are incomplete or output fails.
pub fn write_historical_comparison_pdf(
    path: &Path,
    rows: &[HistoricalComparisonRow],
) -> Result<(), ReportError> {
    const H4: [PrincipalRegime; 4] = [
        PrincipalRegime::HistoricalMatch,
        PrincipalRegime::HistoricalExceeded,
        PrincipalRegime::HistoricalUnder,
        PrincipalRegime::HistoricalMissing,
    ];
    if rows.len() != H4.len()
        || H4
            .iter()
            .any(|regime| !rows.iter().any(|row| row.regime == *regime))
    {
        return Err(ReportError::HistoricalComparison);
    }
    let mut source = String::with_capacity(4_096);
    source.push_str(
        "#set document(title: \"Historical evidence comparison\")\n#set page(paper: \
         \"us-letter\", margin: 0.65in)\n#set text(font: \"Charter\", size: 10pt)\n\n= Historical \
         evidence comparison\n\nThe four cases use one table and one scale for each \
         quantity.\n\n== Belief, decision, outcome, and cost\n\n#table(columns: 6, stroke: none, \
         inset: 5pt, [*Case*], [*Belief*], [*Decision*], [*Outcome*], [*Miss fraction*], [*Cost*],",
    );
    for row in rows {
        writeln!(
            source,
            "[{}], [{:.3} no-knee probability], [{} replicas], [{} backlog events], [{:.3}], \
             [{:.1} event-delay-seconds],",
            row.regime.name(),
            row.final_no_knee_probability,
            row.maximum_target,
            row.peak_backlog,
            row.miss_fraction,
            row.final_cost,
        )?;
    }
    source.push_str(")\n");
    write_pdf(path, &source)
}

/// Writes one complete regime report as a PDF file.
///
/// # Errors
///
/// Returns an error when formatting or file output fails.
pub fn write_regime_report_pdf(path: &Path, report: &RegimeReport<'_>) -> Result<(), ReportError> {
    let summary =
        ReportSummary::from_trace(report.closed_loop.trace, report.closed_loop.controller);
    let mut source = String::with_capacity(8_192);
    write_header(&mut source, report)?;
    write_design(&mut source, report)?;
    write_experiment_figures(&mut source, "closed-loop", report.closed_loop)?;
    if let Some(capacity_evidence) = report.capacity_evidence {
        write_capacity_evidence_summary(&mut source, capacity_evidence)?;
        write_direct_comparison(&mut source, report.closed_loop, capacity_evidence)?;
    }
    write_summary(&mut source, summary)?;
    write_strengths_and_limitations(&mut source, report, summary)?;
    write_capacity_diagnostic(&mut source, report.closed_loop.controller)?;
    validate_document_source(&source)?;
    check_images(report.closed_loop.images)?;
    if let Some(capacity_evidence) = report.capacity_evidence
        && !capacity_evidence.images.is_empty()
    {
        check_images(capacity_evidence.images)?;
    }
    write_pdf(path, &source)
}

fn validate_document_source(source: &str) -> Result<(), ReportCheckError> {
    let section_markers = [
        (ReportSection::Regime, " regime"),
        (ReportSection::Evidence, "== Evidence"),
        (ReportSection::Belief, "== Belief"),
        (ReportSection::Decision, "== Decision"),
        (ReportSection::Outcome, "== Outcome"),
    ];
    let mut sections = section_markers
        .into_iter()
        .filter_map(|(section, marker)| source.find(marker).map(|position| (position, section)))
        .collect::<Vec<_>>();
    sections.sort_by_key(|(position, _)| *position);
    let sections = sections
        .into_iter()
        .map(|(_, section)| section)
        .collect::<Vec<_>>();
    let unit_names = [
        "events",
        "replicas",
        "probability",
        "operations per second",
        "seconds",
        "event-delay-seconds",
        "replica-seconds",
    ];
    let units = unit_names
        .into_iter()
        .filter(|unit| source.contains(unit))
        .collect::<Vec<_>>();
    let metadata_names = [
        ("commit", "Commit:"),
        ("model version", "Model version:"),
        ("artifact identity", "Artifact identity:"),
        ("seed", "Seed:"),
        ("duration", "Duration:"),
        ("generator version", "Generator version:"),
    ];
    let metadata = metadata_names
        .into_iter()
        .filter_map(|(name, marker)| source.contains(marker).then_some(name))
        .collect::<Vec<_>>();
    let artifact_names = ["capacity", "arrival", "reliability", "launch", "rebalance"];
    let artifacts = artifact_names
        .into_iter()
        .filter(|artifact| source.contains(&format!("[{artifact}]")))
        .collect::<Vec<_>>();
    check_document(&DocumentManifest {
        sections: &sections,
        units: &units,
        metadata: &metadata,
        artifacts: &artifacts,
    })
}

fn write_direct_comparison(
    source: &mut String,
    closed_loop: ExperimentReport<'_>,
    capacity_evidence: ExperimentReport<'_>,
) -> Result<(), fmt::Error> {
    let closed = ReportSummary::from_trace(closed_loop.trace, closed_loop.controller);
    let evidence = ReportSummary::from_trace(capacity_evidence.trace, capacity_evidence.controller);
    writeln!(source, "\n#pagebreak()\n== Direct experiment comparison\n")?;
    writeln!(
        source,
        "The columns use one order and one unit for each comparison."
    )?;
    writeln!(
        source,
        "\n#table(columns: 3, stroke: none, inset: 5pt, [*Spine*], [*Closed loop*], [*Capacity \
         evidence*],"
    )?;
    writeln!(
        source,
        "[Evidence], [{} accepted windows], [{} accepted windows],",
        closed.resource_windows, evidence.resource_windows
    )?;
    writeln!(
        source,
        "[Belief], [{:.1} final no-knee probability], [{:.1} final no-knee probability],",
        closed.final_no_knee_probability, evidence.final_no_knee_probability
    )?;
    writeln!(
        source,
        "[Decision], [{} peak target replicas], [{} peak target replicas],",
        closed.maximum_target, evidence.maximum_target
    )?;
    writeln!(
        source,
        "[Outcome], [{} peak backlog events], [{} peak backlog events],",
        closed.peak_backlog, evidence.peak_backlog
    )?;
    writeln!(
        source,
        "[Cost], [{:.3} observed miss fraction], [{:.3} observed miss fraction],",
        closed.total_miss_fraction, evidence.total_miss_fraction
    )?;
    writeln!(source, ")")
}

/// Writes the 50,000-job batch report as a PDF file.
///
/// # Errors
///
/// Returns an error when formatting or file output fails.
pub fn write_batch_report_pdf(
    path: &Path,
    summaries: &[BatchSloSummary],
    images: &[ImageManifestEntry],
) -> Result<(), ReportError> {
    let mut source = String::with_capacity(4_096);
    source.push_str(
        "#set document(title: \"Batch backlog regime\")\n#set page(paper: \"us-letter\", margin: \
         0.65in)\n#set text(font: \"Charter\", size: 10pt)\n#set heading(numbering: \"1.\")\n#set \
         par(justify: true, leading: 0.65em)\n\n= Batch backlog regime\n#text(fill: rgb(80, 80, \
         80))[A deterministic virtual-time case study]\n\n== Abstract\n\nKafka receives 50,000 \
         jobs at one time. Each job needs between one and ten minutes of handler work.\n\nThe \
         experiment varies the allowed completion time. It measures target scale, actuation \
         delay, SLO misses, and replica cost.\n\n#pagebreak()\n#set page(flipped: true)\n== \
         Experimental results\n\n",
    );
    source.push_str(
        "#table(columns: 7, stroke: none, inset: 5pt, [*SLO*], [*Target*], [*Cap*], [*Ready*], \
         [*Complete*], [*Misses*], [*Replica-hours*],",
    );
    for summary in summaries {
        writeln!(
            source,
            "[{}], [{}], [{}], [{}], [{}], [{:.3}], [{:.1}],",
            format_duration(summary.budget_micros),
            summary.target,
            summary.cap,
            format_duration(summary.actuation_micros),
            format_duration(summary.completion_micros),
            summary.miss_fraction,
            summary.replica_seconds / 3_600.0_f64
        )?;
    }
    source.push_str(
        ")\n\nThe SLO changes the loss tradeoff. Compare target scale with realized misses and \
         replica-hours.\n",
    );
    write_batch_strengths_and_limitations(&mut source, summaries)?;
    write_plot_page(
        &mut source,
        "SLO sensitivity",
        "slo-sweep.svg",
        "This plot shows how the latency objective changes target scale, completion time, misses, \
         and replica cost.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Actuation behavior",
        "actuation.svg",
        "This plot separates the initial replicas, requested target, saturation cap, and delayed \
         ready time.",
        true,
    )?;
    check_images(images)?;
    write_pdf(path, &source)
}

fn write_batch_strengths_and_limitations(
    source: &mut String,
    summaries: &[BatchSloSummary],
) -> Result<(), fmt::Error> {
    let violation_count = summaries
        .iter()
        .filter(|summary| summary.miss_fraction > summary.epsilon)
        .count();
    writeln!(
        source,
        "\n#pagebreak()\n#set page(flipped: false)\n== Strengths and limitations\n"
    )?;
    writeln!(source, "=== Algorithm strengths\n")?;
    if let (Some(first), Some(last)) = (summaries.first(), summaries.last()) {
        writeln!(
            source,
            "- The target changes from {} to {} replicas as the latency budget increases.",
            first.target, last.target
        )?;
        writeln!(
            source,
            "- Replica cost changes from {:.1} to {:.1} replica-hours across the sweep.",
            first.replica_seconds / 3_600.0_f64,
            last.replica_seconds / 3_600.0_f64
        )?;
    } else {
        writeln!(source, "- The sweep contains no result.")?;
    }
    writeln!(source, "\n=== Algorithm weaknesses or open questions\n")?;
    writeln!(
        source,
        "- {violation_count} of {} objectives exceed their declared miss allowance.",
        summaries.len()
    )?;
    writeln!(
        source,
        "- One deterministic sweep cannot establish predictive calibration."
    )?;
    writeln!(source, "\n=== Diagnostic strengths\n")?;
    writeln!(
        source,
        "- The table separates target, cap, ready time, completion time, misses, and replica cost."
    )?;
    writeln!(
        source,
        "- Each run stops when its final job settles. The completion column gives the exact \
         virtual duration."
    )?;
    Ok(())
}

/// Writes the repeated capacity calibration report.
///
/// # Errors
///
/// Returns an error when calibration data, formatting, compilation, or file
/// output fails.
pub fn write_capacity_calibration_report_pdf(
    path: &Path,
    calibration: &CapacityCalibration,
    sensitivity: &CapacitySensitivityCalibration,
) -> Result<(), ReportError> {
    let level_index = predictive_coverage_levels()
        .iter()
        .position(|level| (*level - 0.8_f64).abs() <= f64::EPSILON)
        .ok_or(ReportError::MissingCalibrationLevel)?;
    let mut source = String::with_capacity(4_096);
    source.push_str(
        "#set document(title: \"Capacity predictive calibration\")\n#set page(paper: \
         \"us-letter\", margin: 0.65in)\n#set text(font: \"Charter\", size: 10pt)\n#set \
         heading(numbering: \"1.\")\n#set par(justify: true, leading: 0.65em)\n\n= Capacity \
         predictive calibration\n#text(fill: rgb(80, 80, 80))[Independent seeded virtual-time \
         experiments]\n\n== Method\n\nEach trial uses one independent stochastic seed. Each \
         regime assertion must pass before the trial contributes evidence.\n\nThe predictive \
         interval uses only evidence that precedes its observation. Randomized ranks handle \
         discrete count distributions.\n\nThis report checks predictive calibration. It is not \
         simulation-based calibration of parameter draws.\n\n== Results at 80% stated \
         coverage\n\n#table(columns: 5, stroke: none, inset: 5pt, [*Regime*], [*Trials*], \
         [*Observations*], [*Covered*], [*Empirical*],",
    );
    for regime in calibration_regimes(calibration) {
        let mut trial_count = 0_u32;
        let mut observations = 0_u64;
        let mut covered = 0_u64;
        for trial in calibration
            .trials()
            .iter()
            .filter(|trial| trial.regime == regime)
        {
            trial_count = trial_count.saturating_add(1);
            observations = observations.saturating_add(u64::from(trial.observation_count));
            covered = covered.saturating_add(u64::from(trial.covered_counts[level_index]));
        }
        let empirical = if observations == 0 {
            0.0_f64
        } else {
            report_count_f64(covered) / report_count_f64(observations)
        };
        writeln!(
            source,
            "[{}], [{}], [{}], [{}], [{:.1}%],",
            regime.name(),
            trial_count,
            observations,
            covered,
            empirical * 100.0_f64,
        )?;
    }
    source.push_str(
        ")\n\nCoverage above 80% indicates conservative intervals. Coverage below 80% indicates \
         intervals that are too narrow or biased.\n",
    );
    if capacity_calibration_results_match(
        calibration,
        PrincipalRegime::FlatPostKnee,
        PrincipalRegime::DecliningPostKnee,
    ) {
        source.push_str(
            "\nThe flat and declining regimes produced identical calibration results. The \
             accepted evidence did not distinguish the declining throughput branch. Do not use \
             these results to validate collapse inference.\n",
        );
    }
    write_capacity_calibration_plots(&mut source)?;
    source.push_str(
        "#pagebreak()\n== Prior and grid sensitivity\n\nThe prior experiment keeps the grid \
         fixed. It changes the log-normal standard deviation by factors two, four, and \
         eight.\n\nThe grid experiment keeps the reference prior fixed. It changes the \
         peak-capacity ceiling to 640, 1,280, or 2,560 operations per second.\n",
    );
    write_sensitivity_table(&mut source, sensitivity, level_index)?;
    write_plot_page(
        &mut source,
        "Prior-width sensitivity",
        "capacity-prior-sensitivity.svg",
        "The gray line marks 80% coverage. Movement across variants measures sensitivity to prior \
         width.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Grid-ceiling sensitivity",
        "capacity-grid-sensitivity.svg",
        "The gray line marks 80% coverage. Movement across variants measures sensitivity to the \
         grid ceiling.",
        true,
    )?;
    write_pdf(path, &source)
}

fn write_capacity_calibration_plots(source: &mut String) -> Result<(), fmt::Error> {
    write_plot_page(
        source,
        "Coverage by stated level",
        "capacity-coverage.svg",
        "The gray diagonal marks exact calibration. Each colored line represents one operating \
         regime.",
        true,
    )?;
    write_plot_page(
        source,
        "Randomized predictive ranks",
        "capacity-ranks.svg",
        "A calibrated discrete forecast produces uniform randomized ranks. The gray line marks \
         the expected count per decile.",
        true,
    )?;
    write_plot_page(
        source,
        "Forecast error and uncertainty",
        "capacity-error-uncertainty.svg",
        "Each point represents one seeded trial. The gray diagonal separates forecast error from \
         the stated 80% interval width.",
        true,
    )?;
    write_plot_page(
        source,
        "Posterior contraction",
        "capacity-contraction.svg",
        "Each point compares the final 10% to 90% capacity width with its prior width. One means \
         that the marginal width collapsed to one grid value.",
        true,
    )?;
    Ok(())
}

/// Writes the repeated demand calibration report.
///
/// # Errors
///
/// Returns an error when calibration data, formatting, compilation, or file
/// output fails.
pub fn write_demand_calibration_report_pdf(
    path: &Path,
    calibration: &DemandCalibration,
) -> Result<(), ReportError> {
    let level_index = predictive_coverage_levels()
        .iter()
        .position(|level| (*level - 0.8_f64).abs() <= f64::EPSILON)
        .ok_or(ReportError::MissingCalibrationLevel)?;
    let mut source = String::with_capacity(4_096);
    source.push_str(
        "#set document(title: \"Demand predictive calibration\")\n#set page(paper: \"us-letter\", \
         margin: 0.65in)\n#set text(font: \"Charter\", size: 10pt)\n#set heading(numbering: \
         \"1.\")\n#set par(justify: true, leading: 0.65em)\n\n= Demand predictive \
         calibration\n#text(fill: rgb(80, 80, 80))[Independent seeded virtual-time \
         experiments]\n\n== Method\n\nEach trial uses one independent stochastic seed. A regime \
         assertion must pass before the trial contributes evidence.\n\nEach forecast uses only \
         prior evidence. The Gamma-Poisson model gives a negative-binomial count forecast. \
         Randomized ranks account for discrete observations.\n\n== Results at 80% stated \
         coverage\n\n#table(
         columns: 6, stroke: none, inset: 4pt, [*Regime*], [*Evidence*], [*Covered*], \
         [*Empirical*], [*Error*], [*Width*],",
    );
    for regime in demand_calibration_regimes(calibration) {
        let mut observations = 0_u64;
        let mut covered = 0_u64;
        let mut error = 0.0_f64;
        let mut width = 0.0_f64;
        let mut trial_count = 0_u32;
        for trial in calibration
            .trials()
            .iter()
            .filter(|trial| trial.regime == regime)
        {
            let count = u64::from(trial.observation_count);
            observations = observations.saturating_add(count);
            covered = covered.saturating_add(u64::from(trial.covered_counts[level_index]));
            error += trial.mean_absolute_error;
            width += trial.mean_uncertainty;
            trial_count = trial_count.saturating_add(1);
        }
        let empirical = if observations == 0 {
            0.0_f64
        } else {
            report_count_f64(covered) / report_count_f64(observations)
        };
        let denominator = f64::from(trial_count.max(1));
        writeln!(
            source,
            "[{}], [{}], [{}], [{:.1}%], [{:.1}], [{:.1}],",
            regime.name(),
            observations,
            covered,
            empirical * 100.0_f64,
            error / denominator,
            width / denominator,
        )?;
    }
    source.push_str(
        ")\n\nCoverage below the stated level means that the forecast is biased, too narrow, or \
         both. Evidence counts expose missing reporter data.\n",
    );
    write_plot_page(
        &mut source,
        "Coverage by regime",
        "demand-coverage.svg",
        "The gray diagonal marks exact calibration. Each panel isolates one operating regime.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Randomized predictive ranks",
        "demand-ranks.svg",
        "A calibrated count forecast produces uniform randomized ranks. The gray line marks the \
         expected count per decile.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Forecast error and uncertainty",
        "demand-error-uncertainty.svg",
        "Each point represents one seeded trial. Error that exceeds interval width indicates \
         overconfidence or bias.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Posterior contraction",
        "demand-contraction.svg",
        "Each point compares the final relative rate width with its prior relative width.",
        true,
    )?;
    source.push_str(
        "#pagebreak()\n== Diagnostic conclusion\n\nThe forecast fails calibration in every tested \
         regime. The failure is large and systematic. Do not interpret narrow posterior bands as \
         accurate confidence.\n\nThe rolling model retains 64 evidence windows. It also retains \
         the complete one-second prior exposure. At high rates, this fixed exposure biases the \
         posterior rate downward after old evidence leaves the window.\n\nThe missing-reporter \
         regime accepted only sparse evidence. Its table row separates missing evidence from a \
         zero-count observation. This report records the defect. It does not change the \
         algorithm.\n",
    );
    write_pdf(path, &source)
}

/// Writes the repeated partition-shape calibration report.
///
/// # Errors
///
/// Returns an error when calibration data, formatting, compilation, or file
/// output fails.
pub fn write_partition_calibration_report_pdf(
    path: &Path,
    calibration: &PartitionCalibration,
) -> Result<(), ReportError> {
    let level_index = predictive_coverage_levels()
        .iter()
        .position(|level| (*level - 0.8_f64).abs() <= f64::EPSILON)
        .ok_or(ReportError::MissingCalibrationLevel)?;
    let mut source = String::with_capacity(4_096);
    source.push_str(
        "#set document(title: \"Partition predictive calibration\")\n#set page(paper: \
         \"us-letter\", margin: 0.65in)\n#set text(font: \"Charter\", size: 10pt)\n#set \
         heading(numbering: \"1.\")\n#set par(justify: true, leading: 0.65em)\n\n= Partition \
         predictive calibration\n#text(fill: rgb(80, 80, 80))[Independent seeded virtual-time \
         experiments]\n\n== Method\n\nEach observation is one accepted partition assignment. Each \
         forecast uses expected Dirichlet shares from before that tick's evidence.\n\nCoverage \
         uses a highest-density credible set. This set adds partitions in decreasing probability \
         order. Error uses negative log probability. Uncertainty uses predictive entropy.\n\n== \
         Results at 80% stated coverage\n\n#table(columns: 6, stroke: none, inset: 4pt, \
         [*Regime*], [*Evidence*], [*Covered*], [*Empirical*], [*Log loss*], [*Entropy*],",
    );
    for regime in partition_calibration_regimes(calibration) {
        let mut observations = 0_u64;
        let mut covered = 0_u64;
        let mut error = 0.0_f64;
        let mut entropy = 0.0_f64;
        let mut trial_count = 0_u32;
        for trial in calibration
            .trials()
            .iter()
            .filter(|trial| trial.regime == regime)
        {
            observations = observations.saturating_add(u64::from(trial.observation_count));
            covered = covered.saturating_add(u64::from(trial.covered_counts[level_index]));
            error += trial.mean_log_loss;
            entropy += trial.mean_entropy;
            trial_count = trial_count.saturating_add(1);
        }
        let empirical = if observations == 0 {
            0.0_f64
        } else {
            report_count_f64(covered) / report_count_f64(observations)
        };
        let denominator = f64::from(trial_count.max(1));
        writeln!(
            source,
            "[{}], [{}], [{}], [{:.1}%], [{:.3}], [{:.3}],",
            regime.name(),
            observations,
            covered,
            empirical * 100.0_f64,
            error / denominator,
            entropy / denominator,
        )?;
    }
    source.push_str(
        ")\n\nUniform traffic should follow the stated coverage. Concentrated traffic should \
         reduce entropy and log loss after accepted evidence.\n",
    );
    write_plot_page(
        &mut source,
        "Coverage by regime",
        "partition-coverage.svg",
        "The gray diagonal marks exact calibration. Each panel isolates one operating regime.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Randomized predictive ranks",
        "partition-ranks.svg",
        "Uniform ranks indicate calibrated partition probabilities. Partition order defines the \
         randomized cumulative transform.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Log loss and entropy",
        "partition-error-uncertainty.svg",
        "Each point compares realized negative log probability with predictive entropy.",
        true,
    )?;
    write_plot_page(
        &mut source,
        "Entropy contraction",
        "partition-contraction.svg",
        "Each point compares final posterior entropy with uniform prior entropy.",
        true,
    )?;
    source.push_str(
        "#pagebreak()\n== Diagnostic conclusion\n\nUniform regimes achieve approximately 80% \
         empirical coverage for an 80% highest-density set. The hot-partition regime achieves \
         100% because its posterior concentrates on the observed partition.\n\nThe exported model \
         view contains expected shares only. It omits Dirichlet concentration. The simulator \
         therefore cannot plot the posterior distribution of the hottest share without a new \
         model diagnostic.\n",
    );
    write_pdf(path, &source)
}

/// Writes the repeated actuation lead-time calibration report.
///
/// # Errors
///
/// Returns an error when calibration data, formatting, compilation, or file
/// output fails.
pub fn write_lead_time_calibration_report_pdf(
    path: &Path,
    calibration: &LeadTimeCalibration,
) -> Result<(), ReportError> {
    let level_index = predictive_coverage_levels()
        .iter()
        .position(|level| (*level - 0.8_f64).abs() <= f64::EPSILON)
        .ok_or(ReportError::MissingCalibrationLevel)?;
    let mut source = String::with_capacity(6_144);
    source.push_str(
        "#set document(title: \"Lead-time predictive calibration\")\n#set page(paper: \
         \"us-letter\", margin: 0.65in)\n#set text(font: \"Charter\", size: 10pt)\n#set \
         heading(numbering: \"1.\")\n#set par(justify: true, leading: 0.65em)\n\n= Lead-time \
         predictive calibration\n#text(fill: rgb(80, 80, 80))[Independent seeded virtual-time \
         experiments]\n\n== Method\n\nEach completed transition uses the posterior predictive \
         distribution from before its evidence. The distribution mixes all log-normal grid cells. \
         Censored transitions contribute to model updates but not rank histograms.\n\nScale-up \
         and scale-down use separate factors. The report also separates their results.\n\n== \
         Results at 80% stated coverage\n\n#table(columns: 6, stroke: none, inset: 4pt, \
         [*Regime*], [*Direction*], [*Completed*], [*Censored*], [*Covered*], [*Empirical*],",
    );
    let mut total_censored = 0_u64;
    for regime in lead_time_calibration_regimes(calibration) {
        for direction in [TransitionDirection::Up, TransitionDirection::Down] {
            let mut completed = 0_u64;
            let mut censored = 0_u64;
            let mut covered = 0_u64;
            for trial in calibration
                .trials()
                .iter()
                .filter(|trial| trial.regime == regime && trial.direction == direction)
            {
                completed = completed.saturating_add(u64::from(trial.observation_count));
                censored = censored.saturating_add(u64::from(trial.censored_count));
                covered = covered.saturating_add(u64::from(trial.covered_counts[level_index]));
            }
            total_censored = total_censored.saturating_add(censored);
            if completed == 0 {
                writeln!(
                    source,
                    "[{}], [{}], [0], [{}], [0], [—],",
                    regime.name(),
                    transition_direction_name(direction),
                    censored,
                )?;
            } else {
                let empirical = report_count_f64(covered) / report_count_f64(completed);
                writeln!(
                    source,
                    "[{}], [{}], [{}], [{}], [{}], [{:.1}%],",
                    regime.name(),
                    transition_direction_name(direction),
                    completed,
                    censored,
                    covered,
                    empirical * 100.0_f64,
                )?;
            }
        }
    }
    source.push_str(
        ")\n\nThe experiment contains few completed transitions. Treat coverage estimates as \
         sparse evidence. The broad predictive intervals cover every completed transition.\n",
    );
    write_lead_time_plot_pages(&mut source)?;
    writeln!(
        source,
        "#pagebreak()\n== Diagnostic conclusion\n\nAll completed transitions fall inside the \
         stated 80% interval. The predictive intervals remain broad for the simulated 30 to 90 \
         second launch distribution.\n\nThe evidence includes {total_censored} right-censored \
         transitions. Rank histograms exclude them. A survival calibration view remains necessary \
         for complete censoring diagnostics."
    )?;
    write_pdf(path, &source)
}

fn write_lead_time_plot_pages(source: &mut String) -> Result<(), fmt::Error> {
    for (direction, name) in [
        (TransitionDirection::Up, "up"),
        (TransitionDirection::Down, "down"),
    ] {
        let title = transition_direction_name(direction);
        write_plot_page(
            source,
            &format!("Scale-{title} coverage"),
            &format!("lead-time-{name}-coverage.svg"),
            "The gray diagonal marks exact calibration. Each panel isolates one operating regime.",
            true,
        )?;
        write_plot_page(
            source,
            &format!("Scale-{title} predictive ranks"),
            &format!("lead-time-{name}-ranks.svg"),
            "Uniform ranks indicate a calibrated continuous forecast. Sparse bars reflect few \
             completed transitions.",
            true,
        )?;
        write_plot_page(
            source,
            &format!("Scale-{title} error and uncertainty"),
            &format!("lead-time-{name}-error-uncertainty.svg"),
            "Each point compares median error with the 80% predictive interval width.",
            true,
        )?;
        write_plot_page(
            source,
            &format!("Scale-{title} posterior contraction"),
            &format!("lead-time-{name}-contraction.svg"),
            "Each point measures the one-replica parameter posterior. Evidence for other replica \
             deltas updates separate factors.",
            true,
        )?;
    }
    Ok(())
}

fn write_sensitivity_table(
    source: &mut String,
    sensitivity: &CapacitySensitivityCalibration,
    level_index: usize,
) -> Result<(), fmt::Error> {
    source.push_str(
        "#table(columns: 4, stroke: none, inset: 4pt, [*Regime*], [*Variant*], [*Observations*], \
         [*Coverage*],",
    );
    for regime in calibration_sensitivity_regimes(sensitivity) {
        for variant in CapacitySensitivity::ALL {
            let mut observations = 0_u64;
            let mut covered = 0_u64;
            for trial in sensitivity
                .trials()
                .iter()
                .filter(|trial| trial.calibration.regime == regime && trial.sensitivity == variant)
            {
                observations =
                    observations.saturating_add(u64::from(trial.calibration.observation_count));
                covered = covered
                    .saturating_add(u64::from(trial.calibration.covered_counts[level_index]));
            }
            let empirical = if observations == 0 {
                0.0_f64
            } else {
                report_count_f64(covered) / report_count_f64(observations)
            };
            writeln!(
                source,
                "[{}], [{}], [{}], [{:.1}%],",
                regime.name(),
                variant.name(),
                observations,
                empirical * 100.0_f64,
            )?;
        }
    }
    source.push_str(")\n");
    Ok(())
}

fn calibration_regimes(calibration: &CapacityCalibration) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in calibration.trials() {
        if !regimes.contains(&trial.regime) {
            regimes.push(trial.regime);
        }
    }
    regimes
}

fn demand_calibration_regimes(calibration: &DemandCalibration) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in calibration.trials() {
        if !regimes.contains(&trial.regime) {
            regimes.push(trial.regime);
        }
    }
    regimes
}

fn partition_calibration_regimes(calibration: &PartitionCalibration) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in calibration.trials() {
        if !regimes.contains(&trial.regime) {
            regimes.push(trial.regime);
        }
    }
    regimes
}

fn lead_time_calibration_regimes(calibration: &LeadTimeCalibration) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in calibration.trials() {
        if !regimes.contains(&trial.regime) {
            regimes.push(trial.regime);
        }
    }
    regimes
}

const fn transition_direction_name(direction: TransitionDirection) -> &'static str {
    match direction {
        TransitionDirection::Up => "up",
        TransitionDirection::Down => "down",
    }
}

fn calibration_sensitivity_regimes(
    sensitivity: &CapacitySensitivityCalibration,
) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in sensitivity.trials() {
        if !regimes.contains(&trial.calibration.regime) {
            regimes.push(trial.calibration.regime);
        }
    }
    regimes
}

fn capacity_calibration_results_match(
    calibration: &CapacityCalibration,
    left_regime: PrincipalRegime,
    right_regime: PrincipalRegime,
) -> bool {
    let mut left = calibration
        .trials()
        .iter()
        .filter(|trial| trial.regime == left_regime);
    let mut right = calibration
        .trials()
        .iter()
        .filter(|trial| trial.regime == right_regime);
    let mut matched = false;
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) => {
                matched = true;
                if left.seed != right.seed
                    || left.observation_count != right.observation_count
                    || left.covered_counts != right.covered_counts
                    || left.rank_counts != right.rank_counts
                    || left
                        .mean_absolute_error_per_second
                        .partial_cmp(&right.mean_absolute_error_per_second)
                        != Some(Ordering::Equal)
                    || left
                        .mean_uncertainty_per_second
                        .partial_cmp(&right.mean_uncertainty_per_second)
                        != Some(Ordering::Equal)
                    || left
                        .capacity_contraction
                        .partial_cmp(&right.capacity_contraction)
                        != Some(Ordering::Equal)
                {
                    return false;
                }
            }
            (None, None) => return matched,
            _ => return false,
        }
    }
}

fn report_count_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).unwrap_or(0);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(0);
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn write_pdf(path: &Path, source: &str) -> Result<(), ReportError> {
    let root = path.parent().ok_or(ReportError::MissingParent)?;
    let engine = TypstEngine::builder()
        .main_file(source)
        .search_fonts_with(TypstKitFontOptions::default().include_system_fonts(true))
        .with_file_system_resolver(root)
        .build();
    let document = engine
        .compile()
        .output
        .map_err(|diagnostics| ReportError::Compile(format!("{diagnostics:?}")))?;
    let pdf = typst_pdf::pdf(&document, &PdfOptions::default())
        .map_err(|diagnostics| ReportError::Pdf(format!("{diagnostics:?}")))?;
    fs::write(path, pdf)?;
    Ok(())
}

fn write_header(source: &mut String, report: &RegimeReport<'_>) -> Result<(), fmt::Error> {
    writeln!(
        source,
        "#set document(title: \"{} regime\")\n#set page(paper: \"us-letter\", margin: \
         0.65in)\n#set text(font: \"Charter\", size: 10pt)\n#set heading(numbering: \"1.\")\n#set \
         par(justify: true, leading: 0.65em)\n",
        report.regime.name()
    )?;
    writeln!(source, "= {} regime", title(report.regime))?;
    writeln!(
        source,
        "#text(fill: rgb(80, 80, 80))[A deterministic virtual-time case study]"
    )?;
    let metadata = report.closed_loop.metadata;
    writeln!(
        source,
        "\nCommit: `{}`. Model version: `{}`. Generator version: `{}`. Seed: {}. Duration: {}.",
        metadata.commit,
        metadata.model_version,
        metadata.generator_version,
        metadata.seed,
        format_duration(metadata.duration_micros),
    )?;
    writeln!(
        source,
        "\nArtifact identity: source {}, version {}, stream {}.",
        metadata.artifact_identity.source(),
        metadata.artifact_identity.version(),
        metadata.artifact_identity.random_stream(),
    )?;
    writeln!(source, "\n=== Model artifacts\n")?;
    writeln!(
        source,
        "#table(columns: 5, stroke: none, inset: 4pt, [*Family*], [*Schema*], [*Source*], [*Lower \
         tail*], [*Upper tail*],"
    )?;
    for artifact in report.closed_loop.controller.artifacts() {
        let lower = artifact
            .coverage()
            .iter()
            .map(|record| record.lower_tail_probability())
            .fold(0.0_f64, f64::max);
        let upper = artifact
            .coverage()
            .iter()
            .map(|record| record.upper_tail_probability())
            .fold(0.0_f64, f64::max);
        writeln!(
            source,
            "[{}], [{}], [{}], [{lower:.2e}], [{upper:.2e}],",
            artifact_name(artifact.kind()),
            artifact.schema_version(),
            artifact.identity().source(),
        )?;
    }
    writeln!(source, ")")?;
    writeln!(source, "\n== Abstract\n")?;
    writeln!(source, "{}", situation(report.regime))?;
    writeln!(source, "\nThe experiment asks: {}", question(report.regime))?;
    writeln!(
        source,
        "\nThe expected response is: {}",
        expectation(report.regime)
    )?;
    Ok(())
}

const fn artifact_name(kind: PriorArtifactKind) -> &'static str {
    match kind {
        PriorArtifactKind::Capacity => "capacity",
        PriorArtifactKind::Arrival => "arrival",
        PriorArtifactKind::Reliability => "reliability",
        PriorArtifactKind::Launch => "launch",
        PriorArtifactKind::Rebalance => "rebalance",
    }
}

fn write_summary(source: &mut String, summary: ReportSummary) -> Result<(), fmt::Error> {
    writeln!(source, "\n== Observed response\n")?;
    writeln!(
        source,
        "The plant accepted {} arrivals and completed {} useful events. Backlog peaked at {} \
         events.",
        summary.arrivals, summary.completions, summary.peak_backlog
    )?;
    writeln!(
        source,
        "\nActual replicas ranged from {} to {}. The peak controller target was {}.",
        summary.minimum_replicas, summary.maximum_replicas, summary.maximum_target
    )?;
    writeln!(
        source,
        "\nThe smallest reported controller cap was {} replicas. {} events missed the SLO. The \
         total observed miss fraction was {:.3}.",
        summary.minimum_cap, summary.total_misses, summary.total_miss_fraction
    )?;
    writeln!(
        source,
        "\nThe largest interval miss fraction was {:.3}.",
        summary.maximum_miss_fraction
    )?;
    writeln!(
        source,
        "\nThe model accepted {} passive resource windows.",
        summary.resource_windows
    )?;
    writeln!(source, "\n== Run assessment\n")?;
    write_outcome_assessment(source, summary)
}

fn write_design(source: &mut String, report: &RegimeReport<'_>) -> Result<(), fmt::Error> {
    writeln!(source, "\n== Experimental design\n")?;
    writeln!(
        source,
        "The simulator controls all time. The run lasted {} and stopped because {}.",
        format_duration(report.closed_loop.stop.at_micros),
        stop_reason(report.closed_loop.stop.reason)
    )?;
    writeln!(
        source,
        "\nThe latency objective was {:.1} seconds.",
        crate::u64_to_f64(report.regime.budget_micros()) / 1_000_000.0_f64
    )
}

fn write_outcome_assessment(source: &mut String, summary: ReportSummary) -> Result<(), fmt::Error> {
    if summary.final_backlog == 0 {
        writeln!(source, "The plant stopped with no queued events.")?;
    } else {
        writeln!(
            source,
            "The plant stopped with {} queued events.",
            summary.final_backlog
        )?;
    }
    if summary.minimum_cap == summary.maximum_cap {
        writeln!(
            source,
            "\nThe controller cap stayed at {} replicas.",
            summary.minimum_cap
        )?;
    } else {
        writeln!(
            source,
            "\nThe saturation cap ranged from {} to {} replicas.",
            summary.minimum_cap, summary.maximum_cap
        )?;
    }
    writeln!(
        source,
        "\nThe final no-knee posterior probability was {:.3}.",
        summary.final_no_knee_probability
    )?;
    if summary.total_miss_fraction > 0.01_f64 {
        writeln!(
            source,
            "\nThe total observed miss fraction exceeded the declared 1% allowance."
        )?;
    } else {
        writeln!(
            source,
            "\nThe total observed miss fraction stayed within the declared 1% allowance."
        )?;
    }
    if summary.maximum_miss_fraction > 0.01_f64 {
        writeln!(
            source,
            "\nAt least one interval exceeded the allowance. Inspect demand changes and actuation \
             time before you classify that loss."
        )?;
    }
    write_reading_guide(source)?;
    Ok(())
}

fn write_reading_guide(source: &mut String) -> Result<(), fmt::Error> {
    writeln!(source, "\n== How to read this report\n")?;
    writeln!(
        source,
        "Read the plots from evidence to outcome. Do not treat a posterior change as proof of \
         calibration."
    )?;
    writeln!(
        source,
        "\nDark violet shows low posterior mass. Yellow shows high posterior mass within one \
         panel."
    )?;
    writeln!(
        source,
        "\nEach heatmap time slice sums to one. The snapshot pages show the prior, largest \
         update, and final posterior."
    )?;
    Ok(())
}

fn write_strengths_and_limitations(
    source: &mut String,
    report: &RegimeReport<'_>,
    summary: ReportSummary,
) -> Result<(), fmt::Error> {
    writeln!(source, "\n#pagebreak()\n== Strengths and limitations\n")?;
    writeln!(source, "=== Algorithm strengths\n")?;
    let placement_bound = report.regime == PrincipalRegime::HotPartition
        && placement_constraint_binds(report.closed_loop.controller);
    if placement_bound {
        writeln!(
            source,
            "- The controller reports unavoidable placement loss and avoids useless replica \
             growth."
        )?;
        if downscaled_with_backlog(report.closed_loop.trace) {
            writeln!(
                source,
                "- The actual count reached one while backlog remained. Extra replicas did not \
                 add partition capacity."
            )?;
        }
    } else if summary.arrivals == 0 {
        writeln!(
            source,
            "- The controller preserved a safe idle state without unnecessary work."
        )?;
    } else if summary.final_backlog == 0 && summary.total_miss_fraction <= 0.01_f64 {
        writeln!(
            source,
            "- The run cleared its observed backlog and stayed within the declared miss allowance."
        )?;
    } else {
        writeln!(
            source,
            "- This run provides no strong outcome evidence for the algorithm."
        )?;
    }
    writeln!(source, "\n=== Algorithm weaknesses or open questions\n")?;
    if summary.total_miss_fraction > 0.01_f64 && !placement_bound {
        writeln!(
            source,
            "- The total observed miss fraction exceeded the declared allowance."
        )?;
    }
    if summary.maximum_miss_fraction > 0.01_f64 && !placement_bound {
        writeln!(
            source,
            "- At least one interval exceeded the allowance. The report does not yet classify \
             avoidable and unavoidable misses."
        )?;
    }
    if summary.final_backlog > 0 && !placement_bound {
        writeln!(
            source,
            "- The run stopped with {} queued events.",
            summary.final_backlog
        )?;
    }
    if summary.resource_windows == 0 && report.capacity_evidence.is_some() {
        writeln!(
            source,
            "- The primary run accepted no passive resource window. It cannot identify a capacity \
             response."
        )?;
    }
    if let Some(experiment) = report.capacity_evidence {
        let evidence = CapacityEvidenceSummary::from_controller(experiment.controller);
        if evidence.declining_steps == 0 && report.regime == PrincipalRegime::DecliningPostKnee {
            writeln!(
                source,
                "- The additional run observed no throughput decrease at higher concurrency. It \
                 did not identify the declared declining branch."
            )?;
        }
    }
    if let Some(limitation) = known_limitation(report.regime) {
        writeln!(source, "- {limitation}")?;
    }
    writeln!(
        source,
        "- One deterministic run cannot establish predictive calibration. Use the repeated \
         calibration report for that claim."
    )?;
    writeln!(source, "\n=== Diagnostic strengths\n")?;
    if summary.final_backlog > 0 {
        writeln!(
            source,
            "- The report preserves the residual backlog instead of hiding it."
        )?;
    }
    if summary.resource_windows > 0 {
        writeln!(
            source,
            "- The report links {} passive resource windows to posterior updates.",
            summary.resource_windows
        )?;
    }
    Ok(())
}

const fn known_limitation(regime: PrincipalRegime) -> Option<&'static str> {
    match regime {
        PrincipalRegime::ShortBurst => Some(
            "Initial replicas clear the burst before a new replica can launch. The outcome does \
             not prove predictive scale-up.",
        ),
        PrincipalRegime::SeasonalWaves => Some(
            "Initial replicas absorb the waves. The run does not prove that the controller \
             predicts a later wave.",
        ),
        PrincipalRegime::TimerWave => Some(
            "The core receives no pre-release timer forecast. This run cannot test predictive \
             timer scaling.",
        ),
        PrincipalRegime::HotSerializedKey => Some(
            "This run cannot distinguish key serialization from indivisible partition placement. \
             Compare it with an equal many-key control.",
        ),
        PrincipalRegime::TransientFailures => Some(
            "The adapter separates Normal backlog from known Failure timers. This report does not \
             include its paired failure-free control.",
        ),
        PrincipalRegime::PermanentRejections => Some(
            "The retry factor treats permanent failure as final. The report lacks the separate \
             application-correctness posterior.",
        ),
        PrincipalRegime::SnapshotFaults | PrincipalRegime::MissingReporter => Some(
            "The core does not export the decision change caused by missing evidence. Snapshot \
             age alone cannot prove uncertainty growth.",
        ),
        PrincipalRegime::HistoricalMissing => Some(
            "The core does not use the supplied historical series. This run cannot validate \
             history-aware inference.",
        ),
        PrincipalRegime::RebalanceStorm => Some(
            "External actions create the storm. This run does not prove that the controller \
             suppresses harmful actuation.",
        ),
        PrincipalRegime::HandlerContention => Some(
            "The plant changes handler duration. The report must show a matching handler \
             posterior change before it credits the algorithm.",
        ),
        PrincipalRegime::ReplicaCeiling => Some(
            "The configuration ceiling is not a learned saturation cap. Keep configuration and \
             model limits separate.",
        ),
        _ => None,
    }
}

fn placement_constraint_binds(controller: &ControllerTrace) -> bool {
    (0..controller.len()).any(|index| {
        let Some(sample) = controller.sample(index) else {
            return false;
        };
        let Some(costs) = controller.decision_expected_costs(index) else {
            return false;
        };
        let Some((&one_replica, &maximum_replicas)) = costs.first().zip(costs.last()) else {
            return false;
        };
        !sample.hold
            && sample.target == 1
            && one_replica > 0.0_f64
            && (one_replica - maximum_replicas).abs() <= 1.0e-9_f64
    })
}

fn downscaled_with_backlog(trace: &MetricTrace) -> bool {
    trace
        .replicas
        .iter()
        .zip(&trace.backlog)
        .any(|(&replicas, &backlog)| replicas == 1 && backlog > 0)
}

fn write_capacity_evidence_summary(
    source: &mut String,
    experiment: ExperimentReport<'_>,
) -> Result<(), fmt::Error> {
    let summary = CapacityEvidenceSummary::from_controller(experiment.controller);
    writeln!(
        source,
        "\n#pagebreak()\n#set page(flipped: false)\n== Additional capacity evidence\n"
    )?;
    writeln!(
        source,
        "The capacity-evidence experiment lasted {} and stopped because {}.",
        format_duration(experiment.stop.at_micros),
        stop_reason(experiment.stop.reason)
    )?;
    writeln!(
        source,
        "\nThe model accepted {} passive resource windows. Tested concurrency ranged from {:.1} \
         to {:.1} operations.",
        summary.resource_windows, summary.minimum_concurrency, summary.maximum_concurrency
    )?;
    writeln!(
        source,
        "\nObserved attempt throughput ranged from {:.1} to {:.1} operations per second.",
        summary.minimum_throughput, summary.maximum_throughput
    )?;
    writeln!(
        source,
        "\n{} adjacent windows show lower throughput after concurrency increased.",
        summary.declining_steps
    )?;
    if summary.declining_steps == 0 {
        writeln!(
            source,
            "\nThe accepted evidence does not identify a declining throughput branch."
        )?;
    }
    write_capacity_diagnostic(source, experiment.controller)?;
    Ok(())
}

fn write_capacity_diagnostic(
    source: &mut String,
    controller: &ControllerTrace,
) -> Result<(), fmt::Error> {
    let mut first_window = None;
    let mut last_window = None;
    let mut maximum_concurrency = None;
    let mut minimum_throughput = None;
    let mut largest_belief_change = None;
    let mut previous_no_knee = None;
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let CapacityEvidenceSample::Window(window) = sample.capacity_evidence else {
            continue;
        };
        first_window.get_or_insert(index);
        last_window = Some(index);
        if maximum_concurrency.is_none_or(|current| {
            controller.sample(current).is_some_and(|current_sample| {
                let CapacityEvidenceSample::Window(current_window) =
                    current_sample.capacity_evidence
                else {
                    return false;
                };
                window.concurrency > current_window.concurrency
            })
        }) {
            maximum_concurrency = Some(index);
        }
        if minimum_throughput.is_none_or(|current| {
            controller.sample(current).is_some_and(|current_sample| {
                let CapacityEvidenceSample::Window(current_window) =
                    current_sample.capacity_evidence
                else {
                    return false;
                };
                window.throughput_per_second() < current_window.throughput_per_second()
            })
        }) {
            minimum_throughput = Some(index);
        }
        let change = previous_no_knee.map_or(0.0_f64, |previous: f64| {
            (sample.no_knee_probability - previous).abs()
        });
        if largest_belief_change.is_none_or(|(_, largest_change)| change > largest_change) {
            largest_belief_change = Some((index, change));
        }
        previous_no_knee = Some(sample.no_knee_probability);
    }
    let Some(last_window) = last_window else {
        return Ok(());
    };
    writeln!(source, "\n== Capacity evidence summary\n")?;
    writeln!(
        source,
        "The table shows representative windows. The evidence plots show the complete time \
         series.\n"
    )?;
    writeln!(
        source,
        "#table(columns: 5, stroke: none, inset: 4pt, [*Window*], [*Time*], [*Concurrency*], \
         [*Throughput*], [*No-knee probability*],"
    )?;
    let rows = [
        ("First", first_window),
        (
            "Largest belief change",
            largest_belief_change.map(|(index, _)| index),
        ),
        ("Highest concurrency", maximum_concurrency),
        ("Lowest throughput", minimum_throughput),
        ("Last", Some(last_window)),
    ];
    for (row, (label, index)) in rows.iter().enumerate() {
        let Some(index) = index else {
            continue;
        };
        if rows[..row]
            .iter()
            .any(|(_, previous)| previous == &Some(*index))
        {
            continue;
        }
        write_capacity_audit_row(source, label, controller, *index)?;
    }
    writeln!(source, ")\n")?;
    let Some(final_index) = controller.len().checked_sub(1) else {
        return Ok(());
    };
    write_capacity_state_row(source, "Last resource window", controller, last_window)?;
    write_capacity_state_row(source, "Final state", controller, final_index)
}

fn write_capacity_audit_row(
    source: &mut String,
    label: &str,
    controller: &ControllerTrace,
    index: usize,
) -> Result<(), fmt::Error> {
    let Some(sample) = controller.sample(index) else {
        return Ok(());
    };
    let CapacityEvidenceSample::Window(window) = sample.capacity_evidence else {
        return Ok(());
    };
    writeln!(
        source,
        "[{label}], [{}], [{:.1}], [{:.1}], [{:.3}],",
        format_duration(sample.at_micros),
        window.concurrency,
        window.throughput_per_second(),
        sample.no_knee_probability,
    )
}

fn write_capacity_state_row(
    source: &mut String,
    label: &str,
    controller: &ControllerTrace,
    index: usize,
) -> Result<(), fmt::Error> {
    let Some(sample) = controller.sample(index) else {
        return Ok(());
    };
    let knee = posterior_quantiles(controller, PosteriorQuery::Knee, index);
    writeln!(
        source,
        "{label}: time {}, target {}, cap {}, capacity {:.1}/{:.1}/{:.1} operations per second, \
         and knee {:.1}/{:.1}/{:.1} operations.",
        format_duration(sample.at_micros),
        sample.target,
        sample.cap,
        sample.capacity_low_per_second,
        sample.capacity_median_per_second,
        sample.capacity_high_per_second,
        knee[0],
        knee[1],
        knee[2],
    )
}

fn posterior_quantiles(
    controller: &ControllerTrace,
    query: PosteriorQuery,
    index: usize,
) -> [f64; 3] {
    let Some(values) = controller.posterior_values(query) else {
        return [f64::NAN; 3];
    };
    let Some(probabilities) = controller.posterior(query, index) else {
        return [f64::NAN; 3];
    };
    let fallback = values.last().copied().unwrap_or(f64::NAN);
    let mut quantiles = [fallback; 3];
    let thresholds = [0.1_f64, 0.5_f64, 0.9_f64];
    let mut threshold = 0_usize;
    let mut cumulative = 0.0_f64;
    for (&value, &probability) in values.iter().zip(probabilities) {
        cumulative += probability;
        while threshold < thresholds.len() && cumulative >= thresholds[threshold] {
            quantiles[threshold] = value;
            threshold += 1;
        }
    }
    quantiles
}

fn write_experiment_figures(
    source: &mut String,
    directory: &str,
    experiment: ExperimentReport<'_>,
) -> Result<(), fmt::Error> {
    writeln!(source, "\n#pagebreak()\n== Evidence\n")?;
    for index in [0_usize, 6, 8, 9, 10, 11, 12, 13, 14, 17] {
        write_manifest_figure(source, directory, STORY_FIGURES[index], experiment)?;
    }
    writeln!(source, "\n#pagebreak()\n== Belief\n")?;
    writeln!(
        source,
        "Each heatmap column sums to one. Color shows relative probability mass within that \
         factor.\n"
    )?;
    for (index, factor) in MODEL_FACTORS.into_iter().enumerate() {
        let belief_file = format!("beliefs/{}.svg", factor.file);
        let belief_visible = image_is_visible(experiment.images, &belief_file);
        let snapshot_file = format!("snapshots/{}.svg", factor.file);
        let snapshot_visible = factor_changed(experiment.controller, index)
            && image_is_visible(experiment.images, &snapshot_file);
        if !write_factor_header(source, factor, belief_visible || snapshot_visible)? {
            continue;
        }
        if belief_visible {
            write_figure(
                source,
                &format!("{directory}/{belief_file}"),
                factor.belief_caption,
                experiment.stop,
            )?;
        }
        if snapshot_visible {
            write_figure(
                source,
                &format!("{directory}/{snapshot_file}"),
                "The snapshots compare the prior, largest accepted update, and final posterior. \
                 Gray marks 10% and 90%. Orange marks 50%.",
                experiment.stop,
            )?;
        }
        writeln!(source, "]\n")?;
    }
    writeln!(source, "\n#pagebreak()\n== Decision\n")?;
    for index in [15_usize, 16] {
        write_manifest_figure(source, directory, STORY_FIGURES[index], experiment)?;
    }
    writeln!(source, "\n#pagebreak()\n== Outcome\n")?;
    for index in [1_usize, 2, 3, 4, 5, 18] {
        write_manifest_figure(source, directory, STORY_FIGURES[index], experiment)?;
    }
    Ok(())
}

fn write_factor_header(
    source: &mut String,
    factor: ModelFactor,
    visible: bool,
) -> Result<bool, fmt::Error> {
    if !visible {
        return Ok(false);
    }
    writeln!(
        source,
        "#block(breakable: false)[\n=== {}\n",
        factor.heading
    )?;
    Ok(true)
}

fn write_manifest_figure(
    source: &mut String,
    directory: &str,
    figure: FlowFigure,
    experiment: ExperimentReport<'_>,
) -> Result<(), fmt::Error> {
    let file = format!("story/{}", figure.file);
    if image_is_visible(experiment.images, &file) {
        write_flow_figure(source, directory, figure, experiment.stop)?;
    }
    Ok(())
}

fn image_is_visible(images: &[ImageManifestEntry], file: &str) -> bool {
    images
        .iter()
        .find(|image| image.file == file)
        .is_some_and(|image| image.content == PanelContent::Visible)
}

fn factor_changed(controller: &ControllerTrace, index: usize) -> bool {
    let query = match index {
        0 => {
            return controller
                .arrival_posterior(controller.len().saturating_sub(1))
                .is_some_and(|posterior| controller.arrival_prior() != posterior);
        }
        1 => PosteriorQuery::PartitionShare,
        2 => PosteriorQuery::Capacity,
        3 => PosteriorQuery::ServiceTime,
        4 => PosteriorQuery::Collapse,
        5 => PosteriorQuery::Knee,
        6 => PosteriorQuery::NormalRetryProbability,
        7 => PosteriorQuery::FailureRetryProbability,
        8 => PosteriorQuery::LeadTime {
            direction: TransitionDirection::Up,
            replica_delta: 1,
        },
        9 => PosteriorQuery::LeadTime {
            direction: TransitionDirection::Down,
            replica_delta: 1,
        },
        10 => PosteriorQuery::RebalanceTime {
            direction: TransitionDirection::Up,
            replica_delta: 1,
        },
        _ => PosteriorQuery::RebalanceTime {
            direction: TransitionDirection::Down,
            replica_delta: 1,
        },
    };
    controller.posterior_prior(query)
        != controller.posterior(query, controller.len().saturating_sub(1))
}

fn write_flow_figure(
    source: &mut String,
    directory: &str,
    figure: FlowFigure,
    stop: RunStop,
) -> Result<(), fmt::Error> {
    writeln!(
        source,
        "#block(breakable: false)[\n=== {}\n",
        figure.heading
    )?;
    write_figure(
        source,
        &format!("{directory}/story/{}", figure.file),
        figure.caption,
        stop,
    )?;
    writeln!(source, "]\n")
}

fn write_figure(
    source: &mut String,
    image: &str,
    caption: &str,
    stop: RunStop,
) -> Result<(), fmt::Error> {
    writeln!(
        source,
        "#figure(image(\"{image}\", width: 100%), caption: [{caption} Duration: {}. Stop: {}.])\n",
        format_duration(stop.at_micros),
        stop_reason(stop.reason)
    )
}

fn write_plot_page(
    source: &mut String,
    heading: &str,
    image: &str,
    explanation: &str,
    landscape: bool,
) -> Result<(), fmt::Error> {
    writeln!(
        source,
        "\n#pagebreak()\n#set page(flipped: {})\n== {heading}\n",
        if landscape { "true" } else { "false" }
    )?;
    writeln!(source, "{explanation}\n")?;
    let height = if landscape { "6.1in" } else { "8.2in" };
    writeln!(
        source,
        "#figure(image(\"{image}\", width: 100%, height: {height}, fit: \"contain\"))"
    )
}

#[derive(Clone, Copy)]
struct ReportSummary {
    arrivals: u64,
    completions: u64,
    peak_backlog: u64,
    minimum_replicas: u32,
    maximum_replicas: u32,
    maximum_target: u32,
    minimum_cap: u32,
    maximum_cap: u32,
    total_miss_fraction: f64,
    total_misses: u64,
    maximum_miss_fraction: f64,
    final_backlog: u64,
    resource_windows: usize,
    final_no_knee_probability: f64,
}

impl ReportSummary {
    fn from_trace(trace: &MetricTrace, controller: &ControllerTrace) -> Self {
        let mut summary = Self {
            arrivals: 0,
            completions: 0,
            peak_backlog: 0,
            minimum_replicas: u32::MAX,
            maximum_replicas: 0,
            maximum_target: 0,
            minimum_cap: u32::MAX,
            maximum_cap: 0,
            total_miss_fraction: 0.0_f64,
            total_misses: 0,
            maximum_miss_fraction: 0.0_f64,
            final_backlog: 0,
            resource_windows: controller.capacity_evidence_count(CapacityEvidenceKind::Window),
            final_no_knee_probability: controller
                .sample(controller.len().saturating_sub(1))
                .map_or(f64::NAN, |sample| sample.no_knee_probability),
        };
        for index in 0..trace.len() {
            let Some(point) = trace.point(index) else {
                continue;
            };
            summary.arrivals = summary.arrivals.saturating_add(point.arrivals);
            summary.completions = summary.completions.saturating_add(point.useful_completions);
            summary.peak_backlog = summary.peak_backlog.max(point.backlog);
            summary.minimum_replicas = summary.minimum_replicas.min(point.replicas);
            summary.maximum_replicas = summary.maximum_replicas.max(point.replicas);
            summary.maximum_target = summary.maximum_target.max(point.target);
            if point.cap > 0 {
                summary.minimum_cap = summary.minimum_cap.min(point.cap);
                summary.maximum_cap = summary.maximum_cap.max(point.cap);
            }
            summary.maximum_miss_fraction = summary.maximum_miss_fraction.max(point.miss_fraction);
            summary.total_misses = summary.total_misses.saturating_add(point.misses);
            summary.final_backlog = point.backlog;
        }
        if summary.completions > 0 {
            let misses = (0..trace.len())
                .filter_map(|index| trace.point(index))
                .map(|point| point.miss_fraction * crate::u64_to_f64(point.useful_completions))
                .sum::<f64>();
            summary.total_miss_fraction = misses / crate::u64_to_f64(summary.completions);
        }
        if summary.minimum_replicas == u32::MAX {
            summary.minimum_replicas = 0;
        }
        if summary.minimum_cap == u32::MAX {
            summary.minimum_cap = 0;
        }
        summary
    }
}

#[derive(Clone, Copy)]
struct CapacityEvidenceSummary {
    resource_windows: usize,
    declining_steps: usize,
    minimum_concurrency: f64,
    maximum_concurrency: f64,
    minimum_throughput: f64,
    maximum_throughput: f64,
}

impl CapacityEvidenceSummary {
    fn from_controller(controller: &ControllerTrace) -> Self {
        let mut summary = Self {
            resource_windows: 0,
            declining_steps: 0,
            minimum_concurrency: f64::INFINITY,
            maximum_concurrency: 0.0_f64,
            minimum_throughput: f64::INFINITY,
            maximum_throughput: 0.0_f64,
        };
        let mut previous: Option<CapacityWindowSample> = None;
        for index in 0..controller.len() {
            let Some(sample) = controller.sample(index) else {
                continue;
            };
            let CapacityEvidenceSample::Window(window) = sample.capacity_evidence else {
                continue;
            };
            summary.resource_windows += 1;
            if let Some(before) = previous {
                summary.declining_steps += usize::from(
                    window.concurrency > before.concurrency
                        && window.throughput_per_second() < before.throughput_per_second(),
                );
            }
            let throughput = window.throughput_per_second();
            summary.minimum_concurrency = summary.minimum_concurrency.min(window.concurrency);
            summary.maximum_concurrency = summary.maximum_concurrency.max(window.concurrency);
            summary.minimum_throughput = summary.minimum_throughput.min(throughput);
            summary.maximum_throughput = summary.maximum_throughput.max(throughput);
            previous = Some(window);
        }
        if summary.resource_windows == 0 {
            summary.minimum_concurrency = 0.0_f64;
            summary.minimum_throughput = 0.0_f64;
        }
        summary
    }
}

const fn title(regime: PrincipalRegime) -> &'static str {
    match regime {
        PrincipalRegime::Idle => "Idle",
        PrincipalRegime::ApplicationLimited => "Application-limited load",
        PrincipalRegime::LinearThroughput => "Linear throughput",
        PrincipalRegime::FlatPostKnee => "Flat post-knee capacity",
        PrincipalRegime::DecliningPostKnee => "Declining post-knee capacity",
        PrincipalRegime::ShortBurst => "Short burst",
        PrincipalRegime::SeasonalWaves => "Seasonal demand waves",
        PrincipalRegime::HotPartition => "Hot partition",
        PrincipalRegime::TimerWave => "Timer wave",
        PrincipalRegime::HotSerializedKey => "Hot serialized key",
        PrincipalRegime::TransientFailures => "Transient failures",
        PrincipalRegime::PermanentRejections => "Permanent rejections",
        PrincipalRegime::RebalanceStorm => "Rebalance storm",
        PrincipalRegime::HandlerContention => "Handler contention",
        PrincipalRegime::LooseBudgetBacklog => "Loose-budget backlog",
        PrincipalRegime::SnapshotFaults => "Snapshot transport faults",
        PrincipalRegime::MissingReporter => "Missing reporter",
        PrincipalRegime::AggregatorReplacement => "Aggregator replacement",
        PrincipalRegime::ReplicaCeiling => "Replica ceiling",
        PrincipalRegime::HistoricalMatch => "Demand matches history",
        PrincipalRegime::HistoricalExceeded => "Demand exceeds history",
        PrincipalRegime::HistoricalUnder => "Demand stays below history",
        PrincipalRegime::HistoricalMissing => "Historical data is missing",
    }
}

const fn situation(regime: PrincipalRegime) -> &'static str {
    match regime {
        PrincipalRegime::Idle => {
            "No work arrives. This regime tests the controller's idle posture and minimum scale."
        }
        PrincipalRegime::ApplicationLimited => {
            "Demand stays below handler and dependency capacity. The workload does not expose a \
             saturation knee."
        }
        PrincipalRegime::LinearThroughput => {
            "Useful throughput grows with ready handler capacity. More concurrency should continue \
             to add capacity."
        }
        PrincipalRegime::FlatPostKnee => {
            "Dependency throughput stops growing after its concurrency knee. Extra concurrency \
             adds no useful capacity."
        }
        PrincipalRegime::DecliningPostKnee => {
            "Dependency throughput declines after its concurrency knee. Extra concurrency reduces \
             useful capacity."
        }
        PrincipalRegime::ShortBurst => {
            "A finite burst ends before slow replica launches can help. Late scale-up can add cost \
             without reducing latency."
        }
        PrincipalRegime::SeasonalWaves => {
            "Demand arrives in repeated waves. The controller must react without treating each \
             trough as permanent."
        }
        PrincipalRegime::HotPartition => {
            "One partition receives all events. The regime starts with eight replicas. Kafka lets \
             only one owner serve the partition."
        }
        PrincipalRegime::TimerWave => {
            "Known timers release at one virtual instant. The release schedule supplies certain \
             future demand."
        }
        PrincipalRegime::HotSerializedKey => {
            "One key receives all events. Per-key ordering limits useful parallelism despite \
             available replicas."
        }
        PrincipalRegime::TransientFailures => {
            "Some events fail twice before settlement. Retries add work but do not represent new \
             useful demand."
        }
        PrincipalRegime::PermanentRejections => {
            "Some events end as permanent rejections. Rejected work must not justify unlimited \
             scale-up."
        }
        PrincipalRegime::RebalanceStorm => {
            "Replica changes repeatedly pause partition ownership. Actuation creates a direct \
             availability cost."
        }
        PrincipalRegime::HandlerContention => {
            "Handler duration grows with backlog. Increased concurrency changes the service-time \
             distribution."
        }
        PrincipalRegime::LooseBudgetBacklog => {
            "A large finite backlog has a loose latency objective. The controller can trade \
             clearance time against replica cost."
        }
        PrincipalRegime::SnapshotFaults => {
            "Reporter snapshots can arrive late, duplicate, reorder, or disappear. Evidence \
             quality changes through time."
        }
        PrincipalRegime::MissingReporter => {
            "The reporter stops after initial evidence. Model uncertainty must reflect the missing \
             current state."
        }
        PrincipalRegime::AggregatorReplacement => {
            "A replacement aggregator starts from the proper prior. It must relearn from retained \
             evidence."
        }
        PrincipalRegime::ReplicaCeiling => {
            "Demand requires more replicas than configuration permits. The ceiling makes some \
             deadline miss unavoidable."
        }
        PrincipalRegime::HistoricalMatch => {
            "Current demand matches the historical reference. History and live evidence agree."
        }
        PrincipalRegime::HistoricalExceeded => {
            "Current demand exceeds the historical reference. Live evidence must override an \
             optimistic forecast."
        }
        PrincipalRegime::HistoricalUnder => {
            "Current demand stays below the historical reference. Live evidence must prevent \
             persistent overprovisioning."
        }
        PrincipalRegime::HistoricalMissing => {
            "No historical reference exists. The controller must use live evidence and explicit \
             uncertainty."
        }
    }
}

const fn question(regime: PrincipalRegime) -> &'static str {
    match regime {
        PrincipalRegime::Idle => {
            "does the controller avoid unnecessary replicas when demand is absent?"
        }
        PrincipalRegime::ApplicationLimited => {
            "does the controller avoid claiming a saturation knee without informative evidence?"
        }
        PrincipalRegime::LinearThroughput => {
            "does capacity evidence preserve continued scaling while throughput remains linear?"
        }
        PrincipalRegime::FlatPostKnee => {
            "does the posterior identify a flat knee and impose a useful saturation cap?"
        }
        PrincipalRegime::DecliningPostKnee => {
            "does the posterior identify harmful concurrency and reduce the saturation cap?"
        }
        PrincipalRegime::ShortBurst => {
            "does the controller account for launch delay when a burst ends quickly?"
        }
        PrincipalRegime::SeasonalWaves => {
            "does the controller track recurring demand without unstable replica changes?"
        }
        PrincipalRegime::HotPartition => {
            "does partition evidence expose the placement limit and prevent wasteful growth?"
        }
        PrincipalRegime::TimerWave => "does known future timer demand affect scale before release?",
        PrincipalRegime::HotSerializedKey => {
            "does the model distinguish serialized work from parallel work?"
        }
        PrincipalRegime::TransientFailures => {
            "does retry evidence increase required work without corrupting useful throughput?"
        }
        PrincipalRegime::PermanentRejections => {
            "does rejection evidence avoid unsafe scale decisions?"
        }
        PrincipalRegime::RebalanceStorm => {
            "does expected actuation cost suppress harmful replica churn?"
        }
        PrincipalRegime::HandlerContention => {
            "does service-time evidence move when backlog increases handler duration?"
        }
        PrincipalRegime::LooseBudgetBacklog => {
            "does the controller clear backlog within the loose objective at reasonable cost?"
        }
        PrincipalRegime::SnapshotFaults => {
            "does the model remain robust when snapshot transport is unreliable?"
        }
        PrincipalRegime::MissingReporter => {
            "does uncertainty increase when current reporter evidence disappears?"
        }
        PrincipalRegime::AggregatorReplacement => {
            "does replacement restore the prior and then update from evidence?"
        }
        PrincipalRegime::ReplicaCeiling => {
            "does the decision expose missed delay when the replica ceiling binds?"
        }
        PrincipalRegime::HistoricalMatch => {
            "does matching live evidence support the historical forecast?"
        }
        PrincipalRegime::HistoricalExceeded => {
            "does live evidence correct history when current demand is higher?"
        }
        PrincipalRegime::HistoricalUnder => {
            "does live evidence correct history when current demand is lower?"
        }
        PrincipalRegime::HistoricalMissing => {
            "does the model express wider uncertainty without historical evidence?"
        }
    }
}

const fn expectation(regime: PrincipalRegime) -> &'static str {
    match regime {
        PrincipalRegime::Idle => "hold or reduce scale while the backlog stays empty.",
        PrincipalRegime::ApplicationLimited => {
            "meet the SLO without invoking a false capacity limit."
        }
        PrincipalRegime::LinearThroughput => {
            "permit additional replicas while measured throughput continues to grow."
        }
        PrincipalRegime::FlatPostKnee => {
            "concentrate knee mass and prevent scale beyond useful dependency capacity."
        }
        PrincipalRegime::DecliningPostKnee => {
            "concentrate harmful-collapse mass and cap concurrency below the declining region."
        }
        PrincipalRegime::ShortBurst => {
            "avoid late replicas whose launch time exceeds the remaining burst duration."
        }
        PrincipalRegime::SeasonalWaves => {
            "follow sustained wave demand while avoiding rapid scale oscillation."
        }
        PrincipalRegime::HotPartition => {
            "report placement-bound loss and avoid replicas that cannot add partition capacity."
        }
        PrincipalRegime::TimerWave => {
            "include known timer releases in demand before they enter the live queue."
        }
        PrincipalRegime::HotSerializedKey => {
            "recognize limited useful parallelism and avoid wasteful replica growth."
        }
        PrincipalRegime::TransientFailures => {
            "budget retry work while measuring useful completions separately."
        }
        PrincipalRegime::PermanentRejections => {
            "represent rejected work as reliability evidence instead of useful demand."
        }
        PrincipalRegime::RebalanceStorm => {
            "prefer fewer scale actions when transition pauses dominate their benefit."
        }
        PrincipalRegime::HandlerContention => {
            "move service-time mass upward and request capacity from the changed evidence."
        }
        PrincipalRegime::LooseBudgetBacklog => {
            "choose enough replicas to meet the objective without urgent overreaction."
        }
        PrincipalRegime::SnapshotFaults => {
            "accept valid reports, reject invalid order, and widen uncertainty when evidence ages."
        }
        PrincipalRegime::MissingReporter => {
            "hold safer capacity as reporter coverage falls and snapshot age grows."
        }
        PrincipalRegime::AggregatorReplacement => {
            "show prior restoration followed by evidence-driven contraction."
        }
        PrincipalRegime::ReplicaCeiling => {
            "bind at the ceiling and report the residual missed delay."
        }
        PrincipalRegime::HistoricalMatch => {
            "combine compatible historical and live evidence without a large correction."
        }
        PrincipalRegime::HistoricalExceeded => {
            "move demand belief above history and increase the target."
        }
        PrincipalRegime::HistoricalUnder => {
            "move demand belief below history and avoid excess replicas."
        }
        PrincipalRegime::HistoricalMissing => {
            "make decisions from live evidence while retaining wider prior uncertainty."
        }
    }
}

fn format_duration(micros: u64) -> String {
    let seconds = crate::u64_to_f64(micros) / 1_000_000.0_f64;
    if seconds >= 120.0_f64 {
        return format!("{:.1} minutes", seconds / 60.0_f64);
    }
    format!("{seconds:.1} seconds")
}

const fn stop_reason(reason: RunStopReason) -> &'static str {
    match reason {
        RunStopReason::IdleStable => "work drained and replica actuation became stable",
        RunStopReason::DurationComplete => "the declared duration elapsed",
    }
}

/// Regime report generation failure.
#[derive(Debug, Error)]
pub enum ReportError {
    /// Text formatting failed.
    #[error(transparent)]
    Format(#[from] fmt::Error),
    /// File output failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The report or image manifest violates the presentation contract.
    #[error(transparent)]
    Check(#[from] ReportCheckError),
    /// The historical comparison omits one required case.
    #[error("the historical comparison requires all four historical cases")]
    HistoricalComparison,
    /// The output path has no parent directory.
    #[error("the report output path must have a parent directory")]
    MissingParent,
    /// The report configuration omits the required calibration level.
    #[error("the capacity calibration report requires the 80% level")]
    MissingCalibrationLevel,
    /// Typst could not compile the generated document.
    #[error("Typst compilation failed: {0}")]
    Compile(String),
    /// Typst could not export the compiled document.
    #[error("Typst PDF export failed: {0}")]
    Pdf(String),
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
