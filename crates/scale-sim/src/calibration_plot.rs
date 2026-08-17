use std::error::Error as StdError;
use std::fmt::{self, Write};
use std::fs;
use std::iter::once;
use std::path::Path;

use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use crate::{
    CapacityCalibration, CapacityCalibrationTrial, CapacitySensitivity,
    CapacitySensitivityCalibration, CapacitySensitivityTrial, DemandCalibration,
    DemandCalibrationTrial, LeadTimeCalibration, LeadTimeCalibrationTrial, PLOT_FONT_FAMILY,
    PartitionCalibration, PartitionCalibrationTrial, PlotError, PrincipalRegime,
    predictive_coverage_levels,
};
use prosody_scale_core::TransitionDirection;

const WIDTH: u32 = 1_240;
const PANEL_HEIGHT: u32 = 360;
const COLORS: [RGBColor; 8] = [
    RGBColor(55, 105, 145),
    RGBColor(155, 65, 45),
    RGBColor(80, 135, 80),
    RGBColor(130, 95, 150),
    RGBColor(190, 125, 45),
    RGBColor(55, 145, 145),
    RGBColor(170, 85, 115),
    RGBColor(105, 105, 105),
];

/// Writes repeated-run capacity calibration figures.
///
/// # Errors
///
/// Returns an error when no trials exist or drawing fails.
pub fn write_capacity_calibration_figures(
    directory: &Path,
    calibration: &CapacityCalibration,
) -> Result<(), PlotError> {
    if calibration.trials().is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    fs::create_dir_all(directory)?;
    write_summary(directory, calibration)?;
    write_coverage(directory, calibration)?;
    write_ranks(directory, calibration)?;
    write_error_uncertainty(directory, calibration)?;
    write_contraction(directory, calibration)
}

/// Writes exact repeated-run partition calibration data and figures.
///
/// # Errors
///
/// Returns an error when no trials exist or output fails.
pub fn write_partition_calibration_data(
    directory: &Path,
    calibration: &PartitionCalibration,
) -> Result<(), PlotError> {
    if calibration.trials().is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    fs::create_dir_all(directory)?;
    let mut summary = String::from(concat!(
        "regime\tseed\tobservations\tstated_coverage\tcovered\tempirical_coverage\t",
        "mean_log_loss\tmean_entropy\tentropy_contraction\n",
    ));
    for trial in calibration.trials() {
        for (level_index, &level) in predictive_coverage_levels().iter().enumerate() {
            writeln!(
                summary,
                concat!("{}\t{}\t{}\t{:.2}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}",),
                trial.regime.name(),
                trial.seed,
                trial.observation_count,
                level,
                trial.covered_counts[level_index],
                ratio(
                    u64::from(trial.covered_counts[level_index]),
                    u64::from(trial.observation_count),
                ),
                trial.mean_log_loss,
                trial.mean_entropy,
                trial.entropy_contraction,
            )
            .map_err(|_| {
                PlotError::Drawing("partition calibration formatting failed".to_owned())
            })?;
        }
    }
    fs::write(directory.join("partition-calibration.tsv"), summary)?;
    write_categorical_coverage(directory, calibration, "partition-coverage.svg")?;
    write_categorical_ranks(
        directory,
        calibration,
        "accepted assignments",
        "partition-ranks.svg",
    )?;
    write_categorical_error_uncertainty(
        directory,
        calibration,
        "Partition log loss against entropy · gray means equal",
        "mean predictive entropy",
        "mean negative log probability",
        "partition-error-uncertainty.svg",
    )?;
    write_categorical_contraction(
        directory,
        calibration,
        "Partition entropy contraction by regime",
        "prior-to-posterior entropy contraction",
        "partition-contraction.svg",
    )
}

/// Writes exact repeated-run lead-time calibration data and figures.
///
/// # Errors
///
/// Returns an error when no trials exist or output fails.
pub fn write_lead_time_calibration_data(
    directory: &Path,
    calibration: &LeadTimeCalibration,
) -> Result<(), PlotError> {
    if calibration.trials().is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    fs::create_dir_all(directory)?;
    let mut summary = String::from(concat!(
        "regime\tseed\tdirection\tcompleted\tcensored\tstated_coverage\tcovered\t",
        "empirical_coverage\tmean_absolute_error_seconds\tmean_interval_width_seconds\t",
        "posterior_contraction\n",
    ));
    for trial in calibration.trials() {
        for (level_index, &level) in predictive_coverage_levels().iter().enumerate() {
            writeln!(
                summary,
                concat!("{}\t{}\t{}\t{}\t{}\t{:.2}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}",),
                trial.regime.name(),
                trial.seed,
                transition_direction_name(trial.direction),
                trial.observation_count,
                trial.censored_count,
                level,
                trial.covered_counts[level_index],
                ratio(
                    u64::from(trial.covered_counts[level_index]),
                    u64::from(trial.observation_count),
                ),
                trial.mean_absolute_error_seconds,
                trial.mean_uncertainty_seconds,
                trial.posterior_contraction,
            )
            .map_err(|_| {
                PlotError::Drawing("lead-time calibration formatting failed".to_owned())
            })?;
        }
    }
    fs::write(directory.join("lead-time-calibration.tsv"), summary)?;
    for direction in [TransitionDirection::Up, TransitionDirection::Down] {
        let view = LeadTimeDirectionView {
            calibration,
            direction,
        };
        let name = transition_direction_name(direction);
        write_categorical_coverage(directory, &view, &format!("lead-time-{name}-coverage.svg"))?;
        write_categorical_ranks(
            directory,
            &view,
            "completed transitions",
            &format!("lead-time-{name}-ranks.svg"),
        )?;
        write_categorical_error_uncertainty(
            directory,
            &view,
            &format!("Scale-{name} error against uncertainty · gray means equal"),
            "mean 80% interval width (seconds)",
            "mean absolute error (seconds)",
            &format!("lead-time-{name}-error-uncertainty.svg"),
        )?;
        write_categorical_contraction(
            directory,
            &view,
            &format!("Scale-{name} lead-time posterior contraction"),
            "one-replica posterior width contraction",
            &format!("lead-time-{name}-contraction.svg"),
        )?;
    }
    Ok(())
}

fn write_categorical_coverage<Calibration: CategoricalCalibrationPlot>(
    directory: &Path,
    calibration: &Calibration,
    file: &str,
) -> Result<(), PlotError> {
    let regimes = categorical_regimes(calibration);
    let rows = regimes.len().div_ceil(2);
    let height = PANEL_HEIGHT.saturating_mul(
        u32::try_from(rows)
            .map_err(|_| PlotError::Drawing("too many handler regimes".to_owned()))?,
    );
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, height)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        for (index, (panel, regime)) in root
            .split_evenly((rows, 2))
            .into_iter()
            .zip(regimes)
            .enumerate()
        {
            let color = COLORS[index % COLORS.len()];
            let mut chart = ChartBuilder::on(&panel)
                .margin(12_u32)
                .caption(regime.name(), (PLOT_FONT_FAMILY, 21_i32).into_font())
                .x_label_area_size(52_u32)
                .y_label_area_size(68_u32)
                .build_cartesian_2d(0.45_f64..1.0_f64, 0.0_f64..1.05_f64)
                .map_err(|error| drawing_error(&error))?;
            chart
                .configure_mesh()
                .disable_mesh()
                .x_desc("stated central coverage")
                .y_desc("empirical coverage")
                .label_style((PLOT_FONT_FAMILY, 16_i32).into_font())
                .draw()
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(LineSeries::new(
                    [(0.45_f64, 0.45_f64), (1.0_f64, 1.0_f64)],
                    RGBColor(170, 170, 170).stroke_width(1),
                ))
                .map_err(|error| drawing_error(&error))?;
            let points = predictive_coverage_levels()
                .iter()
                .copied()
                .enumerate()
                .map(|(level_index, level)| {
                    let (covered, observations) =
                        categorical_counts(calibration, regime, level_index);
                    (level, ratio(covered, observations))
                })
                .collect::<Vec<_>>();
            chart
                .draw_series(LineSeries::new(points.clone(), color.stroke_width(2)))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(
                    points
                        .into_iter()
                        .map(|point| Circle::new(point, 4_i32, color.filled())),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join(file), svg)?;
    Ok(())
}

fn write_categorical_ranks<Calibration: CategoricalCalibrationPlot>(
    directory: &Path,
    calibration: &Calibration,
    y_label: &str,
    file: &str,
) -> Result<(), PlotError> {
    let regimes = categorical_regimes(calibration);
    let count = u32::try_from(regimes.len())
        .map_err(|_| PlotError::Drawing("too many handler regimes".to_owned()))?;
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT.saturating_mul(count)))
            .into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        for (panel, regime) in root
            .split_evenly((regimes.len(), 1))
            .into_iter()
            .zip(regimes)
        {
            let counts = categorical_rank_counts(calibration, regime);
            let total = counts.iter().copied().map(u64::from).sum::<u64>();
            let expected = count_f64(total) / 10.0_f64;
            let maximum = counts
                .iter()
                .copied()
                .max()
                .map_or(1.0_f64, f64::from)
                .max(expected)
                * 1.08_f64;
            let mut chart = ChartBuilder::on(&panel)
                .margin(12_u32)
                .caption(regime.name(), (PLOT_FONT_FAMILY, 21_i32).into_font())
                .x_label_area_size(48_u32)
                .y_label_area_size(80_u32)
                .build_cartesian_2d(0_u32..10_u32, 0.0_f64..maximum)
                .map_err(|error| drawing_error(&error))?;
            chart
                .configure_mesh()
                .disable_mesh()
                .x_desc(Calibration::RANK_AXIS_LABEL)
                .y_desc(y_label)
                .label_style((PLOT_FONT_FAMILY, 17_i32).into_font())
                .draw()
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(counts.into_iter().zip(0_u32..).map(|(value, bin)| {
                    Rectangle::new(
                        [(bin, 0.0_f64), (bin + 1, f64::from(value))],
                        COLORS[0].filled(),
                    )
                }))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(LineSeries::new(
                    [(0_u32, expected), (10_u32, expected)],
                    RGBColor(135, 135, 135).stroke_width(1),
                ))
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join(file), svg)?;
    Ok(())
}

fn write_categorical_error_uncertainty<Calibration: CategoricalCalibrationPlot>(
    directory: &Path,
    calibration: &Calibration,
    title: &str,
    x_label: &str,
    y_label: &str,
    file: &str,
) -> Result<(), PlotError> {
    let maximum = calibration
        .plot_trials()
        .iter()
        .filter(|trial| calibration.include(trial))
        .map(|trial| Calibration::error(trial).max(Calibration::uncertainty(trial)))
        .fold(0.0_f64, f64::max)
        .max(1.0_f64);
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(title, (PLOT_FONT_FAMILY, 22_i32).into_font())
            .x_label_area_size(56_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0.0_f64..maximum * 1.05_f64, 0.0_f64..maximum * 1.05_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_desc(x_label)
            .y_desc(y_label)
            .label_style((PLOT_FONT_FAMILY, 18_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        chart
            .draw_series(LineSeries::new(
                [(0.0_f64, 0.0_f64), (maximum, maximum)],
                RGBColor(170, 170, 170).stroke_width(1),
            ))
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in categorical_regimes(calibration).into_iter().enumerate() {
            chart
                .draw_series(
                    calibration
                        .plot_trials()
                        .iter()
                        .filter(|trial| {
                            calibration.include(trial) && Calibration::regime(trial) == regime
                        })
                        .map(|trial| {
                            Circle::new(
                                (Calibration::uncertainty(trial), Calibration::error(trial)),
                                4_i32,
                                COLORS[index % COLORS.len()].filled(),
                            )
                        }),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join(file), svg)?;
    Ok(())
}

fn write_categorical_contraction<Calibration: CategoricalCalibrationPlot>(
    directory: &Path,
    calibration: &Calibration,
    title: &str,
    y_label: &str,
    file: &str,
) -> Result<(), PlotError> {
    let regimes = categorical_regimes(calibration);
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(title, (PLOT_FONT_FAMILY, 22_i32).into_font())
            .x_label_area_size(72_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0_usize..regimes.len(), -0.2_f64..1.0_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_labels(regimes.len())
            .x_label_formatter(&|index| {
                regimes
                    .get(*index)
                    .map_or(String::new(), |regime| regime.name().to_owned())
            })
            .y_desc(y_label)
            .label_style((PLOT_FONT_FAMILY, 16_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in regimes.into_iter().enumerate() {
            chart
                .draw_series(
                    calibration
                        .plot_trials()
                        .iter()
                        .filter(|trial| {
                            calibration.include(trial) && Calibration::regime(trial) == regime
                        })
                        .map(|trial| {
                            Circle::new(
                                (index, Calibration::contraction(trial)),
                                4_i32,
                                COLORS[index % COLORS.len()].filled(),
                            )
                        }),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join(file), svg)?;
    Ok(())
}

/// Writes capacity prior and grid sensitivity figures.
///
/// # Errors
///
/// Returns an error when no trials exist or drawing fails.
pub fn write_capacity_sensitivity_figures(
    directory: &Path,
    sensitivity: &CapacitySensitivityCalibration,
) -> Result<(), PlotError> {
    if sensitivity.trials().is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    fs::create_dir_all(directory)?;
    write_sensitivity_summary(directory, sensitivity)?;
    write_sensitivity_coverage(
        directory,
        sensitivity,
        [
            CapacitySensitivity::NarrowPrior,
            CapacitySensitivity::ReferencePrior,
            CapacitySensitivity::WidePrior,
        ],
        "Capacity calibration sensitivity to prior width",
        "capacity-prior-sensitivity.svg",
    )?;
    write_sensitivity_coverage(
        directory,
        sensitivity,
        [
            CapacitySensitivity::LowerGridCeiling,
            CapacitySensitivity::ReferencePrior,
            CapacitySensitivity::HigherGridCeiling,
        ],
        "Capacity calibration sensitivity to grid ceiling",
        "capacity-grid-sensitivity.svg",
    )
}

/// Writes repeated-run demand calibration figures.
///
/// # Errors
///
/// Returns an error when no trials exist or drawing fails.
pub fn write_demand_calibration_figures(
    directory: &Path,
    calibration: &DemandCalibration,
) -> Result<(), PlotError> {
    if calibration.trials().is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    fs::create_dir_all(directory)?;
    write_demand_summary(directory, calibration)?;
    write_demand_coverage(directory, calibration)?;
    write_demand_ranks(directory, calibration)?;
    write_demand_error_uncertainty(directory, calibration)?;
    write_demand_contraction(directory, calibration)
}

fn write_demand_summary(
    directory: &Path,
    calibration: &DemandCalibration,
) -> Result<(), PlotError> {
    let mut summary = String::from(concat!(
        "regime\ttrials\tobservations\tstated_coverage\tcovered\tempirical_coverage\t",
        "mean_observed_count\tmean_predicted_count\tmean_absolute_error\t",
        "mean_interval_width\tmean_rate_contraction\n",
    ));
    for regime in demand_regimes(calibration.trials()) {
        let trials = calibration
            .trials()
            .iter()
            .filter(|trial| trial.regime == regime)
            .collect::<Vec<_>>();
        let denominator = u32::try_from(trials.len())
            .map_err(|_| PlotError::Drawing("too many demand trials".to_owned()))?
            .max(1);
        let mean_error = trials
            .iter()
            .map(|trial| trial.mean_absolute_error)
            .sum::<f64>()
            / f64::from(denominator);
        let mean_observed = trials
            .iter()
            .map(|trial| trial.mean_observed_count)
            .sum::<f64>()
            / f64::from(denominator);
        let mean_predicted = trials
            .iter()
            .map(|trial| trial.mean_predicted_count)
            .sum::<f64>()
            / f64::from(denominator);
        let mean_uncertainty = trials
            .iter()
            .map(|trial| trial.mean_uncertainty)
            .sum::<f64>()
            / f64::from(denominator);
        let mean_contraction = trials
            .iter()
            .map(|trial| trial.rate_contraction)
            .sum::<f64>()
            / f64::from(denominator);
        for (level_index, &level) in predictive_coverage_levels().iter().enumerate() {
            let (covered, observations) = demand_counts(&trials, level_index);
            writeln!(
                summary,
                concat!(
                    "{}\t{}\t{}\t{:.2}\t{}\t{:.6}\t{:.6}\t",
                    "{:.6}\t{:.6}\t{:.6}\t{:.6}",
                ),
                regime.name(),
                denominator,
                observations,
                level,
                covered,
                ratio(covered, observations),
                mean_observed,
                mean_predicted,
                mean_error,
                mean_uncertainty,
                mean_contraction,
            )
            .map_err(|_| PlotError::Drawing("demand calibration formatting failed".to_owned()))?;
        }
    }
    fs::write(directory.join("demand-calibration.tsv"), summary)?;
    Ok(())
}

fn write_demand_coverage(
    directory: &Path,
    calibration: &DemandCalibration,
) -> Result<(), PlotError> {
    let regimes = demand_regimes(calibration.trials());
    let rows = regimes.len().div_ceil(2);
    let height = PANEL_HEIGHT.saturating_mul(
        u32::try_from(rows)
            .map_err(|_| PlotError::Drawing("too many demand regimes".to_owned()))?,
    );
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, height)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        for (index, (panel, regime)) in root
            .split_evenly((rows, 2))
            .into_iter()
            .zip(regimes)
            .enumerate()
        {
            let color = COLORS[index % COLORS.len()];
            let points = predictive_coverage_levels()
                .iter()
                .copied()
                .enumerate()
                .map(|(level_index, level)| {
                    (
                        level,
                        demand_coverage(calibration.trials(), regime, level_index),
                    )
                })
                .collect::<Vec<_>>();
            let mut chart = ChartBuilder::on(&panel)
                .margin(12_u32)
                .caption(regime.name(), (PLOT_FONT_FAMILY, 21_i32).into_font())
                .x_label_area_size(52_u32)
                .y_label_area_size(68_u32)
                .build_cartesian_2d(0.45_f64..1.0_f64, 0.0_f64..1.05_f64)
                .map_err(|error| drawing_error(&error))?;
            chart
                .configure_mesh()
                .disable_mesh()
                .x_desc("stated central coverage")
                .y_desc("empirical coverage")
                .label_style((PLOT_FONT_FAMILY, 16_i32).into_font())
                .draw()
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(LineSeries::new(
                    [(0.45_f64, 0.45_f64), (1.0_f64, 1.0_f64)],
                    RGBColor(170, 170, 170).stroke_width(1),
                ))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(LineSeries::new(points.clone(), color.stroke_width(2)))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(
                    points
                        .into_iter()
                        .map(|point| Circle::new(point, 4_i32, color.filled())),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("demand-coverage.svg"), svg)?;
    Ok(())
}

fn write_demand_ranks(directory: &Path, calibration: &DemandCalibration) -> Result<(), PlotError> {
    let regimes = demand_regimes(calibration.trials());
    let regime_count = u32::try_from(regimes.len())
        .map_err(|_| PlotError::Drawing("too many demand regimes".to_owned()))?;
    let mut svg = String::new();
    {
        let root =
            SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT.saturating_mul(regime_count)))
                .into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        for (panel, regime) in root
            .split_evenly((regimes.len(), 1))
            .into_iter()
            .zip(regimes)
        {
            let counts = demand_rank_counts(calibration.trials(), regime);
            let total = counts.iter().copied().map(u64::from).sum::<u64>();
            let expected = count_f64(total) / 10.0_f64;
            let maximum = counts
                .iter()
                .copied()
                .max()
                .map_or(1.0_f64, f64::from)
                .max(expected)
                * 1.08_f64;
            let mut chart = ChartBuilder::on(&panel)
                .margin(12_u32)
                .caption(regime.name(), (PLOT_FONT_FAMILY, 21_i32).into_font())
                .x_label_area_size(48_u32)
                .y_label_area_size(80_u32)
                .build_cartesian_2d(0_u32..10_u32, 0.0_f64..maximum)
                .map_err(|error| drawing_error(&error))?;
            chart
                .configure_mesh()
                .disable_mesh()
                .x_desc("randomized predictive rank decile")
                .y_desc("accepted observations")
                .label_style((PLOT_FONT_FAMILY, 17_i32).into_font())
                .draw()
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(counts.into_iter().zip(0_u32..).map(|(count, bin)| {
                    Rectangle::new(
                        [(bin, 0.0_f64), (bin + 1, f64::from(count))],
                        COLORS[0].filled(),
                    )
                }))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(LineSeries::new(
                    [(0_u32, expected), (10_u32, expected)],
                    RGBColor(135, 135, 135).stroke_width(1),
                ))
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("demand-ranks.svg"), svg)?;
    Ok(())
}

fn write_demand_error_uncertainty(
    directory: &Path,
    calibration: &DemandCalibration,
) -> Result<(), PlotError> {
    let maximum = calibration
        .trials()
        .iter()
        .map(|trial| trial.mean_absolute_error.max(trial.mean_uncertainty))
        .fold(0.0_f64, f64::max)
        .max(1.0_f64);
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(
                "Demand error against uncertainty · gray means equal",
                (PLOT_FONT_FAMILY, 22_i32).into_font(),
            )
            .x_label_area_size(56_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0.0_f64..maximum * 1.05_f64, 0.0_f64..maximum * 1.05_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_desc("mean 80% count interval width")
            .y_desc("mean absolute count error")
            .label_style((PLOT_FONT_FAMILY, 18_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        chart
            .draw_series(LineSeries::new(
                [(0.0_f64, 0.0_f64), (maximum, maximum)],
                RGBColor(170, 170, 170).stroke_width(1),
            ))
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in demand_regimes(calibration.trials()).into_iter().enumerate() {
            let color = COLORS[index % COLORS.len()];
            chart
                .draw_series(
                    calibration
                        .trials()
                        .iter()
                        .filter(|trial| trial.regime == regime)
                        .map(|trial| {
                            Circle::new(
                                (trial.mean_uncertainty, trial.mean_absolute_error),
                                4_i32,
                                color.filled(),
                            )
                        }),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("demand-error-uncertainty.svg"), svg)?;
    Ok(())
}

fn write_demand_contraction(
    directory: &Path,
    calibration: &DemandCalibration,
) -> Result<(), PlotError> {
    let regimes = demand_regimes(calibration.trials());
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(
                "Arrival-rate posterior contraction by regime",
                (PLOT_FONT_FAMILY, 22_i32).into_font(),
            )
            .x_label_area_size(72_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0_usize..regimes.len(), -0.2_f64..1.0_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_labels(regimes.len())
            .x_label_formatter(&|index| {
                regimes
                    .get(*index)
                    .map_or(String::new(), |regime| regime.name().to_owned())
            })
            .y_desc("prior-to-posterior width contraction")
            .label_style((PLOT_FONT_FAMILY, 16_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in regimes.into_iter().enumerate() {
            chart
                .draw_series(
                    calibration
                        .trials()
                        .iter()
                        .filter(|trial| trial.regime == regime)
                        .map(|trial| {
                            Circle::new(
                                (index, trial.rate_contraction),
                                4_i32,
                                COLORS[index % COLORS.len()].filled(),
                            )
                        }),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("demand-contraction.svg"), svg)?;
    Ok(())
}

fn write_sensitivity_summary(
    directory: &Path,
    sensitivity: &CapacitySensitivityCalibration,
) -> Result<(), PlotError> {
    let level_index = coverage_level_index()?;
    let mut summary =
        String::from("regime\tvariant\ttrials\tobservations\tcovered_80\tempirical_coverage_80\n");
    for regime in sensitivity_regimes(sensitivity.trials()) {
        for variant in CapacitySensitivity::ALL {
            let (trial_count, observations, covered) =
                sensitivity_counts(sensitivity.trials(), regime, variant, level_index);
            let empirical = ratio(covered, observations);
            writeln!(
                summary,
                "{}\t{}\t{}\t{}\t{}\t{empirical:.6}",
                regime.name(),
                variant.name(),
                trial_count,
                observations,
                covered,
            )
            .map_err(|_| PlotError::Drawing("capacity sensitivity formatting failed".to_owned()))?;
        }
    }
    fs::write(directory.join("capacity-sensitivity.tsv"), summary)?;
    Ok(())
}

fn write_sensitivity_coverage(
    directory: &Path,
    sensitivity: &CapacitySensitivityCalibration,
    variants: [CapacitySensitivity; 3],
    title: &str,
    file: &str,
) -> Result<(), PlotError> {
    let level_index = coverage_level_index()?;
    let regimes = sensitivity_regimes(sensitivity.trials());
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(title, (PLOT_FONT_FAMILY, 22_i32).into_font())
            .x_label_area_size(64_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0_usize..variants.len(), 0.0_f64..1.05_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_labels(variants.len())
            .x_label_formatter(&|index| {
                variants
                    .get(*index)
                    .map_or(String::new(), |variant| variant.name().to_owned())
            })
            .x_desc("sensitivity variant")
            .y_desc("empirical coverage for stated 80% interval")
            .label_style((PLOT_FONT_FAMILY, 18_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        chart
            .draw_series(LineSeries::new(
                [(0_usize, 0.8_f64), (variants.len(), 0.8_f64)],
                RGBColor(170, 170, 170).stroke_width(1),
            ))
            .map_err(|error| drawing_error(&error))?;
        for (regime_index, regime) in regimes.into_iter().enumerate() {
            let color = COLORS[regime_index % COLORS.len()];
            let points = variants
                .iter()
                .copied()
                .enumerate()
                .map(|(index, variant)| {
                    (
                        index,
                        sensitivity_coverage(sensitivity.trials(), regime, variant, level_index),
                    )
                })
                .collect::<Vec<_>>();
            chart
                .draw_series(LineSeries::new(points.clone(), color.stroke_width(2)))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(
                    points
                        .into_iter()
                        .map(|point| Circle::new(point, 5_i32, color.filled())),
                )
                .map_err(|error| drawing_error(&error))?;
            let final_coverage = sensitivity_coverage(
                sensitivity.trials(),
                regime,
                variants[variants.len().saturating_sub(1)],
                level_index,
            );
            let style = TextStyle::from((PLOT_FONT_FAMILY, 16_i32).into_font())
                .color(&color)
                .pos(Pos::new(HPos::Left, VPos::Center));
            chart
                .draw_series(once(Text::new(
                    format!("  {}", regime.name()),
                    (variants.len().saturating_sub(1), final_coverage),
                    style,
                )))
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join(file), svg)?;
    Ok(())
}

fn write_summary(directory: &Path, calibration: &CapacityCalibration) -> Result<(), PlotError> {
    let mut summary = String::from(concat!(
        "regime\ttrials\tobservations\tstated_coverage\tcovered\tempirical_coverage\t",
        "mean_absolute_error_ops_s\tmean_interval_width_ops_s\tmean_contraction\n",
    ));
    for regime in regimes(calibration.trials()) {
        let trials = calibration
            .trials()
            .iter()
            .filter(|trial| trial.regime == regime)
            .collect::<Vec<_>>();
        let trial_count = u32::try_from(trials.len())
            .map_err(|_| PlotError::Drawing("too many calibration trials".to_owned()))?;
        let observations = trials
            .iter()
            .map(|trial| u64::from(trial.observation_count))
            .sum::<u64>();
        let denominator = f64::from(trial_count.max(1));
        let mean_error = trials
            .iter()
            .map(|trial| trial.mean_absolute_error_per_second)
            .sum::<f64>()
            / denominator;
        let mean_uncertainty = trials
            .iter()
            .map(|trial| trial.mean_uncertainty_per_second)
            .sum::<f64>()
            / denominator;
        let mean_contraction = trials
            .iter()
            .map(|trial| trial.capacity_contraction)
            .sum::<f64>()
            / denominator;
        for (level_index, &level) in predictive_coverage_levels().iter().enumerate() {
            let covered = trials
                .iter()
                .map(|trial| u64::from(trial.covered_counts[level_index]))
                .sum::<u64>();
            let empirical = if observations == 0 {
                0.0_f64
            } else {
                count_f64(covered) / count_f64(observations)
            };
            writeln!(
                summary,
                "{}\t{}\t{}\t{level:.2}\t{}\t{empirical:.6}\t{mean_error:.6}\t{mean_uncertainty:.\
                 6}\t{mean_contraction:.6}",
                regime.name(),
                trial_count,
                observations,
                covered,
            )
            .map_err(|_| PlotError::Drawing("capacity calibration formatting failed".to_owned()))?;
        }
    }
    fs::write(directory.join("capacity-calibration.tsv"), summary)?;
    Ok(())
}

fn write_coverage(directory: &Path, calibration: &CapacityCalibration) -> Result<(), PlotError> {
    let regimes = regimes(calibration.trials());
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(
                "Capacity predictive coverage · gray means calibrated",
                (PLOT_FONT_FAMILY, 22_i32).into_font(),
            )
            .x_label_area_size(56_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0.45_f64..1.0_f64, 0.0_f64..1.05_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_desc("stated central coverage")
            .y_desc("empirical coverage")
            .label_style((PLOT_FONT_FAMILY, 20_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        chart
            .draw_series(LineSeries::new(
                vec![(0.45_f64, 0.45_f64), (1.0_f64, 1.0_f64)],
                RGBColor(170, 170, 170).stroke_width(1),
            ))
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in regimes.iter().copied().enumerate() {
            let points = predictive_coverage_levels()
                .iter()
                .copied()
                .enumerate()
                .map(|(level_index, level)| {
                    (
                        level,
                        empirical_coverage(calibration.trials(), regime, level_index),
                    )
                })
                .collect::<Vec<_>>();
            let color = COLORS[index % COLORS.len()];
            chart
                .draw_series(LineSeries::new(points.clone(), color.stroke_width(2)))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(
                    points
                        .into_iter()
                        .map(|point| Circle::new(point, 5_i32, color.filled())),
                )
                .map_err(|error| drawing_error(&error))?;
            let final_coverage = empirical_coverage(
                calibration.trials(),
                regime,
                predictive_coverage_levels().len().saturating_sub(1),
            );
            let style = TextStyle::from((PLOT_FONT_FAMILY, 17_i32).into_font())
                .color(&color)
                .pos(Pos::new(HPos::Right, VPos::Center));
            let label = format!("{} · {:.1}%", regime.name(), final_coverage * 100.0_f64);
            chart
                .draw_series(once(Text::new(label, (0.99_f64, final_coverage), style)))
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("capacity-coverage.svg"), svg)?;
    Ok(())
}

fn write_ranks(directory: &Path, calibration: &CapacityCalibration) -> Result<(), PlotError> {
    let regimes = regimes(calibration.trials());
    let regime_count = u32::try_from(regimes.len())
        .map_err(|_| PlotError::Drawing("too many calibration regimes".to_owned()))?;
    let height = PANEL_HEIGHT.saturating_mul(regime_count);
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, height)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        for (panel, regime) in root
            .split_evenly((regimes.len(), 1))
            .into_iter()
            .zip(regimes)
        {
            let counts = rank_counts(calibration.trials(), regime);
            let maximum_count = counts.iter().copied().max().map_or(1, |count| count).max(1);
            let total = counts.iter().copied().map(u64::from).sum::<u64>();
            let expected = count_f64(total) / 10.0_f64;
            let maximum = f64::from(maximum_count).max(expected) * 1.08_f64;
            let mut chart = ChartBuilder::on(&panel)
                .margin(12_u32)
                .caption(regime.name(), (PLOT_FONT_FAMILY, 22_i32).into_font())
                .x_label_area_size(48_u32)
                .y_label_area_size(80_u32)
                .build_cartesian_2d(0_u32..10_u32, 0.0_f64..maximum)
                .map_err(|error| drawing_error(&error))?;
            chart
                .configure_mesh()
                .disable_mesh()
                .x_desc("randomized predictive rank decile")
                .y_desc("observations")
                .label_style((PLOT_FONT_FAMILY, 18_i32).into_font())
                .draw()
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(counts.into_iter().zip(0_u32..).map(|(count, bin)| {
                    Rectangle::new(
                        [(bin, 0.0_f64), (bin + 1, f64::from(count))],
                        COLORS[0].filled(),
                    )
                }))
                .map_err(|error| drawing_error(&error))?;
            chart
                .draw_series(LineSeries::new(
                    [(0_u32, expected), (10_u32, expected)],
                    RGBColor(135, 135, 135).stroke_width(1),
                ))
                .map_err(|error| drawing_error(&error))?;
            let reference_style = TextStyle::from((PLOT_FONT_FAMILY, 15_i32).into_font())
                .color(&RGBColor(100, 100, 100))
                .pos(Pos::new(HPos::Right, VPos::Bottom));
            chart
                .draw_series(once(Text::new(
                    "uniform reference",
                    (10_u32, expected),
                    reference_style,
                )))
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("capacity-ranks.svg"), svg)?;
    Ok(())
}

fn write_error_uncertainty(
    directory: &Path,
    calibration: &CapacityCalibration,
) -> Result<(), PlotError> {
    let maximum = calibration
        .trials()
        .iter()
        .map(|trial| {
            trial
                .mean_absolute_error_per_second
                .max(trial.mean_uncertainty_per_second)
        })
        .fold(0.0_f64, f64::max)
        .max(1.0_f64);
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(
                "Forecast error against uncertainty · gray means equal",
                (PLOT_FONT_FAMILY, 22_i32).into_font(),
            )
            .x_label_area_size(56_u32)
            .y_label_area_size(90_u32)
            .build_cartesian_2d(0.0_f64..maximum * 1.05_f64, 0.0_f64..maximum * 1.05_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_desc("mean 80% interval width (operations/s)")
            .y_desc("mean absolute error (operations/s)")
            .label_style((PLOT_FONT_FAMILY, 20_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        chart
            .draw_series(LineSeries::new(
                [(0.0_f64, 0.0_f64), (maximum, maximum)],
                RGBColor(170, 170, 170).stroke_width(1),
            ))
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in regimes(calibration.trials()).into_iter().enumerate() {
            let color = COLORS[index % COLORS.len()];
            let mut uncertainty_sum = 0.0_f64;
            let mut error_sum = 0.0_f64;
            let mut count = 0_u32;
            for trial in calibration
                .trials()
                .iter()
                .filter(|trial| trial.regime == regime)
            {
                uncertainty_sum += trial.mean_uncertainty_per_second;
                error_sum += trial.mean_absolute_error_per_second;
                count = count.saturating_add(1);
            }
            chart
                .draw_series(
                    calibration
                        .trials()
                        .iter()
                        .filter(|trial| trial.regime == regime)
                        .map(|trial| {
                            Circle::new(
                                (
                                    trial.mean_uncertainty_per_second,
                                    trial.mean_absolute_error_per_second,
                                ),
                                5_i32,
                                color.filled(),
                            )
                        }),
                )
                .map_err(|error| drawing_error(&error))?;
            if count > 0 {
                let denominator = f64::from(count);
                let style = TextStyle::from((PLOT_FONT_FAMILY, 16_i32).into_font())
                    .color(&color)
                    .pos(Pos::new(HPos::Left, VPos::Bottom));
                chart
                    .draw_series(once(Text::new(
                        regime.name(),
                        (uncertainty_sum / denominator, error_sum / denominator),
                        style,
                    )))
                    .map_err(|error| drawing_error(&error))?;
            }
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("capacity-error-uncertainty.svg"), svg)?;
    Ok(())
}

fn write_contraction(directory: &Path, calibration: &CapacityCalibration) -> Result<(), PlotError> {
    let regimes = regimes(calibration.trials());
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
        root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
        let mut chart = ChartBuilder::on(&root)
            .margin(12_u32)
            .caption(
                "Capacity posterior contraction by regime",
                (PLOT_FONT_FAMILY, 22_i32).into_font(),
            )
            .x_label_area_size(72_u32)
            .y_label_area_size(80_u32)
            .build_cartesian_2d(0_usize..regimes.len(), -0.2_f64..1.0_f64)
            .map_err(|error| drawing_error(&error))?;
        chart
            .configure_mesh()
            .disable_mesh()
            .x_labels(regimes.len())
            .x_label_formatter(&|index| {
                regimes
                    .get(*index)
                    .map_or(String::new(), |regime| regime.name().to_owned())
            })
            .x_desc("operating regime")
            .y_desc("prior-to-posterior width contraction")
            .label_style((PLOT_FONT_FAMILY, 18_i32).into_font())
            .draw()
            .map_err(|error| drawing_error(&error))?;
        chart
            .draw_series(LineSeries::new(
                [(0_usize, 0.0_f64), (regimes.len(), 0.0_f64)],
                RGBColor(170, 170, 170).stroke_width(1),
            ))
            .map_err(|error| drawing_error(&error))?;
        for (index, regime) in regimes.into_iter().enumerate() {
            chart
                .draw_series(
                    calibration
                        .trials()
                        .iter()
                        .filter(|trial| trial.regime == regime)
                        .map(|trial| {
                            Circle::new(
                                (index, trial.capacity_contraction),
                                5_i32,
                                COLORS[index % COLORS.len()].filled(),
                            )
                        }),
                )
                .map_err(|error| drawing_error(&error))?;
        }
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(directory.join("capacity-contraction.svg"), svg)?;
    Ok(())
}

fn regimes(trials: &[CapacityCalibrationTrial]) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in trials {
        if !regimes.contains(&trial.regime) {
            regimes.push(trial.regime);
        }
    }
    regimes
}

fn empirical_coverage(
    trials: &[CapacityCalibrationTrial],
    regime: PrincipalRegime,
    level_index: usize,
) -> f64 {
    let (covered, total) = trials.iter().filter(|trial| trial.regime == regime).fold(
        (0_u64, 0_u64),
        |(covered, total), trial| {
            (
                covered + u64::from(trial.covered_counts[level_index]),
                total + u64::from(trial.observation_count),
            )
        },
    );
    if total == 0 {
        0.0_f64
    } else {
        count_f64(covered) / count_f64(total)
    }
}

fn rank_counts(trials: &[CapacityCalibrationTrial], regime: PrincipalRegime) -> [u32; 10] {
    let mut counts = [0_u32; 10];
    for trial in trials.iter().filter(|trial| trial.regime == regime) {
        for (count, add) in counts.iter_mut().zip(trial.rank_counts) {
            *count = count.saturating_add(add);
        }
    }
    counts
}

fn sensitivity_regimes(trials: &[CapacitySensitivityTrial]) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in trials {
        if !regimes.contains(&trial.calibration.regime) {
            regimes.push(trial.calibration.regime);
        }
    }
    regimes
}

fn demand_regimes(trials: &[DemandCalibrationTrial]) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in trials {
        if !regimes.contains(&trial.regime) {
            regimes.push(trial.regime);
        }
    }
    regimes
}

fn demand_counts(trials: &[&DemandCalibrationTrial], level_index: usize) -> (u64, u64) {
    trials
        .iter()
        .fold((0_u64, 0_u64), |(covered, total), trial| {
            (
                covered.saturating_add(u64::from(trial.covered_counts[level_index])),
                total.saturating_add(u64::from(trial.observation_count)),
            )
        })
}

fn demand_coverage(
    trials: &[DemandCalibrationTrial],
    regime: PrincipalRegime,
    level_index: usize,
) -> f64 {
    let (covered, total) = trials.iter().filter(|trial| trial.regime == regime).fold(
        (0_u64, 0_u64),
        |(covered, total), trial| {
            (
                covered.saturating_add(u64::from(trial.covered_counts[level_index])),
                total.saturating_add(u64::from(trial.observation_count)),
            )
        },
    );
    ratio(covered, total)
}

fn demand_rank_counts(trials: &[DemandCalibrationTrial], regime: PrincipalRegime) -> [u32; 10] {
    let mut counts = [0_u32; 10];
    for trial in trials.iter().filter(|trial| trial.regime == regime) {
        for (count, add) in counts.iter_mut().zip(trial.rank_counts) {
            *count = count.saturating_add(add);
        }
    }
    counts
}

struct LeadTimeDirectionView<'a> {
    calibration: &'a LeadTimeCalibration,
    direction: TransitionDirection,
}

trait CategoricalCalibrationPlot {
    type Trial;
    const RANK_AXIS_LABEL: &'static str;

    fn plot_trials(&self) -> &[Self::Trial];
    fn include(&self, trial: &Self::Trial) -> bool;
    fn regime(trial: &Self::Trial) -> PrincipalRegime;
    fn observation_count(trial: &Self::Trial) -> u32;
    fn covered_count(trial: &Self::Trial, level_index: usize) -> u32;
    fn rank_counts(trial: &Self::Trial) -> [u32; 10];
    fn error(trial: &Self::Trial) -> f64;
    fn uncertainty(trial: &Self::Trial) -> f64;
    fn contraction(trial: &Self::Trial) -> f64;
}

impl CategoricalCalibrationPlot for LeadTimeDirectionView<'_> {
    type Trial = LeadTimeCalibrationTrial;

    const RANK_AXIS_LABEL: &'static str = "predictive probability decile";

    fn plot_trials(&self) -> &[Self::Trial] {
        self.calibration.trials()
    }

    fn include(&self, trial: &Self::Trial) -> bool {
        trial.direction == self.direction && trial.observation_count > 0
    }

    fn regime(trial: &Self::Trial) -> PrincipalRegime {
        trial.regime
    }

    fn observation_count(trial: &Self::Trial) -> u32 {
        trial.observation_count
    }

    fn covered_count(trial: &Self::Trial, level_index: usize) -> u32 {
        trial.covered_counts[level_index]
    }

    fn rank_counts(trial: &Self::Trial) -> [u32; 10] {
        trial.rank_counts
    }

    fn error(trial: &Self::Trial) -> f64 {
        trial.mean_absolute_error_seconds
    }

    fn uncertainty(trial: &Self::Trial) -> f64 {
        trial.mean_uncertainty_seconds
    }

    fn contraction(trial: &Self::Trial) -> f64 {
        trial.posterior_contraction
    }
}

impl CategoricalCalibrationPlot for PartitionCalibration {
    type Trial = PartitionCalibrationTrial;

    const RANK_AXIS_LABEL: &'static str = "randomized predictive rank decile";

    fn plot_trials(&self) -> &[Self::Trial] {
        self.trials()
    }

    fn include(&self, _: &Self::Trial) -> bool {
        true
    }

    fn regime(trial: &Self::Trial) -> PrincipalRegime {
        trial.regime
    }

    fn observation_count(trial: &Self::Trial) -> u32 {
        trial.observation_count
    }

    fn covered_count(trial: &Self::Trial, level_index: usize) -> u32 {
        trial.covered_counts[level_index]
    }

    fn rank_counts(trial: &Self::Trial) -> [u32; 10] {
        trial.rank_counts
    }

    fn error(trial: &Self::Trial) -> f64 {
        trial.mean_log_loss
    }

    fn uncertainty(trial: &Self::Trial) -> f64 {
        trial.mean_entropy
    }

    fn contraction(trial: &Self::Trial) -> f64 {
        trial.entropy_contraction
    }
}

fn categorical_regimes<Calibration: CategoricalCalibrationPlot>(
    calibration: &Calibration,
) -> Vec<PrincipalRegime> {
    let mut regimes = Vec::new();
    for trial in calibration
        .plot_trials()
        .iter()
        .filter(|trial| calibration.include(trial))
    {
        let regime = Calibration::regime(trial);
        if !regimes.contains(&regime) {
            regimes.push(regime);
        }
    }
    regimes
}

fn categorical_counts<Calibration: CategoricalCalibrationPlot>(
    calibration: &Calibration,
    regime: PrincipalRegime,
    level_index: usize,
) -> (u64, u64) {
    calibration
        .plot_trials()
        .iter()
        .filter(|trial| calibration.include(trial) && Calibration::regime(trial) == regime)
        .fold((0_u64, 0_u64), |(covered, total), trial| {
            (
                covered.saturating_add(u64::from(Calibration::covered_count(trial, level_index))),
                total.saturating_add(u64::from(Calibration::observation_count(trial))),
            )
        })
}

fn categorical_rank_counts<Calibration: CategoricalCalibrationPlot>(
    calibration: &Calibration,
    regime: PrincipalRegime,
) -> [u32; 10] {
    let mut counts = [0_u32; 10];
    for trial in calibration
        .plot_trials()
        .iter()
        .filter(|trial| calibration.include(trial) && Calibration::regime(trial) == regime)
    {
        for (count, add) in counts.iter_mut().zip(Calibration::rank_counts(trial)) {
            *count = count.saturating_add(add);
        }
    }
    counts
}

fn coverage_level_index() -> Result<usize, PlotError> {
    predictive_coverage_levels()
        .iter()
        .position(|level| (*level - 0.8_f64).abs() <= f64::EPSILON)
        .ok_or_else(|| PlotError::Drawing("the 80% calibration level is missing".to_owned()))
}

const fn transition_direction_name(direction: TransitionDirection) -> &'static str {
    match direction {
        TransitionDirection::Up => "up",
        TransitionDirection::Down => "down",
    }
}

fn sensitivity_counts(
    trials: &[CapacitySensitivityTrial],
    regime: PrincipalRegime,
    sensitivity: CapacitySensitivity,
    level_index: usize,
) -> (u32, u64, u64) {
    trials
        .iter()
        .filter(|trial| trial.calibration.regime == regime && trial.sensitivity == sensitivity)
        .fold(
            (0_u32, 0_u64, 0_u64),
            |(trial_count, observations, covered), trial| {
                (
                    trial_count.saturating_add(1),
                    observations.saturating_add(u64::from(trial.calibration.observation_count)),
                    covered
                        .saturating_add(u64::from(trial.calibration.covered_counts[level_index])),
                )
            },
        )
}

fn sensitivity_coverage(
    trials: &[CapacitySensitivityTrial],
    regime: PrincipalRegime,
    sensitivity: CapacitySensitivity,
    level_index: usize,
) -> f64 {
    let (_, observations, covered) = sensitivity_counts(trials, regime, sensitivity, level_index);
    ratio(covered, observations)
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0_f64
    } else {
        count_f64(numerator) / count_f64(denominator)
    }
}

fn count_f64(value: u64) -> f64 {
    let high = (value >> 32_u32) as u32;
    let low = value as u32;
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn drawing_error<Error>(error: &DrawingAreaErrorKind<Error>) -> PlotError
where
    Error: StdError + Send + Sync + fmt::Debug,
{
    PlotError::Drawing(format!("{error:?}"))
}
