use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::iter::once;
use std::path::Path;

use plotters::coord::Shift;
use plotters::prelude::*;

use crate::{CapacityEvidenceKind, ControllerTrace, PLOT_FONT_FAMILY, PlotError};

const WIDTH: u32 = 1_240;
const HEIGHT: u32 = 430;
const TITLE_HEIGHT: u32 = 64;
const MASS_COLOR: RGBColor = RGBColor(33, 145, 140);
const MEDIAN_COLOR: RGBColor = RGBColor(155, 65, 45);
const INTERVAL_COLOR: RGBColor = RGBColor(135, 135, 135);

/// Writes prior, important-update, and final capacity distributions.
///
/// # Errors
///
/// Returns an error when the trace is empty or output fails.
pub fn write_capacity_belief_svg(
    path: &Path,
    title: &str,
    controller: &ControllerTrace,
) -> Result<(), PlotError> {
    if controller.is_empty() || controller.capacity_posterior_values().is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    let important = important_update(controller);
    let final_index = controller.len() - 1;
    let final_mass = controller
        .capacity_posterior(final_index)
        .ok_or(PlotError::EmptyTrace)?;
    let (important_label, important_mass) = important.map_or_else(
        || {
            (
                "no accepted capacity evidence".to_owned(),
                controller.capacity_prior(),
            )
        },
        |index| {
            let label = controller.sample(index).map_or_else(
                || "accepted capacity evidence".to_owned(),
                |sample| {
                    format!(
                        "{} at {}",
                        evidence_name(sample.capacity_evidence.kind()),
                        format_time(sample.at_micros)
                    )
                },
            );
            (
                label,
                controller
                    .capacity_posterior(index)
                    .map_or(controller.capacity_prior(), |mass| mass),
            )
        },
    );
    let snapshots = [
        Snapshot {
            title: "prior",
            mass: controller.capacity_prior(),
        },
        Snapshot {
            title: &important_label,
            mass: important_mass,
        },
        Snapshot {
            title: "final posterior",
            mass: final_mass,
        },
    ];
    let mut svg = String::new();
    {
        let root = SVGBackend::with_string(&mut svg, (WIDTH, HEIGHT)).into_drawing_area();
        draw(
            &root,
            title,
            controller.capacity_posterior_values(),
            snapshots,
        )
        .map_err(|error| drawing_error(&error))?;
        root.present().map_err(|error| drawing_error(&error))
    }?;
    fs::write(path, svg)?;
    Ok(())
}

fn draw<Backend: DrawingBackend>(
    root: &DrawingArea<Backend, Shift>,
    title: &str,
    values: &[f64],
    snapshots: [Snapshot<'_>; 3],
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    root.fill(&WHITE)?;
    let (title_area, panels_area) = root.split_vertically(TITLE_HEIGHT);
    title_area.draw(&Text::new(
        title,
        (56_i32, 24_i32),
        (PLOT_FONT_FAMILY, 21_i32).into_font().color(&BLACK),
    ))?;
    title_area.draw(&Text::new(
        "capacity probability mass · interval marks 10% to 90%",
        (56_i32, 48_i32),
        (PLOT_FONT_FAMILY, 12_i32)
            .into_font()
            .color(&RGBColor(80, 80, 80)),
    ))?;
    for (area, snapshot) in panels_area.split_evenly((1, 3)).into_iter().zip(snapshots) {
        draw_snapshot(&area, values, snapshot)?;
    }
    Ok(())
}

fn draw_snapshot<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    values: &[f64],
    snapshot: Snapshot<'_>,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let x_min = values.first().copied().unwrap_or(0.0_f64);
    let x_max = values
        .last()
        .copied()
        .unwrap_or(1.0_f64)
        .max(x_min + 1.0_f64);
    let y_max = snapshot
        .mass
        .iter()
        .copied()
        .filter(|mass| mass.is_finite())
        .fold(0.0_f64, f64::max)
        .max(1.0_f64)
        * 1.14_f64;
    let mut chart = ChartBuilder::on(area)
        .margin_left(12_u32)
        .margin_right(12_u32)
        .margin_top(12_u32)
        .margin_bottom(12_u32)
        .caption(snapshot.title, (PLOT_FONT_FAMILY, 14_i32).into_font())
        .x_label_area_size(38_u32)
        .y_label_area_size(52_u32)
        .build_cartesian_2d(x_min..x_max, 0.0_f64..y_max)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(4)
        .y_labels(3)
        .x_desc("operations per second")
        .y_desc("mass")
        .axis_style(RGBColor(180, 180, 180))
        .label_style(
            (PLOT_FONT_FAMILY, 10_i32)
                .into_font()
                .color(&RGBColor(65, 65, 65)),
        )
        .draw()?;
    chart.draw_series(
        values
            .iter()
            .copied()
            .zip(snapshot.mass.iter().copied())
            .flat_map(|(value, mass)| {
                [
                    PathElement::new(
                        vec![(value, 0.0_f64), (value, mass)],
                        MASS_COLOR.stroke_width(1),
                    ),
                    PathElement::new(
                        vec![(value, mass), (value, mass)],
                        MASS_COLOR.stroke_width(3),
                    ),
                ]
            }),
    )?;
    let [low, median, high] = quantiles(values, snapshot.mass);
    for (value, color, width) in [
        (low, INTERVAL_COLOR, 1_u32),
        (high, INTERVAL_COLOR, 1_u32),
        (median, MEDIAN_COLOR, 2_u32),
    ] {
        chart.draw_series(once(PathElement::new(
            vec![(value, 0.0_f64), (value, y_max * 0.96_f64)],
            color.stroke_width(width),
        )))?;
    }
    Ok(())
}

fn important_update(controller: &ControllerTrace) -> Option<usize> {
    let mut previous = controller.capacity_prior();
    let mut selected = None;
    let mut maximum_change = 0.0_f64;
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(posterior) = controller.capacity_posterior(index) else {
            continue;
        };
        if sample.capacity_evidence.kind() != CapacityEvidenceKind::None {
            let change = previous
                .iter()
                .zip(posterior)
                .map(|(before, after)| (before - after).abs())
                .sum::<f64>()
                / 2.0_f64;
            if change > maximum_change {
                maximum_change = change;
                selected = Some(index);
            }
        }
        previous = posterior;
    }
    selected
}

fn quantiles(values: &[f64], mass: &[f64]) -> [f64; 3] {
    let fallback = values.last().copied().unwrap_or(0.0_f64);
    let mut result = [fallback; 3];
    let thresholds = [0.1_f64, 0.5_f64, 0.9_f64];
    let mut threshold_index = 0_usize;
    let mut cumulative = 0.0_f64;
    for (&value, &probability) in values.iter().zip(mass) {
        cumulative += probability;
        while threshold_index < thresholds.len() && cumulative >= thresholds[threshold_index] {
            result[threshold_index] = value;
            threshold_index += 1;
        }
    }
    result
}

const fn evidence_name(kind: CapacityEvidenceKind) -> &'static str {
    match kind {
        CapacityEvidenceKind::None => "no evidence",
        CapacityEvidenceKind::Window => "resource window",
    }
}

fn format_time(micros: u64) -> String {
    format!("{:.1} s", u64_f64(micros) / 1_000_000.0_f64)
}

fn u64_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).map_or(0, |part| part);
    let low = u32::try_from(value & u64::from(u32::MAX)).map_or(0, |part| part);
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn drawing_error<Error>(error: &DrawingAreaErrorKind<Error>) -> PlotError
where
    Error: StdError + Send + Sync + fmt::Debug,
{
    PlotError::Drawing(format!("{error:?}"))
}

#[derive(Clone, Copy)]
struct Snapshot<'a> {
    title: &'a str,
    mass: &'a [f64],
}

#[cfg(test)]
#[path = "capacity_plot_tests.rs"]
mod tests;
