use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::iter::once;
use std::path::Path;
use std::time::Duration;

use plotters::coord::Shift;
use plotters::coord::types::{RangedCoordf64, RangedCoordu64};
use plotters::prelude::*;

use crate::posterior_plot::{PosteriorHeatmap, draw_color_key, draw_posterior_heatmap};
use crate::visual::{AxisScale, LinePattern, Quantity, label_margin, semantic_style, shape};
use crate::{
    ArrivalEvidenceSample, CapacityEvidenceSample, ControllerTrace, ImageManifestEntry,
    MetricTrace, PLOT_FONT_FAMILY, PanelContent, PlotError, ReportSection, RunStop, SeriesCell,
    SeriesHistory, label_inside_image,
};

const WIDTH: u32 = 1_240;
const PANEL_HEIGHT: u32 = 340;
const PANEL_COUNT: u32 = 19;
const LABEL_GAP_FRACTION: f64 = 0.08_f64;
const LABEL_FONT_PIXELS: u32 = 19;
const CHART_MARGIN_LEFT: u32 = 12;
const CHART_MARGIN_RIGHT: u32 = 16;
const COLOR_KEY_WIDTH: u32 = 90;
const STORY_FILES: [&str; PANEL_COUNT as usize] = [
    "01-demand.svg",
    "02-backlog.svg",
    "03-scale.svg",
    "04-saturation-cap.svg",
    "05-latency.svg",
    "06-risk.svg",
    "07-capacity-evidence.svg",
    "08-capacity-posterior.svg",
    "09-arrival-predictive.svg",
    "10-service-inputs.svg",
    "11-shared-resource.svg",
    "12-actuation.svg",
    "13-reporter-coverage.svg",
    "14-snapshot-age.svg",
    "15-reliability-evidence.svg",
    "16-decision-pass.svg",
    "17-decision-loss.svg",
    "18-capacity-trace.svg",
    "19-capacity-coverage.svg",
];

/// Evidence for one regime story.
pub struct RegimeStory<'a> {
    /// Plant and controller metrics.
    pub trace: &'a MetricTrace,
    /// Exact controller decisions and evidence.
    pub controller: &'a ControllerTrace,
    /// Calculated regime inputs.
    pub inputs: &'a SeriesHistory,
    /// Exact duration and stop reason.
    pub stop: RunStop,
    /// Latency objective in microseconds.
    pub budget_micros: u64,
    /// Allowed SLO miss fraction.
    pub allowed_miss_fraction: f64,
}

/// Writes the causal story as separate figures.
///
/// The numbered files preserve the evidence-to-outcome order. Each file has
/// one chart. The report owns its heading and caption.
///
/// # Errors
///
/// Returns an error when the trace is empty or output fails.
pub fn write_regime_story_figures(
    directory: &Path,
    story: &RegimeStory<'_>,
) -> Result<Vec<ImageManifestEntry>, PlotError> {
    let panels = story_panels(story)?;
    if panels.iter().all(|panel| panel.series.is_empty()) {
        return Err(PlotError::EmptyTrace);
    }
    fs::create_dir_all(directory)?;
    let mut manifest = Vec::with_capacity(panels.len());
    for (index, (file, panel)) in STORY_FILES.iter().zip(&panels).enumerate() {
        let content = panel.content();
        let entry = ImageManifestEntry {
            file: format!("story/{file}"),
            section: story_section(index),
            content,
            clipped_label: panel.clipped_label(),
            color_key_present: panel.heatmap.is_some(),
            requires_color_key: panel.heatmap.is_some(),
            comparison_scale: None,
        };
        manifest.push(entry);
        if content != PanelContent::Visible {
            continue;
        }
        let mut svg = String::new();
        {
            let root = SVGBackend::with_string(&mut svg, (WIDTH, PANEL_HEIGHT)).into_drawing_area();
            root.fill(&WHITE).map_err(|error| drawing_error(&error))?;
            draw_panel(&root, panel).map_err(|error| drawing_error(&error))?;
            root.present().map_err(|error| drawing_error(&error))?;
        };
        fs::write(
            directory.join(file),
            svg.replace("<rect ", "<rect shape-rendering=\"crispEdges\" "),
        )?;
    }
    Ok(manifest)
}

fn draw_panel<Backend: DrawingBackend>(
    area: &DrawingArea<Backend, Shift>,
    panel: &StoryPanel,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let key_width = if panel.heatmap.is_some() {
        COLOR_KEY_WIDTH
    } else {
        0
    };
    let (chart_area, key_area) =
        area.split_horizontally(area.dim_in_pixel().0.saturating_sub(key_width));
    let final_micros = panel.horizon_micros.unwrap_or(1).max(1);
    let (minimum, maximum) = panel_bounds(panel);
    let mut chart = ChartBuilder::on(&chart_area)
        .margin_left(CHART_MARGIN_LEFT)
        .margin_right(CHART_MARGIN_RIGHT)
        .margin_top(8_u32)
        .margin_bottom(4_u32)
        .x_label_area_size(48_u32)
        .y_label_area_size(label_margin(
            panel.series.iter().map(|series| series.label.len()),
        ))
        .build_cartesian_2d(0_u64..final_micros, minimum..maximum)?;
    chart
        .configure_mesh()
        .disable_mesh()
        .x_labels(4)
        .y_labels(3)
        .x_desc("virtual time")
        .y_desc(panel.unit)
        .x_label_formatter(&|micros| format_time(*micros))
        .y_label_formatter(&|value| panel.quantity().format(panel.axis.restore(*value)))
        .axis_style(RGBColor(180, 180, 180))
        .label_style(
            (PLOT_FONT_FAMILY, 20_i32)
                .into_font()
                .color(&RGBColor(65, 65, 65)),
        )
        .draw()?;
    let label_positions = label_positions(panel, minimum, maximum);
    draw_heatmap_layer(&mut chart, panel, final_micros)?;
    draw_series_layers(&mut chart, panel, &label_positions)?;
    draw_annotations(&mut chart, panel, minimum, maximum)?;
    if panel.heatmap.is_some() {
        draw_color_key(&key_area)?;
    }
    Ok(())
}

fn draw_series_layers<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    panel: &StoryPanel,
    label_positions: &[Option<f64>],
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let (plot_left, plot_right) = story_plot_horizontal_bounds(panel);
    let preferred_label_x = plot_left + (plot_right - plot_left) * 2_i32 / 3_i32;
    for (index, series) in panel.series.iter().enumerate() {
        let semantic = semantic_style(&series.label);
        let color = match series.style {
            SeriesStyle::LightLine => RGBColor(145, 145, 145),
            _ => semantic.color,
        };
        let points = series
            .at_micros
            .iter()
            .copied()
            .zip(series.values.iter().copied())
            .filter(|(_, value)| value.is_finite());
        match series.style {
            SeriesStyle::Line => {
                chart.draw_series(LineSeries::new(points, shape(semantic, 2)))?;
            }
            SeriesStyle::Step => {
                chart.draw_series(LineSeries::new(step_points(series), shape(semantic, 2)))?;
            }
            SeriesStyle::LightLine => {
                chart.draw_series(LineSeries::new(points, color.stroke_width(1)))?;
            }
            SeriesStyle::Points => {
                chart.draw_series(points.map(|point| Circle::new(point, 4_i32, color.filled())))?;
            }
        }
        if matches!(semantic.pattern, LinePattern::Dashed | LinePattern::Dotted)
            && matches!(series.style, SeriesStyle::Line | SeriesStyle::Step)
        {
            let spacing = if semantic.pattern == LinePattern::Dashed {
                3
            } else {
                2
            };
            chart.draw_series(
                series
                    .at_micros
                    .iter()
                    .copied()
                    .zip(series.values.iter().copied())
                    .enumerate()
                    .filter(|(point, (_, value))| point % spacing == 0 && value.is_finite())
                    .map(|(_, point)| Circle::new(point, 3_i32, color.filled())),
            )?;
        }
        if let Some(label_y) = label_positions[index] {
            draw_label(chart, series, color, preferred_label_x, plot_left, label_y)?;
        }
    }
    Ok(())
}

fn draw_annotations<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    panel: &StoryPanel,
    minimum: f64,
    maximum: f64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    for annotation in &panel.annotations {
        chart.draw_series(once(PathElement::new(
            vec![
                (annotation.at_micros, minimum),
                (annotation.at_micros, maximum),
            ],
            RGBColor(155, 65, 45).stroke_width(1),
        )))?;
        chart.draw_series(once(Text::new(
            annotation.label.clone(),
            (
                annotation.at_micros,
                minimum + (maximum - minimum) * 0.88_f64,
            ),
            (PLOT_FONT_FAMILY, 18_i32)
                .into_font()
                .color(&RGBColor(155, 65, 45)),
        )))?;
    }
    Ok(())
}

fn draw_heatmap_layer<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    panel: &StoryPanel,
    final_micros: u64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let Some(heatmap) = &panel.heatmap else {
        return Ok(());
    };
    draw_posterior_heatmap(chart, heatmap, final_micros)
}

fn draw_label<Backend: DrawingBackend>(
    chart: &mut ChartContext<'_, Backend, Cartesian2d<RangedCoordu64, RangedCoordf64>>,
    series: &StorySeries,
    color: RGBColor,
    preferred_x: i32,
    plot_left: i32,
    label_y: f64,
) -> Result<(), DrawingAreaErrorKind<Backend::ErrorType>> {
    let Some(index) = meaningful_index(&series.values) else {
        return Ok(());
    };
    let point = (series.at_micros[index], series.values[index]);
    let text = series_label_text(series, point.1);
    let plotting_area = chart.plotting_area();
    let point_pixel = plotting_area.map_coordinate(&point);
    let label_y_pixel = plotting_area.map_coordinate(&(point.0, label_y)).1;
    let label_x = label_x_anchor(&text, LABEL_FONT_PIXELS, preferred_x, plot_left);
    let screen = plotting_area.use_screen_coord();
    screen.draw(&PathElement::new(
        vec![point_pixel, (label_x, label_y_pixel)],
        color.stroke_width(1),
    ))?;
    screen.draw(&Text::new(
        text,
        (label_x, label_y_pixel),
        (PLOT_FONT_FAMILY, LABEL_FONT_PIXELS)
            .into_font()
            .color(&color),
    ))?;
    Ok(())
}

fn series_label_text(series: &StorySeries, value: f64) -> String {
    format!("{} {}", series.label, panel_value_label(series, value))
}

fn label_x_anchor(text: &str, font_pixels: u32, preferred_x: i32, plot_left: i32) -> i32 {
    let mut anchor = preferred_x.max(plot_left);
    while anchor > plot_left
        && !label_inside_image((WIDTH, PANEL_HEIGHT), (anchor, 8_i32), text, font_pixels)
    {
        anchor -= 1_i32;
    }
    anchor
}

fn meaningful_index(values: &[f64]) -> Option<usize> {
    values
        .iter()
        .enumerate()
        .rev()
        .find(|(_, value)| value.is_finite() && **value != 0.0_f64)
        .map(|(index, _)| index)
}

fn panel_value_label(series: &StorySeries, value: f64) -> String {
    series.quantity.format(series.axis.restore(value))
}

fn label_positions(panel: &StoryPanel, minimum: f64, maximum: f64) -> Vec<Option<f64>> {
    let mut positions = panel
        .series
        .iter()
        .map(|series| meaningful_index(&series.values).map(|index| series.values[index]))
        .collect::<Vec<_>>();
    let mut order = (0..positions.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        positions[*left]
            .unwrap_or(f64::INFINITY)
            .total_cmp(&positions[*right].unwrap_or(f64::INFINITY))
    });
    let gap = (maximum - minimum) * LABEL_GAP_FRACTION;
    let mut previous = minimum - gap;
    let mut final_position = None;
    for index in order {
        let Some(value) = positions[index] else {
            continue;
        };
        let position = value.max(previous + gap);
        positions[index] = Some(position);
        previous = position;
        final_position = Some(position);
    }
    if let Some(final_position) = final_position {
        let overflow = (final_position - maximum).max(0.0_f64);
        for position in positions.iter_mut().flatten() {
            *position -= overflow;
        }
    }
    positions
}

fn story_panels(story: &RegimeStory<'_>) -> Result<[StoryPanel; PANEL_COUNT as usize], PlotError> {
    let trace = story.trace;
    if trace.at_micros.is_empty() {
        return Err(PlotError::EmptyTrace);
    }
    let budget_seconds = Duration::from_micros(story.budget_micros).as_secs_f64();
    let activity_end_micros = activity_end_micros(trace);
    let resource_windows = capacity_evidence_series(story.controller);
    let mut panels = [
        work_panel(story.inputs),
        queue_panel(trace),
        scale_panel(trace, story.inputs),
        cap_panel(trace),
        latency_panel(trace, budget_seconds),
        risk_panel(trace, story.allowed_miss_fraction),
        evidence_panel(resource_windows),
        posterior_panel(trace, story.controller),
        arrival_predictive_panel(story.controller),
        service_panel(trace, story.inputs, activity_end_micros),
        shared_resource_panel(trace, story.inputs),
        actuation_panel(trace, story.inputs),
        reporter_coverage_panel(trace),
        snapshot_age_panel(trace),
        reliability_evidence_panel(trace),
        decision_deadline_satisfaction_panel(trace, story.controller),
        decision_loss_panel(trace, story.controller),
        capacity_trace_panel(story.controller),
        capacity_predictive_coverage_panel(story.controller),
    ];
    finish_story_panels(&mut panels, story.stop.at_micros);
    Ok(panels)
}

fn finish_story_panels(panels: &mut [StoryPanel], horizon_micros: u64) {
    for panel in panels {
        panel.apply_axis();
        panel.horizon_micros = Some(horizon_micros);
    }
}

fn work_panel(inputs: &SeriesHistory) -> StoryPanel {
    StoryPanel::new(
        "events per interval",
        vec![
            input_series(inputs, "message_count", "current messages", count_value),
            input_series(inputs, "timer_count", "current timers", count_value),
            input_series(
                inputs,
                "historical_message_count",
                "historical messages",
                count_value,
            ),
        ],
    )
}

fn queue_panel(trace: &MetricTrace) -> StoryPanel {
    StoryPanel::new(
        "events per interval",
        vec![
            metric_u64(trace, "backlog", &trace.backlog, count),
            metric_u64(trace, "completed", &trace.useful_completions, count),
        ],
    )
}

fn scale_panel(trace: &MetricTrace, inputs: &SeriesHistory) -> StoryPanel {
    StoryPanel::new(
        "replicas",
        vec![
            metric_u32(trace, "actual", &trace.replicas).step(),
            input_series(
                inputs,
                "historical_replicas",
                "historical replicas",
                count_value,
            )
            .step(),
            input_series(inputs, "external_target", "experiment target", count_value).step(),
            metric_u32(trace, "controller target", &trace.target).step(),
        ],
    )
}

fn cap_panel(trace: &MetricTrace) -> StoryPanel {
    let mut panel = StoryPanel::new(
        "replicas",
        vec![metric_u32(trace, "saturation cap", &trace.cap)],
    );
    for index in 1..trace.cap.len() {
        let before = trace.cap[index - 1];
        let after = trace.cap[index];
        if before != after && before > 0 && after > 0 {
            panel.annotations.push(StoryAnnotation {
                at_micros: trace.at_micros[index],
                label: format!("cap {before} → {after}"),
            });
        }
    }
    panel
}

fn latency_panel(trace: &MetricTrace, budget_seconds: f64) -> StoryPanel {
    StoryPanel::new(
        "seconds",
        vec![
            metric_u64(trace, "p50", &trace.latency_p50_micros, micros),
            metric_u64(trace, "p99", &trace.latency_p99_micros, micros),
            metric_u64(trace, "p99.9", &trace.latency_p999_micros, micros),
            reference(trace, "SLO", budget_seconds),
        ],
    )
}

fn risk_panel(trace: &MetricTrace, allowed_miss_fraction: f64) -> StoryPanel {
    StoryPanel::new(
        "expected cost (event-delay-seconds)",
        vec![
            metric_f64(trace, "realized misses", &trace.miss_fraction),
            metric_f64(trace, "expected cost", &trace.expected_cost),
            metric_f64(
                trace,
                "saturation probability",
                &trace.saturation_probability,
            ),
            metric_f64(trace, "no-knee probability", &trace.no_knee_probability),
            reference(trace, "allowed misses", allowed_miss_fraction),
        ],
    )
}

fn evidence_panel(resource_windows: StorySeries) -> StoryPanel {
    StoryPanel::new("operations per second", vec![resource_windows])
}

fn posterior_panel(trace: &MetricTrace, controller: &ControllerTrace) -> StoryPanel {
    StoryPanel::new(
        "operations per second",
        vec![metric_f64(trace, "median", &trace.capacity_median_per_second).light()],
    )
    .with_heatmap(capacity_posterior_heatmap(controller))
}

fn capacity_posterior_heatmap(controller: &ControllerTrace) -> PosteriorHeatmap {
    let values = controller.capacity_posterior_values().to_vec();
    let width = values.len();
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut probabilities = Vec::with_capacity(controller.len().saturating_mul(width));
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(posterior) = controller.capacity_posterior(index) else {
            continue;
        };
        at_micros.push(sample.at_micros);
        probabilities.extend_from_slice(posterior);
    }
    PosteriorHeatmap {
        at_micros,
        values,
        probabilities,
    }
}

fn arrival_predictive_panel(controller: &ControllerTrace) -> StoryPanel {
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut observed = Vec::with_capacity(controller.len());
    let mut low = Vec::with_capacity(controller.len());
    let mut median = Vec::with_capacity(controller.len());
    let mut high = Vec::with_capacity(controller.len());
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        at_micros.push(sample.at_micros);
        observed.push(match sample.arrival_evidence {
            ArrivalEvidenceSample::None => f64::NAN,
            ArrivalEvidenceSample::Accepted(window) => f64::from(window.count),
        });
        low.push(sample.arrival_predictive_low_count);
        median.push(sample.arrival_predictive_median_count);
        high.push(sample.arrival_predictive_high_count);
    }
    StoryPanel::new(
        "accepted events per exposure",
        vec![
            StorySeries::new("accepted evidence", at_micros.clone(), observed),
            StorySeries::new("predictive 10%", at_micros.clone(), low).light(),
            StorySeries::new("predictive 50%", at_micros.clone(), median),
            StorySeries::new("predictive 90%", at_micros, high).light(),
        ],
    )
}

fn service_panel(trace: &MetricTrace, inputs: &SeriesHistory, horizon_micros: u64) -> StoryPanel {
    StoryPanel::new(
        "seconds",
        vec![
            input_series(inputs, "handler_micros", "base handler", duration_value),
            input_series(
                inputs,
                "dependency_operation_micros",
                "resource service",
                duration_value,
            ),
            metric_u64(
                trace,
                "handler elapsed p99",
                &trace.handler_elapsed_p99_micros,
                micros,
            ),
        ],
    )
    .with_horizon(horizon_micros)
}

fn shared_resource_panel(trace: &MetricTrace, inputs: &SeriesHistory) -> StoryPanel {
    StoryPanel::new(
        "operations per second",
        vec![
            input_series(
                inputs,
                "shared_resource_capacity_per_second",
                "nominal resource capacity",
                count_value,
            ),
            metric_f64(
                trace,
                "completed attempts",
                &trace.attempt_throughput_per_second,
            )
            .points(),
        ],
    )
}

fn actuation_panel(trace: &MetricTrace, inputs: &SeriesHistory) -> StoryPanel {
    StoryPanel::new(
        "seconds",
        vec![
            input_series(
                inputs,
                "launch_delay_micros",
                "configured launch",
                duration_value,
            ),
            metric_f64(trace, "inferred scale up", &trace.lead_time_up_seconds),
            metric_f64(trace, "inferred scale down", &trace.lead_time_down_seconds),
        ],
    )
}

fn reporter_coverage_panel(trace: &MetricTrace) -> StoryPanel {
    StoryPanel::new(
        "missing reporters",
        vec![metric_u32(trace, "missing", &trace.missing_reporters).step()],
    )
}

fn snapshot_age_panel(trace: &MetricTrace) -> StoryPanel {
    let maximum = trace
        .snapshot_age_micros
        .iter()
        .copied()
        .map(micros)
        .fold(0.0_f64, f64::max);
    let upper = maximum + maximum.max(1.0_f64) * 0.2_f64;
    let mut panel = StoryPanel::new(
        "seconds",
        vec![metric_u64(
            trace,
            "oldest accepted report",
            &trace.snapshot_age_micros,
            micros,
        )],
    );
    panel.vertical_bounds = Some((0.0_f64, upper));
    panel
}

fn reliability_evidence_panel(trace: &MetricTrace) -> StoryPanel {
    StoryPanel::new(
        "events per interval",
        vec![
            metric_u64(
                trace,
                "transient failures",
                &trace.transient_failures,
                count,
            ),
            metric_u64(
                trace,
                "permanent rejections",
                &trace.permanent_rejections,
                count,
            ),
        ],
    )
}

fn decision_loss_panel(trace: &MetricTrace, controller: &ControllerTrace) -> StoryPanel {
    StoryPanel::new(
        "replicas; light color means more expected cost",
        vec![
            metric_u32(trace, "actual", &trace.replicas).step(),
            metric_u32(trace, "selected target", &trace.target).points(),
            metric_u32(trace, "saturation cap", &trace.cap).step(),
        ],
    )
    .with_bounded_heatmap(decision_loss_heatmap(controller))
}

fn decision_deadline_satisfaction_panel(
    trace: &MetricTrace,
    controller: &ControllerTrace,
) -> StoryPanel {
    StoryPanel::new(
        "replicas; light color means higher deadline-satisfaction probability",
        vec![
            metric_u32(trace, "actual", &trace.replicas).step(),
            metric_u32(trace, "selected target", &trace.target).points(),
        ],
    )
    .with_bounded_heatmap(decision_deadline_satisfaction_heatmap(controller))
}

fn decision_deadline_satisfaction_heatmap(controller: &ControllerTrace) -> PosteriorHeatmap {
    let Some(first) = controller.decision_deadline_satisfaction_probabilities(0) else {
        return PosteriorHeatmap {
            at_micros: Vec::new(),
            values: Vec::new(),
            probabilities: Vec::new(),
        };
    };
    let values = (1_u32..)
        .zip(first)
        .map(|(replicas, _)| f64::from(replicas))
        .collect();
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut probabilities = Vec::with_capacity(controller.len().saturating_mul(first.len()));
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(satisfactions) = controller.decision_deadline_satisfaction_probabilities(index)
        else {
            continue;
        };
        at_micros.push(sample.at_micros);
        probabilities.extend_from_slice(satisfactions);
    }
    PosteriorHeatmap {
        at_micros,
        values,
        probabilities,
    }
}

fn decision_loss_heatmap(controller: &ControllerTrace) -> PosteriorHeatmap {
    let Some(first) = controller.decision_expected_costs(0) else {
        return PosteriorHeatmap {
            at_micros: Vec::new(),
            values: Vec::new(),
            probabilities: Vec::new(),
        };
    };
    let values = (1_u32..)
        .zip(first)
        .map(|(replicas, _)| f64::from(replicas))
        .collect();
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut expected_costs = Vec::with_capacity(controller.len().saturating_mul(first.len()));
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(costs) = controller.decision_expected_costs(index) else {
            continue;
        };
        at_micros.push(sample.at_micros);
        expected_costs.extend_from_slice(costs);
    }
    PosteriorHeatmap {
        at_micros,
        values,
        probabilities: expected_costs,
    }
}

fn capacity_trace_panel(controller: &ControllerTrace) -> StoryPanel {
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut initial = Vec::with_capacity(controller.len());
    let mut final_state = Vec::with_capacity(controller.len());
    let mut exposed_states = Vec::with_capacity(controller.len());
    let mut completion_states = Vec::with_capacity(controller.len());
    let mut transition_groups = Vec::with_capacity(controller.len());
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let Some(trace) = controller.capacity_trace(index) else {
            continue;
        };
        at_micros.push(sample.at_micros);
        initial.push(f64::from(trace.initial_busy_slots));
        final_state.push(f64::from(trace.final_busy_slots));
        let count = trace
            .state_exposure_seconds
            .iter()
            .filter(|exposure| **exposure > 0.0_f64)
            .count();
        exposed_states.push(usize_f64(count));
        let count = trace
            .state_completion_counts
            .iter()
            .filter(|completions| **completions > 0)
            .count();
        completion_states.push(usize_f64(count));
        transition_groups.push(usize_f64(trace.transition_groups.len()));
    }
    StoryPanel::new(
        "busy-slot states",
        vec![
            StorySeries::new("initial busy slots", at_micros.clone(), initial).points(),
            StorySeries::new("final busy slots", at_micros.clone(), final_state).points(),
            StorySeries::new(
                "states with exposure E_n",
                at_micros.clone(),
                exposed_states,
            )
            .points(),
            StorySeries::new(
                "states with completions D_n",
                at_micros.clone(),
                completion_states,
            )
            .points(),
            StorySeries::new("equal-clock trace groups", at_micros, transition_groups).points(),
        ],
    )
}

fn capacity_predictive_coverage_panel(controller: &ControllerTrace) -> StoryPanel {
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut interval_hit = Vec::with_capacity(controller.len());
    let mut cumulative_coverage = Vec::with_capacity(controller.len());
    let mut covered = 0_u32;
    let mut total = 0_u32;
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        let observed = match sample.capacity_evidence {
            CapacityEvidenceSample::None => continue,
            CapacityEvidenceSample::Window(window) => window.throughput_per_second(),
        };
        let hit = observed >= sample.capacity_predictive_low_per_second
            && observed <= sample.capacity_predictive_high_per_second;
        covered = covered.saturating_add(u32::from(hit));
        total = total.saturating_add(1);
        at_micros.push(sample.at_micros);
        interval_hit.push(f64::from(u8::from(hit)));
        cumulative_coverage.push(f64::from(covered) / f64::from(total));
    }
    StoryPanel::new(
        "fraction",
        vec![
            StorySeries::new("interval hit", at_micros.clone(), interval_hit).points(),
            StorySeries::new(
                "cumulative coverage",
                at_micros.clone(),
                cumulative_coverage,
            ),
            StorySeries::reference(
                "stated 80% coverage",
                at_micros.clone(),
                vec![0.8_f64; at_micros.len()],
            ),
        ],
    )
}

fn capacity_evidence_series(controller: &ControllerTrace) -> StorySeries {
    let mut at_micros = Vec::with_capacity(controller.len());
    let mut throughput = Vec::with_capacity(controller.len());
    for index in 0..controller.len() {
        let Some(sample) = controller.sample(index) else {
            continue;
        };
        at_micros.push(sample.at_micros);
        let value = match sample.capacity_evidence {
            CapacityEvidenceSample::None => f64::NAN,
            CapacityEvidenceSample::Window(window) => window.throughput_per_second(),
        };
        throughput.push(value);
    }
    StorySeries::new("resource window", at_micros, throughput).points()
}

fn metric_u64(
    trace: &MetricTrace,
    label: &'static str,
    values: &[u64],
    convert: fn(u64) -> f64,
) -> StorySeries {
    StorySeries::new(
        label,
        trace.at_micros.clone(),
        values.iter().copied().map(convert).collect(),
    )
}

fn metric_u32(trace: &MetricTrace, label: &'static str, values: &[u32]) -> StorySeries {
    StorySeries::new(
        label,
        trace.at_micros.clone(),
        values.iter().copied().map(f64::from).collect(),
    )
}

fn metric_f64(trace: &MetricTrace, label: &'static str, values: &[f64]) -> StorySeries {
    StorySeries::new(label, trace.at_micros.clone(), values.to_vec())
}

fn reference(trace: &MetricTrace, label: &'static str, value: f64) -> StorySeries {
    StorySeries::reference(
        label,
        trace.at_micros.clone(),
        vec![value; trace.at_micros.len()],
    )
}

fn input_series(
    history: &SeriesHistory,
    name: &str,
    label: &'static str,
    convert: fn(SeriesCell) -> Option<f64>,
) -> StorySeries {
    let mut at_micros = Vec::with_capacity(history.len());
    let mut values = Vec::with_capacity(history.len());
    for row in 0..history.len() {
        let Some(at) = history.at_micros(row) else {
            continue;
        };
        let Some(value) = history.cell(name, row).and_then(convert) else {
            continue;
        };
        at_micros.push(at);
        values.push(value);
    }
    StorySeries::new(label, at_micros, values)
}

fn duration_value(cell: SeriesCell) -> Option<f64> {
    match cell {
        SeriesCell::Unsigned64(value) => Some(micros(value)),
        _ => None,
    }
}

fn count_value(cell: SeriesCell) -> Option<f64> {
    match cell {
        SeriesCell::Unsigned32(value) => Some(f64::from(value)),
        _ => None,
    }
}

fn count(value: u64) -> f64 {
    let high = (value >> 32_u32) as u32;
    let low = value as u32;
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn micros(value: u64) -> f64 {
    Duration::from_micros(value).as_secs_f64()
}

fn panel_bounds(panel: &StoryPanel) -> (f64, f64) {
    if let Some(bounds) = panel.vertical_bounds {
        return bounds;
    }
    let mut values = panel
        .series
        .iter()
        .flat_map(|series| series.values.iter())
        .copied()
        .filter(|value| value.is_finite())
        .chain(
            panel
                .heatmap
                .as_ref()
                .into_iter()
                .flat_map(|heatmap| heatmap.values.iter())
                .copied()
                .filter(|value| value.is_finite()),
        );
    let Some(first) = values.next() else {
        return (0.0_f64, 1.0_f64);
    };
    let (mut minimum, mut maximum) = values.fold((first, first), |(minimum, maximum), value| {
        (minimum.min(value), maximum.max(value))
    });
    minimum = minimum.min(0.0_f64);
    // One event is the smallest useful count span on this plot.
    let span = (maximum - minimum).max(1.0_f64);
    maximum += span * 0.08_f64;
    (minimum, maximum)
}

fn activity_end_micros(trace: &MetricTrace) -> u64 {
    trace
        .at_micros
        .iter()
        .enumerate()
        .rev()
        .find(|(index, _)| {
            trace.arrivals[*index] > 0
                || trace.backlog[*index] > 0
                || trace.timers[*index] > 0
                || trace.useful_completions[*index] > 0
        })
        .map_or(1, |(_, at_micros)| *at_micros)
}

fn format_time(micros: u64) -> String {
    if micros < 1_000_000 {
        let milliseconds = micros / 1_000;
        let tenths = micros % 1_000 / 100;
        return format!("{milliseconds}.{tenths} ms");
    }
    if micros >= 3_600_000_000 {
        return format!(
            "{:.1} h",
            Duration::from_micros(micros).as_secs_f64() / 3_600.0
        );
    }
    if micros >= 120_000_000 {
        return format!(
            "{:.1} min",
            Duration::from_micros(micros).as_secs_f64() / 60.0
        );
    }
    let seconds = micros / 1_000_000;
    let tenths = micros % 1_000_000 / 100_000;
    format!("{seconds}.{tenths} s")
}

fn drawing_error<Error>(error: &DrawingAreaErrorKind<Error>) -> PlotError
where
    Error: StdError + Send + Sync + fmt::Debug,
{
    PlotError::Drawing(format!("{error:?}"))
}

struct StoryPanel {
    unit: &'static str,
    series: Vec<StorySeries>,
    horizon_micros: Option<u64>,
    annotations: Vec<StoryAnnotation>,
    heatmap: Option<PosteriorHeatmap>,
    vertical_bounds: Option<(f64, f64)>,
    axis: AxisScale,
}

impl StoryPanel {
    fn new(unit: &'static str, series: Vec<StorySeries>) -> Self {
        let mut series = deduplicate_series(series);
        let quantity = quantity_for_unit(unit);
        for item in &mut series {
            item.quantity = quantity;
        }
        Self {
            unit,
            series,
            horizon_micros: None,
            annotations: Vec::new(),
            heatmap: None,
            vertical_bounds: None,
            axis: AxisScale::Linear,
        }
    }

    fn quantity(&self) -> Quantity {
        quantity_for_unit(self.unit)
    }

    fn apply_axis(&mut self) {
        if !matches!(self.quantity(), Quantity::Rate | Quantity::Cost) {
            return;
        }
        let range = self
            .series
            .iter()
            .flat_map(|series| series.values.iter().copied())
            .chain(
                self.heatmap
                    .iter()
                    .flat_map(|heatmap| heatmap.values.iter().copied()),
            )
            .filter(|value| value.is_finite() && *value > 0.0_f64)
            .fold(None, |range, value| {
                Some(
                    range.map_or((value, value), |(minimum, maximum): (f64, f64)| {
                        (minimum.min(value), maximum.max(value))
                    }),
                )
            });
        let Some((minimum, maximum)) = range else {
            return;
        };
        self.axis = AxisScale::for_range(minimum, maximum);
        if self.axis == AxisScale::Linear {
            return;
        }
        for value in self.series.iter_mut().flat_map(|series| {
            series.axis = self.axis;
            series.values.iter_mut()
        }) {
            *value = if *value > 0.0_f64 {
                self.axis.project(*value)
            } else {
                f64::NAN
            };
        }
        if let Some(heatmap) = &mut self.heatmap {
            for value in &mut heatmap.values {
                *value = self.axis.project(*value);
            }
        }
    }

    fn content(&self) -> PanelContent {
        let mut finite = self
            .series
            .iter()
            .flat_map(|series| series.values.iter())
            .filter(|value| value.is_finite());
        let Some(first) = finite.next().copied() else {
            return PanelContent::Empty;
        };
        if finite.all(|value| value.to_bits() == first.to_bits()) && self.heatmap.is_none() {
            PanelContent::Unchanged
        } else {
            PanelContent::Visible
        }
    }

    fn clipped_label(&self) -> Option<String> {
        let (plot_left, plot_right) = story_plot_horizontal_bounds(self);
        let preferred_x = plot_left + (plot_right - plot_left) * 2_i32 / 3_i32;
        self.series.iter().find_map(|series| {
            let index = meaningful_index(&series.values)?;
            let text = series_label_text(series, series.values[index]);
            let anchor = label_x_anchor(&text, LABEL_FONT_PIXELS, preferred_x, plot_left);
            (!label_inside_image(
                (WIDTH, PANEL_HEIGHT),
                (anchor, 8_i32),
                &text,
                LABEL_FONT_PIXELS,
            ))
            .then_some(text)
        })
    }

    fn with_horizon(mut self, horizon_micros: u64) -> Self {
        self.horizon_micros = Some(horizon_micros);
        self
    }

    fn with_heatmap(mut self, heatmap: PosteriorHeatmap) -> Self {
        self.heatmap = Some(heatmap);
        self
    }

    fn with_bounded_heatmap(mut self, heatmap: PosteriorHeatmap) -> Self {
        self.vertical_bounds = heatmap
            .values
            .last()
            .map(|maximum| (0.5_f64, maximum + 0.5_f64));
        self.heatmap = Some(heatmap);
        self
    }
}

fn story_plot_horizontal_bounds(panel: &StoryPanel) -> (i32, i32) {
    let key_width = if panel.heatmap.is_some() {
        COLOR_KEY_WIDTH
    } else {
        0
    };
    let y_label_width = label_margin(panel.series.iter().map(|series| series.label.len()));
    let left = CHART_MARGIN_LEFT.saturating_add(y_label_width);
    let right = WIDTH
        .saturating_sub(key_width)
        .saturating_sub(CHART_MARGIN_RIGHT);
    (
        i32::try_from(left).unwrap_or(i32::MAX),
        i32::try_from(right).unwrap_or(i32::MAX),
    )
}

fn story_section(index: usize) -> ReportSection {
    match index {
        0 | 6 | 8..=14 | 17 => ReportSection::Evidence,
        7 => ReportSection::Belief,
        15 | 16 => ReportSection::Decision,
        _ => ReportSection::Outcome,
    }
}

fn usize_f64(value: usize) -> f64 {
    let Ok(value) = u32::try_from(value) else {
        return f64::from(u32::MAX);
    };
    f64::from(value)
}

fn quantity_for_unit(unit: &str) -> Quantity {
    if unit.contains("replica") {
        Quantity::Replicas
    } else if unit.contains("probability") || unit.contains("fraction") {
        Quantity::Probability
    } else if unit.contains("second") && !unit.contains("per second") {
        Quantity::Seconds
    } else if unit.contains("per second") {
        Quantity::Rate
    } else if unit.contains("cost") {
        Quantity::Cost
    } else {
        Quantity::Count
    }
}

struct StoryAnnotation {
    at_micros: u64,
    label: String,
}

struct StorySeries {
    label: String,
    at_micros: Vec<u64>,
    values: Vec<f64>,
    style: SeriesStyle,
    quantity: Quantity,
    axis: AxisScale,
}

impl StorySeries {
    fn new(label: &'static str, at_micros: Vec<u64>, values: Vec<f64>) -> Self {
        Self {
            label: label.to_owned(),
            at_micros,
            values,
            style: SeriesStyle::Line,
            quantity: Quantity::Count,
            axis: AxisScale::Linear,
        }
    }

    fn reference(label: &'static str, at_micros: Vec<u64>, values: Vec<f64>) -> Self {
        Self::new(label, at_micros, values)
    }

    fn points(mut self) -> Self {
        self.style = SeriesStyle::Points;
        self
    }

    fn step(mut self) -> Self {
        self.style = SeriesStyle::Step;
        self
    }

    fn light(mut self) -> Self {
        self.style = SeriesStyle::LightLine;
        self
    }
}

#[derive(Clone, Copy)]
enum SeriesStyle {
    Line,
    Step,
    LightLine,
    Points,
}

fn step_points(series: &StorySeries) -> Vec<(u64, f64)> {
    let mut points = Vec::with_capacity(series.values.len().saturating_mul(2));
    let mut previous = None;
    for (&at_micros, &value) in series.at_micros.iter().zip(&series.values) {
        if !value.is_finite() {
            previous = None;
            continue;
        }
        if let Some(previous_value) = previous {
            points.push((at_micros, previous_value));
        }
        points.push((at_micros, value));
        previous = Some(value);
    }
    points
}

fn deduplicate_series(series: Vec<StorySeries>) -> Vec<StorySeries> {
    let mut distinct: Vec<StorySeries> = Vec::with_capacity(series.len());
    for candidate in series {
        if let Some(existing) = distinct.iter_mut().find(|existing| {
            existing.at_micros == candidate.at_micros && existing.values == candidate.values
        }) {
            existing.label.push_str(" = ");
            existing.label.push_str(&candidate.label);
        } else {
            distinct.push(candidate);
        }
    }
    distinct
}

#[cfg(test)]
mod tests {
    use super::{StoryPanel, StorySeries, finish_story_panels};

    #[test]
    fn story_panels_share_the_experiment_horizon() {
        let mut panels = [StoryPanel::new(
            "events",
            vec![StorySeries::new(
                "early change",
                vec![0, 1_000_000],
                vec![0.0_f64, 1.0_f64],
            )],
        )];
        finish_story_panels(&mut panels, 65_000_000);
        assert_eq!(panels[0].horizon_micros, Some(65_000_000));
    }

    #[test]
    fn merged_story_label_moves_inside_panel() {
        let panel = StoryPanel::new(
            "replicas",
            vec![
                StorySeries::new(
                    "historical replicas",
                    vec![0, 1_000_000],
                    vec![12.0_f64, 12.0_f64],
                ),
                StorySeries::new(
                    "experiment target",
                    vec![0, 1_000_000],
                    vec![12.0_f64, 12.0_f64],
                ),
            ],
        );

        assert_eq!(
            panel.series[0].label,
            "historical replicas = experiment target"
        );
        assert_eq!(panel.clipped_label(), None);
    }

    #[test]
    fn story_label_wider_than_panel_stays_clipped() {
        let mut series = StorySeries::new("wide", vec![0], vec![1.0_f64]);
        series.label = "wide label ".repeat(200);
        let panel = StoryPanel::new("replicas", vec![series]);

        assert!(panel.clipped_label().is_some());
    }
}
