use plotters::prelude::{Color, RGBColor, ShapeStyle};

pub(crate) const AXIS_COLOR: RGBColor = RGBColor(105, 105, 105);
pub(crate) const TEXT_COLOR: RGBColor = RGBColor(35, 35, 35);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Quantity {
    Count,
    Replicas,
    Probability,
    Rate,
    Seconds,
    Cost,
}

impl Quantity {
    pub(crate) fn format(self, value: f64) -> String {
        match self {
            Self::Count | Self::Replicas => format!("{value:.0}"),
            Self::Probability => format!("{value:.2}"),
            Self::Rate if value.abs() >= 100.0_f64 => format!("{value:.0}"),
            Self::Rate | Self::Seconds | Self::Cost => format!("{value:.1}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LinePattern {
    Solid,
    Dashed,
    Dotted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AxisScale {
    Linear,
    Logarithmic,
}

impl AxisScale {
    pub(crate) fn for_range(minimum: f64, maximum: f64) -> Self {
        if minimum > 0.0_f64 && maximum / minimum >= 100.0_f64 {
            Self::Logarithmic
        } else {
            Self::Linear
        }
    }

    pub(crate) fn project(self, value: f64) -> f64 {
        match self {
            Self::Linear => value,
            Self::Logarithmic => value.log10(),
        }
    }

    pub(crate) fn restore(self, value: f64) -> f64 {
        match self {
            Self::Linear => value,
            Self::Logarithmic => 10.0_f64.powf(value),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SemanticStyle {
    pub(crate) color: RGBColor,
    pub(crate) pattern: LinePattern,
}

pub(crate) fn semantic_style(label: &str) -> SemanticStyle {
    let label = label.to_ascii_lowercase();
    if label.contains("actual") || label.contains("observed") || label.contains("accepted") {
        return SemanticStyle {
            color: RGBColor(0, 0, 0),
            pattern: LinePattern::Solid,
        };
    }
    if label.contains("belief") || label.contains("posterior") || label.contains("density") {
        return SemanticStyle {
            color: RGBColor(86, 180, 233),
            pattern: LinePattern::Solid,
        };
    }
    if label.contains("target") || label.contains("selected") || label.contains("median") {
        return SemanticStyle {
            color: RGBColor(0, 114, 178),
            pattern: LinePattern::Solid,
        };
    }
    if label.contains("limit") || label.contains("cap") || label.contains("slo") {
        return SemanticStyle {
            color: RGBColor(213, 94, 0),
            pattern: LinePattern::Dashed,
        };
    }
    if label.contains("historical") || label.contains("predictive") {
        return SemanticStyle {
            color: RGBColor(0, 158, 115),
            pattern: LinePattern::Dashed,
        };
    }
    SemanticStyle {
        color: RGBColor(204, 121, 167),
        pattern: LinePattern::Dotted,
    }
}

pub(crate) fn shape(style: SemanticStyle, width: u32) -> ShapeStyle {
    style.color.stroke_width(width)
}

pub(crate) fn label_margin(labels: impl Iterator<Item = usize>) -> u32 {
    let longest = labels.max().unwrap_or(0);
    let clamped = longest.saturating_mul(11).clamp(72, 220);
    u32::try_from(clamped).unwrap_or(220)
}

#[cfg(test)]
mod tests {
    use super::{AxisScale, LinePattern, Quantity, label_margin, semantic_style};

    #[test]
    fn visual_contract_keeps_semantics_stable() {
        assert_eq!(Quantity::Replicas.format(2.0_f64), "2");
        assert_eq!(Quantity::Count.format(17.0_f64), "17");
        assert_eq!(
            semantic_style("actual replicas"),
            semantic_style("actual ready")
        );
        assert_eq!(
            semantic_style("saturation cap").pattern,
            LinePattern::Dashed
        );
        assert_eq!(
            semantic_style("posterior density").color,
            semantic_style("capacity belief").color
        );
        assert_eq!(label_margin([4, 24].into_iter()), 220);
        assert_eq!(
            AxisScale::for_range(1.0_f64, 100.0_f64),
            AxisScale::Logarithmic
        );
        assert_eq!(AxisScale::for_range(0.0_f64, 100.0_f64), AxisScale::Linear);
    }
}
