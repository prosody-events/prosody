use std::collections::BTreeSet;

use thiserror::Error;

/// One causal section in a generated report.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReportSection {
    /// Regime question and experimental design.
    Regime,
    /// Accepted observations and exposure.
    Evidence,
    /// Model posterior state.
    Belief,
    /// Controller choice.
    Decision,
    /// Realized plant response.
    Outcome,
    /// Cost panels in a batch report.
    Cost,
}

/// Content state for one generated image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PanelContent {
    /// The panel contains no finite observation.
    Empty,
    /// The panel repeats its initial state without a change.
    Unchanged,
    /// The panel contains evidence or a state change.
    Visible,
}

/// One real image produced by a plot writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageManifestEntry {
    /// Image path relative to its report root.
    pub file: String,
    /// Causal section that owns this panel.
    pub section: ReportSection,
    /// Whether the panel is empty, unchanged, or visible.
    pub content: PanelContent,
    /// Text of the first measured label outside the image bounds.
    pub clipped_label: Option<String>,
    /// Whether a heatmap has a labeled numeric color key.
    pub color_key_present: bool,
    /// Whether this image contains a heatmap.
    pub requires_color_key: bool,
    /// Stable scale identity for a direct comparison group.
    pub comparison_scale: Option<&'static str>,
}

/// Real document structure built while the report source is written.
pub struct DocumentManifest<'a> {
    /// Sections in display order.
    pub sections: &'a [ReportSection],
    /// Units used by tables, captions, and axes.
    pub units: &'a [&'a str],
    /// Metadata field names written in the report.
    pub metadata: &'a [&'a str],
    /// Model artifact families written in the report.
    pub artifacts: &'a [&'a str],
}

/// Checks one report document before PDF compilation.
///
/// # Errors
///
/// Returns an error when the document violates the report contract.
pub fn check_document(manifest: &DocumentManifest<'_>) -> Result<(), ReportCheckError> {
    const REQUIRED: [ReportSection; 5] = [
        ReportSection::Regime,
        ReportSection::Evidence,
        ReportSection::Belief,
        ReportSection::Decision,
        ReportSection::Outcome,
    ];
    if manifest.sections != REQUIRED {
        return Err(ReportCheckError::PanelOrder);
    }
    let allowed_units = [
        "events",
        "replicas",
        "probability",
        "operations per second",
        "seconds",
        "event-delay-seconds",
        "replica-seconds",
    ];
    if manifest
        .units
        .iter()
        .any(|unit| !allowed_units.contains(unit))
    {
        return Err(ReportCheckError::Unit);
    }
    for field in [
        "commit",
        "model version",
        "artifact identity",
        "seed",
        "duration",
        "generator version",
    ] {
        if !manifest.metadata.contains(&field) {
            return Err(ReportCheckError::Metadata);
        }
    }
    for artifact in ["capacity", "arrival", "reliability", "launch", "rebalance"] {
        if !manifest.artifacts.contains(&artifact) {
            return Err(ReportCheckError::Artifact);
        }
    }
    Ok(())
}

/// Checks the real images referenced by one report.
///
/// # Errors
///
/// Returns an error when an image violates the visual contract.
pub fn check_images(images: &[ImageManifestEntry]) -> Result<(), ReportCheckError> {
    if let Some((file, label)) = images.iter().find_map(|image| {
        (image.content == PanelContent::Visible)
            .then(|| {
                image
                    .clipped_label
                    .as_ref()
                    .map(|label| (&image.file, label))
            })
            .flatten()
    }) {
        return Err(ReportCheckError::ClippedLabel {
            file: file.clone(),
            label: label.clone(),
        });
    }
    if images.iter().any(|image| {
        image.content == PanelContent::Visible
            && image.requires_color_key
            && !image.color_key_present
    }) {
        return Err(ReportCheckError::ColorKey);
    }
    if !strictly_ordered(images) {
        return Err(ReportCheckError::PanelOrder);
    }
    let scales = images
        .iter()
        .filter_map(|image| image.comparison_scale)
        .collect::<BTreeSet<_>>();
    if scales.len() > 1 {
        return Err(ReportCheckError::ComparisonScale);
    }
    Ok(())
}

fn strictly_ordered(images: &[ImageManifestEntry]) -> bool {
    images
        .windows(2)
        .all(|pair| pair[0].section <= pair[1].section)
}

/// Returns whether one measured text box stays inside its image.
#[must_use]
pub fn label_inside_image(
    image: (u32, u32),
    position: (i32, i32),
    text: &str,
    font_pixels: u32,
) -> bool {
    let character_count = text.chars().count().min(10_000);
    let character_count = u32::try_from(character_count).map_or(10_000, |value| value);
    let width = character_count
        .saturating_mul(font_pixels)
        .saturating_mul(3)
        / 5;
    let Ok(x) = u32::try_from(position.0) else {
        return false;
    };
    let Ok(y) = u32::try_from(position.1) else {
        return false;
    };
    x.saturating_add(width) <= image.0 && y.saturating_add(font_pixels) <= image.1
}

/// A report structure violates the presentation contract.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ReportCheckError {
    /// A required panel is absent or out of order.
    #[error("required report panels are absent or out of order")]
    PanelOrder,
    /// A unit does not use the shared visual specification.
    #[error("a report unit is not in the shared visual specification")]
    Unit,
    /// Reproducibility metadata is incomplete.
    #[error("report metadata is incomplete")]
    Metadata,
    /// Required model artifact metadata is absent.
    #[error("required model artifact metadata is absent")]
    Artifact,
    /// An image label extends outside its bounds.
    #[error("image '{file}' contains clipped label '{label}'")]
    ClippedLabel {
        /// Image path relative to its report root.
        file: String,
        /// Text that extends outside the image bounds.
        label: String,
    },
    /// A heatmap has no labeled numeric color key.
    #[error("a heatmap has no labeled numeric color key")]
    ColorKey,
    /// Direct comparison panels use different scales.
    #[error("direct comparison panels use different scales")]
    ComparisonScale,
}

#[cfg(test)]
mod tests {
    use super::{
        DocumentManifest, ImageManifestEntry, PanelContent, ReportCheckError, ReportSection,
        check_document, check_images, label_inside_image,
    };

    const ORDER: [ReportSection; 5] = [
        ReportSection::Regime,
        ReportSection::Evidence,
        ReportSection::Belief,
        ReportSection::Decision,
        ReportSection::Outcome,
    ];

    #[test]
    fn document_check_enforces_the_complete_spine() {
        let manifest = DocumentManifest {
            sections: &ORDER,
            units: &["events", "replicas", "operations per second"],
            metadata: &[
                "commit",
                "model version",
                "artifact identity",
                "seed",
                "duration",
                "generator version",
            ],
            artifacts: &["capacity", "arrival", "reliability", "launch", "rebalance"],
        };
        assert_eq!(check_document(&manifest), Ok(()));

        let incomplete = DocumentManifest {
            sections: &ORDER[..4],
            ..manifest
        };
        assert_eq!(
            check_document(&incomplete),
            Err(ReportCheckError::PanelOrder)
        );
    }

    #[test]
    fn image_check_uses_real_entries() {
        let images = ORDER.map(|section| ImageManifestEntry {
            file: format!("{section:?}.svg"),
            section,
            content: PanelContent::Visible,
            clipped_label: None,
            color_key_present: true,
            requires_color_key: true,
            comparison_scale: Some("shared-v1"),
        });
        assert_eq!(check_images(&images), Ok(()));

        let mut inconsistent = images;
        inconsistent[1].comparison_scale = Some("other");
        assert_eq!(
            check_images(&inconsistent),
            Err(ReportCheckError::ComparisonScale)
        );
    }

    #[test]
    fn clipped_label_error_names_the_image_and_label() {
        let images = [ImageManifestEntry {
            file: "beliefs/capacity.svg".to_owned(),
            section: ReportSection::Belief,
            content: PanelContent::Visible,
            clipped_label: Some("capacity tail probability".to_owned()),
            color_key_present: true,
            requires_color_key: true,
            comparison_scale: None,
        }];
        let error = ReportCheckError::ClippedLabel {
            file: "beliefs/capacity.svg".to_owned(),
            label: "capacity tail probability".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "image 'beliefs/capacity.svg' contains clipped label 'capacity tail probability'"
        );
        assert_eq!(check_images(&images), Err(error));
    }

    #[test]
    fn label_check_uses_text_extent_and_image_bounds() {
        assert!(label_inside_image(
            (200, 100),
            (20_i32, 20_i32),
            "short",
            12
        ));
        assert!(!label_inside_image(
            (100, 100),
            (80_i32, 20_i32),
            "clipped label",
            12
        ));
    }
}
