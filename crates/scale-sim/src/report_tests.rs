use std::path::Path;

use super::{
    ReportError, image_is_visible, validate_document_source, write_historical_comparison_pdf,
};
use crate::{ImageManifestEntry, PanelContent, ReportSection};

#[test]
fn report_source_check_reads_written_contract_fields() {
    let source = "= Test regime\nCommit: x. Model version: x. Generator version: x. Seed: 1. \
                  Duration: 1 s. Artifact identity: x.\n[capacity][arrival][reliability][launch]
                  [rebalance]\n== Evidence\nevents operations per second seconds\n== \
                  Belief\nprobability\n
                  == Decision\nreplicas\n== Outcome\n== Cost\nevent-delay-seconds replica-seconds";
    assert_eq!(validate_document_source(source), Ok(()));

    let invalid = source.replace("== Decision", "== Choice");
    assert!(validate_document_source(&invalid).is_err());
}

#[test]
fn historical_comparison_requires_all_four_cases() {
    let result = write_historical_comparison_pdf(Path::new("unused.pdf"), &[]);
    assert!(matches!(result, Err(ReportError::HistoricalComparison)));
}

#[test]
fn report_prunes_only_nonvisible_panels() {
    let images = [
        ImageManifestEntry {
            file: "visible.svg".to_owned(),
            section: ReportSection::Evidence,
            content: PanelContent::Visible,
            clipped_label: None,
            color_key_present: false,
            requires_color_key: false,
            comparison_scale: None,
        },
        ImageManifestEntry {
            file: "empty.svg".to_owned(),
            section: ReportSection::Evidence,
            content: PanelContent::Empty,
            clipped_label: None,
            color_key_present: false,
            requires_color_key: false,
            comparison_scale: None,
        },
    ];
    assert!(image_is_visible(&images, "visible.svg"));
    assert!(!image_is_visible(&images, "empty.svg"));
}
