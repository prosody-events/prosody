use std::cmp::Ordering;

use super::{PosteriorHeatmap, PosteriorPanel, format_value, quantiles, select_snapshots};

#[test]
fn snapshot_selects_largest_posterior_change() {
    let panel = PosteriorPanel {
        file: "test",
        unit: "value",
        heatmap: PosteriorHeatmap {
            at_micros: vec![1, 2, 3],
            values: vec![1.0_f64, 2.0_f64, 3.0_f64],
            probabilities: vec![
                0.30_f64, 0.40_f64, 0.30_f64, 0.05_f64, 0.10_f64, 0.85_f64, 0.10_f64, 0.15_f64,
                0.75_f64,
            ],
        },
        prior: vec![0.34_f64, 0.33_f64, 0.33_f64],
        y_label: format_value,
    };

    let selected = select_snapshots(&panel);

    assert_eq!(selected.important, [0.05_f64, 0.10_f64, 0.85_f64]);
    assert_eq!(selected.final_mass, [0.10_f64, 0.15_f64, 0.75_f64]);
    let actual = quantiles(&panel.heatmap.values, selected.final_mass);
    let expected = [1.0_f64, 3.0_f64, 3.0_f64];
    assert_eq!(actual.partial_cmp(&expected), Some(Ordering::Equal));
}

#[test]
fn quantiles_use_exact_discrete_mass() {
    // Keep every cumulative sum away from a threshold: an exact hit is
    // unstable under f64 rounding.
    let values = [1.0_f64, 2.0_f64, 4.0_f64, 8.0_f64];
    let mass = [0.05_f64, 0.15_f64, 0.72_f64, 0.08_f64];

    let actual = quantiles(&values, &mass);
    let expected = [2.0_f64, 4.0_f64, 4.0_f64];
    assert_eq!(actual.partial_cmp(&expected), Some(Ordering::Equal));
}
