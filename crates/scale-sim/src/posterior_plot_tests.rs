use prosody_scale_core::ArrivalPosterior;

use super::{
    ARRIVAL_CELL_COUNT, PosteriorHeatmap, PosteriorPanel, format_value, gamma_mass, quantiles,
    select_snapshots,
};

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
    assert_eq!(
        quantiles(&panel.heatmap.values, selected.final_mass),
        [1.0_f64, 3.0_f64, 3.0_f64]
    );
}

#[test]
fn log_rate_gamma_mass_normalizes() {
    let values = (0..ARRIVAL_CELL_COUNT)
        .map(|index| 2.0_f64.powf(-10.0_f64 + f64::from(index as u32) * 0.25_f64))
        .collect::<Vec<_>>();
    let mut scratch = [0.0_f64; ARRIVAL_CELL_COUNT];

    let mass = gamma_mass(
        ArrivalPosterior {
            shape: 37.0_f64,
            rate: 0.25_f64,
        },
        &values,
        &mut scratch,
    );

    assert!((mass.iter().sum::<f64>() - 1.0_f64).abs() < 1.0e-12_f64);
    assert!(mass.iter().all(|probability| probability.is_finite()));
}
