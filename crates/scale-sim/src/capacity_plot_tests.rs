use super::quantiles;

#[test]
fn snapshot_quantiles_follow_cumulative_mass() {
    let values = [10.0_f64, 20.0_f64, 30.0_f64];
    let mass = [0.05_f64, 0.50_f64, 0.45_f64];

    assert_eq!(quantiles(&values, &mass), [20.0_f64, 20.0_f64, 30.0_f64]);
}
