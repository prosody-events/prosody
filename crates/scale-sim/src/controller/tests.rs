use prosody_scale_core::ThroughputPosteriorCell;
use statrs::distribution::NegativeBinomial;

use super::{
    negative_binomial_quantiles, posterior_predictive_throughput_quantiles, predictive_rank_offset,
};
use crate::PlantError;

#[test]
fn predictive_throughput_includes_poisson_observation_noise() -> Result<(), PlantError> {
    let cells = [ThroughputPosteriorCell {
        throughput_per_second: 10.0_f64,
        probability: 1.0_f64,
    }];

    let quantiles = posterior_predictive_throughput_quantiles(&cells, 1.0_f64)?;

    assert!(
        quantiles
            .iter()
            .zip([6.0_f64, 10.0_f64, 14.0_f64])
            .all(|(actual, expected)| actual.to_bits() == expected.to_bits()),
        "the predictive quantiles must include count variation: {quantiles:?}"
    );
    Ok(())
}

#[test]
fn arrival_prediction_uses_negative_binomial_count_quantiles() -> Result<(), PlantError> {
    let distribution = NegativeBinomial::new(10.0_f64, 0.5_f64)?;
    let quantiles = negative_binomial_quantiles(&distribution);

    assert_eq!(
        quantiles,
        [5.0_f64, 9.0_f64, 16.0_f64],
        "arrival prediction must retain discrete count quantiles"
    );
    Ok(())
}

#[test]
fn predictive_rank_randomization_replays_and_separates_seeds() {
    let first = predictive_rank_offset(7, 15_000_000);
    let replay = predictive_rank_offset(7, 15_000_000);
    let other = predictive_rank_offset(8, 15_000_000);

    assert_eq!(first.to_bits(), replay.to_bits(), "equal seeds must replay");
    assert_ne!(
        first.to_bits(),
        other.to_bits(),
        "different seeds must separate randomized ranks"
    );
    assert!(
        (0.0_f64..1.0_f64).contains(&first),
        "a randomized rank offset must stay in the unit interval"
    );
}
