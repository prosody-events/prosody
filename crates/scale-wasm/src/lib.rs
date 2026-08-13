//! Thin WebAssembly adapter for deterministic laboratory decisions.

use prosody_scale_core::{
    ArrivalPrior, CapacityGrid, Cohort, Configuration, DemandClass, LaunchPrior, ModelTime,
    ObservationBuffer, RebalancePrior, ReliabilityPrior, ScaleDecision, ScaleState,
    ServiceObjective, step,
};

#[cfg(all(target_arch = "wasm32", feature = "threads"))]
pub use wasm_bindgen_rayon::init_thread_pool;

const ERROR_CODE: u64 = u64::MAX;

/// Runs one fixed-grid decision and returns its portable integer encoding.
///
/// Bits 0 through 31 contain the target. Bits 32 through 62 contain the cap.
/// Bit 63 identifies a Hold decision. `u64::MAX` identifies invalid input.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen::prelude::wasm_bindgen)]
#[must_use]
pub fn fixture_decision(offered_events: u32) -> u64 {
    let Ok(objective) = ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64) else {
        return ERROR_CODE;
    };
    // Use one arrival each second and one expected change each day because this
    // fixture measures step cost, not the prior.
    let Ok(arrival_prior) = ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 86_400.0_f64) else {
        return ERROR_CODE;
    };
    let Ok(launch_time_prior) = LaunchPrior::kubernetes() else {
        return ERROR_CODE;
    };
    let Ok(rebalance_time_prior) = RebalancePrior::kip848() else {
        return ERROR_CODE;
    };
    let configuration = Configuration {
        cohort_count_max: 8,
        calendar_segment_count_max: 8,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 8,
        replica_count_max: 32,
        slots_per_replica: 4,
        posterior_sample_count: 64,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        failure_service_weight: 0.3_f64,
        arrival_prior,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior,
        rebalance_time_prior,
        objective,
    };
    let Ok(grid) = CapacityGrid::new(
        &[0.001_f64, 0.002_f64, 0.004_f64],
        &[1_000.0_f64, 2_000.0_f64, 4_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64],
    ) else {
        return ERROR_CODE;
    };
    let Ok(mut state) = ScaleState::new(configuration.clone(), grid) else {
        return ERROR_CODE;
    };
    let Ok(mut scratch) = state.new_scratch() else {
        return ERROR_CODE;
    };
    let Ok(mut observation) = ObservationBuffer::new(&configuration) else {
        return ERROR_CODE;
    };
    if observation.set_arrivals(offered_events, 1_000_000).is_err() {
        return ERROR_CODE;
    }
    let events = f64::from(offered_events);
    if observation
        .push_cohort(Cohort {
            release_micros: 0,
            deadline_micros: 1_000_000,
            offered_events: events,
            partition: 0,
            demand_class: DemandClass::Normal,
        })
        .is_err()
    {
        return ERROR_CODE;
    }
    encode(step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    ))
}

fn encode(decision: ScaleDecision) -> u64 {
    match decision {
        ScaleDecision::Apply(apply) => u64::from(apply.target) | (u64::from(apply.cap) << 32_u32),
        ScaleDecision::Hold(_) => 1_u64 << 63_u32,
    }
}

#[cfg(test)]
mod tests {
    use super::{ERROR_CODE, fixture_decision};

    #[test]
    fn fixture_replays_as_one_integer() {
        let first = fixture_decision(1_000);
        assert_ne!(first, ERROR_CODE);
        assert_eq!(first, fixture_decision(1_000));
    }
}
