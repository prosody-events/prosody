use fearless_simd::{Level, dispatch};
use quickcheck_macros::quickcheck;
use std::array;
use std::time::Duration;
use thiserror::Error;

use super::{
    RepairTargetSelection, SCHEDULED_PARTITION, ScenarioRole, aggregate_scenario_values,
    assignment_max_owner_share, balanced_partition_owner, balanced_partition_range,
    max_owner_share, next_repair_boundary, partition_replica_capacity, placement_supply,
    prepare_work_cohorts, repair_band, sample_hypothetical_transition_times,
    sample_transition_hypotheses, sample_transition_times, scenario_event_count, scenario_horizons,
    scenario_random, select_target, terminal_replica_seconds, write_monotone_runs,
};
use crate::CapacityCurve;
use crate::arrival::MeanRateTrajectory;
use crate::edf::{
    ArrivalPath, EdfScratch, EvaluationWindow, SupplyStep, evaluate_prepared_step, prepare,
};
use crate::partition::PartitionFactor;
use crate::types::{EventCohorts, SlotSecondCohorts};
use crate::{
    ActuationCommitment, ArrivalPrior, ArrivalPriorError, BacklogCohort, CalendarArtifactId,
    CalendarRateSegment, CapacityGrid, CapacityGridError, CapacityPrior, Configuration,
    ConfigurationError, DecisionCurveError, DemandClass, LaunchPrior, ModelTime, ObservationBuffer,
    ObservationError, OccupancyTransition, PosteriorError, PosteriorQuery, RebalancePrior,
    ReliabilityPrior, ResourceWindow, ScaleDecision, ScaleScratch, ScaleState, ScheduledRelease,
    ServiceObjective, step,
};
use crate::{RandomStream, sticky_assignment};

struct I27Measurement {
    replica_seconds: [f64; 8],
    costs: [f64; 8],
    candidate_eight_has_target_eight_event: bool,
    candidate_transition_times: [Option<(u64, u64)>; 8],
}

#[test]
fn posterior_miss_fraction_preserves_tail_probability() -> Result<(), TestError> {
    let (_, mut scratch, _) = test_model()?;
    let stride = scratch.posterior_miss_delay_fraction_sums.len();
    scratch.active_scenario_count = 2;
    scratch.active_inner_count = 1;
    scratch.class_masses.resize(2, 0.0_f64);
    scratch.class_masses[..2].copy_from_slice(&[0.001_f64, 0.999_f64]);
    scratch.scenario_event_count[..2].copy_from_slice(&[1_000_000.0_f64, 10.0_f64]);
    scratch.scenario_missed_work[0] = 1_000_000.0_f64;
    scratch.scenario_missed_work[stride] = 0.0_f64;

    dispatch!(Level::new(), simd => aggregate_scenario_values(simd, &mut scratch));

    let fraction = scratch.posterior_miss_fraction_sums[0];
    assert!(
        approximately_equal(fraction, 0.001_f64),
        "fraction={fraction}"
    );
    Ok(())
}

#[test]
fn empty_scenario_has_zero_posterior_miss_fraction() -> Result<(), TestError> {
    let (_, mut scratch, _) = test_model()?;
    scratch.active_scenario_count = 1;
    scratch.active_inner_count = 1;
    scratch.class_masses[0] = 1.0_f64;
    scratch.scenario_event_count[0] = 0.0_f64;
    scratch.scenario_missed_work[0] = 0.0_f64;

    dispatch!(Level::new(), simd => aggregate_scenario_values(simd, &mut scratch));

    let fraction = scratch.posterior_miss_fraction_sums[0];
    assert!(fraction.is_finite());
    assert!(fraction.total_cmp(&0.0_f64).is_eq());
    Ok(())
}

#[test]
fn authored_arrival_prior_selects_one_after_silence() -> Result<(), TestError> {
    let mut configuration = test_configuration()?;
    configuration.partition_count = 64;
    configuration.replica_count_max = 8;
    configuration.posterior_sample_count = 4_096;
    configuration.arrival_prior = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    let grid = CapacityGrid::new_with_prior(
        &[0.000_5_f64, 0.001_f64, 0.002_f64, 0.004_f64, 0.008_f64],
        &[32_000.0_f64, 64_000.0_f64, 128_000.0_f64, 256_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
        CapacityPrior::LogUniform,
    )?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = state.new_scratch()?;
    let mut selected_report = None;
    let mut targets = [0_u32; 32];
    let mut rates = [0.0_f64; 32];
    for report in 1_u64..=32 {
        let mut observation = ObservationBuffer::new(&configuration)?;
        observation.advance_model_time(ModelTime::from_micros(report * 3_600_000_000))?;
        observation.set_current_replicas(8)?;
        observation.set_arrivals(0, 3_600_000_000)?;
        if let ScaleDecision::Apply(decision) =
            step(&mut state, &mut scratch, observation.observation())
        {
            targets[report as usize - 1] = decision.target;
            rates[report as usize - 1] = decision.diagnostics.arrival_rate_per_second;
            if decision.target == 1 {
                selected_report = Some(report);
                break;
            }
        }
    }
    assert!(
        selected_report.is_some_and(|report| report <= 4),
        "targets={targets:?}, rates={rates:?}"
    );
    Ok(())
}

#[test]
fn placement_supply_uses_the_group_curve_at_useful_concurrency() {
    let curve = CapacityCurve::Knee {
        service_time_seconds: 0.5_f64,
        capacity_per_second: 80.0_f64,
        collapse: 1.0_f64,
    };
    let max_owner_share = 22.0_f64 / 64.0_f64;

    let supply = placement_supply(curve, 0.75_f64, 32, 3, 64, max_owner_share);

    let useful_concurrency = 32.0_f64 * 64.0_f64 / 22.0_f64;
    assert_eq!(
        supply.to_bits(),
        (0.75_f64 * curve.sustainable_throughput(useful_concurrency)).to_bits()
    );
}

#[test]
fn concentrated_placement_prices_equal_supply_at_all_counts() {
    let curve = CapacityCurve::NoKnee {
        service_time_seconds: 0.25_f64,
    };
    let one = placement_supply(curve, 0.8_f64, 32, 1, 64, 1.0_f64);
    let eight = placement_supply(curve, 0.8_f64, 32, 8, 64, 1.0_f64);

    assert_eq!(one.to_bits(), eight.to_bits());
}

#[test]
fn sampled_placement_reduces_balanced_supply_below_the_count_cap() -> Result<(), TestError> {
    let factor = PartitionFactor::new(64)?;
    let mut shares = [0.0_f64; 64];
    let mut random = RandomStream::new(7);
    factor.sample_shares(&mut random, &mut shares);
    let owners = (0_u32..64)
        .map(|partition| partition % 2)
        .collect::<Vec<_>>();
    let mut owner_sums = [0.0_f64; 2];
    let maximum = max_owner_share(&owners, &shares, &mut owner_sums);
    let curve = CapacityCurve::NoKnee {
        service_time_seconds: 1.0_f64,
    };

    let supply = placement_supply(curve, 1.0_f64, 32, 2, 64, maximum);

    assert!(maximum > 0.5_f64);
    assert!(supply < curve.sustainable_throughput(64.0_f64));
    Ok(())
}

#[test]
fn current_supply_uses_the_observed_owner_map() -> Result<(), TestError> {
    let mut factor = PartitionFactor::new(4)?;
    factor.update(&[100, 50, 40, 0]);
    let observed = [0_u32, 0, 1, 1];
    let hypothetical = [0_u32, 1, 0, 1];

    let observed_maximum = factor.maximum_assigned_expected_share(&observed);
    let hypothetical_maximum = factor.maximum_assigned_expected_share(&hypothetical);

    assert!(observed_maximum > hypothetical_maximum);
    Ok(())
}

#[test]
fn observed_owner_id_need_not_be_below_current_replica_count() -> Result<(), TestError> {
    let (_, mut scratch, _) = i27_model(0.02_f64)?;
    let workspace = &mut scratch.scenario_workspaces[0];
    for (partition, owner) in workspace.assignment.iter_mut().enumerate() {
        *owner = [0_u32, 1, 2, 4][partition % 4];
    }
    workspace.partition_share_draws.fill(1.0_f64 / 64.0_f64);

    let maximum = assignment_max_owner_share(workspace);

    assert_eq!(maximum.to_bits(), 0.25_f64.to_bits());
    Ok(())
}

#[test]
fn equal_counts_can_have_path_dependent_supply() -> Result<(), TestError> {
    let current = [0_u32, 0, 1, 1, 2];
    let termination_order = [3_u32, 2, 1, 0];
    let mut direct = [0_u32; 5];
    let mut expanded = [0_u32; 5];
    let mut counts = [0_u32; 4];
    let mut moved = [false; 5];
    sticky_assignment(
        &current,
        2,
        &termination_order,
        &mut direct,
        &mut counts,
        &mut moved,
    )?;
    sticky_assignment(
        &current,
        4,
        &termination_order,
        &mut expanded,
        &mut counts,
        &mut moved,
    )?;
    let expanded_from = expanded;
    sticky_assignment(
        &expanded_from,
        2,
        &termination_order,
        &mut expanded,
        &mut counts,
        &mut moved,
    )?;
    let shares = [0.35_f64, 0.25_f64, 0.2_f64, 0.15_f64, 0.05_f64];
    let mut owner_sums = [0.0_f64; 2];
    let direct_maximum = max_owner_share(&direct, &shares, &mut owner_sums);
    let expanded_maximum = max_owner_share(&expanded, &shares, &mut owner_sums);

    assert_ne!(direct, expanded);
    assert_ne!(direct_maximum.to_bits(), expanded_maximum.to_bits());
    Ok(())
}

#[test]
fn concentrated_owner_capacity_has_no_second_placement_penalty() {
    let supply = 123.0_f64;

    assert_eq!(
        partition_replica_capacity(supply, 8, 1.0_f64).to_bits(),
        supply.to_bits()
    );
}

#[test]
fn hypothetical_prices_are_invariant_to_subinterval_model_time_shifts() -> Result<(), TestError> {
    let (mut baseline, _) = i27_step(None, 0.02_f64, 12)?;
    let (mut shifted, _) = i27_step(None, 0.02_f64, 12)?;
    shifted.model_time = ModelTime::from_micros(baseline.model_time.as_micros() + 100_000);
    let mut baseline_scratch = baseline.new_scratch()?;
    let mut shifted_scratch = shifted.new_scratch()?;
    let mut observation = ObservationBuffer::new(&baseline.configuration)?;
    let inputs = observation.observation();

    let baseline_decision = select_target(
        &mut baseline,
        &mut baseline_scratch,
        inputs.cohorts,
        inputs.backlog,
        inputs.scheduled_releases,
        inputs.calendar,
        inputs.actuation_commitments,
    );
    let shifted_decision = select_target(
        &mut shifted,
        &mut shifted_scratch,
        inputs.cohorts,
        inputs.backlog,
        inputs.scheduled_releases,
        inputs.calendar,
        inputs.actuation_commitments,
    );
    let ScaleDecision::Apply(baseline_apply) = baseline_decision else {
        return Err(TestError::UnexpectedDecision);
    };
    let ScaleDecision::Apply(shifted_apply) = shifted_decision else {
        return Err(TestError::UnexpectedDecision);
    };
    assert_eq!(baseline_apply.target, shifted_apply.target);
    let mut baseline_costs = vec![0.0_f64; baseline_scratch.decision_candidate_count()];
    let mut shifted_costs = vec![0.0_f64; shifted_scratch.decision_candidate_count()];
    baseline_scratch.write_decision_expected_costs(&mut baseline_costs)?;
    shifted_scratch.write_decision_expected_costs(&mut shifted_costs)?;
    assert!(
        baseline_costs
            .iter()
            .zip(&shifted_costs)
            .all(|(left, right)| left.to_bits() == right.to_bits()),
        "baseline={baseline_costs:?}, shifted={shifted_costs:?}"
    );
    Ok(())
}

#[test]
fn exposed_cost_ladder_preserves_paired_decision_differences() -> Result<(), TestError> {
    let (_, scratch) = i27_step(Some((8, 1)), 0.02_f64, 12)?;
    let mut costs = [0.0_f64; 8];
    scratch.write_decision_expected_costs(&mut costs)?;
    let selected = argmin(&scratch.paired_cost_differences[..8]);

    assert_eq!(argmin(&costs), selected, "costs={costs:?}");
    assert!(costs.iter().enumerate().all(|(candidate, cost)| {
        cost.to_bits() == (costs[0] + scratch.paired_cost_differences[candidate]).to_bits()
    }));
    Ok(())
}

#[test]
fn committed_eight_prices_only_the_honest_replica_margin() -> Result<(), TestError> {
    let measurement = i27_measurement(Some((7, 8)), 0.02_f64, 12)?;
    assert_eq!(argmin(&measurement.costs), 0);
    assert!(!measurement.candidate_eight_has_target_eight_event);
    let replica_margin =
        3.0_f64 * (measurement.replica_seconds[7] - measurement.replica_seconds[0]);
    // The objective charges 3.0 per replica-second. The sampled difference is
    // 7.965166 replica-seconds, so the honest margin is 3.0 * 7.965166.
    assert!(
        (replica_margin - 23.895_499_f64).abs() < 0.01_f64,
        "replica_margin={replica_margin}"
    );
    assert!(measurement.costs[0] < measurement.costs[7]);
    Ok(())
}

#[test]
fn inflight_descent_prices_only_the_honest_replica_margin() -> Result<(), TestError> {
    let measurement = i27_measurement(Some((8, 1)), 0.02_f64, 12)?;
    let fresh = i27_measurement(None, 0.02_f64, 12)?;
    assert_eq!(
        &measurement.candidate_transition_times[1..7],
        &fresh.candidate_transition_times[1..7]
    );
    // Physical-transition draws price rung one at 22,542.768. They price rung
    // eight at 22,593.099. The cancel-plus-fresh equality keeps this order.
    assert!(
        (measurement.costs[0] - 22_542.767_764_170_098_f64).abs() < 0.01_f64,
        "costs={:?}",
        measurement.costs
    );
    assert!(
        (measurement.costs[7] - 22_593.099_449_365_41_f64).abs() < 0.01_f64,
        "costs={:?}",
        measurement.costs
    );
    assert!(measurement.costs[0] < measurement.costs[7]);
    Ok(())
}

#[test]
fn cancel_prices_candidate_two_from_the_current_count() -> Result<(), TestError> {
    let retained = i27_measurement(Some((8, 1)), 0.02_f64, 12)?;
    let fresh = i27_measurement(None, 0.02_f64, 12)?;
    assert_eq!(
        retained.candidate_transition_times[1],
        fresh.candidate_transition_times[1]
    );
    Ok(())
}

#[test]
fn transition_request_identity_preserves_the_survival_adjustment() -> Result<(), TestError> {
    let (mut state, _scratch, mut observation) = i27_model(0.02_f64)?;
    observation.advance_model_time(ModelTime::from_micros(13_000_000))?;
    observation.set_resource_observation(
        ResourceWindow::new_with_starts(32.0_f64, 1.0_f64, 500, 500)?,
        32,
        32,
        &[OccupancyTransition::new(500_000, 500, 500)],
    )?;
    if let Some(resource) = observation.observation().resource {
        state.capacity.update(resource, Duration::from_secs(1));
    }
    let requested_at = 13_000_000;
    for scenario in 0_u32..256 {
        let random = scenario_random(scenario, 256, ScenarioRole::Commitment);
        let fresh = sample_transition_times(
            &state,
            &random,
            requested_at,
            requested_at,
            0.0_f64,
            crate::TransitionDirection::Up,
            3,
        );
        let fresh_again = sample_transition_times(
            &state,
            &random,
            requested_at,
            requested_at,
            0.0_f64,
            crate::TransitionDirection::Up,
            3,
        );
        assert_eq!(fresh, fresh_again);

        let retained = sample_transition_times(
            &state,
            &random,
            requested_at,
            requested_at + 1_000_000,
            1.0_f64,
            crate::TransitionDirection::Up,
            3,
        );
        let mut oracle_random = random.clone().domain(requested_at).domain(0);
        let oracle_remaining = state.lead_time.sample_remaining_seconds(
            crate::TransitionDirection::Up,
            3,
            1.0_f64,
            &mut oracle_random,
        );
        let oracle_pause = requested_at
            .saturating_add(1_000_000)
            .saturating_add(super::seconds_to_micros(oracle_remaining));
        assert_eq!(retained.0, oracle_pause);
    }
    Ok(())
}

fn argmin(values: &[f64]) -> usize {
    values
        .iter()
        .enumerate()
        .min_by(|left, right| left.1.total_cmp(right.1))
        .map_or(0, |(index, _)| index)
}

fn i27_measurement(
    commitment: Option<(u32, u32)>,
    service_seconds: f64,
    seasoning_ticks: u64,
) -> Result<I27Measurement, TestError> {
    let (_state, scratch) = i27_step(commitment, service_seconds, seasoning_ticks)?;
    let mut costs = [0.0_f64; 8];
    scratch.write_decision_expected_costs(&mut costs)?;
    let mut replica_seconds = [0.0_f64; 8];
    replica_seconds.copy_from_slice(&scratch.posterior_replica_seconds_sums[..8]);
    let workspace = &scratch.scenario_workspaces[0];
    let first = workspace.trajectory_offsets[7] as usize;
    let last = workspace.trajectory_offsets[8] as usize;
    let candidate_eight_has_target_eight_event =
        workspace.trajectory.targets[first..last].contains(&8);
    let candidate_transition_times = array::from_fn(|candidate_index| {
        let first = workspace.trajectory_offsets[candidate_index] as usize;
        let last = workspace.trajectory_offsets[candidate_index + 1] as usize;
        let target = candidate_index as u32 + 1;
        workspace.trajectory.targets[first..last]
            .iter()
            .position(|event_target| *event_target == target)
            .map(|offset| {
                let event = first + offset;
                (
                    workspace.trajectory.pause_micros[event],
                    workspace.trajectory.sampled_ready_micros[event],
                )
            })
    });
    Ok(I27Measurement {
        replica_seconds,
        costs,
        candidate_eight_has_target_eight_event,
        candidate_transition_times,
    })
}

fn i27_step(
    commitment: Option<(u32, u32)>,
    service_seconds: f64,
    seasoning_ticks: u64,
) -> Result<(ScaleState, ScaleScratch), TestError> {
    let (mut state, mut scratch, mut observation) = i27_model(service_seconds)?;
    for tick in 1_u64..=seasoning_ticks {
        observation.clear();
        let now = tick * 1_000_000;
        observation.advance_model_time(ModelTime::from_micros(now))?;
        let mut partition_arrivals = [0_u32; 64];
        partition_arrivals[0] = u32::MAX;
        observation.set_partition_arrivals(&partition_arrivals, 1_000_000)?;
        observation.set_resource_observation(
            ResourceWindow::new_with_starts(32.0_f64, 1.0_f64, 300, 300)?,
            32,
            32,
            &[OccupancyTransition::new(500_000, 300, 300)],
        )?;
        let evidence = observation.observation();
        if let Some(arrivals) = evidence.arrivals {
            state.arrivals.update(arrivals, None, 1_000_000);
        }
        if let Some(partitions) = evidence.partition_arrivals {
            state.partition_placement.update(partitions.consume());
        }
        if let Some(resource) = evidence.resource {
            state.capacity.update(resource, Duration::from_secs(1));
        }
        state.model_time = ModelTime::from_micros(now);
    }
    observation.clear();
    let decision_micros = seasoning_ticks.saturating_add(1).saturating_mul(1_000_000);
    observation.advance_model_time(ModelTime::from_micros(decision_micros))?;
    observation.set_arrivals(500, 1_000_000)?;
    observation.set_resource_observation(
        ResourceWindow::new_with_starts(32.0_f64, 1.0_f64, 500, 500)?,
        32,
        32,
        &[OccupancyTransition::new(500_000, 500, 500)],
    )?;
    observation.set_current_replicas(8)?;
    let owners = (0_u32..64)
        .map(|partition| partition % 8)
        .collect::<Vec<_>>();
    observation.set_partition_owners(&owners)?;
    observation.set_backlog(BacklogCohort::new(
        decision_micros,
        decision_micros.saturating_sub(1_000_000),
        1_800,
        0,
        DemandClass::Normal,
    )?)?;
    if let Some((from, target)) = commitment {
        observation.push_actuation_commitment(ActuationCommitment::launching(
            from,
            target,
            ModelTime::from_micros(decision_micros.saturating_sub(1_000_000)),
        )?)?;
    }
    let _ = step(&mut state, &mut scratch, observation.observation());
    Ok((state, scratch))
}

fn i27_model(
    service_seconds: f64,
) -> Result<(ScaleState, ScaleScratch, ObservationBuffer), TestError> {
    let configuration = Configuration {
        cohort_count_max: 64,
        calendar_segment_count_max: 64,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 14,
        partition_count: 64,
        replica_count_max: 8,
        slots_per_replica: 32,
        posterior_sample_count: 256,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 64,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::test_prior(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 300.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    };
    let grid = CapacityGrid::new_with_prior(
        &[service_seconds],
        &[10_000.0_f64],
        &[0.0_f64],
        CapacityPrior::LogUniform,
    )?;
    let state = ScaleState::new(configuration.clone(), grid)?;
    let scratch = state.new_scratch()?;
    let observation = ObservationBuffer::new(&configuration)?;
    Ok((state, scratch, observation))
}

#[quickcheck]
fn balanced_partition_ranges_match_owner_order(partition_seed: u8, replica_seed: u8) -> bool {
    let partition_count = usize::from(partition_seed % 64 + 1);
    let replica_count = usize::from(replica_seed % 64 + 1).min(partition_count);
    (0..replica_count).all(|replica| {
        (0..partition_count)
            .filter(|partition| {
                balanced_partition_owner(partition_count, replica_count, *partition) == replica
            })
            .eq(balanced_partition_range(
                partition_count,
                replica_count,
                replica,
            ))
    })
}

#[test]
fn equal_physical_transitions_share_draws_across_rungs() -> Result<(), TestError> {
    let (state, scratch) = idle_ladder_step(256, true)?;
    let workspace_count = scratch
        .scenario_workspaces
        .len()
        .min(scratch.active_scenario_count)
        .max(1);
    let found = scratch
        .scenario_workspaces
        .iter()
        .take(workspace_count)
        .find_map(|workspace| {
            let repairs = produced_repairs(workspace);
            repairs.iter().enumerate().find_map(|(index, left)| {
                repairs[index + 1..]
                    .iter()
                    .find(|right| {
                        left.rung != right.rung
                            && left.requested == right.requested
                            && left.direction == right.direction
                            && left.delta == right.delta
                    })
                    .map(|right| (*left, *right))
            })
        });
    let Some((left, right)) = found else {
        return Err(TestError::UnexpectedDecision);
    };
    assert_eq!(left.launch_residual, right.launch_residual);
    assert_eq!(left.rebalance_residual, right.rebalance_residual);

    let scenario_count = scratch.active_scenario_count;
    assert!((0..scenario_count as u32).any(|scenario| {
        let random = scenario_random(scenario, scenario_count, ScenarioRole::Commitment);
        let hypotheses = sample_transition_hypotheses(&state, &random);
        let sampled = sample_hypothetical_transition_times(
            &state,
            &random,
            left.requested,
            hypotheses,
            left.direction,
            left.delta,
        );
        sampled.0 - left.requested == left.launch_residual
            && sampled.1 - sampled.0 == left.rebalance_residual
    }));
    Ok(())
}

#[test]
fn terminal_cost_uses_the_trajectory_final_target() -> Result<(), TestError> {
    let (state, scratch) = idle_ladder_step(256, false)?;
    let workspace = &scratch.scenario_workspaces[0];
    let candidate = 7_usize;
    let first = workspace.trajectory_offsets[candidate] as usize;
    let last = workspace.trajectory_offsets[candidate + 1] as usize;
    let final_target = workspace.trajectory.targets[last - 1];
    assert_ne!(final_target, candidate as u32 + 1);
    let planning_horizon_micros = scenario_horizons(&state, &scratch.resource_cohorts).0;
    let billing_horizon_micros = workspace
        .trajectory
        .ready_micros
        .iter()
        .copied()
        .max()
        .unwrap_or(planning_horizon_micros)
        .max(planning_horizon_micros);
    let terminal = terminal_replica_seconds(
        state.model_time.as_micros(),
        planning_horizon_micros,
        billing_horizon_micros,
        state.configuration.report_interval_micros,
        final_target,
    );
    let lifetime = super::billing_replica_seconds(
        state.model_time.as_micros(),
        billing_horizon_micros,
        state.current_replicas,
        &workspace.trajectory.targets[first..last],
        &workspace.trajectory.requested_micros[first..last],
        &workspace.trajectory.ready_micros[first..last],
    );
    let worker_count = scratch
        .scenario_workspaces
        .len()
        .min(scratch.active_scenario_count)
        .max(1);
    let scenario_chunk = scratch.active_scenario_count.div_ceil(worker_count);
    let cell = (scenario_chunk - 1) * scratch.posterior_miss_delay_fraction_sums.len() + candidate;
    assert!(
        approximately_equal(scratch.scenario_replica_seconds[cell], lifetime + terminal),
        "actual={}, expected={}",
        scratch.scenario_replica_seconds[cell],
        lifetime + terminal
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct ProducedRepair {
    rung: usize,
    requested: u64,
    direction: crate::TransitionDirection,
    delta: u32,
    launch_residual: u64,
    rebalance_residual: u64,
}

fn produced_repairs(workspace: &super::ScenarioWorkspace) -> Vec<ProducedRepair> {
    let mut repairs = Vec::new();
    for rung in 0..8_usize {
        let first = workspace.trajectory_offsets[rung] as usize;
        let last = workspace.trajectory_offsets[rung + 1] as usize;
        if first == last {
            continue;
        }
        let candidate = rung as u32 + 1;
        let fresh =
            usize::from(first + 1 < last && workspace.trajectory.targets[first + 1] == candidate);
        let repair_first = first + 1 + fresh;
        for event in repair_first..last {
            let replicas = workspace.trajectory.targets[event - 1];
            let target = workspace.trajectory.targets[event];
            let direction = if target > replicas {
                crate::TransitionDirection::Up
            } else {
                crate::TransitionDirection::Down
            };
            let requested = workspace.trajectory.requested_micros[event];
            let pause = workspace.trajectory.pause_micros[event];
            let sampled_ready = workspace.trajectory.sampled_ready_micros[event];
            repairs.push(ProducedRepair {
                rung,
                requested,
                direction,
                delta: target.abs_diff(replicas),
                launch_residual: pause - requested,
                rebalance_residual: sampled_ready - pause,
            });
        }
    }
    repairs
}

#[test]
fn non_monotone_repair_selects_smallest_cover_or_maximum_supply() {
    let supply = [4.0_f64, 9.0_f64, 7.0_f64, 8.0_f64];

    assert_eq!(select_repair_target(&supply, 7.5_f64), 2);
    assert_eq!(select_repair_target(&supply, 10.0_f64), 2);
}

fn select_repair_target(supply: &[f64], rate: f64) -> u32 {
    let mut selection = RepairTargetSelection::new();
    if let Some(target) = supply
        .iter()
        .enumerate()
        .find_map(|(index, &value)| selection.consider(index as u32 + 1, value, rate))
    {
        target
    } else {
        selection.best_target
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct WalkRepair {
    target: u32,
    boundary: usize,
    requested_micros: u64,
    pause_micros: u64,
    ready_micros: u64,
    supply_bits: u64,
}

#[quickcheck]
fn event_driven_successor_matches_naive_walk(
    raw_rates: Vec<i16>,
    raw_supply: Vec<i16>,
    ladder_changes: Vec<i16>,
    timings: Vec<u8>,
    initial: u8,
) -> bool {
    let rates = raw_rates
        .into_iter()
        .take(96)
        .map(|value| f64::from(value).abs() + 1.0_f64)
        .collect::<Vec<_>>();
    let base_supply = raw_supply
        .into_iter()
        .take(8)
        .map(|value| f64::from(value).abs() + 1.0_f64)
        .collect::<Vec<_>>();
    if base_supply.is_empty() {
        return true;
    }
    let changes = if ladder_changes.is_empty() {
        vec![0_i16]
    } else {
        ladder_changes.into_iter().take(16).collect()
    };
    let timings = if timings.is_empty() {
        vec![0_u8]
    } else {
        timings.into_iter().take(32).collect()
    };
    let initial = u32::from(initial) % base_supply.len() as u32 + 1;

    successor_walk_fixture(&rates, &base_supply, &changes, &timings, initial, false)
        == successor_walk_fixture(&rates, &base_supply, &changes, &timings, initial, true)
}

fn successor_walk_fixture(
    rates: &[f64],
    base_supply: &[f64],
    ladder_changes: &[i16],
    timings: &[u8],
    initial: u32,
    event_driven: bool,
) -> Vec<WalkRepair> {
    const REPORT_MICROS: u64 = 100;
    let mut runs = Vec::with_capacity(rates.len());
    write_monotone_runs(rates, &mut runs);
    let mut repairs = Vec::new();
    let mut supply = vec![0.0_f64; base_supply.len()];
    let mut replicas = initial;
    let mut ready_micros = u64::from(timings[0] % 4) * REPORT_MICROS;
    let mut boundary = 0;
    while boundary < rates.len() {
        let requested_micros = (boundary as u64 + 1) * REPORT_MICROS;
        if requested_micros < ready_micros {
            boundary += 1;
            continue;
        }
        write_fixture_ladder(base_supply, ladder_changes, repairs.len(), &mut supply);
        let target = select_repair_target(&supply, rates[boundary]);
        if target != replicas {
            push_fixture_repair(
                &mut repairs,
                &supply,
                (replicas, target),
                boundary,
                requested_micros,
                timings,
                &mut ready_micros,
            );
            replicas = target;
            boundary += 1;
        } else if event_driven {
            boundary = next_repair_boundary(
                rates,
                &runs,
                boundary,
                repair_band(&supply, target, rates[boundary]),
                0,
                REPORT_MICROS,
                ready_micros,
            );
        } else {
            boundary += 1;
        }
    }
    if let Some(&rate) = rates.last()
        && (rates.len() as u64) * REPORT_MICROS < ready_micros
    {
        let boundary = ready_micros.div_ceil(REPORT_MICROS).saturating_sub(1) as usize;
        let requested_micros = (boundary as u64 + 1) * REPORT_MICROS;
        write_fixture_ladder(base_supply, ladder_changes, repairs.len(), &mut supply);
        let target = select_repair_target(&supply, rate);
        if target != replicas {
            push_fixture_repair(
                &mut repairs,
                &supply,
                (replicas, target),
                boundary,
                requested_micros,
                timings,
                &mut ready_micros,
            );
        }
    }
    repairs
}

fn write_fixture_ladder(base: &[f64], changes: &[i16], repair_count: usize, supply: &mut [f64]) {
    let shift = f64::from(changes[repair_count % changes.len()]) / 8.0_f64;
    for (index, value) in supply.iter_mut().enumerate() {
        *value = (base[index] + shift).max(0.25_f64);
    }
}

fn push_fixture_repair(
    repairs: &mut Vec<WalkRepair>,
    supply: &[f64],
    transition: (u32, u32),
    boundary: usize,
    requested_micros: u64,
    timings: &[u8],
    ready_micros: &mut u64,
) {
    let (origin, target) = transition;
    let direction = u64::from(target < origin);
    let delta = u64::from(target.abs_diff(origin));
    let timing_key = requested_micros ^ (direction << 1_u32) ^ (delta << 2_u32);
    let timing_count = u64::try_from(timings.len()).unwrap_or(u64::MAX);
    let timing_index = usize::try_from(timing_key % timing_count).unwrap_or(0);
    let timing = timings[timing_index];
    let pause_micros = requested_micros + u64::from(timing % 5) * 17;
    *ready_micros = pause_micros + u64::from(timing / 5 % 7) * 31;
    repairs.push(WalkRepair {
        target,
        boundary,
        requested_micros,
        pause_micros,
        ready_micros: *ready_micros,
        supply_bits: supply[target as usize - 1].to_bits(),
    });
}

#[test]
fn commitment_domains_form_distinct_midpoint_permutations() {
    const COUNT: usize = 64;
    let coordinates = [0_u64, 1].map(|domain| {
        [0_u64, 1].map(|counter| {
            (0..COUNT)
                .map(|scenario| {
                    let mut random =
                        scenario_random(scenario as u32, COUNT, ScenarioRole::Commitment)
                            .domain(domain);
                    if counter == 1 {
                        let _ = random.open_unit_f64();
                    }
                    random.open_unit_f64()
                })
                .collect::<Vec<_>>()
        })
    });
    for permutation in coordinates.iter().flatten() {
        let mut sorted = permutation.clone();
        sorted.sort_by(f64::total_cmp);
        assert!(sorted.iter().enumerate().all(|(rank, quantile)| {
            let rank = u32::try_from(rank).unwrap_or(u32::MAX);
            let count = u32::try_from(COUNT).unwrap_or(u32::MAX);
            quantile.to_bits() == ((f64::from(rank) + 0.5_f64) / f64::from(count)).to_bits()
        }));
    }
    assert_ne!(coordinates[0][0], coordinates[1][0]);
    assert_ne!(coordinates[0][1], coordinates[1][1]);
    assert_ne!(coordinates[0][0], coordinates[0][1]);
    assert_ne!(coordinates[1][0], coordinates[1][1]);
}

#[test]
fn identical_posterior_decisions_have_identical_cost_ladders() -> Result<(), TestError> {
    let mut configuration = test_configuration()?;
    configuration.posterior_sample_count = 256;
    configuration.arrival_prior = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.advance_model_time(ModelTime::from_micros(1_000_000))?;
    observation.set_current_replicas(4)?;
    observation.set_backlog(BacklogCohort::new(
        1_000_000,
        500_000,
        100,
        0,
        DemandClass::Normal,
    )?)?;
    let _ = step(&mut state, &mut scratch, observation.observation());
    let mut first = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_decision_expected_costs(&mut first)?;

    let _ = step(&mut state, &mut scratch, observation.observation());
    let mut second = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_decision_expected_costs(&mut second)?;

    assert!(
        first
            .iter()
            .zip(&second)
            .all(|(left, right)| left.to_bits() == right.to_bits())
    );
    Ok(())
}

fn idle_ladder_step(
    sample_count: u32,
    alternating_calendar: bool,
) -> Result<(ScaleState, ScaleScratch), TestError> {
    let mut configuration = test_configuration()?;
    configuration.partition_count = 64;
    configuration.calendar_segment_count_max = 64;
    configuration.replica_count_max = 8;
    configuration.posterior_sample_count = sample_count;
    configuration.report_interval_micros = 100_000;
    configuration.arrival_prior = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    configuration.capacity_change_rate_per_second = 1.0_f64 / 300.0_f64;
    let grid = CapacityGrid::new_with_prior(
        &[0.000_5_f64, 0.001_f64, 0.002_f64, 0.004_f64, 0.008_f64],
        &[32_000.0_f64, 64_000.0_f64, 128_000.0_f64, 256_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
        CapacityPrior::LogUniform,
    )?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.advance_model_time(ModelTime::from_micros(240_000_000))?;
    if !alternating_calendar {
        observation.set_arrivals(0, 240_000_000)?;
    }
    if alternating_calendar {
        observation.set_calendar_forecast(
            CalendarArtifactId(1),
            0.001_f64,
            &[
                CalendarRateSegment::new(
                    0,
                    240_000_000,
                    500_000_000,
                    1_000_000_000_000.0_f64,
                    0.001_f64,
                )?,
                CalendarRateSegment::new(1, 500_000_000, 600_000_000, 0.001_f64, 1.0_f64)?,
            ],
        )?;
    }
    observation.set_current_replicas(8)?;
    let commitment = if alternating_calendar {
        ActuationCommitment::rebalancing(
            8,
            4,
            ModelTime::from_micros(238_000_000),
            ModelTime::from_micros(239_000_000),
        )?
    } else {
        ActuationCommitment::launching(8, 1, ModelTime::from_micros(239_000_000))?
    };
    observation.push_actuation_commitment(commitment)?;
    if !matches!(
        step(&mut state, &mut scratch, observation.observation()),
        ScaleDecision::Apply(_)
    ) {
        return Err(TestError::UnexpectedDecision);
    }
    Ok((state, scratch))
}

#[test]
fn idle_pending_descent_cost_ladder_selects_one() -> Result<(), TestError> {
    let (_, scratch) = idle_ladder_step(4_096, false)?;
    let mut costs = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_decision_expected_costs(&mut costs)?;
    // Physical-transition draws give this ladder after posterior aggregation.
    let expected = [
        15_002.668_927_987_197_f64,
        15_071.119_970_947_477_f64,
        15_179.597_407_374_56_f64,
        15_391.932_417_089_418_f64,
        15_472.511_244_575_338_f64,
        15_513.248_026_116_547_f64,
        15_619.660_299_843_654_f64,
        15_022.433_008_180_305_f64,
    ];
    assert!(
        costs
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() < 0.01_f64),
        "costs={costs:?}"
    );
    assert_eq!(argmin(&costs), 0, "costs={costs:?}");
    assert!(costs[7] > costs[0], "costs={costs:?}");
    Ok(())
}

#[test]
fn calendar_wave_retargets_at_mean_boundaries() {
    let trajectory = MeanRateTrajectory::new(&[150.0_f64, 250.0_f64, 250.0_f64, 50.0_f64]);
    let supply = [100.0_f64, 200.0_f64, 300.0_f64];
    let mut target = 2_u32;
    let mut changes = Vec::new();
    for (boundary, rate) in trajectory.rates().enumerate() {
        let next = select_repair_target(&supply, rate);
        if next != target {
            changes.push((boundary + 1, next));
            target = next;
        }
    }

    assert_eq!(changes, vec![(2, 3), (4, 1)]);
}

#[test]
fn partition_deadline_outputs_preserve_work_scale() -> Result<(), TestError> {
    let service_time_seconds = 0.25_f64;
    let supply = 3.0_f64;
    let no_arrivals = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };
    let window = EvaluationWindow {
        start_micros: 0,
        horizon_micros: 4_000_000,
        initial_debt_work: 0.0_f64,
        deadline_budget_micros: 1_000_000,
    };
    let mut raw_outputs = [(0.0_f64, 0.0_f64); 4];
    let mut scaled_outputs = [(0.0_f64, 0.0_f64); 4];
    for replica_count in 1..=4 {
        let mut raw = SlotSecondCohorts::new(2);
        raw.push_values(0, 1_000_000, 4.0_f64, 0);
        raw.push_values(500_000, 2_000_000, 6.0_f64, 1);
        let mut scaled = SlotSecondCohorts::new(2);
        scaled.push_values(0, 1_000_000, 4.0_f64 * service_time_seconds, 0);
        scaled.push_values(500_000, 2_000_000, 6.0_f64 * service_time_seconds, 1);
        let mut raw_scratch = EdfScratch::new(2)?;
        let mut scaled_scratch = EdfScratch::new(2)?;
        prepare(&raw, &mut raw_scratch);
        prepare(&scaled, &mut scaled_scratch);
        let max_owner_share = 1.0_f64 / f64::from(replica_count as u32);
        let raw_capacity = partition_replica_capacity(supply, replica_count, max_owner_share);
        let replica_count_f64 = f64::from(replica_count as u32);
        let scaled_capacity = supply * service_time_seconds / replica_count_f64;
        let raw_outcome = evaluate_prepared_step(
            &raw,
            SupplyStep {
                before: raw_capacity,
                during: raw_capacity,
                after: raw_capacity,
                pause_micros: 0,
                ready_micros: 0,
            },
            window,
            &no_arrivals,
            &mut raw_scratch,
        );
        let scaled_outcome = evaluate_prepared_step(
            &scaled,
            SupplyStep {
                before: scaled_capacity,
                during: scaled_capacity,
                after: scaled_capacity,
                pause_micros: 0,
                ready_micros: 0,
            },
            window,
            &no_arrivals,
            &mut scaled_scratch,
        );
        raw_outputs[replica_count - 1] = (
            raw_outcome.missed_work,
            raw_outcome.late_area + raw_outcome.terminal_late_area,
        );
        scaled_outputs[replica_count - 1] = (
            scaled_outcome.missed_work / service_time_seconds,
            (scaled_outcome.late_area + scaled_outcome.terminal_late_area) / service_time_seconds,
        );
    }
    assert!(raw_outputs.iter().zip(scaled_outputs).all(
        |(&(raw_missed, raw_late), (scaled_missed, scaled_late))| {
            partition_float_matches(raw_missed, scaled_missed)
                && partition_float_matches(raw_late, scaled_late)
        }
    ));
    Ok(())
}

fn partition_float_matches(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1.0e-12_f64.max(1.0e-9_f64 * expected.abs())
}

#[test]
fn report_views_match_the_fixed_model_contract() -> Result<(), TestError> {
    let (state, _scratch, _observation) = test_model()?;
    let artifact = state.capacity_artifact();
    let clock = state.capacity_clock_check();
    let launch = state.launch_component_summary(1);
    let query = PosteriorQuery::CapacityContaminationProbability;
    let value_count = usize::try_from(state.posterior_value_count(query)?)?;
    let mut values = vec![0.0_f64; value_count];
    let mut probabilities = vec![0.0_f64; value_count];

    state.write_posterior(query, &mut values, &mut probabilities)?;

    assert_eq!(artifact.identity().version(), 2);
    assert!(!artifact.coverage().is_empty());
    assert!(artifact.coverage().iter().all(|coverage| {
        coverage.lower_tail_probability() >= 0.0_f64 && coverage.upper_tail_probability() >= 0.0_f64
    }));
    assert_eq!(clock.sample_count, 0);
    assert_eq!(clock.maximum_distance.to_bits(), 0.0_f64.to_bits());
    assert!(clock.rejection_threshold.is_infinite());
    assert!(!clock.rejected);
    assert!(launch.fast_mean_seconds > 0.0_f64);
    assert!(launch.slow_mean_seconds > launch.fast_mean_seconds);
    assert!((0.0_f64..=1.0_f64).contains(&launch.slow_probability));
    assert_eq!(values.len(), probabilities.len());
    assert!(approximately_equal(probabilities.iter().sum(), 1.0_f64));
    assert!(
        state.capacity_class_count() * state.posterior_samples_per_capacity_class_min()
            <= state.configuration().posterior_sample_count
    );
    Ok(())
}

#[test]
fn overdue_backlog_keeps_each_original_deadline() -> Result<(), TestError> {
    let (mut state, mut scratch, mut observation) = test_model()?;
    state.model_time = ModelTime::from_micros(10_000_000);
    observation.set_backlog(BacklogCohort::new(
        10_000_000,
        8_500_000,
        5,
        0,
        DemandClass::Normal,
    )?)?;
    observation.set_backlog(BacklogCohort::new(
        10_000_000,
        8_000_000,
        7,
        1,
        DemandClass::Normal,
    )?)?;
    let input = observation.observation();

    prepare_work_cohorts(
        &state,
        &mut scratch,
        input.cohorts,
        input.backlog,
        input.scheduled_releases,
    );

    assert_eq!(scratch.resource_cohorts.len(), 2);
    assert_eq!(scratch.resource_cohorts.deadline_micros(0), 9_500_000);
    assert_eq!(scratch.resource_cohorts.deadline_micros(1), 9_000_000);
    assert!(approximately_equal(
        scratch.resource_cohorts.work(0),
        5.0_f64,
    ));
    assert!(approximately_equal(
        scratch.resource_cohorts.work(1),
        7.0_f64,
    ));
    Ok(())
}

#[test]
fn scheduled_release_validation_rejects_invalid_inputs() -> Result<(), TestError> {
    let (state, _scratch, mut observation) = test_model()?;
    assert!(
        observation
            .set_scheduled_releases(&[ScheduledRelease {
                release_micros: 1,
                count: 0,
            }])
            .is_err()
    );
    assert!(
        observation
            .set_scheduled_releases(&[
                ScheduledRelease {
                    release_micros: 2,
                    count: 1,
                },
                ScheduledRelease {
                    release_micros: 1,
                    count: 1,
                },
            ])
            .is_err()
    );
    let excess = vec![
        ScheduledRelease {
            release_micros: 1,
            count: 1,
        };
        state.configuration.scheduled_release_count_max as usize + 1
    ];
    assert!(observation.set_scheduled_releases(&excess).is_err());
    Ok(())
}

#[test]
fn configuration_rejects_an_uncovered_planning_horizon() -> Result<(), TestError> {
    let mut configuration = test_configuration()?;
    configuration.report_interval_micros = ArrivalPrior::MAXIMUM_PATH_MICROS;

    assert_eq!(
        configuration.validate(),
        Err(ConfigurationError::PlanningHorizonDomain)
    );
    Ok(())
}

#[test]
fn ingestion_rejects_deadlines_beyond_the_arrival_domain() -> Result<(), TestError> {
    let configuration = test_configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let now_micros = 17_u64;
    observation.advance_model_time(ModelTime::from_micros(now_micros))?;
    let deadline_max =
        now_micros + ArrivalPrior::MAXIMUM_PATH_MICROS - configuration.objective.budget_micros();

    assert_eq!(
        observation.push_cohort(crate::Cohort {
            release_micros: now_micros,
            deadline_micros: deadline_max + 1,
            offered_events: 1.0_f64,
            partition: 0,
            demand_class: DemandClass::Normal,
        }),
        Err(ObservationError::DeadlineHorizon)
    );
    assert_eq!(
        observation.set_scheduled_releases(&[ScheduledRelease {
            release_micros: deadline_max - configuration.objective.budget_micros() + 1,
            count: 1,
        }]),
        Err(ObservationError::DeadlineHorizon)
    );
    Ok(())
}

#[test]
fn scenario_horizon_uses_only_artifact_support() -> Result<(), TestError> {
    let (state, _scratch, _observation) = test_model()?;
    let cohorts = EventCohorts::new(1);
    let first = scenario_horizons(&state, &cohorts);
    let second = scenario_horizons(&state, &cohorts);
    let support_seconds = state
        .configuration
        .launch_time_prior
        .coverage_support_seconds()
        .1
        + state
            .configuration
            .rebalance_time_prior
            .coverage_support_seconds()
            .1;
    let report = state.configuration.report_interval_micros;
    let expected_disturbance = report + (2.0_f64 * support_seconds * 1_000_000.0_f64) as u64;

    assert_eq!(first, second, "the horizon must be equal across seeds");
    assert_eq!(first.1, expected_disturbance);
    Ok(())
}

#[quickcheck]
fn scheduled_releases_are_exact_and_idempotent(raw: Vec<(u8, u16)>) -> bool {
    let Ok((mut state, mut scratch, mut observation)) = test_model() else {
        return false;
    };
    let now_micros = 10_000_000_u64;
    state.model_time = ModelTime::from_micros(now_micros);

    let mut releases = raw
        .into_iter()
        .take(state.configuration.scheduled_release_count_max as usize - 3)
        .map(|(seconds, count)| ScheduledRelease {
            release_micros: now_micros.saturating_add(u64::from(seconds).saturating_mul(1_000_000)),
            count: u32::from(count).saturating_add(1),
        })
        .collect::<Vec<_>>();
    releases.push(ScheduledRelease {
        release_micros: now_micros,
        count: 3,
    });
    releases.push(ScheduledRelease {
        release_micros: now_micros.saturating_add(149_999_999),
        count: 5,
    });
    releases.push(ScheduledRelease {
        release_micros: now_micros.saturating_add(150_000_001),
        count: 7,
    });
    releases.sort_unstable_by_key(|release| release.release_micros);

    if observation.set_scheduled_releases(&releases).is_err()
        || observation.set_scheduled_releases(&releases).is_err()
    {
        return false;
    }
    let input = observation.observation();
    let expected = input
        .scheduled_releases
        .iter()
        .filter(|release| release.release_micros > now_micros)
        .copied()
        .collect::<Vec<_>>();
    prepare_work_cohorts(
        &state,
        &mut scratch,
        input.cohorts,
        input.backlog,
        input.scheduled_releases,
    );

    if scratch.resource_cohorts.len() != expected.len() {
        return false;
    }
    expected.iter().enumerate().all(|(index, release)| {
        scratch.resource_cohorts.release_micros(index) == release.release_micros
            && scratch.resource_cohorts.deadline_micros(index)
                == release
                    .release_micros
                    .saturating_add(state.configuration.objective.budget_micros())
            && approximately_equal(
                scratch.resource_cohorts.work(index),
                f64::from(release.count),
            )
            && scratch.resource_cohorts.partition(index) == SCHEDULED_PARTITION
    })
}

#[quickcheck]
fn scheduled_counts_are_equal_across_scenarios(count: u16) -> bool {
    let scheduled_count = f64::from(count) + 1.0_f64;
    let mut cohorts = EventCohorts::new(1);
    cohorts.push_values(2_000_000, 3_000_000, scheduled_count, SCHEDULED_PARTITION);
    let empty = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[4.0_f64],
        rates: &[0.0_f64],
    };
    let busy = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[4.0_f64],
        rates: &[17.0_f64],
    };
    let empty_count = scenario_event_count(
        2.0_f64, 3.0_f64, &empty, 0.0_f64, 4.0_f64, &cohorts, 4_000_000,
    );
    let busy_count = scenario_event_count(
        2.0_f64, 3.0_f64, &busy, 0.0_f64, 4.0_f64, &cohorts, 4_000_000,
    );

    approximately_equal(empty_count - 5.0_f64, scheduled_count)
        && approximately_equal(busy_count - 73.0_f64, scheduled_count)
}

#[test]
fn curve_class_columns_equal_cell_columns_with_rolled_masses() -> Result<(), TestError> {
    let configuration = test_configuration()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64, 1.0_f64])?;
    let state = ScaleState::new(configuration, grid)?;
    let draw_count = 3_u32;
    let mut cell_column = 0.0_f64;
    let mut class_column = 0.0_f64;
    for class in 0..state.capacity_classes.len() {
        let members = state.capacity_classes.members(class);
        let class_mass = members
            .iter()
            .map(|&cell| state.capacity.curve_and_probability(cell).1)
            .sum::<f64>();
        let representative = state.capacity_classes.representative(class);
        let (curve, _) = state.capacity.curve_and_probability(representative);
        let path_mean = (0_u32..draw_count)
            .map(|draw| curve.sustainable_throughput(64.0_f64) + f64::from(draw))
            .sum::<f64>()
            / f64::from(draw_count);
        class_column += class_mass * path_mean;
        for &cell in members {
            let mass = state.capacity.curve_and_probability(cell).1;
            cell_column += mass * path_mean;
        }
    }

    assert!(approximately_equal(class_column, cell_column));
    Ok(())
}

fn approximately_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= f64::EPSILON * left.abs().max(right.abs()).max(1.0_f64)
}

fn test_model() -> Result<(ScaleState, ScaleScratch, ObservationBuffer), TestError> {
    let configuration = test_configuration()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let state = ScaleState::new(configuration.clone(), grid)?;
    let scratch = state.new_scratch()?;
    let observation = ObservationBuffer::new(&configuration)?;
    Ok((state, scratch, observation))
}

fn test_configuration() -> Result<Configuration, TestError> {
    Ok(Configuration {
        cohort_count_max: 1,
        calendar_segment_count_max: 1,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 4,
        replica_count_max: 4,
        slots_per_replica: 32,
        posterior_sample_count: 64,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::test_prior(1.0_f64, 1.0e12_f64, 1.0e-12_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    })
}

#[derive(Debug, Error)]
enum TestError {
    #[error("the controller returned an unexpected decision")]
    UnexpectedDecision,
    #[error(transparent)]
    Assignment(#[from] crate::AssignmentError),
    #[error(transparent)]
    Integer(#[from] TryFromIntError),
    #[error(transparent)]
    LeadTime(#[from] crate::LeadTimePriorError),
    #[error(transparent)]
    Arrival(#[from] ArrivalPriorError),
    #[error(transparent)]
    Capacity(#[from] CapacityGridError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    DecisionCurve(#[from] DecisionCurveError),
    #[error(transparent)]
    Observation(#[from] ObservationError),
    #[error(transparent)]
    Posterior(#[from] PosteriorError),
    #[error(transparent)]
    ResourceWindow(#[from] crate::ResourceWindowError),
}
use std::num::TryFromIntError;
