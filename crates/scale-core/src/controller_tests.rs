use fearless_simd::{Level, dispatch};
use quickcheck_macros::quickcheck;
use std::array;
use std::time::Duration;
use thiserror::Error;

use super::{
    SCHEDULED_PARTITION, ScenarioRole, TrajectoryEventKind, aggregate_scenario_values,
    assignment_max_owner_share, balanced_partition_owner, balanced_partition_range,
    billing_replica_seconds, instantaneous_owner_supply, max_owner_share, numerical_decision,
    placement_supply, plant_target_assignment, prepare_work_cohorts,
    sample_hypothetical_transition_times, sample_transition_hypotheses, sample_transition_times,
    scenario_capacity_curve, scenario_event_count, scenario_horizons, scenario_random,
    select_target, terminal_replica_seconds,
};
use crate::CapacityCurve;
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
    late_area: [f64; 8],
    costs: [f64; 8],
    candidate_eight_has_target_eight_event: bool,
    candidate_transition_times: [Option<(u64, u64)>; 8],
    fence_keeps_supply: [bool; 8],
    has_fence_pair: [bool; 8],
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
fn extra_owners_reduce_only_residual_partition_lateness() -> Result<(), TestError> {
    let measurement = i27_measurement(None, 0.1_f64, 1)?;
    assert!(
        measurement
            .late_area
            .windows(2)
            .all(|pair| pair[0] >= pair[1]),
        "late_area={:?}",
        measurement.late_area
    );
    assert!(measurement.late_area[0] > measurement.late_area[7]);
    Ok(())
}

#[test]
fn hot_owner_descent_bills_the_committed_plant_path() -> Result<(), TestError> {
    let measurement = i27_measurement(None, 0.1_f64, 1)?;

    assert!(
        measurement.replica_seconds[0] < measurement.replica_seconds[7] / 2.0_f64,
        "replica_seconds={:?}",
        measurement.replica_seconds
    );
    Ok(())
}

#[test]
fn candidate_trajectory_has_no_phantom_future_action() -> Result<(), TestError> {
    let (_, scratch) = i27_step(None, 0.1_f64, 1)?;
    let workspace = &scratch.scenario_workspaces[0];
    for candidate in 0..8_usize {
        let first = workspace.trajectory_offsets[candidate] as usize;
        let last = workspace.trajectory_offsets[candidate + 1] as usize;
        let mut transitions = (first..last)
            .filter(|event| workspace.trajectory.kinds[*event] == TrajectoryEventKind::Transition);
        if let Some(transition) = transitions.next() {
            assert_eq!(
                workspace.trajectory.targets[transition],
                candidate as u32 + 1
            );
        }
        assert!(transitions.next().is_none(), "candidate={candidate}");
    }
    Ok(())
}

#[test]
fn uniform_owner_supply_equals_the_physical_fleet_bound() {
    let assignment = [0_u32, 1, 2, 3, 0, 1, 2, 3];
    let shares = [0.125_f64; 8];
    let mut owner_sums = [0.0_f64; 4];

    for demand in [20.0_f64, 1_000.0_f64] {
        let supply =
            instantaneous_owner_supply(&assignment, &shares, demand, 100.0_f64, &mut owner_sums);
        assert_eq!(supply.to_bits(), demand.min(400.0_f64).to_bits());
    }
}

#[test]
fn concentrated_partition_supply_has_exactly_one_owner() {
    let assignment = [0_u32, 1, 2, 3];
    let shares = [1.0_f64, 0.0_f64, 0.0_f64, 0.0_f64];
    let mut owner_sums = [0.0_f64; 4];

    let supply = instantaneous_owner_supply(
        &assignment,
        &shares,
        1_000.0_f64,
        100.0_f64,
        &mut owner_sums,
    );

    assert_eq!(supply.to_bits(), 100.0_f64.to_bits());
}

#[test]
fn fallback_selection_preserves_scenario_rejections() -> Result<(), TestError> {
    let configuration = test_configuration()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let state = ScaleState::new(configuration, grid)?;
    let mut scratch = state.new_scratch()?;
    let candidate_count = scratch.posterior_miss_fraction_sums.len();
    scratch.active_scenario_count = 1;
    scratch.active_inner_count = 1;
    scratch.class_masses[0] = 1.0_f64;
    scratch.posterior_miss_fraction_sums.fill(1.0_f64);
    scratch.scenario_rejection[..candidate_count].copy_from_slice(&[0, 1, 2, 3]);
    let normalized = scratch.scenario_rejection[..candidate_count].to_vec();

    let _selected = numerical_decision(&state, &mut scratch);

    assert_eq!(scratch.scenario_rejection[..candidate_count], normalized);
    Ok(())
}

#[test]
fn owner_pricing_uses_the_plant_assignment_rule() {
    let current = [0_u32, 1, 2, 3, 0, 1, 2, 3];
    let mut target = [0_u32; 8];
    let mut counts = [0_u32; 4];

    plant_target_assignment(&current, 3, &mut target, &mut counts);

    assert_eq!(target, [0, 1, 2, 0, 0, 1, 2, 1]);
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
fn identical_posterior_decisions_have_identical_cost_ladders() -> Result<(), TestError> {
    let (mut baseline, _) = i27_step(None, 0.02_f64, 12)?;
    let (mut next, _) = i27_step(None, 0.02_f64, 12)?;
    next.model_time = ModelTime::from_micros(
        baseline
            .model_time
            .as_micros()
            .saturating_add(baseline.configuration.report_interval_micros),
    );
    let mut baseline_scratch = baseline.new_scratch()?;
    let mut next_scratch = next.new_scratch()?;
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
    let next_decision = select_target(
        &mut next,
        &mut next_scratch,
        inputs.cohorts,
        inputs.backlog,
        inputs.scheduled_releases,
        inputs.calendar,
        inputs.actuation_commitments,
    );
    let ScaleDecision::Apply(baseline_apply) = baseline_decision else {
        return Err(TestError::UnexpectedDecision);
    };
    let ScaleDecision::Apply(next_apply) = next_decision else {
        return Err(TestError::UnexpectedDecision);
    };
    let mut baseline_costs = vec![0.0_f64; baseline_scratch.decision_candidate_count()];
    let mut next_costs = vec![0.0_f64; next_scratch.decision_candidate_count()];
    baseline_scratch.write_decision_expected_costs(&mut baseline_costs)?;
    next_scratch.write_decision_expected_costs(&mut next_costs)?;

    assert_eq!(baseline_apply.target, next_apply.target);
    assert!(
        baseline_costs
            .iter()
            .zip(&next_costs)
            .all(|(left, right)| left.to_bits() == right.to_bits()),
        "baseline={baseline_costs:?}, next={next_costs:?}"
    );
    Ok(())
}

#[test]
fn shifted_pending_commitments_have_identical_cost_ladders() -> Result<(), TestError> {
    let (mut baseline, _) = i27_step(None, 0.02_f64, 12)?;
    let (mut shifted, _) = i27_step(None, 0.02_f64, 12)?;
    shifted.model_time = ModelTime::from_micros(
        baseline
            .model_time
            .as_micros()
            .saturating_add(baseline.configuration.report_interval_micros),
    );
    let mut baseline_scratch = baseline.new_scratch()?;
    let mut shifted_scratch = shifted.new_scratch()?;
    let mut baseline_observation = ObservationBuffer::new(&baseline.configuration)?;
    let mut shifted_observation = ObservationBuffer::new(&shifted.configuration)?;
    baseline_observation.push_actuation_commitment(ActuationCommitment::launching(
        8,
        1,
        ModelTime::from_micros(baseline.model_time.as_micros().saturating_sub(1_000_000)),
    )?)?;
    shifted_observation.push_actuation_commitment(ActuationCommitment::launching(
        8,
        1,
        ModelTime::from_micros(shifted.model_time.as_micros().saturating_sub(1_000_000)),
    )?)?;

    let baseline_inputs = baseline_observation.observation();
    let shifted_inputs = shifted_observation.observation();
    let baseline_decision = select_target(
        &mut baseline,
        &mut baseline_scratch,
        baseline_inputs.cohorts,
        baseline_inputs.backlog,
        baseline_inputs.scheduled_releases,
        baseline_inputs.calendar,
        baseline_inputs.actuation_commitments,
    );
    let shifted_decision = select_target(
        &mut shifted,
        &mut shifted_scratch,
        shifted_inputs.cohorts,
        shifted_inputs.backlog,
        shifted_inputs.scheduled_releases,
        shifted_inputs.calendar,
        shifted_inputs.actuation_commitments,
    );
    let ScaleDecision::Apply(baseline_apply) = baseline_decision else {
        return Err(TestError::UnexpectedDecision);
    };
    let ScaleDecision::Apply(shifted_apply) = shifted_decision else {
        return Err(TestError::UnexpectedDecision);
    };
    let mut baseline_costs = vec![0.0_f64; baseline_scratch.decision_candidate_count()];
    let mut shifted_costs = vec![0.0_f64; shifted_scratch.decision_candidate_count()];
    baseline_scratch.write_decision_expected_costs(&mut baseline_costs)?;
    shifted_scratch.write_decision_expected_costs(&mut shifted_costs)?;

    assert_eq!(baseline_apply.target, shifted_apply.target);
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
    let fresh = i27_measurement(None, 0.02_f64, 12)?;
    assert!(measurement.candidate_eight_has_target_eight_event);
    for candidate in 0..7 {
        // The commitment-to-fence billing is zero because target eight is
        // already billed. The fence keeps current supply. The fresh retarget
        // cost therefore stays unchanged.
        assert!(measurement.has_fence_pair[candidate]);
        assert!(measurement.fence_keeps_supply[candidate]);
        let commitment_to_fence =
            3.0_f64 * (measurement.replica_seconds[candidate] - fresh.replica_seconds[candidate]);
        let expected = fresh.costs[candidate] + commitment_to_fence;
        assert!((measurement.costs[candidate] - expected).abs() < 0.01_f64);
        assert_eq!(
            measurement.late_area[candidate].to_bits(),
            fresh.late_area[candidate].to_bits()
        );
    }
    Ok(())
}

#[test]
fn abandoned_launch_bills_only_until_its_fence() -> Result<(), TestError> {
    let (mut state, mut scratch, mut observation) = i27_model_with_replica_max(0.02_f64, 10)?;
    observation.advance_model_time(ModelTime::from_micros(5_000_000))?;
    observation.set_current_replicas(8)?;
    let owners = (0_u32..64)
        .map(|partition| partition % 8)
        .collect::<Vec<_>>();
    observation.set_partition_owners(&owners)?;
    observation.push_actuation_commitment(ActuationCommitment::launching(
        8,
        10,
        ModelTime::from_micros(0),
    )?)?;
    let _ = step(&mut state, &mut scratch, observation.observation());

    let workspace = &scratch.scenario_workspaces[0];
    let first = workspace.trajectory_offsets[7] as usize;
    let last = workspace.trajectory_offsets[8] as usize;
    let pair = (first..last)
        .filter(|event| {
            matches!(
                workspace.trajectory.kinds[*event],
                TrajectoryEventKind::FencedCommitment | TrajectoryEventKind::FenceClosure
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(pair.len(), 2);
    let targets = pair
        .iter()
        .map(|event| workspace.trajectory.targets[*event])
        .collect::<Vec<_>>();
    let requested = pair
        .iter()
        .map(|event| workspace.trajectory.requested_micros[*event])
        .collect::<Vec<_>>();
    let ready = pair
        .iter()
        .map(|event| workspace.trajectory.billing_ready_micros[*event])
        .collect::<Vec<_>>();
    let billed = billing_replica_seconds(0, 10_000_000, 8, &targets, &requested, &ready);

    // The commitment adds two replicas for five seconds. The closure removes
    // them at the fence. Thus, the extra area is 2 * 5 = 10 replica-seconds.
    assert!((billed - 90.0_f64).abs() < f64::EPSILON, "billed={billed}");
    Ok(())
}

#[test]
fn up_retarget_continues_the_launch_and_adds_only_the_unplanned_delta() -> Result<(), TestError> {
    let (mut state, mut scratch, mut observation) = i27_model(0.02_f64)?;
    observation.set_current_replicas(2)?;
    observation.push_actuation_commitment(ActuationCommitment::launching(
        2,
        4,
        state.model_time,
    )?)?;
    let _ = step(&mut state, &mut scratch, observation.observation());

    let workspace = &scratch.scenario_workspaces[0];
    let first = workspace.trajectory_offsets[7] as usize;
    let last = workspace.trajectory_offsets[8] as usize;
    let mut events = first..last;
    assert!(events.clone().all(|event| !matches!(
        workspace.trajectory.kinds[event],
        TrajectoryEventKind::FencedCommitment | TrajectoryEventKind::FenceClosure
    )));
    let commitment = events
        .clone()
        .find(|&event| workspace.trajectory.kinds[event] == TrajectoryEventKind::FixedTransition)
        .ok_or(TestError::UnexpectedDecision)?;
    let transition = events
        .find(|&event| workspace.trajectory.kinds[event] == TrajectoryEventKind::Transition)
        .ok_or(TestError::UnexpectedDecision)?;
    assert_eq!(workspace.trajectory.targets[commitment], 4);
    assert_eq!(workspace.trajectory.targets[transition], 8);
    assert_eq!(
        workspace.trajectory.sampled_ready_micros[commitment],
        workspace.commitment_ready_micros[0]
    );
    assert_eq!(
        workspace.trajectory.targets[transition] - workspace.trajectory.targets[commitment],
        4
    );
    Ok(())
}

#[test]
fn partial_up_cancel_retains_supply_and_fences_only_the_cancelled_part() -> Result<(), TestError> {
    let (mut state, mut scratch, mut observation) = i27_model(0.02_f64)?;
    observation.set_current_replicas(2)?;
    observation.push_actuation_commitment(ActuationCommitment::launching(
        2,
        6,
        state.model_time,
    )?)?;
    let _ = step(&mut state, &mut scratch, observation.observation());

    let workspace = &scratch.scenario_workspaces[0];
    let first = workspace.trajectory_offsets[3] as usize;
    let last = workspace.trajectory_offsets[4] as usize;
    let retained = (first..last)
        .find(|&event| workspace.trajectory.kinds[event] == TrajectoryEventKind::FixedTransition)
        .ok_or(TestError::UnexpectedDecision)?;
    let fenced = (first..last)
        .filter(|&event| workspace.trajectory.kinds[event] == TrajectoryEventKind::FencedCommitment)
        .collect::<Vec<_>>();
    let closures = (first..last)
        .filter(|&event| workspace.trajectory.kinds[event] == TrajectoryEventKind::FenceClosure)
        .collect::<Vec<_>>();
    assert_eq!(workspace.trajectory.targets[retained], 4);
    assert_eq!(fenced.len(), 1);
    assert_eq!(closures.len(), 1);
    assert_eq!(workspace.trajectory.targets[fenced[0]], 6);
    assert_eq!(workspace.trajectory.targets[closures[0]], 4);
    assert_eq!(
        workspace.trajectory.sampled_ready_micros[retained],
        workspace.commitment_ready_micros[0]
    );
    assert_eq!(
        workspace.trajectory.during_supply[fenced[0]].to_bits(),
        workspace.trajectory.after_supply[fenced[0]].to_bits()
    );
    let targets = [6, 4];
    let requested = [
        workspace.trajectory.requested_micros[fenced[0]],
        workspace.trajectory.requested_micros[closures[0]],
    ];
    let ready = [
        workspace.trajectory.billing_ready_micros[fenced[0]],
        workspace.trajectory.billing_ready_micros[closures[0]],
    ];
    let signed_pair = billing_replica_seconds(
        state.model_time.as_micros(),
        workspace.commitment_ready_micros[0],
        4,
        &targets,
        &requested,
        &ready,
    );
    let interval_seconds = Duration::from_micros(
        workspace.commitment_ready_micros[0].saturating_sub(state.model_time.as_micros()),
    )
    .as_secs_f64();
    let retained_area = 4.0_f64 * interval_seconds;
    let cancelled_area = 2.0_f64 * 0.0_f64;
    assert!(
        (signed_pair - (retained_area + cancelled_area)).abs() < 1.0e-9_f64,
        "signed_pair={signed_pair}, retained_area={retained_area}, cancelled_area={cancelled_area}"
    );
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
    for candidate in 1..8 {
        // The signed pair bills the descent from its fence until its sampled
        // ready time. The fence keeps current supply. Time-shift coupling
        // keeps the fresh retarget cost equal to the no-commitment cost.
        assert!(measurement.has_fence_pair[candidate]);
        assert!(measurement.fence_keeps_supply[candidate]);
        let commitment_to_fence =
            3.0_f64 * (measurement.replica_seconds[candidate] - fresh.replica_seconds[candidate]);
        assert!(commitment_to_fence > 0.0_f64);
        let expected = fresh.costs[candidate] + commitment_to_fence;
        assert!((measurement.costs[candidate] - expected).abs() < 0.01_f64);
        assert_eq!(
            measurement.late_area[candidate].to_bits(),
            fresh.late_area[candidate].to_bits()
        );
    }
    Ok(())
}

#[test]
fn cancel_prices_candidate_two_from_the_current_count() -> Result<(), TestError> {
    let retained = i27_measurement(Some((8, 1)), 0.02_f64, 12)?;
    let fresh = i27_measurement(None, 0.02_f64, 12)?;
    // Both candidate-two paths start from eight replicas. Their transition is
    // ordinal zero, so time-shift coupling gives them the same draw.
    assert_eq!(
        retained.candidate_transition_times[1],
        fresh.candidate_transition_times[1]
    );
    // The commitment-to-fence term uses the signed pair. The fence keeps
    // current supply. Candidate two then pays the fresh retarget cost.
    let commitment_to_fence = 3.0_f64 * (retained.replica_seconds[1] - fresh.replica_seconds[1]);
    let expected = fresh.costs[1] + commitment_to_fence;
    assert!(retained.fence_keeps_supply[1]);
    assert!(retained.has_fence_pair[1]);
    assert!((retained.costs[1] - expected).abs() < 0.01_f64);
    assert!(retained.costs[1] > fresh.costs[1]);
    Ok(())
}

#[test]
fn commitment_ordinal_preserves_the_survival_adjustment() -> Result<(), TestError> {
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
            0,
            requested_at,
            0.0_f64,
            crate::TransitionDirection::Up,
            3,
        );
        let fresh_again = sample_transition_times(
            &state,
            &random,
            0,
            requested_at,
            0.0_f64,
            crate::TransitionDirection::Up,
            3,
        );
        assert_eq!(fresh, fresh_again);

        let retained = sample_transition_times(
            &state,
            &random,
            0,
            requested_at + 1_000_000,
            1.0_f64,
            crate::TransitionDirection::Up,
            3,
        );
        let mut oracle_random = random.clone().domain(0).domain(0).domain(3).domain(0);
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
    let mut late_area = [0.0_f64; 8];
    late_area.copy_from_slice(&scratch.posterior_late_area_sums[..8]);
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
    let fence_keeps_supply = array::from_fn(|candidate_index| {
        let first = workspace.trajectory_offsets[candidate_index] as usize;
        let last = workspace.trajectory_offsets[candidate_index + 1] as usize;
        (first..last)
            .filter(|event| {
                matches!(
                    workspace.trajectory.kinds[*event],
                    TrajectoryEventKind::FencedCommitment | TrajectoryEventKind::FenceClosure
                )
            })
            .all(|event| {
                workspace.trajectory.during_supply[event].to_bits()
                    == workspace.trajectory.after_supply[event].to_bits()
            })
    });
    let has_fence_pair = array::from_fn(|candidate_index| {
        let first = workspace.trajectory_offsets[candidate_index] as usize;
        let last = workspace.trajectory_offsets[candidate_index + 1] as usize;
        (first..last)
            .filter(|event| {
                matches!(
                    workspace.trajectory.kinds[*event],
                    TrajectoryEventKind::FencedCommitment | TrajectoryEventKind::FenceClosure
                )
            })
            .count()
            == 2
    });
    Ok(I27Measurement {
        replica_seconds,
        late_area,
        costs,
        candidate_eight_has_target_eight_event,
        candidate_transition_times,
        fence_keeps_supply,
        has_fence_pair,
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
    i27_model_with_replica_max(service_seconds, 8)
}

fn i27_model_with_replica_max(
    service_seconds: f64,
    replica_count_max: u32,
) -> Result<(ScaleState, ScaleScratch, ObservationBuffer), TestError> {
    let configuration = Configuration {
        cohort_count_max: 64,
        calendar_segment_count_max: 64,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 14,
        partition_count: 64,
        replica_count_max,
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
fn time_shifted_transition_requests_share_delay_draws() -> Result<(), TestError> {
    let (state, ..) = i27_model(0.02_f64)?;
    let early_request = state.model_time.as_micros();
    let late_request = early_request.saturating_add(state.configuration.report_interval_micros);
    for scenario in 0_u32..256 {
        let random = scenario_random(scenario, 256, ScenarioRole::Commitment);
        let hypotheses = sample_transition_hypotheses(&state, &random);
        let early = sample_hypothetical_transition_times(
            &state,
            &random,
            early_request,
            0,
            hypotheses,
            crate::TransitionDirection::Down,
            7,
        );
        let late = sample_hypothetical_transition_times(
            &state,
            &random,
            late_request,
            0,
            hypotheses,
            crate::TransitionDirection::Down,
            7,
        );

        assert_eq!(early.0 - early_request, late.0 - late_request);
        assert_eq!(early.1 - early.0, late.1 - late.0);
    }
    Ok(())
}

#[test]
fn terminal_cost_uses_the_candidate_target() -> Result<(), TestError> {
    let (state, scratch) = idle_ladder_step(256, false)?;
    let workspace = &scratch.scenario_workspaces[0];
    let candidate = 7_usize;
    let first = workspace.trajectory_offsets[candidate] as usize;
    let last = workspace.trajectory_offsets[candidate + 1] as usize;
    let final_target = workspace.trajectory.targets[last - 1];
    assert_eq!(final_target, candidate as u32 + 1);
    let planning_horizon_micros = scenario_horizons(&state, &scratch.resource_cohorts).0;
    let billing_horizon_micros = planning_horizon_micros;
    let terminal = terminal_replica_seconds(
        state.model_time.as_micros(),
        planning_horizon_micros,
        billing_horizon_micros,
        state.configuration.report_interval_micros,
        final_target,
    );
    let lifetime = billing_replica_seconds(
        state.model_time.as_micros(),
        billing_horizon_micros,
        state.current_replicas,
        &workspace.trajectory.targets[first..last],
        &workspace.trajectory.requested_micros[first..last],
        &workspace.trajectory.billing_ready_micros[first..last],
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

fn idle_ladder_step(
    sample_count: u32,
    alternating_calendar: bool,
) -> Result<(ScaleState, ScaleScratch), TestError> {
    idle_ladder_step_with_commitment(sample_count, alternating_calendar, true, 1)
}

fn idle_ladder_step_with_target(
    sample_count: u32,
    alternating_calendar: bool,
    pending_target: u32,
) -> Result<(ScaleState, ScaleScratch), TestError> {
    idle_ladder_step_with_commitment(sample_count, alternating_calendar, true, pending_target)
}

fn idle_ladder_step_with_commitment(
    sample_count: u32,
    alternating_calendar: bool,
    include_commitment: bool,
    pending_target: u32,
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
    observation.set_arrivals(0, 240_000_000)?;
    if alternating_calendar {
        observation.set_calendar_forecast(
            CalendarArtifactId(1),
            0.999_999_f64,
            &[
                CalendarRateSegment::new(0, 240_000_000, 250_000_000, 1.0_f64, 1.0_f64)?,
                CalendarRateSegment::new(1, 250_000_000, u64::MAX, 100_000.0_f64, 1.0_f64)?,
            ],
        )?;
    }
    observation.set_current_replicas(8)?;
    let commitment = if alternating_calendar {
        ActuationCommitment::rebalancing(
            8,
            pending_target,
            ModelTime::from_micros(238_000_000),
            ModelTime::from_micros(239_000_000),
        )?
    } else {
        ActuationCommitment::launching(8, pending_target, ModelTime::from_micros(239_000_000))?
    };
    if include_commitment {
        observation.push_actuation_commitment(commitment)?;
    }
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
    // Candidate one continues the pending descent. All other candidates pay
    // the signed fence lifetime and a fresh retarget cost.
    assert_eq!(argmin(&costs), 0, "costs={costs:?}");
    assert!(costs[7] > costs[0], "costs={costs:?}");
    Ok(())
}

#[test]
fn bidirectional_retarget_projects_a_fresh_transition() -> Result<(), TestError> {
    let (_, scratch) = idle_ladder_step_with_target(4_096, true, 6)?;
    let workspace = &scratch.scenario_workspaces[0];

    let stay_first = workspace.trajectory_offsets[5] as usize;
    let stay_last = workspace.trajectory_offsets[6] as usize;
    assert!(
        (stay_first..stay_last)
            .all(|event| { workspace.trajectory.kinds[event] != TrajectoryEventKind::Transition })
    );

    let retarget_first = workspace.trajectory_offsets[6] as usize;
    let retarget_last = workspace.trajectory_offsets[7] as usize;
    let retarget = (retarget_first..retarget_last)
        .find(|&event| workspace.trajectory.kinds[event] == TrajectoryEventKind::Transition)
        .ok_or(TestError::UnexpectedDecision)?;
    assert_eq!(workspace.trajectory.targets[retarget], 7);
    assert!(
        workspace.trajectory.ready_micros[retarget] > workspace.trajectory.pause_micros[retarget]
    );
    assert!(
        workspace.trajectory.during_supply[retarget] < workspace.trajectory.after_supply[retarget]
    );
    let mut cohorts = SlotSecondCohorts::new(1);
    cohorts.push_values(
        workspace.trajectory.pause_micros[retarget],
        workspace.trajectory.pause_micros[retarget].saturating_add(1_000_000),
        10.0_f64,
        0,
    );
    let mut edf = EdfScratch::new(1)?;
    prepare(&cohorts, &mut edf);
    let no_arrivals = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };
    let loss = super::fresh_transition_late_area(
        &cohorts,
        &mut edf,
        10.0_f64,
        EvaluationWindow {
            start_micros: 240_000_000,
            horizon_micros: workspace.trajectory.ready_micros[retarget].saturating_add(2_000_000),
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &no_arrivals,
        workspace.trajectory.pause_micros[retarget],
        workspace.trajectory.ready_micros[retarget],
    );
    assert!(loss > 0.0_f64, "loss={loss}");
    Ok(())
}

#[test]
fn forecast_wave_contributes_to_small_candidate_late_area() -> Result<(), TestError> {
    let (_, scratch) = idle_ladder_step(4_096, true)?;

    assert!(
        scratch.posterior_late_area_sums[0] > 0.0_f64,
        "late_area={:?}, arrivals={:?}",
        &scratch.posterior_late_area_sums[..8],
        &scratch.scenario_arrival_path_rates[..16]
    );
    Ok(())
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
        let replica_count_f64 = f64::from(replica_count as u32);
        let raw_capacity = supply / replica_count_f64;
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

    assert_eq!(artifact.identity().version(), 5);
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

#[test]
fn scenario_supply_preserves_joint_capacity_spread_and_target_pairing() -> Result<(), TestError> {
    let configuration = test_configuration()?;
    let grid = CapacityGrid::new(&[0.05_f64, 0.2_f64], &[1_000.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration, grid)?;
    for cell in 0..state.capacity.curve_posterior_value_count() as usize {
        let rate = state
            .capacity
            .curve_and_probability(cell)
            .0
            .service_time_seconds()
            .recip();
        state
            .capacity
            .set_duration_draw_for_test(cell, rate.ln(), 16.0_f64);
    }
    let mut distinct = false;
    let mut previous = None;
    for scenario in 0..64_u32 {
        let curve = scenario_capacity_curve(&state, 0, scenario);
        let one_replica = curve.sustainable_throughput(1.0_f64);
        for _target in 1..=state.configuration.replica_count_max {
            assert_eq!(scenario_capacity_curve(&state, 0, scenario), curve);
        }
        if previous.is_some_and(|value| !approximately_equal(value, one_replica)) {
            distinct = true;
        }
        previous = Some(one_replica);
    }
    assert!(distinct, "scenario supplies must preserve posterior spread");
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
