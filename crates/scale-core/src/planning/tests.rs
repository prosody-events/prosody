use quickcheck_macros::quickcheck;
use std::time::Duration;

use super::{
    ActionColumns, billing_replica_seconds, complete_horizon_micros,
    next_report_boundary_at_or_after, select_action, terminal_replica_seconds,
};

#[test]
fn complete_horizon_covers_the_latest_known_boundary_and_one_budget() {
    assert_eq!(
        complete_horizon_micros(10, 30, 20, 5),
        35,
        "the latest action completion must define the common horizon"
    );
    assert_eq!(
        complete_horizon_micros(40, 30, 20, 5),
        45,
        "the next report must define the common horizon when it is latest"
    );
    assert_eq!(
        complete_horizon_micros(10, 30, 50, 5),
        55,
        "the latest known deadline must define the common horizon"
    );
}
#[test]
fn action_selection_uses_the_lowest_total_cost() {
    let columns = ActionColumns {
        late_area_sums: &[12.0_f64, 1.0_f64, 4.0_f64],
        replica_seconds_sums: &[1.0_f64, 5.0_f64, 2.0_f64],
        miss_fraction_sums: &[0.0_f64; 3],
        epsilon: 0.5_f64,
        rate: 3.0_f64,
    };

    assert_eq!(select_action(&columns).index, 2);
}

#[test]
fn action_selection_uses_the_smallest_index_for_an_exact_cost_tie() {
    let columns = ActionColumns {
        late_area_sums: &[3.0_f64, 0.0_f64, 6.0_f64],
        replica_seconds_sums: &[1.0_f64, 2.0_f64, 0.0_f64],
        miss_fraction_sums: &[0.0_f64; 3],
        epsilon: 0.5_f64,
        rate: 3.0_f64,
    };

    assert_eq!(select_action(&columns).index, 0);
}

#[quickcheck]
fn action_selection_is_the_cost_argmin(
    late_codes: Vec<u16>,
    replica_codes: Vec<u16>,
    rate_code: u8,
) -> bool {
    let count = late_codes.len().min(replica_codes.len()).max(1);
    let mut late_area_sums = vec![0.0_f64; count];
    let mut replica_seconds_sums = vec![0.0_f64; count];
    for (index, (late, replicas)) in late_codes.into_iter().zip(replica_codes).enumerate() {
        late_area_sums[index] = f64::from(late);
        replica_seconds_sums[index] = f64::from(replicas);
    }
    let rate = f64::from(rate_code) + 1.0_f64;
    let miss_fraction_sums = vec![0.0_f64; count];
    let columns = ActionColumns {
        late_area_sums: &late_area_sums,
        replica_seconds_sums: &replica_seconds_sums,
        miss_fraction_sums: &miss_fraction_sums,
        epsilon: 0.5_f64,
        rate,
    };
    let expected = (0..count)
        .min_by(|left, right| {
            columns
                .cost(*left)
                .total_cmp(&columns.cost(*right))
                .then_with(|| left.cmp(right))
        })
        .unwrap_or(0);

    select_action(&columns).index == expected
}

#[quickcheck]
fn feasible_action_is_never_beaten_by_an_infeasible_action(
    feasible_cost: u16,
    infeasible_cost: u16,
) -> bool {
    let late_area_sums = [f64::from(feasible_cost), f64::from(infeasible_cost)];
    let columns = ActionColumns {
        late_area_sums: &late_area_sums,
        replica_seconds_sums: &[0.0_f64; 2],
        miss_fraction_sums: &[0.0_f64, 1.0_f64],
        epsilon: 0.5_f64,
        rate: 1.0_f64,
    };

    let selection = select_action(&columns);
    selection.index == 0 && !selection.used_fallback
}

#[quickcheck]
fn empty_feasible_set_selects_the_smallest_miss_fraction(
    first_excess: u16,
    second_excess: u16,
) -> bool {
    let first = f64::from(first_excess) + 2.0_f64;
    let second = f64::from(second_excess) + 2.0_f64;
    let columns = ActionColumns {
        late_area_sums: &[0.0_f64; 2],
        replica_seconds_sums: &[0.0_f64; 2],
        miss_fraction_sums: &[first, second],
        epsilon: 0.5_f64,
        rate: 1.0_f64,
    };
    let expected = usize::from(second < first);
    let selection = select_action(&columns);

    selection.index == expected && selection.used_fallback
}

#[test]
fn billing_replica_seconds_integrates_pod_lifetime_changes() {
    let targets = [2, 4];
    let requested_micros = [3_000_000_u64, 7_000_000_u64];
    let ready_micros = [5_000_000_u64, 9_000_000_u64];

    let area = billing_replica_seconds(
        1_000_000,
        11_000_000,
        1,
        &targets,
        &requested_micros,
        &ready_micros,
    );

    assert!(area.total_cmp(&26.0_f64).is_eq(), "area={area}");
}

#[quickcheck]
fn one_descent_bills_origin_until_ready_then_target(
    origin_code: u8,
    target_code: u8,
    request_code: u16,
    pause_code: u16,
    ready_code: u16,
) -> bool {
    let origin = u32::from(origin_code % 31) + 2;
    let target = u32::from(target_code) % (origin - 1) + 1;
    let requested = u64::from(request_code) * 1_000;
    let pause = requested + u64::from(pause_code) * 1_000;
    let ready = pause + u64::from(ready_code) * 1_000;
    let end = ready + 1_000_000;
    let billed = billing_replica_seconds(0, end, origin, &[target], &[requested], &[ready]);
    let expected = f64::from(origin) * Duration::from_micros(ready).as_secs_f64()
        + f64::from(target) * Duration::from_micros(end - ready).as_secs_f64();
    let held = f64::from(origin) * Duration::from_micros(end).as_secs_f64();

    (billed - expected).abs() <= f64::EPSILON * expected.abs().max(1.0_f64) && billed < held
}

#[quickcheck]
fn one_ascent_bills_joining_replicas_from_request(
    origin_code: u8,
    delta_code: u8,
    request_code: u16,
    pause_code: u16,
    ready_code: u16,
) -> bool {
    let origin = u32::from(origin_code % 31) + 1;
    let target = origin + u32::from(delta_code % 31) + 1;
    let requested = u64::from(request_code) * 1_000;
    let pause = requested + u64::from(pause_code) * 1_000;
    let ready = pause + u64::from(ready_code) * 1_000;
    let end = ready + 1_000_000;
    let billed = billing_replica_seconds(0, end, origin, &[target], &[requested], &[ready]);
    let held = f64::from(origin) * Duration::from_micros(end).as_secs_f64();
    let expected =
        held + f64::from(target - origin) * Duration::from_micros(end - requested).as_secs_f64();

    (billed - expected).abs() <= f64::EPSILON * expected.abs().max(1.0_f64) && billed >= held
}

#[quickcheck]
fn every_action_pays_its_reached_state_for_one_terminal_budget(
    origin_code: u8,
    target_codes: Vec<u8>,
    ready_code: u16,
    budget_code: u16,
) -> bool {
    let origin = u32::from(origin_code % 31) + 2;
    let ready = u64::from(ready_code) * 1_000;
    let budget = (u64::from(budget_code) + 1) * 1_000;
    let horizon = ready + 1_000_000;
    let hold_lifetime = billing_replica_seconds(0, horizon, origin, &[], &[], &[]);
    let terminal_unit = terminal_replica_seconds(0, horizon, horizon, budget, 1);
    let hold = hold_lifetime + terminal_replica_seconds(0, horizon, horizon, budget, origin);
    let expected_hold = hold_lifetime + f64::from(origin) * terminal_unit;
    if (hold - expected_hold).abs() > f64::EPSILON * expected_hold.abs().max(1.0_f64) {
        return false;
    }

    target_codes.into_iter().all(|target_code| {
        let target = u32::from(target_code) % origin + 1;
        let lifetime = billing_replica_seconds(0, horizon, origin, &[target], &[0], &[ready]);
        let total = lifetime + terminal_replica_seconds(0, horizon, horizon, budget, target);
        let expected = lifetime + f64::from(target) * terminal_unit;
        let exact = (total - expected).abs() <= f64::EPSILON * expected.abs().max(1.0_f64);
        exact && (target >= origin || total < hold)
    })
}

#[test]
fn terminal_membership_reaches_the_first_report_boundary() {
    // The first 3 s report boundary follows the 2 s horizon. Two replicas
    // therefore cost 2 * 1 = 2.
    assert!(
        terminal_replica_seconds(0, 2_000_000, 2_000_000, 3_000_000, 2)
            .total_cmp(&2.0_f64)
            .is_eq()
    );
}

#[test]
fn report_boundary_uses_the_named_epoch() {
    assert!(
        next_report_boundary_at_or_after(5.0_f64, 3.0_f64, 9.0_f64)
            .total_cmp(&11.0_f64)
            .is_eq()
    );
}
