use quickcheck_macros::quickcheck;

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
        missed_work_sums: &[0.0_f64; 3],
        event_count: 1.0_f64,
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
        missed_work_sums: &[0.0_f64; 3],
        event_count: 1.0_f64,
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
    let missed_work_sums = vec![0.0_f64; count];
    let columns = ActionColumns {
        late_area_sums: &late_area_sums,
        replica_seconds_sums: &replica_seconds_sums,
        missed_work_sums: &missed_work_sums,
        event_count: 1.0_f64,
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
        missed_work_sums: &[0.0_f64, 2.0_f64],
        event_count: 2.0_f64,
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
        missed_work_sums: &[first, second],
        event_count: 1.0_f64,
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
    let pod_lifetime_micros = [3_000_000_u64, 7_000_000_u64];

    let area = billing_replica_seconds(1_000_000, 11_000_000, 1, &targets, &pod_lifetime_micros);

    assert!(area.total_cmp(&26.0_f64).is_eq(), "area={area}");
}

#[test]
fn terminal_membership_reaches_the_first_report_boundary() {
    // The 2 s planning horizon caps the 8 s drain. The next 3 s report
    // boundary is 6 s. Two replicas therefore cost 2 * 4 = 8.
    assert!(
        terminal_replica_seconds(0, 2_000_000, 8.0_f64, 3_000_000, 2)
            .total_cmp(&8.0_f64)
            .is_eq()
    );
    // No terminal work needs no successor continuation.
    assert!(
        terminal_replica_seconds(0, 2_000_000, 0.0_f64, 3_000_000, 2)
            .total_cmp(&0.0_f64)
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
