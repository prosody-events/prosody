use std::cmp::Ordering;

use super::{ActionColumns, complete_horizon_micros, replica_seconds, select_action};

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
fn action_selection_applies_the_chance_constraint_before_loss() {
    let columns = ActionColumns {
        violation_weight_sums: &[10.0_f64, 6.0_f64, 0.0_f64],
        excess_delay_sums: &[0.0_f64, 10.0_f64, 1.0_f64],
        replica_seconds_sums: &[1.0_f64, 3.0_f64, 2.0_f64],
        demand_floor: 0,
        scenario_weight_sum: 100.0_f64,
        slo_violation_probability: 0.05_f64,
    };

    assert_eq!(select_action(&columns), 2);
}

#[test]
fn action_selection_uses_the_best_attainable_violation_rate() {
    let columns = ActionColumns {
        violation_weight_sums: &[20.0_f64, 10.5_f64, 10.0_f64],
        excess_delay_sums: &[0.0_f64, 0.0_f64, 10.0_f64],
        replica_seconds_sums: &[1.0_f64, 2.0_f64, 3.0_f64],
        demand_floor: 0,
        scenario_weight_sum: 100.0_f64,
        slo_violation_probability: 0.01_f64,
    };

    assert_eq!(select_action(&columns), 2);
}

#[test]
fn action_selection_resolves_equal_loss_to_the_smallest_target() {
    let columns = ActionColumns {
        violation_weight_sums: &[2.0_f64; 3],
        excess_delay_sums: &[1.0_f64; 3],
        replica_seconds_sums: &[1.0_f64; 3],
        demand_floor: 0,
        scenario_weight_sum: 100.0_f64,
        slo_violation_probability: 0.01_f64,
    };

    assert_eq!(select_action(&columns), 0);
}

#[test]
fn action_selection_uses_replica_seconds_after_equal_loss() {
    let columns = ActionColumns {
        violation_weight_sums: &[2.0_f64; 3],
        excess_delay_sums: &[1.0_f64; 3],
        replica_seconds_sums: &[3.0_f64, 1.0_f64, 2.0_f64],
        demand_floor: 0,
        scenario_weight_sum: 100.0_f64,
        slo_violation_probability: 0.01_f64,
    };

    assert_eq!(select_action(&columns), 1);
}

#[test]
fn the_demand_floor_excludes_actions_a_repair_overrides() {
    let columns = ActionColumns {
        violation_weight_sums: &[0.0_f64; 3],
        excess_delay_sums: &[0.0_f64; 3],
        replica_seconds_sums: &[1.0_f64, 2.0_f64, 3.0_f64],
        demand_floor: 1,
        scenario_weight_sum: 100.0_f64,
        slo_violation_probability: 0.01_f64,
    };

    // Every action misses nothing, but the first action cannot serve the
    // known arrival rate. The floor keeps replica-seconds from selecting
    // an action the repair policy would immediately override.
    assert_eq!(select_action(&columns), 1);
}

#[test]
fn infeasible_actions_order_by_excess_delay() {
    let columns = ActionColumns {
        violation_weight_sums: &[20.0_f64, 10.0_f64],
        excess_delay_sums: &[3.0_f64, 1.0_f64],
        replica_seconds_sums: &[1.0_f64, 3.0_f64],
        demand_floor: 0,
        scenario_weight_sum: 100.0_f64,
        slo_violation_probability: 0.01_f64,
    };

    // Both actions exceed a zero allowance, so expected excess delay
    // orders them.
    assert_eq!(
        super::compare_actions(0, 1, &columns, 0.0_f64),
        Ordering::Greater
    );
}

#[test]
fn replica_seconds_integrates_physical_membership_changes() {
    let targets = [2, 4];
    let membership_seconds = [3.0_f64, 7.0_f64];

    let area = replica_seconds(1.0_f64, 11.0_f64, 1, &targets, &membership_seconds);

    assert!(area.total_cmp(&26.0_f64).is_eq(), "area={area}");
}
