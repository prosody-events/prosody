use super::{complete_horizon_micros, replica_seconds, select_action};

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
    let missed_work_sums = [2.0_f64, 1.0_f64, 0.0_f64];
    let excess_delay_sums = [0.0_f64, 10.0_f64, 1.0_f64];
    let replica_seconds_sums = [1.0_f64, 3.0_f64, 2.0_f64];

    let selected = select_action(
        &missed_work_sums,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0,
        0.01,
    );

    assert_eq!(selected, 2);
}

#[test]
fn action_selection_minimizes_excess_delay_when_no_action_passes() {
    let missed_work_sums = [20.0_f64, 10.0_f64, 2.0_f64];
    let excess_delay_sums = [3.0_f64, 1.0_f64, 2.0_f64];
    let replica_seconds_sums = [1.0_f64, 3.0_f64, 2.0_f64];

    let selected = select_action(
        &missed_work_sums,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0,
        0.01,
    );

    assert_eq!(selected, 1);
}

#[test]
fn action_selection_resolves_equal_loss_to_the_smallest_target() {
    let missed_work_sums = [2.0_f64; 3];
    let excess_delay_sums = [1.0_f64; 3];
    let replica_seconds_sums = [1.0_f64; 3];

    let selected = select_action(
        &missed_work_sums,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0_f64,
        0.01_f64,
    );

    assert_eq!(selected, 0);
}

#[test]
fn action_selection_uses_replica_seconds_after_equal_loss() {
    let missed_work_sums = [2.0_f64; 3];
    let excess_delay_sums = [1.0_f64; 3];
    let replica_seconds_sums = [3.0_f64, 1.0_f64, 2.0_f64];

    let selected = select_action(
        &missed_work_sums,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0_f64,
        0.01_f64,
    );

    assert_eq!(selected, 1);
}

#[test]
fn replica_seconds_integrates_physical_membership_changes() {
    let targets = [2, 4];
    let membership_seconds = [3.0_f64, 7.0_f64];

    let area = replica_seconds(1.0_f64, 11.0_f64, 1, &targets, &membership_seconds);

    assert_eq!(area, 26.0_f64);
}
