use super::{
    PredictiveObservations, RootEvent, group_particles, replica_seconds, root_event,
    select_root_action,
};

#[test]
fn hold_action_ends_at_the_next_report() {
    assert_eq!(
        root_event(7_000_000, &[]),
        RootEvent::Report {
            at_micros: 7_000_000
        }
    );
}

#[test]
fn transition_action_ends_at_an_earlier_completion() {
    assert_eq!(
        root_event(20_000_000, &[9.0_f64, 12.5_f64]),
        RootEvent::ActionComplete {
            at_micros: 12_500_000
        }
    );
}

#[test]
fn transition_action_ends_at_an_earlier_report() {
    assert_eq!(
        root_event(7_000_000, &[9.0_f64, 12.5_f64]),
        RootEvent::Report {
            at_micros: 7_000_000
        }
    );
}

#[test]
fn observation_nodes_equal_complete_observable_histories() {
    let elapsed_micros = [20, 10, 10, 10];
    let arrivals = [7, 5, 5, 5];
    let completions = [6, 4, 4, 4];
    let backlog = [1, 1, 1, 1];
    let warm_replicas = [2, 2, 2, 2];
    let transition_complete = [1, 1, 1, 0];
    let observations = PredictiveObservations {
        elapsed_micros: &elapsed_micros,
        arrivals: &arrivals,
        completions: &completions,
        backlog: &backlog,
        warm_replicas: &warm_replicas,
        transition_complete: &transition_complete,
    };
    let mut order = [0; 4];
    let mut offsets = Vec::with_capacity(5);

    group_particles(&observations, &mut order, &mut offsets);

    assert_eq!(offsets, [0, 1, 3, 4]);
    assert_eq!(&order[1..3], &[1, 2]);
}

#[test]
fn action_selection_applies_the_chance_constraint_before_loss() {
    let pass_counts = [98.0_f64, 99.0_f64, 100.0_f64];
    let excess_delay_sums = [0.0_f64, 10.0_f64, 1.0_f64];
    let replica_seconds_sums = [1.0_f64, 3.0_f64, 2.0_f64];

    let selected = select_root_action(
        &pass_counts,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0,
        0.99,
    );

    assert_eq!(selected, 2);
}

#[test]
fn action_selection_minimizes_excess_delay_when_no_action_passes() {
    let pass_counts = [80.0_f64, 90.0_f64, 98.0_f64];
    let excess_delay_sums = [3.0_f64, 1.0_f64, 2.0_f64];
    let replica_seconds_sums = [1.0_f64, 3.0_f64, 2.0_f64];

    let selected = select_root_action(
        &pass_counts,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0,
        0.99,
    );

    assert_eq!(selected, 1);
}

#[test]
fn action_selection_resolves_equal_loss_to_the_smallest_target() {
    let pass_counts = [0.0_f64; 3];
    let excess_delay_sums = [1.0_f64; 3];
    let replica_seconds_sums = [1.0_f64; 3];

    let selected = select_root_action(
        &pass_counts,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0_f64,
        0.99_f64,
    );

    assert_eq!(selected, 0);
}

#[test]
fn action_selection_uses_replica_seconds_after_equal_loss() {
    let pass_counts = [0.0_f64; 3];
    let excess_delay_sums = [1.0_f64; 3];
    let replica_seconds_sums = [3.0_f64, 1.0_f64, 2.0_f64];

    let selected = select_root_action(
        &pass_counts,
        &excess_delay_sums,
        &replica_seconds_sums,
        100.0_f64,
        0.99_f64,
    );

    assert_eq!(selected, 1);
}

#[test]
fn replica_seconds_integrates_ready_membership_changes() {
    let targets = [2, 4];
    let ready_seconds = [3.0_f64, 7.0_f64];

    let area = replica_seconds(1.0_f64, 11.0_f64, 1, &targets, &ready_seconds);

    assert_eq!(area, 26.0_f64);
}
