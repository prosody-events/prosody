use quickcheck_macros::quickcheck;

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
fn action_selection_uses_the_lowest_total_cost() {
    let columns = ActionColumns {
        late_area_sums: &[12.0_f64, 1.0_f64, 4.0_f64],
        replica_seconds_sums: &[1.0_f64, 5.0_f64, 2.0_f64],
        rate: 3.0_f64,
        demand_floor: 0,
    };

    assert_eq!(select_action(&columns), 2);
}

#[test]
fn action_selection_uses_the_smallest_index_for_an_exact_cost_tie() {
    let columns = ActionColumns {
        late_area_sums: &[3.0_f64, 0.0_f64, 6.0_f64],
        replica_seconds_sums: &[1.0_f64, 2.0_f64, 0.0_f64],
        rate: 3.0_f64,
        demand_floor: 0,
    };

    assert_eq!(select_action(&columns), 0);
}

#[test]
fn the_demand_floor_excludes_actions_a_repair_overrides() {
    let columns = ActionColumns {
        late_area_sums: &[0.0_f64, 10.0_f64, 20.0_f64],
        replica_seconds_sums: &[0.0_f64; 3],
        rate: 3.0_f64,
        demand_floor: 1,
    };

    assert_eq!(select_action(&columns), 1);
}

#[quickcheck]
fn action_selection_is_the_cost_argmin_above_the_floor(
    late_codes: Vec<u16>,
    replica_codes: Vec<u16>,
    floor_code: u8,
    rate_code: u8,
) -> bool {
    let count = late_codes.len().min(replica_codes.len()).max(1);
    let mut late_area_sums = vec![0.0_f64; count];
    let mut replica_seconds_sums = vec![0.0_f64; count];
    for index in 0..late_codes.len().min(replica_codes.len()) {
        late_area_sums[index] = f64::from(late_codes[index]);
        replica_seconds_sums[index] = f64::from(replica_codes[index]);
    }
    let demand_floor = usize::from(floor_code) % count;
    let rate = f64::from(rate_code) + 1.0_f64;
    let columns = ActionColumns {
        late_area_sums: &late_area_sums,
        replica_seconds_sums: &replica_seconds_sums,
        rate,
        demand_floor,
    };
    let expected = (demand_floor..count)
        .min_by(|left, right| {
            columns
                .cost(*left)
                .total_cmp(&columns.cost(*right))
                .then_with(|| left.cmp(right))
        })
        .unwrap_or(demand_floor);

    select_action(&columns) == expected
}

#[test]
fn replica_seconds_integrates_physical_membership_changes() {
    let targets = [2, 4];
    let membership_seconds = [3.0_f64, 7.0_f64];

    let area = replica_seconds(1.0_f64, 11.0_f64, 1, &targets, &membership_seconds);

    assert!(area.total_cmp(&26.0_f64).is_eq(), "area={area}");
}
