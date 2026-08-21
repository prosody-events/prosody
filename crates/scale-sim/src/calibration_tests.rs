use quickcheck_macros::quickcheck;

use super::{CapacityCalibration, RANK_BIN_COUNT, rank_bin};
use crate::{W6AblationArm, w6_witness::W6_ABLATION_WITNESSES};

#[test]
fn capacity_calibration_retains_all_w6_ablation_witnesses() {
    let calibration = CapacityCalibration {
        trials: Vec::new(),
        w6_ablation_witnesses: W6_ABLATION_WITNESSES,
    };
    let witnesses = calibration.w6_ablation_witnesses();

    assert_eq!(
        witnesses.map(|witness| witness.arm),
        [
            W6AblationArm::LandedCompletion,
            W6AblationArm::DeletedProduct,
            W6AblationArm::CompletionMarginal,
            W6AblationArm::ProperJoint,
            W6AblationArm::DirectOracle,
        ]
    );
    assert!(witnesses.iter().all(|witness| {
        (witness.completion_log_score + witness.conditional_path_log_score
            - witness.joint_log_score)
            .abs()
            <= 64.0_f64 * f64::EPSILON
            && witness.window_count == 180
    }));
    assert_eq!(
        witnesses[3].joint_log_score.to_bits(),
        witnesses[4].joint_log_score.to_bits()
    );
}

#[quickcheck]
fn predictive_rank_maps_to_one_decile(raw_rank: u64) -> bool {
    let rank = unit_interval(raw_rank);
    let bin = rank_bin(rank);
    bin < RANK_BIN_COUNT && rank >= lower_bound(bin) && rank <= upper_bound(bin)
}

fn unit_interval(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).unwrap_or(0);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(0);
    let numerator = f64::from(high) * 4_294_967_296.0_f64 + f64::from(low);
    numerator / 18_446_744_073_709_551_615.0_f64
}

fn lower_bound(bin: usize) -> f64 {
    u32::try_from(bin).map_or(0.0_f64, f64::from) / 10.0_f64
}

fn upper_bound(bin: usize) -> f64 {
    u32::try_from(bin.saturating_add(1)).map_or(1.0_f64, f64::from) / 10.0_f64
}
