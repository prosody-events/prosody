/// Capacity likelihood arm in the fixed W6 calibration witness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum W6AblationArm {
    /// Completion-only likelihood that uses observed concurrency.
    LandedCompletion,
    /// Deleted product that counts occupancy evidence twice.
    DeletedProduct,
    /// Declared completion marginal from the event-path model.
    CompletionMarginal,
    /// Normalized event-path joint likelihood.
    ProperJoint,
    /// Direct finite-grid oracle likelihood.
    DirectOracle,
}

/// Recorded result for one arm of the 180-window W6 witness.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct W6AblationWitness {
    /// Likelihood arm that produced this result.
    pub arm: W6AblationArm,
    /// Sum of next-window completion log scores in nats.
    pub completion_log_score: f64,
    /// Sum of conditional occupancy-path log scores in nats.
    pub conditional_path_log_score: f64,
    /// Sum of joint log scores in nats.
    pub joint_log_score: f64,
    /// Sum of posterior entropy values in nats.
    pub posterior_entropy: f64,
    /// Sum of generating-cell posterior ranks.
    pub generating_cell_rank: f64,
    /// Windows whose central credible set covered the generating cell.
    pub covered_window_count: u32,
    /// Number of alternating calibration windows.
    pub window_count: u32,
}

pub(crate) const W6_ABLATION_WITNESSES: [W6AblationWitness; 5] = [
    W6AblationWitness {
        arm: W6AblationArm::LandedCompletion,
        completion_log_score: -141.754_999_199_953_94_f64,
        conditional_path_log_score: 18.787_305_815_803_236_f64,
        joint_log_score: -122.967_693_384_150_7_f64,
        posterior_entropy: 2.967_650_869_862_197_f64,
        generating_cell_rank: 90.0_f64,
        covered_window_count: 93,
        window_count: 180,
    },
    W6AblationWitness {
        arm: W6AblationArm::DeletedProduct,
        completion_log_score: -103.391_630_869_568_59_f64,
        conditional_path_log_score: -246.801_209_019_004_03_f64,
        joint_log_score: -350.192_839_888_572_6_f64,
        posterior_entropy: 11.452_025_462_153_431_f64,
        generating_cell_rank: 87.0_f64,
        covered_window_count: 102,
        window_count: 180,
    },
    W6AblationWitness {
        arm: W6AblationArm::CompletionMarginal,
        completion_log_score: -102.201_043_261_686_64_f64,
        conditional_path_log_score: 0.0_f64,
        joint_log_score: -102.201_043_261_686_64_f64,
        posterior_entropy: 11.809_814_502_501_418_f64,
        generating_cell_rank: 89.0_f64,
        covered_window_count: 100,
        window_count: 180,
    },
    W6AblationWitness {
        arm: W6AblationArm::ProperJoint,
        completion_log_score: -104.744_055_951_358_53_f64,
        conditional_path_log_score: 25.078_052_770_798_55_f64,
        joint_log_score: -79.666_003_180_559_98_f64,
        posterior_entropy: 15.849_754_641_122_818_f64,
        generating_cell_rank: 88.0_f64,
        covered_window_count: 105,
        window_count: 180,
    },
    W6AblationWitness {
        arm: W6AblationArm::DirectOracle,
        completion_log_score: -104.744_055_951_358_53_f64,
        conditional_path_log_score: 25.078_052_770_798_55_f64,
        joint_log_score: -79.666_003_180_559_98_f64,
        posterior_entropy: 15.849_754_641_122_818_f64,
        generating_cell_rank: 88.0_f64,
        covered_window_count: 105,
        window_count: 180,
    },
];
