use crate::random::sample_gamma;
use crate::{ConfigurationError, PriorArtifactIdentity, RandomStream};

// Jeffreys' multinomial prior assigns one half-unit to each partition.
// Its total strength is half the configured partition count.
const JEFFREYS_CONCENTRATION: f64 = 0.5_f64;
const UNIFORM_CONCENTRATION: f64 = 1.0_f64;
const PRIOR_CHECK_SAMPLE_COUNT: u32 = 4_096;
const PRIOR_CHECK_FAILURE_PROBABILITY: f64 = 1.0e-9_f64;
const PARTITION_PRIOR_ARTIFACT: PriorArtifactIdentity =
    PriorArtifactIdentity::new(0x5041_5254_4954, 1, 0x5041_5254_0000_0001);

/// Prior-predictive partition-share quantiles and sensitivity.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PartitionPriorPredictiveCheck {
    artifact: PriorArtifactIdentity,
    partition_count: u32,
    sample_count: u32,
    quantile_rank_error_max: f64,
    hottest_share_quantiles: [f64; 3],
    share_entropy_quantiles: [f64; 3],
    uniform_hottest_share_quantiles: [f64; 3],
    uniform_share_entropy_quantiles: [f64; 3],
}

impl PartitionPriorPredictiveCheck {
    /// Returns the versioned check identity and random stream.
    #[must_use]
    pub const fn artifact(self) -> PriorArtifactIdentity {
        self.artifact
    }

    /// Returns the checked partition count.
    #[must_use]
    pub const fn partition_count(self) -> u32 {
        self.partition_count
    }

    /// Returns the number of exact Dirichlet draws.
    #[must_use]
    pub const fn sample_count(self) -> u32 {
        self.sample_count
    }

    /// Returns the simultaneous empirical CDF error bound.
    #[must_use]
    pub const fn quantile_rank_error_max(self) -> f64 {
        self.quantile_rank_error_max
    }

    /// Returns the Jeffreys 5th, 50th, and 95th hottest-share percentiles.
    #[must_use]
    pub const fn hottest_share_quantiles(self) -> [f64; 3] {
        self.hottest_share_quantiles
    }

    /// Returns the Jeffreys 5th, 50th, and 95th entropy percentiles.
    #[must_use]
    pub const fn share_entropy_quantiles(self) -> [f64; 3] {
        self.share_entropy_quantiles
    }

    /// Returns the uniform-prior hottest-share sensitivity percentiles.
    #[must_use]
    pub const fn uniform_hottest_share_quantiles(self) -> [f64; 3] {
        self.uniform_hottest_share_quantiles
    }

    /// Returns the uniform-prior entropy sensitivity percentiles.
    #[must_use]
    pub const fn uniform_share_entropy_quantiles(self) -> [f64; 3] {
        self.uniform_share_entropy_quantiles
    }
}

/// Runs the recorded partition prior check for one supported partition count.
///
/// The check uses exact Gamma draws for each Dirichlet coordinate. The
/// uniform prior is a sensitivity result only.
///
/// # Errors
///
/// Returns an error when the partition count is zero or exceeds the platform.
pub fn partition_prior_predictive_check(
    partition_count: u32,
) -> Result<PartitionPriorPredictiveCheck, ConfigurationError> {
    if partition_count == 0 {
        return Err(ConfigurationError::ZeroBound {
            name: "partition_count",
        });
    }
    let jeffreys = sample_prior_quantiles(partition_count, JEFFREYS_CONCENTRATION, 0)?;
    let uniform = sample_prior_quantiles(partition_count, UNIFORM_CONCENTRATION, 1)?;
    let sample_count = f64::from(PRIOR_CHECK_SAMPLE_COUNT);
    let quantile_rank_error_max =
        (-(PRIOR_CHECK_FAILURE_PROBABILITY / 2.0_f64).ln() / (2.0_f64 * sample_count)).sqrt();
    Ok(PartitionPriorPredictiveCheck {
        artifact: PARTITION_PRIOR_ARTIFACT,
        partition_count,
        sample_count: PRIOR_CHECK_SAMPLE_COUNT,
        quantile_rank_error_max,
        hottest_share_quantiles: jeffreys.0,
        share_entropy_quantiles: jeffreys.1,
        uniform_hottest_share_quantiles: uniform.0,
        uniform_share_entropy_quantiles: uniform.1,
    })
}

pub(crate) struct PartitionFactor {
    count_sums: Vec<f64>,
    partition_count: u32,
}

impl PartitionFactor {
    pub(crate) fn new(partition_count: u32) -> Result<Self, ConfigurationError> {
        let partition_count_u32 = partition_count;
        let partition_count =
            usize::try_from(partition_count).map_err(|_| ConfigurationError::PlatformLimit)?;
        Ok(Self {
            count_sums: vec![0.0_f64; partition_count],
            partition_count: partition_count_u32,
        })
    }

    pub(crate) fn update(&mut self, counts: &[u32]) {
        assert_eq!(
            counts.len(),
            self.count_sums.len(),
            "partition evidence must match the configured partition count"
        );
        for (partition, &count) in counts.iter().enumerate() {
            self.count_sums[partition] += f64::from(count);
        }
    }

    pub(crate) fn maximum_expected_share(&self) -> f64 {
        let prior = JEFFREYS_CONCENTRATION;
        let prior_total = prior * f64::from(self.partition_count);
        let total = self.count_sums.iter().copied().sum::<f64>();
        let maximum = self.count_sums.iter().copied().fold(0.0_f64, f64::max);
        (prior + maximum) / (prior_total + total)
    }

    /// Draws one joint share vector in random partition order.
    ///
    /// Each output is the total share in the first `n` moved partitions.
    pub(crate) fn sample_moved_prefix(
        &self,
        random: &mut RandomStream,
        partition_order: &mut [u32],
        share_draws: &mut [f64],
        moved_prefix: &mut [f64],
    ) {
        assert_eq!(
            partition_order.len(),
            self.count_sums.len(),
            "the order must contain each partition"
        );
        assert_eq!(
            share_draws.len(),
            self.count_sums.len(),
            "each partition must have one share draw"
        );
        assert_eq!(
            moved_prefix.len(),
            self.count_sums.len() + 1,
            "the prefix must include the empty subset"
        );
        for (partition, slot) in partition_order.iter_mut().enumerate() {
            *slot = partition as u32;
        }
        for end in (1..partition_order.len()).rev() {
            let bound = end as u32 + 1;
            partition_order.swap(end, random.index_below(bound) as usize);
        }

        let prior = JEFFREYS_CONCENTRATION;
        let total = loop {
            let mut total = 0.0_f64;
            for (draw, count) in share_draws.iter_mut().zip(&self.count_sums) {
                *draw = sample_gamma(prior + count, random);
                total += *draw;
            }
            if total > 0.0_f64 && total.is_finite() {
                break total;
            }
        };
        moved_prefix[0] = 0.0_f64;
        for (rank, &partition) in partition_order.iter().enumerate() {
            let share = share_draws[partition as usize] / total;
            moved_prefix[rank + 1] = (moved_prefix[rank] + share).min(1.0_f64);
        }
        moved_prefix[self.count_sums.len()] = 1.0_f64;
    }

    pub(crate) const fn value_count(&self) -> u32 {
        self.partition_count
    }

    pub(crate) fn write_expected_shares(&self, probabilities: &mut [f64]) -> bool {
        if probabilities.len() != self.count_sums.len() {
            return false;
        }
        let prior = JEFFREYS_CONCENTRATION;
        let total = prior * f64::from(self.partition_count) + self.count_sums.iter().sum::<f64>();
        for (probability, count) in probabilities.iter_mut().zip(&self.count_sums) {
            *probability = (prior + count) / total;
        }
        true
    }
}

fn sample_prior_quantiles(
    partition_count: u32,
    concentration: f64,
    domain: u64,
) -> Result<([f64; 3], [f64; 3]), ConfigurationError> {
    let partition_count =
        usize::try_from(partition_count).map_err(|_| ConfigurationError::PlatformLimit)?;
    let sample_count = PRIOR_CHECK_SAMPLE_COUNT as usize;
    let mut draws = vec![0.0_f64; partition_count];
    let mut hottest = Vec::with_capacity(sample_count);
    let mut entropy = Vec::with_capacity(sample_count);
    let mut random = RandomStream::new(PARTITION_PRIOR_ARTIFACT.random_stream()).domain(domain);
    for _ in 0..sample_count {
        let total = loop {
            let mut total = 0.0_f64;
            for draw in &mut draws {
                *draw = sample_gamma(concentration, &mut random);
                total += *draw;
            }
            if total > 0.0_f64 && total.is_finite() {
                break total;
            }
        };
        let mut maximum = 0.0_f64;
        let mut sample_entropy = 0.0_f64;
        for draw in &draws {
            let share = *draw / total;
            maximum = maximum.max(share);
            if share > 0.0_f64 {
                sample_entropy -= share * share.ln();
            }
        }
        hottest.push(maximum);
        entropy.push(sample_entropy);
    }
    hottest.sort_unstable_by(f64::total_cmp);
    entropy.sort_unstable_by(f64::total_cmp);
    Ok((selected_quantiles(&hottest), selected_quantiles(&entropy)))
}

fn selected_quantiles(sorted: &[f64]) -> [f64; 3] {
    let last = sorted.len() - 1;
    [sorted[last / 20], sorted[last / 2], sorted[last * 19 / 20]]
}
