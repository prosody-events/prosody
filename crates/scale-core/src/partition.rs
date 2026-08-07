use crate::RandomStream;
use crate::random::sample_gamma;

const PRIOR_CONCENTRATION: f64 = 1.0_f64;

pub(crate) struct PartitionFactor {
    count_sums: Vec<f64>,
    partition_count: u32,
}

impl PartitionFactor {
    pub(crate) fn new(partition_count: u32) -> Result<Self, crate::ConfigurationError> {
        let partition_count_u32 = partition_count;
        let partition_count = usize::try_from(partition_count)
            .map_err(|_| crate::ConfigurationError::PlatformLimit)?;
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

    #[cfg(test)]
    pub(crate) fn expected_share(&self, partition: u32) -> f64 {
        let partition = partition as usize;
        assert!(
            partition < self.count_sums.len(),
            "the partition must be inside the configured range"
        );
        let prior = PRIOR_CONCENTRATION / f64::from(self.partition_count);
        let total = self.count_sums.iter().copied().sum::<f64>();
        (prior + self.count_sums[partition]) / (PRIOR_CONCENTRATION + total)
    }

    pub(crate) fn maximum_expected_share(&self) -> f64 {
        let prior = PRIOR_CONCENTRATION / f64::from(self.partition_count);
        let total = self.count_sums.iter().copied().sum::<f64>();
        let maximum = self.count_sums.iter().copied().fold(0.0_f64, f64::max);
        (prior + maximum) / (PRIOR_CONCENTRATION + total)
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

        let prior = PRIOR_CONCENTRATION / f64::from(self.partition_count);
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
        let prior = PRIOR_CONCENTRATION / f64::from(self.partition_count);
        let total = PRIOR_CONCENTRATION + self.count_sums.iter().sum::<f64>();
        for (probability, count) in probabilities.iter_mut().zip(&self.count_sums) {
            *probability = (prior + count) / total;
        }
        true
    }
}
