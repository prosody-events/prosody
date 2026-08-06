use std::time::Duration;

use crate::ArrivalPosterior;

const ALPHA_PRIOR: f64 = 1.0_f64;
const BETA_SECONDS_PRIOR: f64 = 1.0_f64;
const WINDOW_COUNT: usize = 64;

/// One consumable count and exposure update.
#[derive(Debug, Eq, PartialEq)]
pub struct ArrivalEvidence {
    count: u32,
    exposure_micros: u64,
    token: EvidenceToken,
}

impl ArrivalEvidence {
    pub(crate) const fn new(count: u32, exposure_micros: u64) -> Self {
        Self {
            count,
            exposure_micros,
            token: EvidenceToken,
        }
    }
}

pub(crate) struct ArrivalFactor {
    counts: [u32; WINDOW_COUNT],
    exposures_seconds: [f64; WINDOW_COUNT],
    count_sum: f64,
    exposure_seconds_sum: f64,
    cursor: usize,
    length: usize,
}

impl ArrivalFactor {
    pub(crate) const fn new() -> Self {
        Self {
            counts: [0; WINDOW_COUNT],
            exposures_seconds: [0.0_f64; WINDOW_COUNT],
            count_sum: 0.0_f64,
            exposure_seconds_sum: 0.0_f64,
            cursor: 0,
            length: 0,
        }
    }

    pub(crate) fn update(&mut self, evidence: ArrivalEvidence) {
        let ArrivalEvidence {
            count,
            exposure_micros,
            token,
        } = evidence;
        let exposure_seconds = Duration::from_micros(exposure_micros).as_secs_f64();
        if self.length == WINDOW_COUNT {
            self.count_sum -= f64::from(self.counts[self.cursor]);
            self.exposure_seconds_sum -= self.exposures_seconds[self.cursor];
        } else {
            self.length += 1;
        }
        self.counts[self.cursor] = count;
        self.exposures_seconds[self.cursor] = exposure_seconds;
        self.count_sum += f64::from(count);
        self.exposure_seconds_sum += exposure_seconds;
        self.cursor = (self.cursor + 1) % WINDOW_COUNT;
        drop(token);
    }

    pub(crate) fn expected_rate(&self) -> f64 {
        self.shape() / self.rate()
    }

    pub(crate) fn posterior(&self) -> ArrivalPosterior {
        ArrivalPosterior {
            shape: self.shape(),
            rate: self.rate(),
        }
    }

    #[cfg(test)]
    pub(crate) fn predictive_probability(&self, count: u32, exposure_seconds: f64) -> f64 {
        let probability = self.rate() / (self.rate() + exposure_seconds);
        let mut mass = probability.powf(self.shape());
        for value in 0..count {
            mass *= (f64::from(value) + self.shape()) / f64::from(value + 1);
            mass *= 1.0_f64 - probability;
        }
        mass
    }

    fn shape(&self) -> f64 {
        ALPHA_PRIOR + self.count_sum
    }

    fn rate(&self) -> f64 {
        BETA_SECONDS_PRIOR + self.exposure_seconds_sum
    }
}

#[derive(Debug, Eq, PartialEq)]
struct EvidenceToken;

impl Drop for EvidenceToken {
    fn drop(&mut self) {}
}
