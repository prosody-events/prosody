use std::time::Duration;

/// A continuous-time persistence-or-redraw transition kernel.
///
/// The retained probability is `exp(-rate * elapsed)`. This definition makes
/// successive transitions equal one transition over their total elapsed time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ChangePointKernel {
    rate_per_second: f64,
}

impl ChangePointKernel {
    pub(crate) const fn new(rate_per_second: f64) -> Self {
        Self { rate_per_second }
    }

    pub(crate) fn probabilities(self, elapsed: Duration) -> TransitionProbabilities {
        let elapsed_seconds = elapsed.as_secs_f64();
        let retained = (-self.rate_per_second * elapsed_seconds).exp();
        TransitionProbabilities {
            retained,
            redrawn: 1.0_f64 - retained,
        }
    }

    pub(crate) const fn rate_per_second(self) -> f64 {
        self.rate_per_second
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TransitionProbabilities {
    pub(crate) retained: f64,
    pub(crate) redrawn: f64,
}
