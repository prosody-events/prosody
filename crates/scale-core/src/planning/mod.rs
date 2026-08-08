#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootEvent {
    Report { at_micros: u64 },
    ActionComplete { at_micros: u64 },
}

impl RootEvent {
    pub(crate) const fn at_micros(self) -> u64 {
        match self {
            Self::Report { at_micros } | Self::ActionComplete { at_micros } => at_micros,
        }
    }
}

/// Columnar observations available to one recourse action.
pub(crate) struct PredictiveObservations<'a> {
    pub(crate) elapsed_micros: &'a [u64],
    pub(crate) arrivals: &'a [u32],
    pub(crate) completions: &'a [u32],
    pub(crate) backlog: &'a [u32],
    pub(crate) warm_replicas: &'a [u32],
    pub(crate) transition_complete: &'a [u8],
}

impl PredictiveObservations<'_> {
    fn key(&self, particle: usize) -> (u64, u32, u32, u32, u32, u8) {
        (
            self.elapsed_micros[particle],
            self.arrivals[particle],
            self.completions[particle],
            self.backlog[particle],
            self.warm_replicas[particle],
            self.transition_complete[particle],
        )
    }
}

/// Groups particles by equal observable histories without allocating.
///
/// Each adjacent offset pair identifies one belief node in `particle_order`.
pub(crate) fn group_particles(
    observations: &PredictiveObservations<'_>,
    particle_order: &mut [u32],
    node_offsets: &mut Vec<u32>,
) {
    for (particle, slot) in particle_order.iter_mut().enumerate() {
        *slot = particle as u32;
    }
    particle_order.sort_unstable_by_key(|&particle| observations.key(particle as usize));
    node_offsets.clear();
    node_offsets.push(0);
    for ordered in 1..particle_order.len() {
        let prior = particle_order[ordered - 1] as usize;
        let current = particle_order[ordered] as usize;
        if observations.key(prior) != observations.key(current) {
            node_offsets.push(ordered as u32);
        }
    }
    node_offsets.push(particle_order.len() as u32);
}

/// Selects the root action from columnar posterior values.
///
/// The columns have one cell for each ordered replica target. Expected
/// replica-seconds order feasible actions. Expected excess delay and then
/// replica-seconds order infeasible actions. Target order resolves ties.
pub(crate) fn select_root_action(
    pass_counts: &[f64],
    excess_delay_sums: &[f64],
    replica_seconds_sums: &[f64],
    sample_count: f64,
    required_probability: f64,
) -> usize {
    if let Some((index, _replica_seconds)) = pass_counts
        .iter()
        .enumerate()
        .filter(|(_, passes)| **passes / sample_count >= required_probability)
        .map(|(index, _passes)| (index, replica_seconds_sums[index]))
        .min_by(|left, right| left.1.total_cmp(&right.1))
    {
        return index;
    }
    excess_delay_sums
        .iter()
        .enumerate()
        .min_by(|left, right| {
            left.1.total_cmp(right.1).then_with(|| {
                replica_seconds_sums[left.0].total_cmp(&replica_seconds_sums[right.0])
            })
        })
        .map_or(0, |(index, _loss)| index)
}

/// Integrates ready replica count over one virtual-time interval.
///
/// The target and readiness columns have equal lengths. Readiness times are
/// monotonic. A target becomes ready at its paired time.
pub(crate) fn replica_seconds(
    start_seconds: f64,
    end_seconds: f64,
    initial_replicas: u32,
    targets: &[u32],
    ready_seconds: &[f64],
) -> f64 {
    assert_eq!(targets.len(), ready_seconds.len());
    assert!(end_seconds >= start_seconds);
    let mut cursor = start_seconds;
    let mut replicas = initial_replicas;
    let mut area = 0.0_f64;
    for (&target, &ready) in targets.iter().zip(ready_seconds) {
        let boundary = ready.clamp(cursor, end_seconds);
        area += f64::from(replicas) * (boundary - cursor);
        cursor = boundary;
        if ready >= end_seconds {
            return area;
        }
        replicas = target;
    }
    area + f64::from(replicas) * (end_seconds - cursor).max(0.0_f64)
}

pub(crate) fn root_event(report_at_micros: u64, ready_at_seconds: &[f64]) -> RootEvent {
    let report = RootEvent::Report {
        at_micros: report_at_micros,
    };
    ready_at_seconds.last().map_or(report, |ready| {
        let ready_at_micros = seconds_to_micros(*ready);
        if ready_at_micros < report_at_micros {
            RootEvent::ActionComplete {
                at_micros: ready_at_micros,
            }
        } else {
            report
        }
    })
}

fn seconds_to_micros(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64) as u64
}

#[cfg(test)]
mod tests;
