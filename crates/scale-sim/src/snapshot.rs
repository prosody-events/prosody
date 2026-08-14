use crate::PlantError;

/// One full reporter snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Snapshot {
    /// Fixed sender index.
    pub sender: u32,
    /// Sender startup identity.
    pub incarnation: u64,
    /// Monotonic sequence within the incarnation.
    pub sequence: u64,
    /// Reporter model time.
    pub observed_at_micros: u64,
    /// Complete arrival count for the reported interval.
    pub arrival_count: u64,
}

/// Deterministic transport fault pattern.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct FaultPattern {
    /// Drop each Nth send. Zero disables drops.
    pub drop_every: u32,
    /// Duplicate each Nth send. Zero disables duplication.
    pub duplicate_every: u32,
    /// Base delivery delay.
    pub delay_micros: u64,
    /// Extra delay for odd sequences. This value causes reorder.
    pub odd_sequence_delay_micros: u64,
}

/// Bounded best-effort snapshot transport.
pub struct SnapshotChannel {
    fault: FaultPattern,
    sent: u32,
    pending: Vec<Delivery>,
}

impl SnapshotChannel {
    /// Allocates a bounded pending-delivery array.
    ///
    /// # Errors
    ///
    /// Returns an error when the capacity does not fit this platform.
    pub fn new(delivery_count_max: u32, fault: FaultPattern) -> Result<Self, PlantError> {
        let capacity =
            usize::try_from(delivery_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if capacity == 0 {
            return Err(PlantError::ZeroBound {
                name: "delivery_count_max",
            });
        }
        Ok(Self {
            fault,
            sent: 0,
            pending: Vec::with_capacity(capacity),
        })
    }

    /// Sends one snapshot through the deterministic fault pattern.
    ///
    /// # Errors
    ///
    /// Returns an error when pending delivery capacity is insufficient.
    pub fn send(&mut self, snapshot: Snapshot) -> Result<(), PlantError> {
        self.sent = self.sent.wrapping_add(1);
        if applies(self.sent, self.fault.drop_every) {
            return Ok(());
        }
        let odd_delay = if snapshot.sequence % 2 == 1 {
            self.fault.odd_sequence_delay_micros
        } else {
            0
        };
        let at_micros = snapshot
            .observed_at_micros
            .saturating_add(self.fault.delay_micros)
            .saturating_add(odd_delay);
        self.push(Delivery {
            at_micros,
            snapshot,
        })?;
        if applies(self.sent, self.fault.duplicate_every) {
            self.push(Delivery {
                at_micros: at_micros.saturating_add(1),
                snapshot,
            })?;
        }
        Ok(())
    }

    /// Applies all deliveries due at or before the supplied model time.
    pub fn deliver(&mut self, now_micros: u64, table: &mut SnapshotTable) {
        let mut pending = 0_usize;
        while pending < self.pending.len() {
            if self.pending[pending].at_micros <= now_micros {
                let delivery = self.pending.swap_remove(pending);
                table.apply(delivery.snapshot);
            } else {
                pending += 1;
            }
        }
    }

    fn push(&mut self, delivery: Delivery) -> Result<(), PlantError> {
        if self.pending.len() == self.pending.capacity() {
            return Err(PlantError::DeliveryCapacity);
        }
        self.pending.push(delivery);
        Ok(())
    }
}

/// Latest accepted state for one reporter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReporterState {
    /// Sender startup identity.
    pub incarnation: u64,
    /// Highest accepted sequence.
    pub sequence: u64,
    /// Reporter model time.
    pub observed_at_micros: u64,
    /// Full reported arrival count.
    pub arrival_count: u64,
}

/// One complete cumulative-counter interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArrivalInterval {
    /// Inclusive reporter time for interval assignment.
    pub start_micros: u64,
    /// Exclusive reporter time for interval assignment.
    pub end_micros: u64,
    /// Arrivals added after the prior accepted snapshot.
    pub count: u64,
    /// Model-time exposure covered by the interval.
    pub exposure_micros: u64,
}

/// Bounded per-reporter baselines for cumulative arrival counters.
pub struct SnapshotCursor {
    incarnations: Vec<u64>,
    sequences: Vec<u64>,
    observed_at_micros: Vec<u64>,
    arrival_counts: Vec<u64>,
    present: Vec<bool>,
}

impl SnapshotCursor {
    /// Allocates one fixed baseline row for each reporter.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero or unsupported reporter count.
    pub fn new(reporter_count_max: u32) -> Result<Self, PlantError> {
        if reporter_count_max == 0 {
            return Err(PlantError::ZeroBound {
                name: "reporter_count_max",
            });
        }
        let count = usize::try_from(reporter_count_max).map_err(|_| PlantError::PlatformLimit)?;
        Ok(Self {
            incarnations: vec![0; count],
            sequences: vec![0; count],
            observed_at_micros: vec![0; count],
            arrival_counts: vec![0; count],
            present: vec![false; count],
        })
    }

    /// Consumes the next complete interval from one accepted reporter row.
    #[must_use]
    pub fn next(&mut self, sender: u32, table: &SnapshotTable) -> Option<ArrivalInterval> {
        let sender_index = sender as usize;
        let current = table.reporter(sender)?;
        if sender_index >= self.present.len() {
            return None;
        }
        let same_incarnation =
            self.present[sender_index] && self.incarnations[sender_index] == current.incarnation;
        let newer = same_incarnation && self.sequences[sender_index] < current.sequence;
        let monotonic = self.arrival_counts[sender_index] <= current.arrival_count
            && self.observed_at_micros[sender_index] < current.observed_at_micros;
        let interval = (newer && monotonic).then(|| ArrivalInterval {
            start_micros: self.observed_at_micros[sender_index],
            end_micros: current.observed_at_micros,
            count: current.arrival_count - self.arrival_counts[sender_index],
            exposure_micros: current.observed_at_micros - self.observed_at_micros[sender_index],
        });
        if !same_incarnation || self.sequences[sender_index] < current.sequence {
            self.present[sender_index] = true;
            self.incarnations[sender_index] = current.incarnation;
            self.sequences[sender_index] = current.sequence;
            self.observed_at_micros[sender_index] = current.observed_at_micros;
            self.arrival_counts[sender_index] = current.arrival_count;
        }
        interval
    }

    pub(crate) fn clear(&mut self) {
        self.present.fill(false);
    }
}

/// Bounded aggregator table stored as columns.
pub struct SnapshotTable {
    incarnations: Vec<u64>,
    sequences: Vec<u64>,
    observed_at_micros: Vec<u64>,
    arrival_counts: Vec<u64>,
    present: Vec<bool>,
}

impl SnapshotTable {
    /// Allocates one fixed row for each possible reporter.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero or unsupported reporter count.
    pub fn new(reporter_count_max: u32) -> Result<Self, PlantError> {
        if reporter_count_max == 0 {
            return Err(PlantError::ZeroBound {
                name: "reporter_count_max",
            });
        }
        let count = usize::try_from(reporter_count_max).map_err(|_| PlantError::PlatformLimit)?;
        Ok(Self {
            incarnations: vec![0; count],
            sequences: vec![0; count],
            observed_at_micros: vec![0; count],
            arrival_counts: vec![0; count],
            present: vec![false; count],
        })
    }

    /// Returns the latest accepted snapshot for one reporter.
    #[must_use]
    pub fn reporter(&self, sender: u32) -> Option<ReporterState> {
        let sender = sender as usize;
        if sender >= self.present.len() || !self.present[sender] {
            return None;
        }
        Some(ReporterState {
            incarnation: self.incarnations[sender],
            sequence: self.sequences[sender],
            observed_at_micros: self.observed_at_micros[sender],
            arrival_count: self.arrival_counts[sender],
        })
    }

    pub(crate) fn clear(&mut self) {
        self.present.fill(false);
    }

    fn apply(&mut self, snapshot: Snapshot) {
        let sender = snapshot.sender as usize;
        if sender >= self.present.len() {
            return;
        }
        let new_incarnation =
            !self.present[sender] || self.incarnations[sender] != snapshot.incarnation;
        if !new_incarnation && snapshot.sequence <= self.sequences[sender] {
            return;
        }
        self.present[sender] = true;
        self.incarnations[sender] = snapshot.incarnation;
        self.sequences[sender] = snapshot.sequence;
        self.observed_at_micros[sender] = snapshot.observed_at_micros;
        self.arrival_counts[sender] = snapshot.arrival_count;
    }
}

#[derive(Clone, Copy)]
struct Delivery {
    at_micros: u64,
    snapshot: Snapshot,
}

fn applies(sequence: u32, every: u32) -> bool {
    every > 0 && sequence.is_multiple_of(every)
}
