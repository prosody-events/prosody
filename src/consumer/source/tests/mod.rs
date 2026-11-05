//! Property tests for [`admit`], the core of the partition read loop.
//!
//! A model queue stands in for one librdkafka partition queue. Like
//! librdkafka, it calls `notify_one` only when a record arrives at an empty
//! queue. The test polls the stream by hand, so every interleaving of
//! arrivals, permit releases, and reads is reachable.

use super::admit;
use color_eyre::eyre::{Result, bail, ensure};
use futures::Stream;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::iter::{empty, once};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

mod kafka;

/// One event in the life of a partition.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// A fetch appends this many records to the queue.
    Arrive(u8),
    /// The oldest record in flight completes and frees its permit.
    Complete,
    /// The partition task polls its record stream once.
    Read,
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::Arrive(u8::arbitrary(g) % 4 + 1),
            1 => Self::Complete,
            _ => Self::Read,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Arrive(count) if count > 1 => Box::new([Self::Arrive(1), Self::Read].into_iter()),
            Self::Arrive(_) | Self::Complete => Box::new(once(Self::Read)),
            Self::Read => Box::new(empty()),
        }
    }
}

/// A partition queue that numbers its records from zero.
#[derive(Default)]
struct Queue {
    records: RefCell<VecDeque<u64>>,
    arrived: Cell<u64>,
    polled: Cell<u64>,
    ready: Notify,
}

impl Queue {
    /// Appends `count` records. Notifies only on a change from empty, as the
    /// librdkafka non-empty callback does.
    fn arrive(&self, count: u8) {
        let mut records = self.records.borrow_mut();
        let was_empty = records.is_empty();
        for _ in 0..count {
            records.push_back(self.arrived.get());
            self.arrived.set(self.arrived.get() + 1);
        }
        if was_empty {
            self.ready.notify_one();
        }
    }

    fn poll(&self) -> Option<u64> {
        let record = self.records.borrow_mut().pop_front()?;
        self.polled.set(self.polled.get() + 1);
        Some(record)
    }
}

/// The records the stream yielded, with the permits they still hold.
#[derive(Default)]
struct Admitted {
    yielded: u64,
    in_flight: VecDeque<OwnedSemaphorePermit>,
}

impl Admitted {
    /// Polls the stream once and checks every invariant.
    fn read<S>(&mut self, stream: Pin<&mut S>, queue: &Queue, permits: usize) -> Result<()>
    where
        S: Stream<Item = (u64, OwnedSemaphorePermit)>,
    {
        let mut context = Context::from_waker(Waker::noop());
        match stream.poll_next(&mut context) {
            Poll::Ready(Some((record, permit))) => {
                ensure!(
                    record == self.yielded,
                    "yielded {record}, expected {}",
                    self.yielded
                );
                self.yielded += 1;
                self.in_flight.push_back(permit);
            }
            Poll::Ready(None) => bail!("the stream ended"),
            Poll::Pending => {}
        }

        let waiting = queue.polled.get() - self.yielded;
        ensure!(waiting <= 1, "{waiting} records wait for a permit");
        ensure!(
            self.in_flight.len() <= permits,
            "more records than permits in flight"
        );
        Ok(())
    }
}

/// For any interleaving of arrivals, completions, and reads, `admit` yields
/// each record once and in order. It holds at most one record while it waits
/// for a permit, and never more records in flight than permits. When every
/// permit is free, it reads every record that arrived, with no notification
/// beyond those librdkafka sends.
///
/// Falsify: wait on `ready` at the top of the loop in `admit`, before the
/// poll. The final drain then stalls behind a notification that never comes.
#[quickcheck]
fn prop_admit_reads_each_record_once_in_order(ops: Vec<Op>, permits: u8) -> Result<()> {
    let permits = usize::from(permits % 4 + 1);
    let semaphore = Arc::new(Semaphore::new(permits));
    let queue = Queue::default();
    let mut stream = pin!(admit(|| queue.poll(), &queue.ready, &semaphore));
    let mut admitted = Admitted::default();

    for op in ops {
        match op {
            Op::Arrive(count) => queue.arrive(count),
            Op::Complete => drop(admitted.in_flight.pop_front()),
            Op::Read => admitted.read(stream.as_mut(), &queue, permits)?,
        }
    }

    // With every permit free, each record needs at most two reads: one that
    // polls it and one that admits it.
    for _ in 0..2 * queue.arrived.get() + 2 {
        admitted.in_flight.clear();
        admitted.read(stream.as_mut(), &queue, permits)?;
    }
    ensure!(
        admitted.yielded == queue.arrived.get(),
        "lost wakeup: {} of {} records read",
        admitted.yielded,
        queue.arrived.get()
    );
    Ok(())
}
