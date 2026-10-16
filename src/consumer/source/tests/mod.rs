//! Property tests for [`arrivals`], the core of the partition read loop.
//!
//! A model queue stands in for one librdkafka partition queue. Like
//! librdkafka, it calls `notify_one` only when a record arrives at an empty
//! queue. The test polls the stream by hand, so every interleaving of
//! arrivals and reads is reachable.

use super::arrivals;
use color_eyre::eyre::{Result, bail, ensure};
use futures::Stream;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::iter::{empty, once};
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};
use tokio::sync::Notify;

mod kafka;

/// One event in the life of a partition.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// A fetch appends this many records to the queue.
    Arrive(u8),
    /// The partition task polls its record stream once.
    Read,
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Arrive(u8::arbitrary(g) % 4 + 1)
        } else {
            Self::Read
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Arrive(count) if count > 1 => Box::new([Self::Arrive(1), Self::Read].into_iter()),
            Self::Arrive(_) => Box::new(once(Self::Read)),
            Self::Read => Box::new(empty()),
        }
    }
}

/// A partition queue that numbers its records from zero.
#[derive(Default)]
struct Queue {
    records: RefCell<VecDeque<u64>>,
    arrived: Cell<u64>,
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
        self.records.borrow_mut().pop_front()
    }
}

/// Polls the stream once, and checks that a yielded record is the next one.
fn read<S: Stream<Item = u64>>(stream: Pin<&mut S>, yielded: &mut u64) -> Result<()> {
    let mut context = Context::from_waker(Waker::noop());
    match stream.poll_next(&mut context) {
        Poll::Ready(Some(record)) => {
            ensure!(record == *yielded, "yielded {record}, expected {yielded}");
            *yielded += 1;
        }
        Poll::Ready(None) => bail!("the stream ended"),
        Poll::Pending => {}
    }
    Ok(())
}

/// For any interleaving of arrivals and reads, `arrivals` yields each record
/// once and in order. It reads every record that arrived, with no
/// notification beyond those librdkafka sends.
///
/// Falsify: wait on `ready` at the top of the loop in `arrivals`, before the
/// poll. The final drain then stalls behind a notification that never comes.
#[quickcheck]
fn prop_arrivals_reads_each_record_once_in_order(ops: Vec<Op>) -> Result<()> {
    let queue = Queue::default();
    let mut stream = pin!(arrivals(|| queue.poll(), &queue.ready));
    let mut yielded = 0;

    for op in ops {
        match op {
            Op::Arrive(count) => queue.arrive(count),
            Op::Read => read(stream.as_mut(), &mut yielded)?,
        }
    }

    for _ in 0..=queue.arrived.get() {
        read(stream.as_mut(), &mut yielded)?;
    }
    ensure!(
        yielded == queue.arrived.get(),
        "lost wakeup: {yielded} of {} records read",
        queue.arrived.get()
    );
    Ok(())
}
