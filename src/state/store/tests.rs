//! Fetch sizes grow monotonically to the transport maximum.

use super::FetchSchedule;
use quickcheck::quickcheck;
use std::num::NonZeroUsize;

quickcheck! {
    fn prop_fetch_schedule(first: Option<usize>, max: NonZeroUsize) -> bool {
        let first = first.and_then(NonZeroUsize::new);
        let mut fetch = FetchSchedule::new(first, max);
        let mut previous = fetch.next();
        if previous != first.map_or(max, |n| n.min(max)) {
            return false;
        }
        for _ in 0..usize::BITS {
            let next = fetch.next();
            if !(1..=max.get()).contains(&next.get()) || next < previous {
                return false;
            }
            previous = next;
        }
        previous == max
    }
}
