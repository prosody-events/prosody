//! The route preference for one response destination.

use crate::router::Preference;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Relaxed;

const UNKNOWN: u8 = 0;
const DIRECT: u8 = 1;
const ADVERTISED: u8 = 2;

/// The endpoint that most recently answered for one node.
pub struct Destination {
    preferred: AtomicU8,
}

impl Default for Destination {
    fn default() -> Self {
        Self {
            preferred: AtomicU8::new(UNKNOWN),
        }
    }
}

impl Destination {
    /// Returns the endpoint that most recently answered.
    pub(crate) fn preferred(&self) -> Option<Preference> {
        match self.preferred.load(Relaxed) {
            DIRECT => Some(Preference::Direct),
            ADVERTISED => Some(Preference::Advertised),
            _ => None,
        }
    }

    /// Records the endpoint that answered.
    pub(crate) fn prefer(&self, preference: Option<Preference>) {
        let value = match preference {
            None => UNKNOWN,
            Some(Preference::Direct) => DIRECT,
            Some(Preference::Advertised) => ADVERTISED,
        };
        self.preferred.store(value, Relaxed);
    }
}
