use core::convert::Infallible;

use rand::{Rng, SeedableRng, TryRng};
use rand_xoshiro::Xoshiro256PlusPlus;

const UNIT_SCALE: f64 = 1.0_f64 / 9_007_199_254_740_992.0_f64;

/// Deterministic random stream indexed by a key and counter.
///
/// The stream holds no hidden platform state. Equal keys and counters produce
/// equal values on native and WebAssembly targets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RandomStream {
    key: u64,
    counter: u64,
    generator: Xoshiro256PlusPlus,
}

impl RandomStream {
    /// Creates one stream at counter zero.
    #[must_use]
    pub fn new(key: u64) -> Self {
        Self {
            key,
            counter: 0,
            generator: Xoshiro256PlusPlus::seed_from_u64(key),
        }
    }

    /// Creates one independent stream from a stable domain identifier.
    #[must_use]
    pub fn domain(self, domain: u64) -> Self {
        Self::new(mix(self.key ^ domain))
    }

    /// Returns the next uniformly distributed integer.
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        self.generator.next_u64()
    }

    /// Returns the next value in the open unit interval.
    #[must_use]
    pub fn open_unit_f64(&mut self) -> f64 {
        let mantissa = self.next_u64() >> 11_u32;
        let high = (mantissa >> 27_u32) as u32;
        let low = (mantissa & 0x07ff_ffff) as u32;
        let exact = f64::from(high) * 134_217_728.0_f64 + f64::from(low);
        (exact + 0.5_f64) * UNIT_SCALE
    }

    /// Returns an unbiased integer below a positive bound.
    pub(crate) fn index_below(&mut self, bound: u32) -> u32 {
        assert!(bound > 0, "a random index bound must be positive");
        let bound = u64::from(bound);
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let product = u128::from(self.next_u64()) * u128::from(bound);
            if product as u64 >= threshold {
                return (product >> 64) as u32;
            }
        }
    }

    /// Returns the next counter value.
    #[must_use]
    pub const fn counter(&self) -> u64 {
        self.counter
    }
}

impl TryRng for RandomStream {
    type Error = Infallible;

    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok(self.next_u64() as u32)
    }

    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        Ok(Self::next_u64(self))
    }

    fn try_fill_bytes(&mut self, destination: &mut [u8]) -> Result<(), Self::Error> {
        self.generator.try_fill_bytes(destination)
    }
}

const fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
