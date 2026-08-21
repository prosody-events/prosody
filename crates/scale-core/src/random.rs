use core::convert::Infallible;

use rand::{Rng, SeedableRng, TryRng};
use rand_distr::{Distribution, StandardNormal};
use rand_xoshiro::Xoshiro256PlusPlus;
use statrs::function::gamma::ln_gamma;

const UNIT_SCALE: f64 = 1.0_f64 / 9_007_199_254_740_992.0_f64;
const PTRS_THRESHOLD: f64 = 10.0_f64;

/// A finite positive mean accepted by the internal Poisson sampler.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PoissonMean(f64);

impl PoissonMean {
    pub(crate) fn new(mean: f64) -> Option<Self> {
        (mean.is_finite() && mean > 0.0_f64).then_some(Self(mean))
    }

    /// Multiplies values from a model that validated their complete domain.
    pub(crate) fn from_product(rate: f64, duration: f64) -> Self {
        debug_assert!(rate.is_finite() && rate > 0.0_f64, "validated rate");
        debug_assert!(
            duration.is_finite() && duration > 0.0_f64,
            "validated duration"
        );
        debug_assert!((rate * duration).is_finite(), "validated Poisson product");
        Self(rate * duration)
    }
}

/// Deterministic random stream indexed by a key and counter.
///
/// The stream holds no hidden platform state. Equal keys and counters produce
/// equal values on native and WebAssembly targets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RandomStream {
    key: u64,
    counter: u64,
    generator: Xoshiro256PlusPlus,
    stratification: Option<Stratification>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Stratification {
    scenario: u32,
    count: u32,
    role: u64,
    multiplier: u32,
    offset: u32,
}

impl RandomStream {
    /// Creates one stream at counter zero.
    #[must_use]
    pub fn new(key: u64) -> Self {
        Self {
            key,
            counter: 0,
            generator: Xoshiro256PlusPlus::seed_from_u64(key),
            stratification: None,
        }
    }

    /// Creates a stream with one midpoint stratum per coordinate.
    pub(crate) fn stratified(key: u64, scenario: u32, count: u32, role: u64) -> Self {
        let mut stream = Self::new(key);
        let (multiplier, offset) = permutation_parameters(count, role);
        stream.stratification = Some(Stratification {
            scenario,
            count,
            role,
            multiplier,
            offset,
        });
        stream
    }

    /// Creates one independent stream from a stable domain identifier.
    #[must_use]
    pub fn domain(self, domain: u64) -> Self {
        let key = mix(self.key ^ domain);
        self.stratification.map_or_else(
            || Self::new(key),
            |stratification| {
                Self::stratified(
                    key,
                    stratification.scenario,
                    stratification.count,
                    mix(stratification.role ^ domain),
                )
            },
        )
    }

    /// Returns the next uniformly distributed integer.
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        let value = self.stratification.map_or_else(
            || self.generator.next_u64(),
            |stratification| {
                let quantile = stratified_quantile(stratification, self.counter);
                let scaled = (quantile * 9_007_199_254_740_992.0_f64).floor() as u64;
                scaled.min(9_007_199_254_740_991) << 11_u32
            },
        );
        self.counter = self.counter.wrapping_add(1);
        value
    }

    /// Returns the next value in the open unit interval.
    #[must_use]
    pub fn open_unit_f64(&mut self) -> f64 {
        if let Some(stratification) = self.stratification {
            let quantile = stratified_quantile(stratification, self.counter);
            self.counter = self.counter.wrapping_add(1);
            return quantile;
        }
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

fn stratified_quantile(stratification: Stratification, counter: u64) -> f64 {
    let rank = if counter == 0 {
        apply_permutation(
            stratification.scenario,
            stratification.count,
            stratification.multiplier,
            stratification.offset,
        )
    } else {
        let role = mix(stratification.role ^ counter);
        let multiplier = if role & 1 == 0 {
            1
        } else {
            stratification.count - 1
        };
        let offset = role.rotate_right(17) as u32 % stratification.count;
        apply_permutation(
            stratification.scenario,
            stratification.count,
            multiplier,
            offset,
        )
    };
    (f64::from(rank) + 0.5_f64) / f64::from(stratification.count)
}

pub(crate) fn sample_gamma(shape: f64, random: &mut RandomStream) -> f64 {
    let adjusted_shape = if shape < 1.0_f64 {
        shape + 1.0_f64
    } else {
        shape
    };
    let d = adjusted_shape - 1.0_f64 / 3.0_f64;
    let c = (9.0_f64 * d).sqrt().recip();
    let sample = loop {
        let normal: f64 = StandardNormal.sample(&mut *random);
        let base = 1.0_f64 + c * normal;
        if base <= 0.0_f64 {
            continue;
        }
        let value = base * base * base;
        let uniform = random.open_unit_f64();
        let normal_squared = normal * normal;
        if uniform < 1.0_f64 - 0.0331_f64 * normal_squared * normal_squared {
            break d * value;
        }
        if uniform.ln() < 0.5_f64 * normal_squared + d * (1.0_f64 - value + value.ln()) {
            break d * value;
        }
    };
    if shape < 1.0_f64 {
        sample * random.open_unit_f64().powf(shape.recip())
    } else {
        sample
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
        // Byte fills consume the public stream counter like every other draw.
        for chunk in destination.chunks_mut(size_of::<u64>()) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        Ok(())
    }
}

/// Samples a Poisson variate from a mean that construction already validated.
pub(crate) fn sample_poisson(mean: PoissonMean, random: &mut RandomStream) -> u64 {
    if mean.0 < PTRS_THRESHOLD {
        return sample_poisson_inversion(mean.0, random);
    }
    sample_poisson_ptrs(mean.0, random)
}

pub(crate) fn count_as_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn sample_poisson_inversion(mean: f64, random: &mut RandomStream) -> u64 {
    let limit = (-mean).exp();
    let mut product = 1.0_f64;
    let mut count = 0_u64;
    loop {
        product *= random.open_unit_f64();
        if product <= limit {
            return count;
        }
        count += 1;
    }
}

fn sample_poisson_ptrs(mean: f64, random: &mut RandomStream) -> u64 {
    let root = mean.sqrt();
    let b = 0.931_f64 + 2.53_f64 * root;
    let a = -0.059_f64 + 0.02483_f64 * b;
    let inverse_alpha = 1.1239_f64 + 1.1328_f64 / (b - 3.4_f64);
    let v_r = 0.9277_f64 - 3.6224_f64 / (b - 2.0_f64);
    loop {
        let u = random.open_unit_f64() - 0.5_f64;
        let v = random.open_unit_f64();
        let us = 0.5_f64 - u.abs();
        let candidate = ((2.0_f64 * a / us + b) * u + mean + 0.43_f64).floor();
        if us >= 0.07_f64 && v <= v_r && candidate >= 0.0_f64 {
            return candidate as u64;
        }
        if candidate < 0.0_f64 || (us < 0.013_f64 && v > us) {
            continue;
        }
        let lhs = (v * inverse_alpha / (a / (us * us) + b)).ln();
        let rhs = -mean + candidate * mean.ln() - ln_gamma(candidate + 1.0_f64);
        if lhs <= rhs {
            return candidate as u64;
        }
    }
}

const fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
pub(crate) fn permuted_rank(scenario: u32, count: u32, role: u64) -> u32 {
    let (multiplier, offset) = permutation_parameters(count, role);
    apply_permutation(scenario, count, multiplier, offset)
}

fn permutation_parameters(count: u32, role: u64) -> (u32, u32) {
    let mut multiplier = (role as u32 | 1) % count;
    while greatest_common_divisor(multiplier, count) != 1 {
        multiplier = (multiplier + 2) % count;
    }
    let offset = role.rotate_right(17) as u32 % count;
    (multiplier, offset)
}

fn apply_permutation(scenario: u32, count: u32, multiplier: u32, offset: u32) -> u32 {
    ((u64::from(multiplier) * u64::from(scenario) + u64::from(offset)) % u64::from(count)) as u32
}

const fn greatest_common_divisor(mut left: u32, mut right: u32) -> u32 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

#[cfg(test)]
#[path = "random_tests.rs"]
mod tests;
