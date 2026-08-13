use rand::TryRng;

use super::{PoissonMean, RandomStream, count_as_f64, sample_poisson};

#[test]
fn poisson_sampler_matches_its_first_two_moments() {
    for (seed, expected) in [(11_u64, 0.2_f64), (12, 7.0_f64), (13, 100.0_f64)] {
        let mean = PoissonMean(expected);
        let mut random = RandomStream::new(seed);
        let mut sum = 0.0_f64;
        let mut sum_squared = 0.0_f64;
        for _ in 0_u32..50_000 {
            let value = count_as_f64(sample_poisson(mean, &mut random));
            sum += value;
            sum_squared += value * value;
        }
        let sample_mean = sum / 50_000.0_f64;
        let sample_variance = sum_squared / 50_000.0_f64 - sample_mean * sample_mean;
        assert!((sample_mean - expected).abs() < 0.03_f64 * expected.sqrt().max(1.0_f64));
        assert!((sample_variance - expected).abs() < 0.06_f64 * expected.max(1.0_f64));
    }
}

#[test]
fn byte_fill_advances_the_public_counter() {
    let mut random = RandomStream::new(7);
    let mut bytes = [0_u8; 17];
    let result = random.try_fill_bytes(&mut bytes);
    assert!(result.is_ok());
    assert_eq!(random.counter(), 3);
}
