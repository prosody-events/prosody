use quickcheck_macros::quickcheck;

use super::{RANK_BIN_COUNT, rank_bin};

#[quickcheck]
fn predictive_rank_maps_to_one_decile(raw_rank: u64) -> bool {
    let rank = unit_interval(raw_rank);
    let bin = rank_bin(rank);
    bin < RANK_BIN_COUNT && rank >= lower_bound(bin) && rank <= upper_bound(bin)
}

fn unit_interval(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).map_or(0, |part| part);
    let low = u32::try_from(value & u64::from(u32::MAX)).map_or(0, |part| part);
    let numerator = f64::from(high) * 4_294_967_296.0_f64 + f64::from(low);
    numerator / 18_446_744_073_709_551_615.0_f64
}

fn lower_bound(bin: usize) -> f64 {
    u32::try_from(bin).map_or(0.0_f64, f64::from) / 10.0_f64
}

fn upper_bound(bin: usize) -> f64 {
    u32::try_from(bin.saturating_add(1)).map_or(1.0_f64, f64::from) / 10.0_f64
}
