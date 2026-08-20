//! Runs one bounded principal regime with hot-path measurements.

use std::env;

use prosody_scale_sim::{PrincipalRegime, run_principal_regime_profiled};

const DEFAULT_SEED: u64 = 7;
const DEFAULT_TICK_COUNT_MAX: u64 = 70;

#[hotpath::main]
fn main() {
    let mut arguments = env::args().skip(1);
    let regime_name = match arguments.next() {
        Some(name) => name,
        None => PrincipalRegime::RebalanceStorm.name().to_owned(),
    };
    let seed_text = arguments.next();
    let tick_text = arguments.next();
    if arguments.next().is_some() {
        print_usage();
        return;
    }
    let Some(regime) = PrincipalRegime::ALL
        .iter()
        .find(|regime| regime.name() == regime_name)
        .copied()
    else {
        print_usage();
        return;
    };
    let seed = match seed_text {
        Some(value) => match value.parse::<u64>() {
            Ok(seed) => seed,
            Err(_) => {
                print_usage();
                return;
            }
        },
        None => DEFAULT_SEED,
    };
    let tick_count_max = match tick_text {
        Some(value) => match value.parse::<u64>() {
            Ok(tick_count_max) if tick_count_max > 0 => tick_count_max,
            Ok(_) | Err(_) => {
                print_usage();
                return;
            }
        },
        None => DEFAULT_TICK_COUNT_MAX,
    };
    if let Err(error) = run_principal_regime_profiled(regime, seed, tick_count_max) {
        eprintln!("hotspot probe failed: {error}");
    }
}

fn print_usage() {
    eprintln!("usage: hotspot_probe [regime-name] [seed] [tick-count-max]");
}
