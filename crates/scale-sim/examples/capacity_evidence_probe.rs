//! Runs one regime experiment and prints the posterior trace.

use std::env;

use prosody_scale_sim::{
    PrincipalRegime, PrincipalRun, RegimeExperiment, run_capacity_evidence_regime,
    run_principal_regime, validate_principal_regime,
};

fn print_trace(run: &PrincipalRun) {
    let mut nan_count = 0_usize;
    for index in 0..run.controller().len() {
        let Some(sample) = run.controller().sample(index) else {
            continue;
        };
        if !sample.no_knee_probability.is_finite() {
            nan_count += 1;
        }
        let (occ, rate) = match sample.capacity_evidence {
            prosody_scale_sim::CapacityEvidenceSample::Window(window) => {
                (window.concurrency, window.throughput_per_second())
            }
            _ => (f64::NAN, f64::NAN),
        };
        eprintln!(
            "sample at {:>7.1}s occ {:>7.2} rate {:>8.2} no_knee {:.5} target {} hold {} cost \
             {:.1} rank {:.3} pred {:.1}/{:.1}/{:.1}",
            sample.at_micros as f64 / 1e6,
            occ,
            rate,
            sample.no_knee_probability,
            sample.target,
            sample.hold,
            sample.expected_cost,
            sample.capacity_predictive_rank,
            sample.capacity_predictive_low_per_second,
            sample.capacity_predictive_median_per_second,
            sample.capacity_predictive_high_per_second,
        );
        if let Some(costs) = run.controller().decision_expected_costs(index) {
            let ladder: Vec<String> = costs.iter().map(|cost| format!("{cost:.4e}")).collect();
            eprintln!("  costs [{}]", ladder.join(", "));
        }
    }
    eprintln!("non-finite no_knee samples: {nan_count}");
}

fn main() {
    let name = env::args()
        .nth(1)
        .unwrap_or_else(|| PrincipalRegime::LinearThroughput.name().to_owned());
    let closed = env::args().nth(2).as_deref() == Some("closed");
    let Some(regime) = PrincipalRegime::ALL
        .iter()
        .find(|regime| regime.name() == name)
        .copied()
    else {
        eprintln!("unknown regime: {name}");
        return;
    };
    let (run, experiment) = if closed {
        (run_principal_regime(regime), RegimeExperiment::ClosedLoop)
    } else {
        (
            run_capacity_evidence_regime(regime),
            RegimeExperiment::CapacityEvidence,
        )
    };
    let run = match run {
        Ok(run) => run,
        Err(error) => {
            eprintln!("run failed: {error}");
            return;
        }
    };
    print_trace(&run);
    match validate_principal_regime(regime, experiment, &run) {
        Ok(()) => eprintln!("validation passed"),
        Err(error) => eprintln!("validation failed: {error}"),
    }
}
