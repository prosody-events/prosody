use super::*;
use color_eyre::eyre::{Result, eyre};
use std::collections::HashMap;

#[test]
fn checksums_match_baseline() -> Result<()> {
    // Recorded baseline of calculate_checksum output for the prosody keyspace's
    // migrations, captured against the deployed cluster. A mismatch means
    // calculate_checksum now produces different output, which would break
    // migration idempotency checks against existing deployments. A missing
    // entry means a recorded migration was renamed or removed, silently
    // dropping it from a deployment's applied set.
    let expected: HashMap<&str, &str> = [
        (
            "20250613_create_timers.cql",
            "56b19957531ae94f21f41f9db74e05eef52f19bdc290d3fa94d619dbff4e14e4",
        ),
        (
            "20251023_add_timer_types.cql",
            "eb46357d9e6aae54433e3446d580c643a226062909eeb1ff6e01ef49a9776dad",
        ),
        (
            "20251126_create_deferred_offsets.cql",
            "dea2a6622c7284d7966abb9681bb3cea0e70327d93f02f313cdc906204261d6f",
        ),
        (
            "20251223_create_deferred_timers.cql",
            "7e111666bd66fb41826cfcf0f8daadd5e0bc114b6dacda359cf808030e40b13e",
        ),
        (
            "20260217_add_singleton_slots.cql",
            "c9833d9d611b2e8fe36dc9a651f3a8f3eb9d1567d69224df63f54c24c11623a6",
        ),
        (
            "20260319_create_deduplication.cql",
            "72568cda66e2055d552b5da9310ed518aed82fdbfe2b69db4c460cf7fb37a516",
        ),
        (
            "20260506_add_tag.cql",
            "bd2708438d6214cc21d2398b15878862a00e22f3f980a0e49a17effd28d20ff6",
        ),
        (
            "20260513_add_timer_slab_watermark.cql",
            "8b5d3472e1f016610c57c40264e4ecaf317eb2b5529706b9fd4d49a588a66799",
        ),
    ]
    .into();

    let migrations = load_embedded_migrations("prosody")?;
    let by_filename: HashMap<&str, &str> = migrations
        .iter()
        .map(|m| (m.filename.as_str(), m.checksum.as_str()))
        .collect();

    for (filename, want) in expected {
        let got = by_filename.get(filename).ok_or_else(|| {
            eyre!("recorded migration {filename} not found among embedded migrations")
        })?;
        assert_eq!(*got, want, "checksum mismatch for {filename}");
    }
    Ok(())
}
