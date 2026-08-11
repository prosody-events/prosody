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
            "20260409_add_deferred_next_hints.cql",
            "2d1651f42627b805e9bada5d6883e64db00574854424b80f5958e40b10e5e016",
        ),
        (
            "20260424_deduplication_unified_compaction.cql",
            "9a957eadcd4b231bb47e3eb3f458b461ce1d8b567915bd818b40e0838bd29bc5",
        ),
        (
            "20260506_add_tag.cql",
            "bd2708438d6214cc21d2398b15878862a00e22f3f980a0e49a17effd28d20ff6",
        ),
        (
            "20260513_add_timer_slab_watermark.cql",
            "8b5d3472e1f016610c57c40264e4ecaf317eb2b5529706b9fd4d49a588a66799",
        ),
        (
            "20260522_create_keyed_state.cql",
            "719fc64b2c4445a8dccac072fd9a1acd31c88234b2cf98c50e5685b54cf8b1c3",
        ),
        (
            "20260722_create_keyed_state_publication.cql",
            "319c1163ded1575088ba761e6138f30b5f6adeaf57dc5514e8c7cfd30808f25c",
        ),
        (
            "20260801_create_peer_directory.cql",
            "feff017837acbcde99a20f33d9bf8de1bd537940b4b736a76bd983388d6b7ce4",
        ),
    ]
    .into();

    let migrations = load_embedded_migrations("prosody")?;
    let by_filename: HashMap<&str, &str> = migrations
        .iter()
        .map(|m| (m.filename.as_str(), m.checksum.as_str()))
        .collect();

    for (filename, want) in &expected {
        let got = by_filename.get(filename).ok_or_else(|| {
            eyre!("recorded migration {filename} not found among embedded migrations")
        })?;
        assert_eq!(got, want, "checksum mismatch for {filename}");
    }

    // The baseline must also be complete: an embedded migration without a
    // recorded checksum is unprotected against accidental edits after it has
    // been applied to a deployment.
    let mut unrecorded: Vec<String> = by_filename
        .iter()
        .filter(|(filename, _)| !expected.contains_key(*filename))
        .map(|(filename, checksum)| format!("(\"{filename}\", \"{checksum}\")"))
        .collect();
    unrecorded.sort();
    assert!(
        unrecorded.is_empty(),
        "embedded migrations missing from the baseline; record them:\n{}",
        unrecorded.join("\n")
    );
    Ok(())
}
