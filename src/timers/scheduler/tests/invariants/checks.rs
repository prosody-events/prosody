//! The invariant checks that run after each op.

use super::super::persisted_watermark;
use super::fixture::Fixture;
use crate::timers::datetime::CompactDateTime;
use crate::timers::scheduler::actor::load::cleanup_step;
use crate::timers::scheduler::actor::owns_slab;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::TriggerStore;
use crate::timers::test_support::Memory;
use futures::TryStreamExt;
use std::collections::BTreeSet;
use std::result::Result as StdResult;

/// Verifies that the BATCH-driven past-watermark insert lowered the
/// watermark to `slab_id - 1`, preserved ownership through the prior
/// watermark, and kept an entry in the slab during the write.
pub(super) fn assert_past_watermark_pull(
    fixture: &Fixture,
    slab_id: SlabId,
    old_watermark: Option<SlabId>,
) -> StdResult<(), String> {
    let state = &fixture.actor.state;
    let expected_watermark = slab_id.checked_sub(1);
    if state.last_persisted_watermark != expected_watermark {
        return Err(format!(
            "past-watermark schedule: expected watermark {expected_watermark:?}, got {:?}",
            state.last_persisted_watermark
        ));
    }
    if let Some(old_watermark) = old_watermark
        && state
            .highest_loaded_slab_id
            .is_none_or(|h| h < old_watermark)
    {
        return Err(format!(
            "past-watermark schedule: highest_loaded_slab_id {:?} did not preserve ownership \
             through old watermark {old_watermark}",
            state.highest_loaded_slab_id
        ));
    }
    if !fixture
        .actor
        .timers
        .slabs(fixture.segment.slab_size)
        .contains(&slab_id)
    {
        return Err(format!(
            "past-watermark schedule: slab {slab_id} has no entry"
        ));
    }
    Ok(())
}

/// Wraps `cleanup_step` so we can verify P4 (safety) plus the progress
/// properties that keep slab metadata and the watermark from getting
/// pinned behind empty rows.
pub(super) async fn check_cleanup_effects(fixture: &mut Fixture) -> StdResult<(), String> {
    let before_active = fixture.actor.timers.slabs(fixture.segment.slab_size);
    let before_store_slabs: BTreeSet<SlabId> = fixture
        .store
        .get_slab_range(0..=SlabId::MAX)
        .try_collect()
        .await
        .map_err(|e| format!("get_slab_range before cleanup: {e:?}"))?;
    let before_watermark = fixture.actor.state.last_persisted_watermark;

    let now_slab = fixture.current_slab();

    let expected_deletes = cleanup_candidates_for_test(
        fixture.actor.state.highest_loaded_slab_id,
        now_slab,
        &before_active,
        &before_store_slabs,
    );

    let actor = &mut fixture.actor;
    cleanup_step(&mut actor.state, &actor.timers).await;

    let after_store_slabs: BTreeSet<SlabId> = fixture
        .store
        .get_slab_range(0..=SlabId::MAX)
        .try_collect()
        .await
        .map_err(|e| format!("get_slab_range after cleanup: {e:?}"))?;
    let deleted: BTreeSet<SlabId> = before_store_slabs
        .difference(&after_store_slabs)
        .copied()
        .collect();

    for &slab_id in &deleted {
        if before_active.contains(&slab_id) {
            return Err(format!(
                "P4: cleanup deleted slab {slab_id} that had active triggers"
            ));
        }
        if slab_id >= now_slab {
            return Err(format!(
                "P4: cleanup deleted slab {slab_id} >= now_slab {now_slab}"
            ));
        }
    }

    for slab_id in expected_deletes {
        if after_store_slabs.contains(&slab_id) {
            return Err(format!(
                "cleanup progress: eligible slab {slab_id} was not deleted"
            ));
        }
    }

    for ((key, time, ty), model) in &fixture.expected {
        if model.in_store && model.memory.is_some() {
            let slab_id = Slab::from_time(fixture.segment.slab_size, *time).id();
            if !after_store_slabs.contains(&slab_id) {
                return Err(format!(
                    "cleanup reloadability: active stored timer ({key}, {time:?}, {ty:?}) lost \
                     slab metadata for slab {slab_id}"
                ));
            }
        }
    }

    let expected_watermark = expected_watermark_after_cleanup_for_test(
        before_watermark,
        fixture.actor.state.highest_loaded_slab_id,
        now_slab,
        &before_active,
    );
    if fixture.actor.state.last_persisted_watermark != expected_watermark {
        return Err(format!(
            "watermark progress: expected {expected_watermark:?}, got {:?}",
            fixture.actor.state.last_persisted_watermark
        ));
    }

    Ok(())
}

pub(super) async fn check_invariants(fixture: &Fixture) -> StdResult<(), String> {
    // P3: every slab clustering row sits above the persisted watermark.
    let watermark = fixture.actor.state.last_persisted_watermark;
    let store_slabs: BTreeSet<SlabId> = fixture
        .store
        .get_slab_range(0..=SlabId::MAX)
        .try_collect()
        .await
        .map_err(|e| format!("get_slab_range: {e:?}"))?;
    if let Some(w) = watermark
        && let Some(&min_slab) = store_slabs.iter().next()
        && min_slab <= w
    {
        return Err(format!(
            "P3 violated: watermark={w} but slab {min_slab} present"
        ));
    }
    // A start discards a stale watermark, which stays in the store until
    // cleanup writes a new one.
    let persisted_watermark = persisted_watermark(&fixture.store).await;
    let discarded =
        watermark.is_none() && persisted_watermark.is_some_and(|w| w >= fixture.current_slab());
    if persisted_watermark != watermark && !discarded {
        return Err(format!(
            "watermark drift: actor={watermark:?} store={persisted_watermark:?}"
        ));
    }

    // P1 + P2: walk the whole generator universe, so both directions of
    // the parity hold.
    let timers = &fixture.actor.timers;
    for (key, time, ty) in &fixture.universe {
        let expected = fixture.model_for(key, *time, *ty);
        let slab_id = Slab::from_time(fixture.segment.slab_size, *time).id();

        // DB membership mirrors `expected`: every stored triple has a
        // store row; every Unscheduled triple has none.
        let store_times: BTreeSet<CompactDateTime> = fixture
            .store
            .get_key_times(*ty, key)
            .try_collect()
            .await
            .map_err(|e| format!("get_key_times: {e:?}"))?;
        let in_db = store_times.contains(time);
        if in_db != expected.in_store {
            return Err(format!(
                "DB mismatch for ({key}, {time:?}, {ty:?}): in_db={in_db} expected={}",
                expected.in_store
            ));
        }
        if in_db && !store_slabs.contains(&slab_id) {
            return Err(format!(
                "P8: stored trigger ({key}, {time:?}, {ty:?}) sits in unregistered slab {slab_id}"
            ));
        }

        let listed = timers
            .list(key, *ty)
            .into_iter()
            .find_map(|(at, listed)| (at == *time).then_some(listed));
        let owned = owns_slab(&fixture.actor.state, slab_id);
        if listed != expected.memory.and_then(Memory::listed) {
            return Err(format!(
                "memory mismatch for ({key}, {time:?}, {ty:?}): listed={listed:?} expected={:?} \
                 (in_store={}, owned={owned})",
                expected.memory, expected.in_store
            ));
        }
        if expected.in_store && owned && expected.memory.is_none() {
            return Err(format!(
                "model mismatch for ({key}, {time:?}, {ty:?}): owned stored timer has no entry"
            ));
        }
    }

    // Kept entries list nothing, so the count of entries checks them.
    let held = fixture
        .expected
        .values()
        .filter(|model| model.memory.is_some());
    let entries = usize::try_from(timers.snapshot(CompactDateTime::MIN).active)
        .map_err(|e| format!("entry count: {e}"))?;
    if entries != held.count() {
        return Err(format!("memory holds {entries} entries, not the model's"));
    }
    for slab_id in timers.slabs(fixture.segment.slab_size) {
        if !owns_slab(&fixture.actor.state, slab_id) {
            return Err(format!(
                "P2: memory holds a timer in unowned slab {slab_id}"
            ));
        }
    }

    Ok(())
}
fn cleanup_candidates_for_test(
    highest_loaded: Option<SlabId>,
    now_slab: SlabId,
    active: &BTreeSet<SlabId>,
    store_slabs: &BTreeSet<SlabId>,
) -> Vec<SlabId> {
    let Some(highest_loaded) = highest_loaded else {
        return Vec::new();
    };
    let Some(end) = now_slab.checked_sub(1).map(|s| s.min(highest_loaded)) else {
        return Vec::new();
    };
    store_slabs
        .range(..=end)
        .copied()
        .filter(|slab_id| !active.contains(slab_id))
        .collect()
}

fn watermark_candidate_for_test(
    highest_loaded: Option<SlabId>,
    now_slab: SlabId,
    active: &BTreeSet<SlabId>,
) -> Option<SlabId> {
    let highest_loaded = highest_loaded?;
    let end = now_slab.checked_sub(1)?.min(highest_loaded);

    active
        .range(..=end)
        .next()
        .copied()
        .and_then(|first_active| first_active.checked_sub(1))
        .or(Some(end))
}

fn expected_watermark_after_cleanup_for_test(
    current_watermark: Option<SlabId>,
    highest_loaded: Option<SlabId>,
    now_slab: SlabId,
    active: &BTreeSet<SlabId>,
) -> Option<SlabId> {
    let candidate = watermark_candidate_for_test(highest_loaded, now_slab, active);
    match (current_watermark, candidate) {
        (_, None) => current_watermark,
        (Some(current), Some(candidate)) if candidate <= current => Some(current),
        (_, Some(candidate)) => Some(candidate),
    }
}
