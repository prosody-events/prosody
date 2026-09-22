//! Finalize retry keeps the dirty buffer whole and rewrites one event marker.

use super::{Fixture, message};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::error::ErrorCategory;
use crate::state::cell::Values;
use crate::state::session::Finalized;
use crate::state::session::Promoted;
use crate::state::session::sealed::StateLifecycle;
use crate::state::store::CellRead;
use crate::state::tests::cell_suite::{Poison, cell_at, value_cell};
use crate::state::{CollectionId, StateName, StateType};
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};

/// A collection failing mid-stage exits `finalize` with the buffer **whole**
/// — the drain runs strictly after the whole per-collection aggregate
/// succeeded, never per collection — so the retried `finalize` re-stages the
/// same buffered ops idempotently and converges once the store heals. A
/// per-collection (or pre-error) drain would strand the healed retry with an
/// empty buffer.
#[tokio::test]
async fn failed_finalize_keeps_the_buffer_whole_for_retry() -> Result<()> {
    let fx = Fixture::with_collections(&["cart", "wishlist"])?;
    let cart = fx.value_name.clone();
    let wishlist = StateName::try_new("wishlist")?;
    // Poison wishlist's stage: `write_provisional` fails Transient while cart
    // (racing under buffer_unordered) may or may not have staged first.
    fx.set_poison(Some(Poison::WriteProvisional(
        wishlist.clone(),
        ErrorCategory::Transient,
    )));
    let (event, dedup_id) = message(1);
    // The scope stays alive across the dirty probes below: its `Drop` would
    // clear the very buffer the failed finalize must leave whole.
    let scope = fx.session(event);
    let session = scope.handle();

    session
        .seed(StateType::Application, &cart, &value_cell(), Some(b"c1"))
        .await;
    session
        .seed(
            StateType::Application,
            &wishlist,
            &value_cell(),
            Some(b"w1"),
        )
        .await;

    assert!(
        session.finalize().await.is_err(),
        "the poisoned stage must fail the aggregate",
    );
    let touched: Vec<StateName> = fx
        .dirty
        .touched(&fx.state_key.key)
        .into_iter()
        .map(|((_, name), ..)| name)
        .collect();
    assert_eq!(
        touched.len(),
        2,
        "a failed finalize keeps BOTH collections buffered: {touched:?}",
    );
    assert!(touched.contains(&cart) && touched.contains(&wishlist));

    // Heal the store: the retried finalize re-stages from the intact buffer
    // and the settle converges to both buffered values.
    fx.set_poison(None);
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the healed retry must re-stage from the intact buffer");
    };
    fx.dedup.insert(dedup_id).await?;
    assert!(matches!(staged.promote(|| false).await, Promoted::Complete));
    for (name, expected) in [(&cart, b"c1"), (&wishlist, b"w1")] {
        let id = CollectionId::new(fx.state_key.clone(), StateType::Application, name.clone());
        assert_eq!(
            CellRead::<Values>::read(&fx.cell_store(), &id, value_cell().as_ref())
                .await?
                .0
                .into_inner(),
            Some(Bytes::from_static(expected)),
            "{name:?} must commit its buffered value on the healed retry",
        );
    }
    Ok(())
}

/// A retry attempt re-runs `finalize`: the second stage **rebuilds** the same
/// event's durable marker from its own staged set — never keeps the first
/// attempt's frozen payload, never resolves it as prior event — the settle
/// converges to the retried values, and no event marker stands afterwards.
/// The two attempts stage *different* cell sets so a kept (stale) marker is
/// observable: recovery resolves exactly the coordinates the marker lists, so
/// a stale list would strand the retry's extra cell. An example because the
/// lifecycle trace generator does not carry retry re-finalize, and the
/// idempotent same-event marker overwrite is a narrow protocol edge the
/// value-projection model does not observe.
#[tokio::test]
async fn retry_refinalize_overwrites_the_same_event_marker() -> Result<()> {
    let fx = Fixture::new()?;
    let (event, dedup_id) = message(1);
    let session = fx.session(event).handle();
    let extra = cell_at(7);

    session
        .seed(
            StateType::Application,
            &fx.value_name,
            &value_cell(),
            Some(b"v1"),
        )
        .await;
    // Attempt one's receipt is deliberately dropped — the discarded stage the
    // retry boundary pairs with `reset`.
    assert!(matches!(session.finalize().await?, Finalized::Staged(_)));

    // The retry boundary: discard the attempt's dirty ops, then re-dispatch
    // the same event.
    session.discard_dirty();

    // The retry stages a superset — the Value cell again plus one more cell —
    // so the rebuilt marker's coordinate list differs from attempt one's.
    session
        .seed(
            StateType::Application,
            &fx.value_name,
            &value_cell(),
            Some(b"v2"),
        )
        .await;
    session
        .seed(StateType::Application, &fx.value_name, &extra, Some(b"w"))
        .await;
    let Finalized::Staged(staged) = session.finalize().await? else {
        bail!("the retry re-stage must mint a receipt");
    };

    // The unsettled durable marker is the retry's, rebuilt whole: same event,
    // and its frozen coordinate list is attempt two's staged set — not
    // attempt one's single cell.
    let marker = fx
        .cells
        .unsettled_marker_of(&fx.value_id())
        .ok_or_else(|| eyre!("no unsettled marker after the re-stage"))?;
    assert_eq!(marker.event(), event, "the marker stays the same event's");
    assert_eq!(
        marker.staged(),
        [value_cell(), extra.clone()],
        "the re-run rebuilds the marker from its own staged set"
    );

    fx.dedup.insert(dedup_id).await?;
    assert!(matches!(staged.promote(|| false).await, Promoted::Complete));

    assert_eq!(
        fx.committed_value().await?,
        Some(Bytes::from_static(b"v2")),
        "the retried attempt's value wins"
    );
    assert_eq!(
        CellRead::<Values>::read(&fx.cell_store(), &fx.value_id(), extra.as_ref())
            .await?
            .0
            .into_inner(),
        Some(Bytes::from_static(b"w")),
        "the retry's extra cell commits with the rest of its stage"
    );
    assert!(
        fx.cells.unsettled_marker_of(&fx.value_id()).is_none(),
        "the settle deleted the single (overwritten) event marker"
    );
    Ok(())
}
