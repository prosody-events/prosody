//! Parked writes check the attempt fence before staging.

use super::*;

/// Journal atomicity at the invocation's final fence: a `set` that reaches
/// **both** of its stages and only then meets a terminated session stages
/// nothing at all — neither the entry nor the keyset write reaches the event
/// overlay.
///
/// The schedule parks the invocation inside its cold keyset read, which sits
/// *below* the read's own liveness guard, so the read still returns `Ok` and
/// the body runs to completion holding write admission. The control run — the
/// identical schedule without the termination — proves the park point is really
/// past both stages: it leaves exactly two staged cells. Red-proven by moving
/// the journal replay above the final validation in `WriteOperation::merge`:
/// the fenced run then stages the same two cells the control does.
#[test]
pub(super) fn map_set_fenced_at_the_final_check_stages_nothing() -> Result<()> {
    runtime()?.block_on(async {
        let control = parked_set("fence_journal_control", false).await?;
        assert!(control.outcome.is_ok(), "the control set must succeed");
        assert_eq!(
            control.staged, 2,
            "the control stages the entry write and the keyset write"
        );

        let fenced = parked_set("fence_journal_fenced", true).await?;
        match &fenced.outcome {
            Err(error) if map_item_terminated(error) => {}
            other => bail!(
                "the fenced set must report Terminated, got ok={}",
                other.is_ok()
            ),
        }
        assert_eq!(
            fenced.staged, 0,
            "a fenced invocation replays nothing: both staged mutations are discarded"
        );
        assert_eq!(
            fenced.cleared, 0,
            "a fenced invocation stages no section clear either"
        );
        Ok(())
    })
}

/// What one parked-`set` run produced: the call's outcome and what its event
/// overlay holds afterwards, read straight off the dirty store.
pub(super) struct ParkedSet {
    pub(super) outcome: Result<(), MapStateError<JsonCodecError>>,
    pub(super) staged: usize,
    pub(super) cleared: usize,
}

/// Seeds a two-key tracked map cold, parks a `set` of a fresh key inside its
/// keyset read, optionally terminates the session while it is parked, and
/// reports what the invocation left behind.
pub(super) async fn parked_set(name: &str, terminate: bool) -> Result<ParkedSet> {
    let fx = GateFixture::new(name)?;
    let id = fx.id("m")?;
    let cref = CollectionRef::new(id.clone(), None);
    let mut seed = vec![(
        map::keyset_cell(),
        Some(Bytes::from(tracked_frame(&[1, 2]))),
    )];
    for k in 1..=2_i64 {
        seed.push((
            map::entry_cell_for(&I64KeyCodec::encode(&k)),
            Some(json_entry(k)?),
        ));
    }
    fx.counting.write_resolved(&cref, &seed, &[]).await?;

    let dirty: Arc<DirtyStore> = Arc::default();
    let session = fx.session_with_dirty(1, dirty.clone());
    let map = map_state::<I64KeyCodec, JsonCodec>("m")
        .bind(&session)
        .map_err(|e| eyre!("bind: {e}"))?;

    // Park in the keyset read's cold cache-fill: past the read's liveness
    // guard, holding write admission, with both stages still ahead.
    fx.holds.read().arm(1);
    let writer = tokio::spawn({
        let map = map.clone();
        async move { map.set(&9, Value::from(9_i64)).await }
    });
    timeout(HANG_GUARD, fx.holds.read().entered())
        .await
        .map_err(|_| eyre!("the set never reached the keyset-read hold"))?;
    if terminate {
        session.terminate();
    }
    fx.holds.read().release();
    let outcome = timeout(HANG_GUARD, writer)
        .await
        .map_err(|_| eyre!("the set hung"))??;

    Ok(ParkedSet {
        outcome,
        staged: dirty.collection_snapshot(&id).len(),
        cleared: dirty.cleared_sections(&id).len(),
    })
}
