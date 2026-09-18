use super::*;

#[tokio::test]
async fn leaked_handle_reads_in_window_then_terminated_after_teardown() -> Result<()> {
    let (context, _cell_store, _floor_id, _pending_id) = two_collections().await?;
    let (g, committed, _aborted) = guard();
    // A current-pin handle leaked past the hook: settle never bumps the
    // epoch, so this clone stays current through settlement.
    let leaked: Handle = handle(&context, FLOOR)?;
    let session = context.test_lifecycle().map_err(|e| eyre!("bind: {e}"))?;

    let probe = ViewProbe::<AsFinal>::new();
    // Graceful window read happens inside the probe's hook (floor == base).
    settle(&probe, context, g, Ok(0)).await;
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    let obs = probe.observation().ok_or_else(|| eyre!("hook fires"))?;
    assert_eq!(
        obs.floor,
        Some(Ok(Some(json!("floor")))),
        "the hook-window read returns committed data (graceful completion)",
    );

    // Before teardown the leaked current-pin handle still reads committed.
    assert_eq!(
        leaked.get().await.map_err(|e| e.to_string()),
        Ok(Some(json!("floor"))),
        "a current-pin read is admitted before teardown",
    );

    // Teardown: the scope's Drop flips termination synchronously.
    session.terminate();

    match leaked.get().await {
        Err(CellStateError::Access(StateAccessError::Terminated)) => Ok(()),
        other => bail!("a post-teardown read must error Terminated, got {other:?}"),
    }
}

/// Current-pin precedence under shutdown: after settle closes the gate,
/// a current-pin mutation errors `SessionClosed` even when the session is
/// also terminated, because `mutate_permit` checks the closed gate before
/// the termination watch. The stale-pin `Terminated`-not-`SessionClosed`
/// half is pinned in
/// `gate_suite::stale_mutator_on_closed_session_is_terminated_not_closed`.
/// Falsify by swapping the closed/termination order in `mutate_permit`: the
/// current-pin mutation then hits the termination check first and errors
/// `Terminated`. (Swapping pin/closed is inert here — the handle is
/// current-pin, so the pin check never fires; that ordering is the
/// stale-pin sibling's to pin, above.)
#[tokio::test]
async fn current_pin_hook_mutation_is_closed_even_under_shutdown() -> Result<()> {
    let (context, _cell_store, _floor_id, _pending_id) = two_collections().await?;
    let leaked: Handle = handle(&context, PENDING)?;
    let session = context.test_lifecycle().map_err(|e| eyre!("bind: {e}"))?;
    // Close the gate the way settle does (acquire, mark Closed, drop the
    // permit before the mutation), then flip termination: the gate stays
    // Closed AND is_terminated() is true, current pin.
    let permit = session.close_gate().await;
    drop(permit);
    session.terminate();
    match leaked.set(json!("x")).await {
        Err(CellStateError::Access(StateAccessError::SessionClosed)) => Ok(()),
        other => bail!("closed-before-terminated precedence broke: {other:?}"),
    }
}
