//! Errors and dropped futures release session admission.

use super::*;
use crate::state::query::tests::query_buffer;

/// The error-yield gate-release pin (map): a `Tracked` map stream whose entry
/// holds undecodable bytes yields `Err` — and MUST release the session gate
/// before that yield reaches user code. Otherwise a caller that catches the
/// error and, with the stream still alive, issues another op on the same
/// session deadlocks: the suspended generator holds the gate the next op waits
/// on (the chunked-stream contract on `SessionGate` — the gate is never held
/// across a yield to user code, error items included). Mechanism under
/// chunking: the corrupt bytes **fetch AND fail to decode under the chunk
/// permit**; the permit still dies with the chunk future's scope before the
/// forwarding loop's `chunk?` yields the `Err`, so the gate is released before
/// the `Err` reaches user code.
///
/// A failing chunk yields none of its items. Valid key 3 precedes corrupt
/// key 7 in one chunk. If the chunk emits items before projection completes,
/// key 3 appears first and fails `first.is_err()`. If the stream holds the
/// permit across the error yield, the next `get` cannot acquire the gate.
#[test]
pub(super) fn map_stream_error_yield_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_map_stream_error")?;
        let id = fx.id("m")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Valid key 3 precedes corrupt key 7 in the same tracked chunk.
        // The chunk must emit only the error from key 7.
        let valid = 3_i64;
        let corrupt = 7_i64;
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[valid, corrupt]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&valid)),
                        Some(Bytes::from_static(b"null")),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&corrupt)),
                        Some(Bytes::from_static(b"\x00\x01\x02 not json")),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        let stream = handle.entries(query_buffer()).stream();
        futures::pin_mut!(stream);
        // Projection fails at key 7 before the chunk emits any item.
        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended without yielding the decode error"))?;
        assert!(
            first.is_err(),
            "the failing chunk must emit only the error from key 7"
        );

        // The stream is still alive (held in scope, not dropped). A follow-up op
        // on the same session must not be starved by a gate the suspended stream
        // holds: post-fix the permit was released before the Err yield, so this
        // completes; pre-fix it parks forever and the guard trips.
        let absent = timeout(HANG_GUARD, handle.get(&999))
            .await
            .map_err(|_| {
                eyre!(
                    "a session op after a stream Err hung: the stream held the gate across the \
                     error yield"
                )
            })?
            .map_err(|e| eyre!("probe get: {e}"))?;
        assert!(absent.is_none(), "the probe key is absent");
        Ok(())
    })
}

/// The error-yield gate-release pin (deque twin of
/// [`map_stream_error_yield_releases_the_gate`]): a point-get deque stream
/// whose element holds undecodable bytes yields `Err` and MUST release the gate
/// before the yield. Same mechanism (corrupt bytes fetch AND fail to decode
/// under the chunk permit, which dies with the chunk future before `chunk?`
/// yields the `Err`). Valid index 0 precedes corrupt index 1 in one chunk.
/// A failing chunk yields none of its items. If index 0 appears first, the
/// assertion fails.
#[test]
pub(super) fn deque_stream_error_yield_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_deque_stream_error")?;
        let id = fx.id("d")?;
        let dref = CollectionRef::new(id.clone(), None);

        // Valid index 0 precedes corrupt index 1 in the same chunk.
        // The chunk must emit only the error from index 1.
        fx.counting
            .write_resolved(
                &dref,
                &[
                    (
                        deque::meta_cell(),
                        Some(Bytes::from(deque::seed_frame(0, 2))),
                    ),
                    (
                        deque::entry_cell_for(&I64KeyCodec::encode(&0)),
                        Some(Bytes::from_static(b"null")),
                    ),
                    (
                        deque::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(Bytes::from_static(b"\x00\x01\x02 not json")),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = deque_state::<JsonCodec>("d")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        let stream = handle.values().stream();
        futures::pin_mut!(stream);
        // Projection fails at index 1 before the chunk emits any item.
        let first = stream
            .next()
            .await
            .ok_or_else(|| eyre!("the stream ended without yielding the decode error"))?;
        assert!(
            first.is_err(),
            "the failing chunk must emit only the error from index 1"
        );

        // The stream is still alive: a follow-up op must not be starved.
        let empty = timeout(HANG_GUARD, handle.is_empty())
            .await
            .map_err(|_| {
                eyre!(
                    "a session op after a stream Err hung: the stream held the gate across the \
                     error yield"
                )
            })?
            .map_err(|e| eyre!("probe is_empty: {e}"))?;
        assert!(!empty, "the seeded window is non-empty");
        Ok(())
    })
}

/// The cancel-safety pin (the futurelock posture's safe half): dropping a
/// session-op future — while it HOLDS the gate, and while it is QUEUED on it —
/// releases the gate, so the next op and the settle acquire both proceed. The
/// hang-guard deadline is exactly that — a hang-guard, never the assertion.
#[test]
pub(super) fn dropped_session_op_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_cancel")?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Drop a HOLDING op: a get parked in its withheld fill, gate held.
        fx.holds.read().arm(1);
        let holding = tokio::spawn({
            let handle = handle.clone();
            async move { handle.get().await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the holding op never reached its hold"))?;
        holding.abort();
        assert!(
            holding.await.is_err(),
            "the holding op was dropped mid-gate"
        );

        // The next op proceeds.
        timeout(HANG_GUARD, handle.set(Value::from(1_i64)))
            .await
            .map_err(|_| eyre!("hang-guard: the gate was not released by the drop"))?
            .map_err(|e| eyre!("set: {e}"))?;

        // Drop a QUEUED op: A holds (withheld), B queues, B is dropped, A
        // completes, and the settle acquire still proceeds. A holds via a map
        // key the session never buffered — the value cell's set above would
        // answer from the dirty overlay and never reach the withheld lower
        // read.
        let map = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        fx.holds.read().arm(1);
        let holding = tokio::spawn({
            let map = map.clone();
            async move { map.get(&42).await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the second holding op never reached its hold"))?;
        let queued = tokio::spawn({
            let handle = handle.clone();
            async move { handle.get().await }
        });
        let_task_park().await;
        queued.abort();
        assert!(queued.await.is_err(), "the queued op was dropped");
        fx.holds.read().release();
        timeout(HANG_GUARD, holding)
            .await
            .map_err(|_| eyre!("the holding op hung"))??
            .map_err(|e| eyre!("get: {e}"))?;

        // The settle acquire proceeds (the drop-is-safe futurelock half).
        let permit = timeout(HANG_GUARD, session.close_gate())
            .await
            .map_err(|_| eyre!("hang-guard: settle's close never acquired the gate"))?;
        drop(permit);
        Ok(())
    })
}

/// The chunk-fetch cancellation pin (the stream cousin of
/// [`dropped_session_op_releases_the_gate`]): a stream whose chunk fetch parks
/// holding the gate is dropped — while it HOLDS the gate, and while a second op
/// is QUEUED behind it — and the RAII permit is released, so the next op and
/// settle's close acquire both proceed. Drop-releases-via-RAII is green by
/// construction; the falsification that guards it is detaching the chunk fetch
/// into a `tokio::spawn` (the design's forbidden detachment) so an abort of the
/// stream task cannot cancel the fetch — then the gate stays held and the
/// follow-up op's hang-guard trips.
#[test]
pub(super) fn dropped_stream_chunk_fetch_releases_the_gate() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_stream_cancel")?;
        // Seed a small deque window cold, valued by index.
        let id = fx.id("d")?;
        let dref = CollectionRef::new(id.clone(), None);
        let mut seeded = vec![(
            deque::meta_cell(),
            Some(Bytes::from(deque::seed_frame(0, 3))),
        )];
        for i in 0..3_i64 {
            seeded.push((
                deque::entry_cell_for(&I64KeyCodec::encode(&i)),
                Some(Bytes::from(serde_json::to_vec(&Value::from(i))?)),
            ));
        }
        fx.counting.write_resolved(&dref, &seeded, &[]).await?;

        let session = fx.session(1);
        let handle = deque_state::<JsonCodec>("d")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        // Warm the bounds cell so the armed hold lands on the first ENTRY read —
        // i.e. inside a chunk fetch, holding the gate.
        assert_eq!(handle.len().await.map_err(|e| eyre!("{e}"))?, 3);

        // Drop a HOLDING stream: its chunk fetch parks in the withheld entry
        // read, gate held.
        fx.holds.read().arm(1);
        let stream_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                let stream = handle.values().stream();
                futures::pin_mut!(stream);
                let _ = stream.next().await;
            }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the chunk fetch never reached its hold"))?;
        stream_task.abort();
        assert!(
            stream_task.await.is_err(),
            "the stream was dropped mid-chunk-fetch"
        );

        // The next op proceeds — the dropped generator released the gate.
        timeout(HANG_GUARD, handle.is_empty())
            .await
            .map_err(|_| {
                eyre!("hang-guard: the chunk fetch's permit was not released by the drop")
            })?
            .map_err(|e| eyre!("is_empty: {e}"))?;

        // Drop a QUEUED next(): A (a chunk fetch) holds, a queued op B waits, B
        // is dropped, A completes, and settle's close still proceeds.
        fx.holds.read().arm(1);
        let holding = tokio::spawn({
            let handle = handle.clone();
            async move {
                let stream = handle.values().stream();
                futures::pin_mut!(stream);
                stream.next().await.transpose()
            }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the second chunk fetch never reached its hold"))?;
        let queued = tokio::spawn({
            let handle = handle.clone();
            async move { handle.len().await }
        });
        let_task_park().await;
        queued.abort();
        assert!(queued.await.is_err(), "the queued op was dropped");
        fx.holds.read().release();
        timeout(HANG_GUARD, holding)
            .await
            .map_err(|_| eyre!("the holding stream hung"))??
            .map_err(|e| eyre!("stream: {e}"))?;

        // Settle's close acquire proceeds (the drop-is-safe half).
        let permit = timeout(HANG_GUARD, session.close_gate())
            .await
            .map_err(|_| eyre!("hang-guard: settle's close never acquired the gate"))?;
        drop(permit);
        Ok(())
    })
}
