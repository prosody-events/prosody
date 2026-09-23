//! Plan streams keep key order, stop at errors, and fence after their last
//! item.

use super::*;

/// Ordered-window invariant: a range plan yields its cells in key order under
/// every completion order of its concurrent resolutions. Its driver is a
/// `buffered` window, never `buffer_unordered`.
///
/// The seed count stays at or below [`SHARD_FANOUT_CONCURRENCY`]. Every
/// resolution therefore runs at the same time, and the release order is the
/// completion order.
#[test]
fn prop_range_plan_yields_key_order_under_any_release_order() {
    fn prop(order: ReleaseOrder) -> TestResult {
        let ReleaseOrder(order) = order;
        let expected: Vec<i64> = (0..order.len() as i64).collect();
        let debug = format!("release={order:?}");
        // One fresh current-thread runtime per iteration. The fixture's parked
        // count means something only when the collector and the releaser share
        // one thread.
        let runtime = match Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime,
            Err(e) => return TestResult::error(format!("runtime: {e}")),
        };
        match runtime.block_on(ranged_keys(&order)) {
            Ok(keys) if keys == expected => TestResult::passed(),
            Ok(keys) => TestResult::error(format!("out of order: {keys:?} for {debug}")),
            Err(e) => TestResult::error(format!("{debug}: {e:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ReleaseOrder) -> TestResult);
}

/// The strongest ordered-window case, pinned directly. The plan yields
/// ascending keys even when the test releases every resolution in fully
/// reversed order. A `buffer_unordered` regression fails this case hardest.
#[tokio::test(flavor = "current_thread")]
async fn range_plan_yields_key_order_under_full_reverse_release() -> Result<()> {
    let n = SHARD_FANOUT_CONCURRENCY;
    let reverse: Vec<usize> = (0..n).rev().collect();
    let keys = ranged_keys(&reverse).await?;
    let expected: Vec<i64> = (0..n as i64).collect();
    if keys != expected {
        return Err(eyre!(
            "a buffered range plan must yield ascending keys under reversed release; got {keys:?}"
        ));
    }
    Ok(())
}

/// Terminate-at-first-error. A range plan runs over three ascending cells, and
/// the middle payload does not decode. The plan yields the low cell, then the
/// decode error, then ends. It never produces the high cell past the error.
#[test]
fn range_plan_terminates_at_first_error() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let session = plain_session()?;
        let name = StateName::try_new(PLAIN_PROBE)?;
        // Ascending keys. The middle cell's bytes are not a valid `i64` frame.
        for (key, bytes) in [
            (-1_i64, [0_u8; 8].as_slice()),
            (0, b"not an i64".as_slice()),
            (5, [0_u8; 8].as_slice()),
        ] {
            let cell = CellKey {
                section: PlainLayout::CELLS.section(),
                coordinate: I64KeyCodec::encode(&key),
            };
            session
                .seed(StateType::Application, &name, &cell, Some(bytes))
                .await;
        }

        let cells = bind_plain(&session)?;
        let plan = cells
            .read(async |op| {
                op.range::<_, &[u8]>(
                    PlainLayout::CELLS,
                    Bound::Unbounded,
                    Direction::Forward,
                    Bound::Unbounded,
                )
            })
            .await;
        let stream = plan.projected::<Values>();
        futures::pin_mut!(stream);
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item);
        }
        match items.as_slice() {
            [Ok((key, value)), Err(CellStateError::Codec(_))] => {
                if *key != -1 || *value != 0 {
                    return Err(eyre!("unexpected first item: ({key}, {value})"));
                }
                Ok(())
            }
            _ => Err(eyre!(
                "expected the low cell then a codec error and nothing more; got {items:?}"
            )),
        }
    })
}

/// Every source fences exhaustion, including exhaustion caused by a result
/// limit. Reset after the last permitted item must produce `Terminated`.
/// The parent module covers empty coordinate plans separately.
#[tokio::test]
async fn plan_fences_after_its_last_item() -> Result<()> {
    for use_range in [true, false] {
        for limit in [Some(NonZeroUsize::MIN), None] {
            let session = plain_session()?;
            let cells = bind_plain(&session)?;
            cells
                .write(async |op| {
                    op.set(PlainLayout::CELLS.at(&7), 7)?;
                    op.set(PlainLayout::CELLS.at(&8), 8)
                })
                .await
                .map_err(|e| eyre!("seed: {e}"))?;

            let plan = cells
                .read(async |op| {
                    if use_range {
                        op.range::<_, &[u8]>(
                            PlainLayout::CELLS,
                            Bound::Unbounded,
                            Direction::Forward,
                            Bound::Unbounded,
                        )
                    } else {
                        op.coordinates::<_, &[u8]>(
                            PlainLayout::CELLS,
                            vec![I64KeyCodec::encode(&7), I64KeyCodec::encode(&8)],
                        )
                    }
                })
                .await;
            let plan = match limit {
                Some(limit) => plan.with_limit(Some(limit)),
                None => plan,
            };
            let stream = plan.projected::<Values>();
            futures::pin_mut!(stream);
            let count = limit.map_or(2, NonZeroUsize::get);
            for expected in 7..7 + i64::try_from(count)? {
                match stream.next().await {
                    Some(Ok((key, value))) if key == expected && value == expected => {}
                    other => return Err(eyre!("expected {expected}, got {other:?}")),
                }
            }

            session.reset(RepinProof::for_test()).await;
            match stream.next().await {
                Some(Err(CellStateError::Access(StateAccessError::Terminated))) => {}
                other => {
                    return Err(eyre!(
                        "the post-reset pull must be Terminated, got {other:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// One full resolve window runs concurrently. Every gated resolver of a
/// `get_many` parks before the test releases the first gate.
#[tokio::test(flavor = "current_thread")]
async fn get_many_resolves_full_window_concurrently() -> Result<()> {
    let n = RESOLVE_FANOUT;
    let ladder = Arc::new(GateLadder::new(n));
    let session = gate_session(ladder.clone())?;
    let cells = bind_gated(&session)?;
    // Seed all n as staged writes in the same session, with payload == key.
    // The batch read then answers from the overlay, so the pin isolates the
    // resolve schedule.
    seed_gated(&cells, n).await?;

    let keys: Vec<i64> = (0..n as i64).collect();
    let collector = async {
        cells
            .read(async |op| op.get_many(GatedLayout::CELLS, &keys).await)
            .await
            .map_err(|e| eyre!("get_many: {e}"))
    };
    // The join polls the collector first. The full window therefore parks
    // under the one buffered(RESOLVE_FANOUT) window before any release.
    let releaser = async {
        if ladder.parked() != n {
            bail!(
                "all {n} resolves must be in flight before release; parked = {}",
                ladder.parked()
            );
        }
        for idx in 0..n {
            ladder.release(idx);
        }
        Ok(())
    };
    // The deadline is a hang-guard, never the assertion.
    let (collected, outcome) = timeout(
        Duration::from_secs(30),
        Box::pin(async { tokio::join!(collector, releaser) }),
    )
    .await
    .map_err(|_| eyre!("resolve fan-out hung"))?;
    outcome?;
    let out = collected?;
    assert_eq!(out.len(), n, "aligned output");
    for (index, value) in out.iter().enumerate() {
        assert_eq!(
            *value,
            Some(index as i64),
            "position {index} resolved in order"
        );
    }
    Ok(())
}

/// Compile-time regression pin for the `-> impl Future + Send` desugar, which
/// guards against rustc #100013. Both managed plan drivers must stay `Send`,
/// because a collection's stream method returns one out of a `Send` future. A
/// source whose `Send` the compiler cannot prove fails to compile here.
#[test]
fn plan_streams_are_send() -> Result<()> {
    fn assert_send<T: Send>(_value: T) {}

    TEST_RUNTIME.block_on(async {
        let session = gate_session(Arc::new(GateLadder::new(0)))?;
        let cells = bind_gated(&session)?;
        let range = cells
            .read(async |op| {
                op.range::<_, &[u8]>(
                    GatedLayout::CELLS,
                    Bound::Unbounded,
                    Direction::Forward,
                    Bound::Unbounded,
                )
            })
            .await;
        assert_send(range.projected::<Values>());
        let points = cells
            .read(async |op| op.coordinates::<_, &[u8]>(GatedLayout::CELLS, Vec::new()))
            .await;
        assert_send(points.projected::<Values>());
        Ok(())
    })
}
