//! Focused probe tests for invariants the fault-script model does not express.

use super::*;

/// A mid-stream error after the scan has pinned a source terminates with
/// `Err`. There is no silent restart that would repeat or skip data. This test
/// needs the range-scan arm and a precise fault position. The fault script
/// behind [`prop_probe_and_pin`] has no mid-stream fault point.
///
/// Falsify: restart on a post-pin error. The reader would then yield a
/// duplicated prefix or swallow the error.
#[tokio::test]
async fn scan_midstream_error_propagates() -> Result<()> {
    let env = ScriptedEnv::new(deque_state::<JsonCodec>("d-mid"))?;
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    env.commit(GROUP_A, tp_a, &key, 1, |h| async move {
        for i in 0..SCAN_ARM_LEN {
            h.push_back(Value::from(i as i64))
                .await
                .map_err(|e| eyre!("push: {e}"))?;
        }
        Ok(())
    })
    .await?;
    // Yield exactly five present cells, then fault mid-stream.
    env.fault(GROUP_A, tp_a, &key, FaultPoint::AfterYields(5))?;
    env.publish(GROUP_A, tp_a).await;

    let reader = env.reader_eager()?;
    let items: Vec<Result<Value, StateReaderError>> =
        reader.values(key).stream().collect::<Vec<_>>().await;
    // Five yielded prefix elements, then an error terminates the stream.
    assert_eq!(items.len(), 6, "five-element prefix + terminating error");
    assert!(items[..5].iter().all(Result::is_ok), "prefix yielded");
    assert!(items[5].is_err(), "mid-stream error terminates");
    Ok(())
}

/// `get_many` returns `Err` when the lowest source errors and the next source
/// answers all `None`. The all-`None` buffer means source B holds none of the
/// batch's cells. Absence is not provable through a failed source. A point
/// read treats no data plus an error the same way. The deque fault script
/// never exercises this batch case, so it gets its own test. Source A, the
/// lowest, faults at open, and source B is admitted but empty.
///
/// Falsify: return the remembered all-`None` buffer instead of the error.
/// That would mask a transient store failure as a false absence for the
/// batch.
#[tokio::test]
async fn get_many_error_beats_all_none() -> Result<()> {
    let env = ScriptedEnv::new(map_state::<I64KeyCodec, JsonCodec>("m-err-none"))?;
    let key = Key::from("user-1");
    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // Source A, the lowest, faults at open. Source B is published but holds
    // none of the batch cells, so it answers an all-`None` buffer.
    env.fault(GROUP_A, tp_a, &key, FaultPoint::AtOpen)?;
    env.publish(GROUP_A, tp_a).await;
    env.publish(GROUP_B, tp_b).await;
    let reader = env.reader_eager()?;

    match reader.get_many(key, &[0, 1]).await {
        Err(error) if error.classify_error() == ErrorCategory::Transient => Ok(()),
        other => bail!("expected a Transient store error, got {other:?}"),
    }
}

/// A source that violates its contract answers a batch read with fewer values
/// than requested. The uncached batch path checks that alignment in every
/// build. The read fails instead of zipping the short buffer into a misaligned
/// answer. `CommittedCellSource` is a downstream trait, so a debug assertion
/// cannot hold this line in a release build.
///
/// Falsify: remove the length check from the uncached arm of `cached_batch`.
/// `get_many` then answers a two-cell batch with one value.
#[tokio::test]
async fn short_batch_buffer_fails_the_uncached_read() -> Result<()> {
    let env = ScriptedEnv::new(map_state::<I64KeyCodec, JsonCodec>("m-short-batch"))?;
    let key = Key::from("user-1");
    let tp_a = topic("topic-a");

    env.commit(GROUP_A, tp_a, &key, 1, |h| async move {
        h.set(&0, Value::from("A0"))
            .await
            .map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.publish(GROUP_A, tp_a).await;
    // Arm the fault after seeding: `commit` writes through the same source.
    env.fault(GROUP_A, tp_a, &key, FaultPoint::ShortBatch)?;
    let reader = env.reader_eager()?;

    match reader.get_many(key, &[0, 1]).await {
        Err(error) if error.classify_error() == ErrorCategory::Transient => {
            assert!(
                error.to_string().contains("batch read returned 1 answers"),
                "expected the alignment error, got {error}"
            );
            Ok(())
        }
        other => bail!("expected a Transient alignment error, got {other:?}"),
    }
}

/// The lowest-ordered source with any `Some` answers the entire `get_many`
/// batch. There is no per-cell splice from a different source. Source A, the
/// lowest, holds only key 0. Source B holds only key 1. The batch resolves
/// entirely from A, so key 1 reads A's `None` and never B's tagged value.
///
/// Falsify: splice per cell, filling each absent slot from the next source.
/// Key 1 would then carry B's value and the `None` assert goes red.
#[tokio::test]
async fn get_many_answers_from_one_source() -> Result<()> {
    let env = ScriptedEnv::new(map_state::<I64KeyCodec, JsonCodec>("m-coherent"))?;
    let key = Key::from("user-1");
    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A is the lowest source and holds only key 0; B is the decoy and holds
    // only key 1, tagged distinctly.
    env.commit(GROUP_A, tp_a, &key, 1, |h| async move {
        h.set(&0, Value::from("A0"))
            .await
            .map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.commit(GROUP_B, tp_b, &key, 2, |h| async move {
        h.set(&1, Value::from("B1"))
            .await
            .map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.publish(GROUP_A, tp_a).await;
    env.publish(GROUP_B, tp_b).await;
    let reader = env.reader_eager()?;

    let got = reader.get_many(key, &[0, 1]).await?;
    assert_eq!(
        got,
        vec![Some(Value::from("A0")), None],
        "the whole batch resolves from the lowest source A; B's key 1 is never spliced in"
    );
    Ok(())
}

/// The lowest-ordered source with data pins and answers the whole scan. This
/// test proves it through the source-call trace: the decoy source B is never
/// read, so its recorded read count stays zero. The deque's bounds read pins
/// source A, and every later read in the scan addresses that one pinned
/// source.
///
/// Falsify: reverse the snapshot's source-preference order, pinning the
/// highest source instead. Then B pins, its values answer the scan, and both
/// its read count and the value assert go red.
#[tokio::test]
async fn scan_reads_only_pinned_source() -> Result<()> {
    let env = ScriptedEnv::new(deque_state::<JsonCodec>("d-coherent"))?;
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // Both sources hold a full deque, wide enough for the range-scan arm. A
    // is the lowest source and must answer alone. B's values are tagged
    // distinctly so a splice would be visible.
    let mut segments = Vec::new();
    for (group, tp, base, event) in [
        (GROUP_A, tp_a, 0i64, 1u128),
        (GROUP_B, tp_b, 1000i64, 2u128),
    ] {
        let state_key = env
            .commit(group, tp, &key, event, move |h| async move {
                for i in 0..SCAN_ARM_LEN {
                    h.push_back(Value::from(base + i as i64))
                        .await
                        .map_err(|e| eyre!("push: {e}"))?;
                }
                Ok(())
            })
            .await?;
        segments.push(state_key.segment_id);
        env.publish(group, tp).await;
    }
    let (segment_a, segment_b) = (segments[0], segments[1]);

    let reader = env.reader_eager()?;

    let scanned: Vec<Value> = reader
        .values(key)
        .stream()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let expected: Vec<Value> = (0..SCAN_ARM_LEN).map(|i| Value::from(i as i64)).collect();
    assert_eq!(
        scanned, expected,
        "the whole scan resolves from the lowest source A"
    );
    assert!(env.cells.reads(segment_a) >= 1, "source A was scanned");
    assert_eq!(
        env.cells.reads(segment_b),
        0,
        "source B (decoy) was never opened"
    );
    Ok(())
}

/// Two reads on one reader overlap: they drive cell I/O concurrently, sharing
/// no admission.
///
/// The source holds every committed point read at a two-party meeting point.
/// Both reads must arrive before either returns. Serialized reads would park
/// the first read there forever. The deadline is only the hang guard, and the
/// meeting point is the assertion. The read cache is disabled, so its same-key
/// single flight cannot merge the two reads.
///
/// Falsify: route both reads through one shared admission. Take a single
/// session-wide permit around the read instead of one session per operation.
/// The second read then never reaches the meeting point, and the deadline
/// fires.
#[tokio::test]
async fn concurrent_reads_on_one_reader_overlap() -> Result<()> {
    let mut env = ScriptedEnv::new(
        value_state::<JsonCodec>("probe-concurrent").read_cache(ReadCachePolicy::Disabled),
    )?;
    env.cells.rendezvous(2);
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle
            .set(Value::from(7_i64))
            .await
            .map_err(|e| eyre!("set: {e}"))?;
        Ok(())
    })
    .await?;
    env.publish(GROUP_A, tp).await;
    let reader = env.reader_eager()?;

    let both = timeout(Duration::from_secs(30), async {
        tokio::join!(reader.get(key.clone()), reader.get(key.clone()))
    })
    .await
    .map_err(|_| eyre!("the two reads never met: they did not overlap"))?;

    assert_eq!(
        both.0.map_err(|e| eyre!("first read: {e}"))?,
        Some(Value::from(7_i64)),
        "the first concurrent read"
    );
    assert_eq!(
        both.1.map_err(|e| eyre!("second read: {e}"))?,
        Some(Value::from(7_i64)),
        "the second concurrent read"
    );
    Ok(())
}

/// One session selects its source once. The first scoped operation probes for
/// a source and publishes its selection onto the session. Every later
/// operation on that session starts from that selection and addresses it
/// directly. No source below it is probed again.
///
/// The lowest source is published but holds nothing. That makes a second
/// probe observable. The probe must read the empty source and get `None`
/// before it reaches the source that answers. A second probe therefore shows
/// up as a second read of the empty source.
///
/// Falsify: have the reader engine start each invocation unselected, or stop
/// publishing the first selection back to the session. The second `get` then
/// re-probes and the empty source's read count rises to two.
#[tokio::test]
async fn one_session_selects_its_source_once() -> Result<()> {
    /// Sorts below [`GROUP_A`], so this source is probed first.
    const EMPTY_GROUP: &str = "group-000";

    let env = ScriptedEnv::new(
        value_state::<JsonCodec>("probe-select").read_cache(ReadCachePolicy::Disabled),
    )?;
    let key = Key::from("user-1");
    let tp_empty = topic("topic-empty");
    let tp_a = topic("topic-a");

    let empty = source_state_key(tp_empty, EMPTY_GROUP, &key, env.count)?.segment_id;
    env.publish(EMPTY_GROUP, tp_empty).await;
    let answering = env
        .commit(GROUP_A, tp_a, &key, 1, |handle| async move {
            handle
                .set(Value::from("A"))
                .await
                .map_err(|e| eyre!("set: {e}"))
        })
        .await?
        .segment_id;
    env.publish(GROUP_A, tp_a).await;

    let reader = env.reader_eager()?;
    let session = reader.session(key).await?;
    let handle = env.descriptor.bind(&session)?;

    assert_eq!(
        handle.get().await.map_err(|e| eyre!("first read: {e}"))?,
        Some(Value::from("A")),
        "the first source with data answers the first read"
    );
    assert_eq!(
        (env.cells.reads(empty), env.cells.reads(answering)),
        (1, 1),
        "the probe read the empty source, then the one that answered"
    );

    assert_eq!(
        handle.get().await.map_err(|e| eyre!("second read: {e}"))?,
        Some(Value::from("A")),
        "the selected source answers the second read"
    );
    assert_eq!(
        (env.cells.reads(empty), env.cells.reads(answering)),
        (1, 2),
        "the second read addressed the selection without probing again"
    );
    Ok(())
}
