use super::*;

// --- Deque parity -----------------------------------------------------------

/// One deque op against the erased seam.
#[derive(Clone, Debug)]
enum DequeOp {
    PushBack,
    PushFront,
    PopFront,
    PopBack,
    Clear,
    Scan,
    Commit,
    Rollback,
}

impl Arbitrary for DequeOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 8 {
            0 => DequeOp::PushBack,
            1 => DequeOp::PushFront,
            2 => DequeOp::PopFront,
            3 => DequeOp::PopBack,
            4 => DequeOp::Clear,
            5 => DequeOp::Scan,
            6 => DequeOp::Commit,
            _ => DequeOp::Rollback,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            DequeOp::PushBack | DequeOp::PushFront => Box::new(once(DequeOp::PopFront)),
            _ => empty_shrinker(),
        }
    }
}

#[derive(Clone, Debug)]
struct DequeTrace(Vec<DequeOp>);

impl Arbitrary for DequeTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(
            Vec::<DequeOp>::arbitrary(g)
                .into_iter()
                .take(MAX_OPS)
                .collect(),
        )
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(DequeTrace))
    }
}

/// Asserts the erased deque's endpoint peeks agree with `visible.front()` /
/// `.back()`.
async fn assert_deque_peeks<P: ParityPayload>(
    handle: &BoxDequeState<P>,
    visible: &VecDeque<P>,
) -> Result<bool> {
    let front = handle
        .peek_front()
        .await
        .map_err(|e| eyre!("peek_front: {e}"))?;
    if !opt_same::<P>(front.as_ref(), visible.front()) {
        return Ok(false);
    }
    let back = handle
        .peek_back()
        .await
        .map_err(|e| eyre!("peek_back: {e}"))?;
    Ok(opt_same::<P>(back.as_ref(), visible.back()))
}

/// Drives a deque trace through the erased handle and a `(floor, visible)`
/// `VecDeque` model, asserting `len`, every positional `get`, the endpoint
/// peeks (`peek_front`/`peek_back` against `visible.front()`/`.back()`), and a
/// full forward scan agree with `visible` after every op. `visible` is the
/// read-your-writes deque; `floor` is the last committed snapshot. `commit`
/// promotes `visible` to `floor`; `rollback` reverts `visible` to `floor`. Both
/// are issued through the **erased** handle only — the typed handle shares the
/// overlay, so calling its commit would mask a no-op erased commit.
fn run_deque_parity<P>(ops: &[DequeOp]) -> Result<bool>
where
    P: ParityPayload + Send + Sync + 'static,
{
    executor::block_on(async {
        let ctx = parity_context::<P>()?;
        let handle = ctx
            .deque_state(DEQUE_NAME)
            .map_err(|e| eyre!("vend deque: {e}"))?;
        let mut floor: VecDeque<P> = VecDeque::new();
        let mut visible: VecDeque<P> = VecDeque::new();
        let mut sampler = Gen::new(8);
        for op in ops {
            match op {
                DequeOp::Scan => {}
                DequeOp::PushBack => {
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .push_back(value.clone())
                        .await
                        .map_err(|e| eyre!("erased push_back: {e}"))?;
                    visible.push_back(value);
                }
                DequeOp::PushFront => {
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .push_front(value.clone())
                        .await
                        .map_err(|e| eyre!("erased push_front: {e}"))?;
                    visible.push_front(value);
                }
                DequeOp::PopFront => {
                    let erased = handle
                        .pop_front()
                        .await
                        .map_err(|e| eyre!("pop_front: {e}"))?;
                    let expected = visible.pop_front();
                    if !opt_same::<P>(erased.as_ref(), expected.as_ref()) {
                        return Ok(false);
                    }
                }
                DequeOp::PopBack => {
                    let erased = handle
                        .pop_back()
                        .await
                        .map_err(|e| eyre!("pop_back: {e}"))?;
                    let expected = visible.pop_back();
                    if !opt_same::<P>(erased.as_ref(), expected.as_ref()) {
                        return Ok(false);
                    }
                }
                DequeOp::Clear => {
                    handle
                        .clear()
                        .await
                        .map_err(|e| eyre!("erased deque clear: {e}"))?;
                    visible.clear();
                }
                DequeOp::Commit => {
                    handle
                        .commit()
                        .await
                        .map_err(|e| eyre!("erased deque commit: {e}"))?;
                    floor = visible.clone();
                }
                DequeOp::Rollback => {
                    handle.rollback().await;
                    visible = floor.clone();
                }
            }
            let len = handle.len().await.map_err(|e| eyre!("deque len: {e}"))?;
            if len != visible.len()
                || handle
                    .is_empty()
                    .await
                    .map_err(|e| eyre!("is_empty: {e}"))?
                    != visible.is_empty()
            {
                return Ok(false);
            }
            for index in 0..visible.len() {
                let erased = handle
                    .get(index)
                    .await
                    .map_err(|e| eyre!("deque get: {e}"))?;
                if !opt_same::<P>(erased.as_ref(), visible.get(index)) {
                    return Ok(false);
                }
            }
            if !assert_deque_peeks(&handle, &visible).await? {
                return Ok(false);
            }
            let scanned = drain_cursor(&handle.scan(Direction::Forward)).await?;
            if scanned.len() != visible.len()
                || scanned
                    .iter()
                    .zip(visible.iter())
                    .any(|(a, b)| !P::same(a, b))
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Erased deque parity for `serde_json::Value`.
#[test]
fn prop_erased_deque_parity_json() {
    fn prop(DequeTrace(ops): DequeTrace) -> TestResult {
        match run_deque_parity::<Value>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("deque parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("deque trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(DequeTrace) -> TestResult);
}

/// Erased deque parity for `BinaryPayload`.
#[test]
fn prop_erased_deque_parity_binary() {
    fn prop(DequeTrace(ops): DequeTrace) -> TestResult {
        match run_deque_parity::<BinaryPayload>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("binary deque parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("binary deque trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(DequeTrace) -> TestResult);
}
