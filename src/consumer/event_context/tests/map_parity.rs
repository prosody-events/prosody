use super::*;

// --- Map parity -------------------------------------------------------------

/// One map op against the erased seam.
#[derive(Clone, Debug)]
enum MapOp {
    Get(usize),
    Set(usize),
    Remove(usize),
    Clear,
    Scan,
    Commit,
    Rollback,
}

impl MapOp {
    fn key(idx: usize) -> String {
        KEYS[idx % KEYS.len()].to_owned()
    }
}

impl Arbitrary for MapOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 7 {
            0 => MapOp::Get(usize::arbitrary(g)),
            1 => MapOp::Set(usize::arbitrary(g)),
            2 => MapOp::Remove(usize::arbitrary(g)),
            3 => MapOp::Clear,
            4 => MapOp::Scan,
            5 => MapOp::Commit,
            _ => MapOp::Rollback,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            MapOp::Set(i) => Box::new(once(MapOp::Remove(*i))),
            _ => empty_shrinker(),
        }
    }
}

#[derive(Clone, Debug)]
struct MapTrace(Vec<MapOp>);

impl Arbitrary for MapTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(
            Vec::<MapOp>::arbitrary(g)
                .into_iter()
                .take(MAX_OPS)
                .collect(),
        )
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(MapTrace))
    }
}

async fn assert_map_scans<P: ParityPayload>(
    handle: &BoxMapState<P>,
    visible: &BTreeMap<String, P>,
) -> Result<bool> {
    let scanned = drain_cursor(&handle.scan(KeyScanConfig::default())).await?;
    if scanned.len() != visible.len()
        || scanned
            .iter()
            .zip(visible)
            .any(|((key, value), (expected_key, expected_value))| {
                key != expected_key || !P::same(value, expected_value)
            })
    {
        return Ok(false);
    }
    if drain_cursor(&handle.keys(KeyScanConfig::default())).await?
        != visible.keys().cloned().collect::<Vec<_>>()
    {
        return Ok(false);
    }
    let config = KeyScanConfig {
        dir: Direction::Forward,
        limit: Some(NonZeroUsize::MIN),
        start: Bound::Included(KEYS[1].to_owned()),
        end: Bound::Included(KEYS[2].to_owned()),
    };
    let expected = visible
        .range(KEYS[1].to_owned()..=KEYS[2].to_owned())
        .take(1)
        .collect::<Vec<_>>();
    let constrained = drain_cursor(&handle.scan(config.clone())).await?;
    if constrained.len() != expected.len()
        || constrained
            .iter()
            .zip(expected)
            .any(|((key, value), (expected_key, expected_value))| {
                key != expected_key || !P::same(value, expected_value)
            })
    {
        return Ok(false);
    }
    Ok(drain_cursor(&handle.keys(config)).await?
        == visible
            .range(KEYS[1].to_owned()..=KEYS[2].to_owned())
            .take(1)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>())
}

/// Drives a map trace through the erased handle and a `(floor, visible)`
/// `BTreeMap` model, asserting after every op that each pooled key reads equal
/// (`get` and `contains_key` both) and a full forward scan yields exactly
/// `visible`'s key-ordered entries. `visible` is the read-your-writes map;
/// `floor` is the last committed snapshot. `commit` promotes `visible` to
/// `floor`; `rollback` reverts `visible` to `floor`. Both are issued through
/// the **erased** handle only — the typed handle shares the overlay, so
/// calling its commit would mask a no-op erased commit.
fn run_map_parity<P>(ops: &[MapOp]) -> Result<bool>
where
    P: ParityPayload + Send + Sync + 'static,
{
    executor::block_on(async {
        let ctx = parity_context::<P>()?;
        let handle = ctx
            .map_state(MAP_NAME)
            .map_err(|e| eyre!("vend map: {e}"))?;
        let mut floor: BTreeMap<String, P> = BTreeMap::new();
        let mut visible: BTreeMap<String, P> = BTreeMap::new();
        let mut sampler = Gen::new(8);
        for op in ops {
            match op {
                MapOp::Scan => {}
                MapOp::Get(i) => {
                    // Exercise the specific-key read path; the after-op sweep
                    // below verifies its result against the model.
                    let key = MapOp::key(*i);
                    let erased = handle
                        .get(key.clone())
                        .await
                        .map_err(|e| eyre!("erased map get: {e}"))?;
                    if !opt_same::<P>(erased.as_ref(), visible.get(&key)) {
                        return Ok(false);
                    }
                }
                MapOp::Set(i) => {
                    let key = MapOp::key(*i);
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .set(key.clone(), value.clone())
                        .await
                        .map_err(|e| eyre!("erased map set: {e}"))?;
                    visible.insert(key, value);
                }
                MapOp::Remove(i) => {
                    let key = MapOp::key(*i);
                    handle
                        .remove(key.clone())
                        .await
                        .map_err(|e| eyre!("erased map remove: {e}"))?;
                    visible.remove(&key);
                }
                MapOp::Clear => {
                    handle
                        .clear()
                        .await
                        .map_err(|e| eyre!("erased map clear: {e}"))?;
                    visible.clear();
                }
                MapOp::Commit => {
                    handle
                        .commit()
                        .await
                        .map_err(|e| eyre!("erased map commit: {e}"))?;
                    floor = visible.clone();
                }
                MapOp::Rollback => {
                    handle.rollback().await;
                    visible = floor.clone();
                }
            }
            if !assert_keys_visible(&handle, &visible).await? {
                return Ok(false);
            }
            // Batch reads preserve input positions, including duplicates and
            // an untracked key, and agree with the same visible-state model.
            let query = [KEYS[2], "absent", KEYS[0], KEYS[2]];
            let keys = query.map(str::to_owned).to_vec();
            let batch = handle
                .get_many(keys)
                .await
                .map_err(|e| eyre!("erased map get_many: {e}"))?;
            if batch.len() != query.len()
                || batch
                    .iter()
                    .zip(query)
                    .any(|(actual, key)| !opt_same::<P>(actual.as_ref(), visible.get(key)))
            {
                return Ok(false);
            }
            if !assert_map_scans(&handle, &visible).await? {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Erased map parity for `serde_json::Value`.
#[test]
fn prop_erased_map_parity_json() {
    fn prop(MapTrace(ops): MapTrace) -> TestResult {
        match run_map_parity::<Value>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("map parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("map trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(MapTrace) -> TestResult);
}

/// Erased map parity for `BinaryPayload`.
#[test]
fn prop_erased_map_parity_binary() {
    fn prop(MapTrace(ops): MapTrace) -> TestResult {
        match run_map_parity::<BinaryPayload>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("binary map parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("binary map trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(MapTrace) -> TestResult);
}
