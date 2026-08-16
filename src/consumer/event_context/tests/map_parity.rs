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
            // A forward scan must yield exactly the visible key-ordered entries.
            let scanned = drain_cursor(&handle.scan(Direction::Forward)).await?;
            let expected: Vec<(String, P)> = visible
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if scanned.len() != expected.len()
                || scanned
                    .iter()
                    .zip(&expected)
                    .any(|((sk, sv), (ek, ev))| sk != ek || !P::same(sv, ev))
            {
                return Ok(false);
            }
            // The key-only cursor is the value-free twin of the scan: it must
            // yield exactly the same visible keys in the same key order,
            // exercised here through the erased seam.
            let scanned_keys = drain_cursor(&handle.keys(Direction::Forward)).await?;
            let expected_keys: Vec<String> = visible.keys().cloned().collect();
            if scanned_keys != expected_keys {
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
