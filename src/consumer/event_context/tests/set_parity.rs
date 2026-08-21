use super::*;

type SetOp = (u8, usize);

fn run_set_parity(ops: &[SetOp]) -> Result<bool> {
    executor::block_on(async {
        let context = parity_context::<Value>()?;
        let handle = context.set_state(SET_NAME)?;
        let mut floor = BTreeSet::new();
        let mut visible = BTreeSet::new();
        for &(operation, index) in ops.iter().take(MAX_OPS) {
            let key = KEYS[index % KEYS.len()].to_owned();
            match operation % 6 {
                0 => {
                    if handle.contains(key.clone()).await? != visible.contains(&key) {
                        return Ok(false);
                    }
                }
                1 => {
                    handle.insert(key.clone()).await?;
                    visible.insert(key);
                }
                2 => {
                    handle.remove(key.clone()).await?;
                    visible.remove(&key);
                }
                3 => {
                    handle.clear().await?;
                    visible.clear();
                }
                4 => {
                    handle.commit().await?;
                    floor = visible.clone();
                }
                _ => {
                    handle.rollback().await;
                    visible = floor.clone();
                }
            }
            for pooled in KEYS {
                if handle.contains((*pooled).to_owned()).await? != visible.contains(*pooled) {
                    return Ok(false);
                }
            }
            let query = [KEYS[2], "absent", KEYS[0], KEYS[2]];
            if handle.is_empty().await? != visible.is_empty()
                || handle
                    .contains_many(query.map(str::to_owned).to_vec())
                    .await?
                    != query.map(|member| visible.contains(member))
                || drain_cursor(&handle.keys(KeyScanConfig::default())).await?
                    != visible.iter().cloned().collect::<Vec<_>>()
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Erased set operations preserve the visible membership model.
/// FALSIFICATION: make operation 1 skip the erased insert call.
#[test]
fn prop_erased_set_parity() {
    fn prop(mut ops: Vec<SetOp>) -> TestResult {
        ops.truncate(MAX_OPS);
        match run_set_parity(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("set parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("set trace failed: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<SetOp>) -> TestResult);
}

#[test]
fn erased_set_parity() -> Result<()> {
    assert!(run_set_parity(&[(1, 1), (1, 0), (0, 0), (2, 0), (3, 0)])?);
    Ok(())
}
