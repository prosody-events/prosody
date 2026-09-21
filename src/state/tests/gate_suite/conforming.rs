//! Valid operations within one attempt preserve access.

use super::*;

/// A conforming within-attempt op for the conforming-handler property.
#[derive(Clone, Debug)]
pub(super) enum ValueOp {
    Get,
    Set(i64),
    Clear,
    Commit,
    Rollback,
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => ValueOp::Get,
            1 => ValueOp::Set(i64::from(u8::arbitrary(g))),
            2 => ValueOp::Clear,
            3 => ValueOp::Commit,
            _ => ValueOp::Rollback,
        }
    }
}

/// A conforming handler — every op issued within its own attempt, including a
/// scan-then-rollback-then-rescan — NEVER observes the fence. The epoch is
/// stable within one attempt, so the pin always matches; any op that errored
/// `Terminated` would surface here as a failed property (the `?` propagates
/// it). The subject is called for every generated op, so this is a real
/// property, not a tautology. Red-proven by inverting `ensure_live`'s pin
/// compare to `session.attempt_current()`: every conforming op then fences and
/// the property's `?` fails.
#[test]
pub(super) fn conforming_within_attempt_never_fenced() {
    fn property(ops: Vec<ValueOp>) -> Result<bool> {
        runtime()?.block_on(async {
            let fx = GateFixture::new("fence_conforming")?;
            let session = fx.session(1);
            let value = value_state::<JsonCodec>("v")
                .bind(&session)
                .map_err(|e| eyre!("bind v: {e}"))?;
            for op in ops {
                match op {
                    ValueOp::Get => {
                        value.get().await.map_err(|e| eyre!("get: {e}"))?;
                    }
                    ValueOp::Set(n) => {
                        value
                            .set(Value::from(n))
                            .await
                            .map_err(|e| eyre!("set: {e}"))?;
                    }
                    ValueOp::Clear => {
                        value.clear().await.map_err(|e| eyre!("clear: {e}"))?;
                    }
                    ValueOp::Commit => {
                        value.commit().await.map_err(|e| eyre!("commit: {e}"))?;
                    }
                    ValueOp::Rollback => {
                        // Infallible; within an attempt it is Applied/NoOp,
                        // never fenced.
                        let _ = value.rollback().await;
                    }
                }
            }
            // scan → rollback → rescan on a map, same attempt.
            let map = map_state::<I64KeyCodec, JsonCodec>("m")
                .bind(&session)
                .map_err(|e| eyre!("bind m: {e}"))?;
            for k in 0..3_i64 {
                map.set(&k, Value::from(k))
                    .await
                    .map_err(|e| eyre!("map set: {e}"))?;
            }
            {
                let stream = map.entries(KeyQuery::new(Direction::Forward));
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    item.map_err(|e| eyre!("scan: {e}"))?;
                }
            }
            let _ = map.rollback().await;
            {
                let stream = map.entries(KeyQuery::new(Direction::Forward));
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    item.map_err(|e| eyre!("rescan: {e}"))?;
                }
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(Vec<ValueOp>) -> Result<bool>);
}
