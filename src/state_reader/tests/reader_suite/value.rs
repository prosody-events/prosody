//! Committed value reads and recovery evidence.

use super::*;
use std::iter::empty;

/// A value trace operation. The reader observes the last committed `Set`, or
/// `None` before the first `Set` and after a `Clear`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ValueOp {
    /// Overwrite the committed value with `Value::from(b)`.
    Set(u8),
    /// Clear the committed value.
    Clear,
    /// Raw residue with a generated value, verdict, clear, and evidence
    /// location.
    Residue(u8, u8),
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            _ => Self::Residue(u8::arbitrary(g), u8::arbitrary(g) % 8),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set(b) => Box::new(b.shrink().map(Self::Set)),
            Self::Clear => Box::new(empty()),
            Self::Residue(b, mode) => {
                Box::new((b, mode).shrink().map(|(b, mode)| Self::Residue(b, mode)))
            }
        }
    }
}

/// Drives a Value trace: commit each event, mirror it into an `Option<Value>`
/// model, and after every event assert `reader.get(key)` equals the model and
/// `reader.contains(key)` reports whether the model holds a value.
///
/// FALSIFICATION: perturb `ReadSession::collection_id_for` (session.rs) to bind
/// the wrong partition/state-type → the point `get` reads an empty/foreign
/// collection → mismatch on the first committed event.
pub(in crate::state_reader::tests) async fn run_reader_value_trace<B: ReaderBackend>(
    backend: &B,
    descriptor: ValueDescriptor<JsonCodec>,
    case: &ReaderCase<'_>,
    trace: Trace<ValueOp>,
) -> Result<bool> {
    let registry = backend.registry();
    let state_key = seed_source(backend, descriptor, case).await?;

    let mut model: Option<Value> = None;
    for (index, ops) in trace.events_ops().enumerate() {
        let staged: Vec<ValueOp> = ops.to_vec();
        let for_handle = staged.clone();
        owner_commit_cell(
            backend.owner_cell(),
            &registry,
            &state_key,
            descriptor,
            index as u128,
            move |handle| async move {
                for op in for_handle {
                    match op {
                        ValueOp::Set(b) => handle
                            .set(Value::from(b))
                            .await
                            .map_err(|e| eyre!("set: {e}"))?,
                        ValueOp::Clear => handle.clear().await.map_err(|e| eyre!("clear: {e}"))?,
                        ValueOp::Residue(..) => {}
                    }
                }
                Ok(())
            },
        )
        .await?;
        for (op_index, op) in staged.into_iter().enumerate() {
            match op {
                ValueOp::Set(b) => model = Some(Value::from(b)),
                ValueOp::Clear => model = None,
                ValueOp::Residue(value, mode) => {
                    if !reader_residue(
                        backend.owner_cell(),
                        backend.deps().backend().cells(),
                        &state_key,
                        (index * 1000 + op_index) as u128,
                        value,
                        mode,
                    )
                    .await?
                    {
                        return Ok(false);
                    }
                }
            }
        }

        let deps = backend.deps();
        let reader = StateReader::new(&deps, case.sub.clone(), descriptor)?;
        if reader.get(case.key.clone()).await? != model
            || reader.contains(case.key.clone()).await? != model.is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}
