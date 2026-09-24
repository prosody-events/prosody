//! The generated operation traces of the transparency property.

use super::*;

/// The keys a transparency trace addresses (all in [`SECTION`]).
pub(super) const POOL: u8 = 5;

/// One op of a [`CacheTrace`]. The staged lifecycle is clean by construction
/// (`Stage` only when idle; `Commit`/`Abort`/`Promote` only when staged), so
/// any prefix is itself a valid trace — which is how `shrink` minimises.
#[derive(Clone, Debug)]
pub(super) enum CacheOp {
    /// `write_resolved` of `(key, value)` cells; `clear` erases [`SECTION`]
    /// with the written present cells as survivors.
    Write {
        cells: Vec<(u8, Option<u8>)>,
        clear: bool,
    },
    /// `write_provisional` of present-data writes under a frozen marker;
    /// `clear` freezes a [`SECTION`] clear into it.
    Stage { writes: Vec<(u8, u8)>, clear: bool },
    /// Promotes the unsettled stage through `commit_provisional`.
    Commit,
    /// `abort_provisional` of the unsettled stage.
    Abort,
    /// Resolves cells through the raw verb and leaves the marker unchanged.
    Promote,
    /// A point read of one pool key.
    Get(u8),
    /// A presence read of one pool key.
    Contains(u8),
    /// A full-section scan.
    Scan,
    /// Advance the fixed clock by N milliseconds (sub-second-grain, so the
    /// floor arithmetic is exercised).
    Advance(u16),
    /// Toggle the fjall publish fault seam.
    FaultPuts(bool),
    /// Adds removal failures below the cache-disablement limit.
    FaultDeletes(u8),
}

/// A generated cell-op trace with an optional per-trace collection TTL.
#[derive(Clone, Debug)]
pub(super) struct CacheTrace {
    pub(super) ttl: Option<u32>,
    pub(super) ops: Vec<CacheOp>,
}

impl Arbitrary for CacheTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ttl = if bool::arbitrary(g) {
            None
        } else {
            Some(1 + u32::from(u8::arbitrary(g) % 8))
        };
        let len = usize::arbitrary(g) % 20;
        let mut staged = false;
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            let roll = u8::arbitrary(g) % 16;
            // While a stage stands, only reads, clock movement, faults, and
            // the stage's own settle are legal — per-key serialization means
            // no handler write can interleave a stage and its settle, and the
            // settlement cache update argument (the staged rows still hold the verdict's
            // data when commit_provisional runs) rests on exactly that.
            let op = match roll {
                0..=3 if !staged => {
                    let n = 1 + usize::arbitrary(g) % 3;
                    let cells = (0..n)
                        .map(|_| {
                            (
                                u8::arbitrary(g) % POOL,
                                bool::arbitrary(g).then(|| u8::arbitrary(g)),
                            )
                        })
                        .collect();
                    CacheOp::Write {
                        cells,
                        clear: u8::arbitrary(g) % 4 == 0,
                    }
                }
                0..=6 => {
                    if staged {
                        // Settle the unsettled stage.
                        staged = false;
                        match u8::arbitrary(g) % 3 {
                            0 => CacheOp::Commit,
                            1 => CacheOp::Abort,
                            _ => CacheOp::Promote,
                        }
                    } else {
                        staged = true;
                        // Distinct keys per stage (an event stages each cell
                        // at most once).
                        let mut keys: Vec<u8> = (0..POOL).collect();
                        let n = 1 + usize::arbitrary(g) % 3;
                        let mut writes = Vec::with_capacity(n);
                        for _ in 0..n {
                            let i = usize::arbitrary(g) % keys.len();
                            writes.push((keys.swap_remove(i), u8::arbitrary(g)));
                        }
                        CacheOp::Stage {
                            writes,
                            clear: u8::arbitrary(g) % 4 == 0,
                        }
                    }
                }
                7..=8 => CacheOp::Get(u8::arbitrary(g) % POOL),
                9..=10 => CacheOp::Contains(u8::arbitrary(g) % POOL),
                11 => CacheOp::Scan,
                12..=13 => CacheOp::Advance(u16::arbitrary(g) % 12_000),
                14 => CacheOp::FaultPuts(bool::arbitrary(g)),
                _ => CacheOp::FaultDeletes(1 + u8::arbitrary(g) % 2),
            };
            ops.push(op);
        }
        Self { ttl, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ttl = self.ttl;
        let ops = self.ops.clone();
        // A prefix of a clean-lifecycle trace is itself clean, so truncation
        // is a safe shrink; also try dropping the TTL entirely.
        let prefixes = (0..ops.len()).map(move |n| Self {
            ttl,
            ops: ops[..n].to_vec(),
        });
        let drop_ttl = self
            .ttl
            .map(|_| Self {
                ttl: None,
                ops: self.ops.clone(),
            })
            .into_iter();
        Box::new(drop_ttl.chain(prefixes))
    }
}
