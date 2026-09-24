//! Replays a transparency trace against the cached and uncached stores.

use super::*;

/// An unsettled stage's replay bookkeeping.
pub(super) struct Staged {
    writes: Vec<(CellKey, ProvisionalWrite)>,
    clears: Vec<SectionClear>,
}

/// The cache knowledge and expiry for each modeled cell.
pub(super) type WarmModel = HashMap<u8, (u64, CacheEntry<Bytes>)>;

/// The one-shot replay state of [`prop_cached_is_transparent`], carried
/// through every op and the per-op verification passes.
pub(super) struct Replay {
    pub(super) subject: Cached<TtlAwareCellStore<MemoryCellStore>>,
    pub(super) twin: MemoryCellStore,
    pub(super) counting: CountingCellStore<MemoryCellStore>,
    pub(super) id: CollectionId,
    pub(super) cref: CollectionRef,
    pub(super) twin_ref: CollectionRef,
    pub(super) now: Arc<AtomicU64>,
    pub(super) ttl_ms: Option<u64>,
    pub(super) clock: u64,
    pub(super) stage_seq: u128,
    pub(super) staged: Option<Staged>,
    pub(super) fault_puts: bool,
    pub(super) warm: WarmModel,
    pub(super) fail_puts: Arc<AtomicBool>,
    pub(super) fail_deletes: Arc<AtomicU64>,
}

impl Replay {
    /// The modeled fjall expiry of a write-through landing now.
    fn write_expiry(&self) -> u64 {
        self.ttl_ms
            .map_or(u64::MAX, |ttl| (self.clock - self.clock % 1_000) + ttl)
    }

    /// Whether the warm model says `key` is a live hit at the current clock.
    pub(super) fn is_warm<P: Projection>(&self, key: u8) -> bool {
        self.warm.get(&key).is_some_and(|(expiry, entry)| {
            (*expiry == u64::MAX || self.clock < *expiry)
                && !matches!(P::from_cached(entry.clone()), Read::Unknown)
        })
    }

    /// Publish-through model update: the batch either warms every cell (a
    /// clean atomic publish) or — under the puts fault — lands nothing and
    /// failed-publish cache guard deletes every cell.
    fn model_publish(&mut self, cells: impl IntoIterator<Item = (u8, Option<Bytes>)>) {
        let expiry = self.write_expiry();
        for (key, value) in cells {
            if self.fault_puts {
                self.warm.remove(&key);
            } else {
                self.warm.insert(key, (expiry, Values::into_cached(value)));
            }
        }
    }

    /// One `write_resolved` through both stores, updating the warm model.
    async fn step_write(&mut self, cells: &[(u8, Option<u8>)], clear: bool) -> Result<()> {
        let mut resolved: Vec<(CellKey, Option<Bytes>)> = Vec::new();
        let mut seen: HashSet<u8> = HashSet::new();
        for (key, value) in cells {
            // Last-writer-wins within one batch is the store's contract only
            // per distinct cell; keep cells distinct.
            if seen.insert(*key) {
                resolved.push((cell_at(*key), value.map(bytes)));
            }
        }
        let clears: Vec<SectionClear> = clear
            .then(|| SectionClear::frozen_resolved(SECTION, &resolved))
            .into_iter()
            .collect();
        self.subject
            .write_resolved(&self.cref, &resolved, &clears)
            .await
            .map_err(|e| eyre!("subject write: {e:?}"))?;
        self.twin
            .write_resolved(&self.twin_ref, &resolved, &clears)
            .await
            .map_err(|e| eyre!("twin write: {e:?}"))?;
        if !clears.is_empty() {
            // section-clear cache guard whole-section delete ran before the lower write.
            self.warm.clear();
        }
        self.model_publish(
            resolved
                .iter()
                .map(|(cell, value)| (cell.coordinate.as_bytes()[0], value.clone())),
        );
        Ok(())
    }

    /// Stages cells through both stores and updates the warm model.
    async fn step_stage(&mut self, writes: &[(u8, u8)], clear: bool) -> Result<()> {
        self.stage_seq += 1;
        let event = probe(self.stage_seq);
        let mut staged: Vec<(CellKey, ProvisionalWrite)> = Vec::new();
        for (key, value) in writes {
            // The committed base read, off the twin — identical to the
            // subject's by the parity just asserted, and it leaves the
            // subject's cache untouched.
            let prev = CellRead::<Values>::read(&self.twin, &self.id, cell_at(*key).as_ref())
                .await
                .map(|(committed, _)| committed)
                .map_err(|e| eyre!("twin prev read: {e:?}"))?;
            staged.push((
                cell_at(*key),
                ProvisionalWrite::new(Some(bytes(*value)), prev, event),
            ));
        }
        let clears: Vec<SectionClear> = clear
            .then(|| SectionClear::frozen(SECTION, &staged))
            .into_iter()
            .collect();
        let marker =
            EventMarker::frozen(event, &staged, clears.clone(), &evidence([].into(), None));
        self.subject
            .write_provisional(&self.cref, listed(&marker, &staged)?)
            .await
            .map_err(|e| eyre!("subject stage: {e:?}"))?;
        self.twin
            .write_provisional(&self.twin_ref, listed(&marker, &staged)?)
            .await
            .map_err(|e| eyre!("twin stage: {e:?}"))?;
        self.model_publish(
            staged
                .iter()
                .map(|(cell, write)| (cell.coordinate.as_bytes()[0], write.prev().cloned())),
        );
        self.staged = Some(Staged {
            writes: staged,
            clears,
        });
        Ok(())
    }

    /// Runs one op against both stores, updating the warm model.
    pub(super) async fn step(&mut self, op: &CacheOp) -> Result<()> {
        match op {
            CacheOp::Write { cells, clear } => self.step_write(cells, *clear).await?,
            CacheOp::Stage { writes, clear } => self.step_stage(writes, *clear).await?,
            CacheOp::Commit => {
                let Some(staged) = self.staged.take() else {
                    return Err(eyre!("commit without an unsettled stage"));
                };
                let marker = self
                    .twin
                    .marker_state(self.twin_ref.id())
                    .await?
                    .staged
                    .ok_or_else(|| eyre!("commit needs the staged marker"))?;
                self.subject
                    .commit_provisional(&self.cref, &marker, &staged.writes)
                    .await
                    .map_err(|e| eyre!("subject commit: {e:?}"))?;
                self.twin
                    .commit_provisional(&self.twin_ref, &marker, &staged.writes)
                    .await
                    .map_err(|e| eyre!("twin commit: {e:?}"))?;
                let staged_keys: Vec<u8> = staged
                    .writes
                    .iter()
                    .map(|(cell, _)| cell.coordinate.as_bytes()[0])
                    .collect();
                if !staged.clears.is_empty() {
                    // Scoped section-clear cache guard: everything but the staged coordinates goes.
                    self.warm.retain(|key, _| staged_keys.contains(key));
                }
                if self.fault_puts {
                    // The transform failed; the fallback delete landed.
                    for key in &staged_keys {
                        self.warm.remove(key);
                    }
                }
                for (cell, write) in &staged.writes {
                    if let Some((_, entry)) = self.warm.get_mut(&cell.coordinate.as_bytes()[0]) {
                        *entry = Values::into_cached(write.data().cloned());
                    }
                }
            }
            CacheOp::Abort => {
                let Some(staged) = self.staged.take() else {
                    return Err(eyre!("abort without an unsettled stage"));
                };
                self.subject
                    .abort_provisional(&self.cref, &staged.writes)
                    .await
                    .map_err(|e| eyre!("subject abort: {e:?}"))?;
                self.twin
                    .abort_provisional(&self.twin_ref, &staged.writes)
                    .await
                    .map_err(|e| eyre!("twin abort: {e:?}"))?;
                self.model_publish(
                    staged.writes.iter().map(|(cell, write)| {
                        (cell.coordinate.as_bytes()[0], write.prev().cloned())
                    }),
                );
            }
            CacheOp::Promote => {
                let Some(staged) = self.staged.take() else {
                    return Err(eyre!("promote without an unsettled stage"));
                };
                let cells: Vec<CellKey> =
                    staged.writes.iter().map(|(cell, _)| cell.clone()).collect();
                self.subject
                    .mark_resolved(&self.cref, &cells)
                    .await
                    .map_err(|e| eyre!("subject promote: {e:?}"))?;
                self.twin
                    .mark_resolved(&self.twin_ref, &cells)
                    .await
                    .map_err(|e| eyre!("twin promote: {e:?}"))?;
                // Raw promotion removes cached entries and leaves the marker unchanged.
                for cell in &cells {
                    self.warm.remove(&cell.coordinate.as_bytes()[0]);
                }
            }
            CacheOp::Get(key) => self.check_get(*key).await?,
            CacheOp::Contains(key) => self.check_presence(slice::from_ref(key)).await?,
            CacheOp::Scan => self.check_scan().await?,
            CacheOp::Advance(ms) => {
                self.clock += u64::from(*ms);
                self.now.store(self.clock, Ordering::Relaxed);
            }
            CacheOp::FaultPuts(on) => {
                self.fault_puts = *on;
                self.fail_puts.store(*on, Ordering::Relaxed);
            }
            CacheOp::FaultDeletes(n) => {
                self.fail_deletes.store(u64::from(*n), Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// One parity get of `key`, updating the warm model with the fill.
    async fn check_get(&mut self, key: u8) -> Result<()> {
        let falls_through = !self.is_warm::<Values>(key);
        let before = self.counting.lower_reads();
        let other_reads = (
            self.counting.batch_cache_reads(),
            self.counting.presence_reads(),
        );
        let subject = CellRead::<Values>::read(&self.subject, &self.id, cell_at(key).as_ref())
            .await
            .map(|(committed, _)| committed)
            .map_err(|e| eyre!("subject get: {e:?}"))?;
        let twin = CellRead::<Values>::read(&self.twin, &self.id, cell_at(key).as_ref())
            .await
            .map(|(committed, _)| committed)
            .map_err(|e| eyre!("twin get: {e:?}"))?;
        if subject.get() != twin.get() {
            return Err(eyre!(
                "get({key}) diverged: subject {:?}, twin {:?}",
                subject.get(),
                twin.get()
            ));
        }
        assert_eq!(
            self.counting.lower_reads() - before,
            usize::from(falls_through),
            "the model predicts each value read"
        );
        assert_eq!(
            (
                self.counting.batch_cache_reads(),
                self.counting.presence_reads()
            ),
            other_reads,
            "a value point does not fetch another projection or batch"
        );
        if falls_through && !self.fault_puts {
            self.warm
                .insert(key, (u64::MAX, Values::into_cached(twin.into_inner())));
        }
        Ok(())
    }

    /// Compares full-section scans without a cache update.
    async fn check_scan(&mut self) -> Result<()> {
        let before = self.counting.lower_scans();
        let reads = (
            self.counting.lower_reads(),
            self.counting.batch_cache_reads(),
            self.counting.presence_reads(),
            self.counting.presence_scans(),
        );
        let subject = scan_forward(&self.subject, &self.id, 0, Bound::Included(255)).await?;
        let twin = scan_forward(&self.twin, &self.id, 0, Bound::Included(255)).await?;
        if subject != twin {
            return Err(eyre!("scan diverged: subject {subject:?}, twin {twin:?}"));
        }
        assert_eq!(
            self.counting.lower_scans() - before,
            1,
            "each scan reads the lower store"
        );
        assert_eq!(
            (
                self.counting.lower_reads(),
                self.counting.batch_cache_reads(),
                self.counting.presence_reads(),
                self.counting.presence_scans()
            ),
            reads,
            "a scan adds no other reads"
        );
        Ok(())
    }

    /// Checks presence answers, cache knowledge, and lower read counts.
    async fn check_presence(&mut self, keys: &[u8]) -> Result<()> {
        let batch = batch_of(keys.iter().copied())?;
        let missed: CellBuffer<u8> = keys
            .iter()
            .copied()
            .filter(|key| !self.is_warm::<Presence>(*key))
            .collect();
        let before = self.counting.presence_reads();
        let value_reads = (
            self.counting.lower_reads(),
            self.counting.batch_cache_reads(),
        );
        let presence =
            CellRead::<Presence>::read_many(&self.subject, &self.id, SECTION, &batch.as_ref())
                .await?;
        let expected =
            CellRead::<Values>::read_many(&self.twin, &self.id, SECTION, &batch.as_ref()).await?;
        assert_eq!(
            self.counting.presence_reads() - before,
            usize::from(!missed.is_empty()),
            "the model predicts each presence batch"
        );
        assert_eq!(
            (
                self.counting.lower_reads(),
                self.counting.batch_cache_reads()
            ),
            value_reads,
            "presence does not fetch values"
        );
        assert_eq!(presence.len(), expected.len());
        for ((key, (present, _)), (value, _)) in keys.iter().zip(presence).zip(expected) {
            assert_eq!(
                present.get().is_some(),
                value.get().is_some(),
                "presence differs at {key}"
            );
            if missed.contains(key) && !self.fault_puts {
                self.warm.insert(
                    *key,
                    (u64::MAX, Presence::into_cached(present.into_inner())),
                );
            }
        }
        Ok(())
    }

    /// Checks both projections and the full scan after each operation.
    pub(super) async fn verify(&mut self) -> Result<()> {
        let keys: Vec<_> = (0..POOL).rev().chain([0, POOL]).collect();
        self.check_presence(&keys).await?;
        for key in 0..POOL {
            self.check_get(key).await?;
        }
        self.check_scan().await
    }
}
