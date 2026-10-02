//! The overlay and store scan merge.

use super::*;

/// One scan-trace step: seed the model/store, durably clear a section, or run
/// a scan and assert it.
#[derive(Clone, Debug)]
enum ScanStep {
    Seed(SeedOp),
    SeedClear(SeedClear),
    Scan(ScanReq),
}

impl Arbitrary for ScanStep {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0..=2 => Self::Seed(SeedOp::arbitrary(g)),
            3 => Self::SeedClear(SeedClear::arbitrary(g)),
            _ => Self::Scan(ScanReq::arbitrary(g)),
        }
    }
}

/// A seed mutation interleaving committed and dirty cells at overlapping and
/// disjoint `(section idx, coord)`s.
#[derive(Clone, Copy, Debug)]
enum SeedOp {
    CommitSet(u8, u8, u8),
    CommitClear(u8, u8),
    DirtySet(u8, u8, u8),
    DirtyClear(u8, u8),
}

impl Arbitrary for SeedOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = section_idx(u8::arbitrary(g));
        let c = u8::arbitrary(g) % CELLS;
        match u8::arbitrary(g) % 4 {
            0 => Self::CommitSet(s, c, u8::arbitrary(g)),
            1 => Self::CommitClear(s, c),
            2 => Self::DirtySet(s, c, u8::arbitrary(g)),
            _ => Self::DirtyClear(s, c),
        }
    }
}

/// A direct durable section clear (`write_resolved` with a frozen
/// [`SectionClear`]): the cleared section collapses to exactly the survivor
/// cells written alongside. Drives all four gap statements live — whole
/// section (no survivors), below the first, between adjacent, and above the
/// last — so the existing three-way scan parity + row-shape probe then cover
/// gap-tombstoned sections.
#[derive(Clone, Debug)]
pub(super) struct SeedClear {
    pub(super) sect: u8,
    /// Survivor `(coord, value)`s (deduped last-writer-wins in the runner).
    pub(super) survivors: Vec<(u8, u8)>,
}

impl Arbitrary for SeedClear {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            sect: section_idx(u8::arbitrary(g)),
            survivors: capped_vec::<(u8, u8)>(g, 3)
                .into_iter()
                .map(|(c, v)| (c % CELLS, v))
                .collect(),
        }
    }
}

/// Apply a durable section clear to `store` and mirror it into `model`'s
/// committed leg. Survivors are deduped last-writer-wins (no batch timestamp
/// ties — a repeated coordinate would put two writes of one row in one batch),
/// written alongside the frozen [`SectionClear`], and the cleared section's
/// committed leg collapses to exactly those survivors. Shared by the overlay
/// trace's `CommitClearSection` and the bottom-scan trace's `SeedClear`, which
/// stage the identical durable clear beneath their respective readers.
pub(super) async fn seed_section_clear<S: CellStore>(
    store: &S,
    collection: &CollectionRef,
    model: &mut CellModel,
    clear: SeedClear,
) -> Result<()> {
    let s = clear.sect;
    let mut survivors: BTreeMap<u8, u8> = BTreeMap::new();
    for (c, v) in clear.survivors {
        survivors.insert(c, v);
    }
    let cells: Vec<(CellKey, Option<Bytes>)> = survivors
        .iter()
        .map(|(&c, &v)| (cell_in(s, c), Some(bytes(v))))
        .collect();
    let section_clear =
        SectionClear::frozen_resolved(SECTIONS[s as usize % SECTIONS.len()], &cells);
    store
        .write_resolved(collection, &cells, slice::from_ref(&section_clear))
        .await?;
    model.committed.retain(|&(sect, _), _| sect != s);
    for (&c, &v) in &survivors {
        model.committed.insert((s, c), Some(bytes(v)));
    }
    Ok(())
}

/// One [`ScanReq`] edge kind: an inclusive/exclusive anchor, or `Unbounded`
/// (open on that side). Sampled uniformly, so the scan property covers the full
/// Direction × 3-start × 3-end space — exercising the exclusive-anchor
/// statements, the two section-only `_all` statements, and the `past_end` skip.
#[derive(Clone, Copy, Debug)]
pub(super) enum EdgeKind {
    Included,
    Excluded,
    Unbounded,
}

impl Arbitrary for EdgeKind {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[Self::Included, Self::Excluded, Self::Unbounded])
            .unwrap_or(&Self::Included)
    }
}

/// A scan request over one sampled section with random anchor, direction, per-
/// edge kind ([`EdgeKind`]), optional end, and an optional early-stop
/// prefix length.
#[derive(Clone, Copy, Debug)]
pub(super) struct ScanReq {
    pub(super) sect: u8,
    pub(super) start: u8,
    pub(super) forward: bool,
    pub(super) start_kind: EdgeKind,
    pub(super) end_kind: EdgeKind,
    pub(super) end: u8,
    pub(super) fetch_hint: Option<u8>,
    pub(super) partial: Option<u8>,
}

impl Arbitrary for ScanReq {
    fn arbitrary(g: &mut Gen) -> Self {
        // Anchors range over `0..=CELLS` so they fall between cells and below /
        // above every cell.
        Self {
            sect: section_idx(u8::arbitrary(g)),
            start: u8::arbitrary(g) % (CELLS + 1),
            forward: bool::arbitrary(g),
            start_kind: EdgeKind::arbitrary(g),
            end_kind: EdgeKind::arbitrary(g),
            end: u8::arbitrary(g) % (CELLS + 1),
            fetch_hint: Option::<u8>::arbitrary(g),
            partial: bool::arbitrary(g).then(|| u8::arbitrary(g) % (CELLS + 1)),
        }
    }
}

/// A shrinkable scan trace.
#[derive(Clone, Debug)]
pub(crate) struct ScanTrace {
    steps: Vec<ScanStep>,
}

impl Arbitrary for ScanTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.steps.shrink().map(|steps| Self { steps }))
    }
}

/// The oracle scan result: `model`'s visible cells of the request's section
/// filtered to its range and ordered in the scan direction.
pub(super) fn scan_oracle(model: &CellModel, req: ScanReq) -> Vec<(u8, Bytes)> {
    let mut cells: Vec<(u8, Bytes)> = model
        .visible_ordered(req.sect)
        .into_iter()
        .filter(|(c, _)| in_scan_range(req, *c))
        .collect();
    if !req.forward {
        cells.reverse();
    }
    cells
}

/// Whether coordinate `c` lies in the scan request's range, mirroring
/// [`Scan::contains`]: `start`/`end` are direction-relative (forward: `start`
/// low, `end` high; backward inverted) and each bound's exclusivity drops its
/// endpoint.
fn in_scan_range(req: ScanReq, c: u8) -> bool {
    // `start` is the low side forward, the high side backward; `end` inverts.
    // An `Unbounded` edge opens its side unconditionally.
    let on_start_side = match (req.forward, req.start_kind) {
        (_, EdgeKind::Unbounded) => true,
        (true, EdgeKind::Included) => c >= req.start,
        (true, EdgeKind::Excluded) => c > req.start,
        (false, EdgeKind::Included) => c <= req.start,
        (false, EdgeKind::Excluded) => c < req.start,
    };
    let within_end = match (req.forward, req.end_kind) {
        (_, EdgeKind::Unbounded) => true,
        (true, EdgeKind::Included) => c <= req.end,
        (true, EdgeKind::Excluded) => c < req.end,
        (false, EdgeKind::Included) => c >= req.end,
        (false, EdgeKind::Excluded) => c > req.end,
    };
    on_start_side && within_end
}

/// Builds the [`Scan`] request from a [`ScanReq`] and owned anchor coordinates.
/// `req.start_kind`/`req.end_kind` choose each edge (incl/excl/unbounded).
fn scan_of<'a>(req: ScanReq, start: &'a Coordinate, end: &'a Coordinate) -> Scan<'a> {
    let edge = |kind, coordinate| match kind {
        EdgeKind::Included => Bound::Included(coordinate),
        EdgeKind::Excluded => Bound::Excluded(coordinate),
        EdgeKind::Unbounded => Bound::Unbounded,
    };
    let start = edge(req.start_kind, start.as_bytes());
    let end = edge(req.end_kind, end.as_bytes());
    Scan {
        section: SECTIONS[req.sect as usize % SECTIONS.len()],
        start,
        dir: if req.forward {
            Direction::Forward
        } else {
            Direction::Backward
        },
        end,
        fetch_hint: req
            .fetch_hint
            .and_then(|n| NonZeroUsize::new(usize::from(n))),
    }
}

/// Collects an overlay scan, mapping each cell to `(coordinate byte, bytes)`.
/// `take` caps how many items are drained before the stream is dropped (`None`
/// drains to exhaustion); an early `Some(k)` drop must corrupt nothing.
pub(super) async fn collect_scan<S>(
    overlay: &Overlay<S>,
    id: &CollectionId,
    req: &ScanReq,
    take: Option<usize>,
) -> Result<Vec<(u8, Bytes)>>
where
    S: CellStore,
{
    let start = Coordinate::from_bytes(vec![req.start]);
    let end = Coordinate::from_bytes(vec![req.end]);
    let stream = overlay.scan::<Values>(id, scan_of(*req, &start, &end));
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while take.is_none_or(|k| out.len() < k)
        && let Some(item) = stream.next().await
    {
        let (key, value) = item?;
        out.push((coord_of(&key), value));
    }
    Ok(out)
}

/// Collects the payload-free twin of [`collect_scan`].
pub(super) async fn collect_scan_coordinates<S>(
    overlay: &Overlay<S>,
    id: &CollectionId,
    req: &ScanReq,
) -> Result<Vec<u8>>
where
    S: CellStore,
{
    let start = Coordinate::from_bytes(vec![req.start]);
    let end = Coordinate::from_bytes(vec![req.end]);
    let stream = overlay.scan::<Presence>(id, scan_of(*req, &start, &end));
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(coord_of(&item?.0));
    }
    Ok(out)
}

/// Compares both scan projections with a committed model after interleaved
/// writes and section clears. The trace covers both directions, all edge kinds,
/// and fetch hints.
pub(crate) async fn run_bottom_scan_trace<S, P>(
    store: S,
    trace: ScanTrace,
    probe: &P,
) -> Result<bool>
where
    S: CellStore,
    P: ShapeProbe,
{
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    let mut model = CellModel::default();

    for step in trace.steps {
        let seeded = matches!(&step, ScanStep::Seed(_) | ScanStep::SeedClear(_));
        match step {
            // Every seed lands in committed state; a "dirty" seed becomes a
            // committed set/clear so the bottom store alone holds the data.
            ScanStep::Seed(SeedOp::CommitSet(s, c, b) | SeedOp::DirtySet(s, c, b)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_in(s, c), Some(bytes(b)))], &[])
                    .await?;
                model.committed.insert((s, c), Some(bytes(b)));
            }
            ScanStep::Seed(SeedOp::CommitClear(s, c) | SeedOp::DirtyClear(s, c)) => {
                store
                    .write_resolved(&collection_ref, &[(cell_in(s, c), None)], &[])
                    .await?;
                model.committed.insert((s, c), None);
            }
            ScanStep::SeedClear(clear) => {
                seed_section_clear(&store, &collection_ref, &mut model, clear).await?;
            }
            ScanStep::Scan(req) => {
                let expected = scan_oracle(&model, req);
                let start = Coordinate::from_bytes(vec![req.start]);
                let end = Coordinate::from_bytes(vec![req.end]);
                let scan = scan_of(req, &start, &end);
                let stream = CellRead::<Values>::scan(&store, &id, scan);
                futures::pin_mut!(stream);
                let mut got = Vec::new();
                while let Some(item) = stream.next().await {
                    let (key, value) = item?;
                    got.push((coord_of(&key), value));
                }
                if got != expected {
                    return Ok(false);
                }
                let keys = CellRead::<Presence>::scan(&store, &id, scan)
                    .map(|row| row.map(|(cell, ())| cell));
                futures::pin_mut!(keys);
                let mut got_keys = Vec::new();
                while let Some(key) = keys.next().await {
                    got_keys.push(coord_of(&key?));
                }
                if got_keys != expected.iter().map(|(key, _)| *key).collect::<Vec<_>>() {
                    return Ok(false);
                }
            }
        }

        // After each seed (the direct `write_resolved` path: `None` ⇒ row
        // delete, a clear ⇒ gap erase): the stored `kind=Cell` rows across
        // both sampled sections equal the committed-present set, pinning the
        // ReadUncommitted-clear row-absence and gap-erase paths with no
        // oracle in the loop.
        if seeded {
            let present: RowKeys = model
                .committed
                .iter()
                .filter(|(_, value)| value.is_some())
                .map(|(&(s, c), _)| row_key(&cell_in(s, c)))
                .collect();
            if probe.cell_rows(&id).await? != present {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
