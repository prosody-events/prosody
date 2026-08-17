use super::{
    Bytes, CellKey, CellKind, CollectionId, EncodedBlob, Encoding, EventRef, INITIAL_VERSION,
    PreparedStatement, StateType,
};

/// The four partition-key column values of a collection's Cassandra partition.
#[derive(Clone, Copy)]
pub(super) struct Pk<'a> {
    pub(super) segment_id: &'a crate::SegmentId,
    pub(super) key: &'a str,
    pub(super) state_type: StateType,
    pub(super) name: &'a str,
}

impl<'a> Pk<'a> {
    pub(super) fn of(id: &'a CollectionId) -> Self {
        Self {
            segment_id: &id.state_key().segment_id,
            key: id.state_key().key.as_ref(),
            state_type: id.state_type(),
            name: id.name().as_str(),
        }
    }
}

/// The cell column values bound by the stage / resolved-write paths.
///
/// `encoding` and `version` are shared by `data` and `prev_data` and present
/// iff **either** blob is present — a clear-over-present stage carries a null
/// `data` with a non-null `prev_data` and still needs an encoding to decode it.
pub(super) struct CellBlobs {
    encoding: Option<Encoding>,
    data: Option<Bytes>,
    prev_data: Option<Bytes>,
}

impl CellBlobs {
    pub(super) fn new(encoding: Encoding, data: Option<Bytes>, prev_data: Option<Bytes>) -> Self {
        let encoding = data.as_ref().or(prev_data.as_ref()).map(|_| encoding);
        Self {
            encoding,
            data,
            prev_data,
        }
    }

    pub(super) fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    pub(super) fn prev_data(&self) -> Option<&[u8]> {
        self.prev_data.as_deref()
    }

    pub(super) fn encoding(&self) -> Option<Encoding> {
        self.encoding
    }

    pub(super) fn version(&self) -> Option<i32> {
        self.encoding().map(|_| INITIAL_VERSION)
    }
}

pub(super) struct MarkerBlob {
    pub(super) payload: EncodedBlob,
    pub(super) event: EventRef,
}

/// The key + clustering columns addressing one cell in its partition: the four
/// partition-key columns and the cell's `section`/`coordinate`. `kind` is
/// **not** carried — each [`RowShape`] binds its own `kind` (`Cell` vs
/// `Marker`), so one address type serves both a cell row and the marker row.
#[derive(Clone, Copy)]
pub(super) struct CellAddr<'a> {
    pub(super) pk: Pk<'a>,
    pub(super) section: i8,
    pub(super) coordinate: &'a [u8],
}

impl<'a> CellAddr<'a> {
    pub(super) fn new(pk: Pk<'a>, cell: &'a CellKey) -> Self {
        Self {
            pk,
            section: i8::from(cell.section),
            coordinate: cell.coordinate.as_bytes(),
        }
    }

    /// The collection's **fixed marker address**: `(section = 0,
    /// coordinate = empty)`. Every marker statement binds this one position
    /// (with `kind = Marker`), so marker churn compacts to a single entry.
    pub(super) fn marker(pk: Pk<'a>) -> Self {
        Self {
            pk,
            section: 0,
            coordinate: &[],
        }
    }
}

/// One durable row bound into a same-partition `UNLOGGED BATCH`: the prepared
/// statement it targets and the [`RowShape`] that binds exactly that
/// statement's columns. [`scylla::statement::batch::Batch`] binds its
/// statement list 1:1 with the value list, so each row must serialize
/// precisely the columns of the statement
/// [`crate::cassandra::BatchRow::statement`] returns — kept consistent at the
/// construction sites, which pair each shape with its own statement.
pub(super) struct CellBatchRow<'a> {
    pub(super) statement: &'a PreparedStatement,
    pub(super) row: RowShape<'a>,
}

/// The column shape a [`CellBatchRow`] binds — one variant per distinct bind
/// tuple. A cell promote, a cell delete, and a marker delete share the
/// key-only [`Key`](Self::Key) shape: they differ only in statement and
/// constant `kind`, both carried as data. The two one-coordinate gap deletes
/// (`gap_below`/`gap_above`) share [`GapEdge`](Self::GapEdge) the same way.
pub(super) enum RowShape<'a> {
    /// Stage a provisional cell (`kind=Cell`): the full `data | prev_data |
    /// event` shape plus shared `encoding`/`version`.
    Stage(StageRow<'a>),
    /// Write a resolved value (`kind=Cell`): committed `data` +
    /// encoding/version, nulling `prev_data`/`event`.
    Resolved(ResolvedRow<'a>),
    /// Upsert the collection's event-marker row (`kind=Marker`) at the fixed
    /// address, at the collection TTL so it co-expires with the staged cells.
    MarkerWrite(MarkerWriteRow<'a>),
    /// Key columns only, binding the carried [`CellKind`]: a cell promote
    /// (`kind=Cell`, nulling `prev_data`/`event` while keeping `data` and its
    /// TTL), a `cell_delete` (`kind=Cell`), or a `marker_delete`
    /// (`kind=Marker`).
    Key(KeyRow<'a>),
    /// Whole-section gap delete (`gap_section`): a cleared section with no
    /// survivors — pk + `kind=Cell` + section, no coordinate predicate.
    GapSection(GapSectionRow<'a>),
    /// One-edge gap delete (`gap_below` / `gap_above`): the open range below
    /// the first or above the last survivor — one bound coordinate, borrowed
    /// from the frozen survivor list.
    GapEdge(GapEdgeRow<'a>),
    /// Open-interval gap delete (`gap_between`): the range between two
    /// adjacent survivors — two bound coordinates.
    GapBetween(GapBetweenRow<'a>),
}

/// The `write_provisional[_no_ttl]` bind shape. `ttl` selects the with-/no-TTL
/// statement **and** the bound column count — kept consistent with the carried
/// statement at the single construction site.
pub(super) struct StageRow<'a> {
    pub(super) ttl: Option<i32>,
    pub(super) data: Option<&'a [u8]>,
    pub(super) prev_data: Option<&'a [u8]>,
    pub(super) encoding: Option<Encoding>,
    pub(super) version: Option<i32>,
    pub(super) event: EventRef,
    pub(super) addr: CellAddr<'a>,
}

/// The `write_resolved[_no_ttl]` bind shape (committed `data` +
/// encoding/version; `prev_data`/`event` nulled by the statement).
pub(super) struct ResolvedRow<'a> {
    pub(super) ttl: Option<i32>,
    pub(super) data: Option<&'a [u8]>,
    pub(super) encoding: Option<Encoding>,
    pub(super) version: Option<i32>,
    pub(super) addr: CellAddr<'a>,
}

/// The `marker_write[_no_ttl]` bind shape: the encoded marker payload with its
/// encoding/version, the staging event, and the fixed marker address. `ttl`
/// selects the with-/no-TTL statement and the bound column count, exactly like
/// [`StageRow`].
pub(super) struct MarkerWriteRow<'a> {
    pub(super) ttl: Option<i32>,
    pub(super) payload: &'a [u8],
    pub(super) encoding: Encoding,
    pub(super) event: EventRef,
    pub(super) addr: CellAddr<'a>,
}

/// The key-only bind shape shared by `mark_resolved`, `cell_delete`, and
/// `marker_delete`: the four PK columns, the constant `kind`, and the row's
/// `section`/`coordinate`.
pub(super) struct KeyRow<'a> {
    pub(super) kind: CellKind,
    pub(super) addr: CellAddr<'a>,
}

/// The `gap_section` bind shape: pk + `kind=Cell` + the cleared section.
pub(super) struct GapSectionRow<'a> {
    pub(super) pk: Pk<'a>,
    pub(super) section: i8,
}

/// The `gap_below`/`gap_above` bind shape: [`GapSectionRow`]'s columns plus
/// the one bound survivor coordinate.
pub(super) struct GapEdgeRow<'a> {
    pub(super) pk: Pk<'a>,
    pub(super) section: i8,
    pub(super) coordinate: &'a [u8],
}

/// The `gap_between` bind shape: [`GapSectionRow`]'s columns plus the two
/// adjacent survivor coordinates bounding the open interval.
pub(super) struct GapBetweenRow<'a> {
    pub(super) pk: Pk<'a>,
    pub(super) section: i8,
    pub(super) low: &'a [u8],
    pub(super) high: &'a [u8],
}
