use super::{
    Bytes, CassandraCellStoreError, CellBlobs, CellBuffer, CompactDuration, Coordinate,
    CoordinateBatch, PER_STATEMENT_OVERHEAD, ProvisionalCell, SmallVec, encode_payload,
    select_encoding,
};

/// Encodes a cell's `data` and `prev` payloads into their bound columns.
/// It selects one shared encoding from the larger payload because the row has
/// one encoding column for both provisional blobs.
pub(super) fn encode_cell_blobs(
    data: Option<&Bytes>,
    prev: Option<&Bytes>,
) -> Result<CellBlobs, CassandraCellStoreError> {
    let payload_len = data
        .into_iter()
        .chain(prev)
        .map(Bytes::len)
        .max()
        .unwrap_or(0);
    let encoding = select_encoding(payload_len);
    let data = data
        .map(|payload| encode_payload(payload, encoding))
        .transpose()?;
    let prev_data = prev
        .map(|payload| encode_payload(payload, encoding))
        .transpose()?;
    Ok(CellBlobs::new(encoding, data, prev_data))
}

/// The batch-packing weight of a cell row: its blob bytes plus the fixed
/// [`PER_STATEMENT_OVERHEAD`]. Over-counts rather than under-counts, so a
/// packed batch never exceeds the byte budget it was sized against.
pub(super) fn blob_weight(blob: &CellBlobs) -> u64 {
    let blob_bytes = blob.data().map_or(0_u64, |b| b.len() as u64)
        + blob.prev_data().map_or(0_u64, |b| b.len() as u64);
    PER_STATEMENT_OVERHEAD + blob_bytes
}

/// Converts a per-write TTL to the `i32` the driver binds to `USING TTL ?`.
/// The input is pre-validated against Cassandra's ceiling at registration, so
/// the saturating conversion is only a defensive floor.
pub(super) fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}

/// Converts a blob-TTL read (`decode`'s `blob_ttl`) into the cache-fill
/// remaining duration. A NULL (`None`) means the cell has no TTL — it never
/// expires. A present value is the whole remaining seconds (a FLOOR), so a
/// fjall entry stamped `now + remaining` never outlives the durable row —
/// including `0`, which means *sub-second remaining* and must stamp an
/// (almost) immediate expiry, never "never" (a negative is treated the same,
/// defensively). Collapsing `0` into `None` would let the fjall entry outlive
/// a durable row that dies within the second.
pub(super) fn ttl_seconds_to_duration(ttl: Option<i32>) -> Option<CompactDuration> {
    ttl.map(|s| CompactDuration::new(u32::try_from(s).unwrap_or(0)))
}

/// Keeps provisional cells from a recovery batch and discards their TTLs.
/// The input already follows ascending coordinate order.
pub(super) fn decode_provisional_batch(
    rows: CellBuffer<Option<super::decode::BorrowedCellTtlRow<'_>>>,
    coordinates: &[&Coordinate],
) -> Result<CellBuffer<(Coordinate, ProvisionalCell)>, CassandraCellStoreError> {
    let mut out: CellBuffer<(Coordinate, ProvisionalCell)> = SmallVec::with_capacity(rows.len());
    for (&coordinate, row) in coordinates.iter().zip(rows) {
        if let Some(row) = row
            && let Some(provisional) = super::decode::try_decode_provisional_cell_ttl(row)?
        {
            out.push((Coordinate::clone(coordinate), provisional));
        }
    }
    Ok(out)
}

/// Returns the sorted, distinct coordinates for one recovery query.
pub(super) fn sorted_unique_coordinates(batch: &CoordinateBatch) -> CellBuffer<&Coordinate> {
    let mut coordinates: CellBuffer<&Coordinate> = SmallVec::with_capacity(batch.len());
    coordinates.extend(batch.iter());
    coordinates.sort_unstable();
    coordinates.dedup();
    coordinates
}
