//! Write-only scylla `SerializeValue` impls for keyed-state discriminators.
//!
//! Each persisted discriminator binds directly into a `SerializeRow` tuple
//! through its own `SerializeValue` impl, rather than being hand-converted to
//! a driver primitive (`i8`/`i16`) at the call site. The impls mirror the
//! [`TimerType`](crate::timers::TimerType) bridge in
//! [`crate::cassandra`]: delegate to the discriminator's `as_iN()` and let the
//! integer's own `serialize` write the cell.
//!
//! These impls are **serialize-only by design**, the same rationale as the
//! [`EventRef`](crate::state::EventRef) UDT bridge in
//! [`super::udt`]. Reads do *not* go through a matching
//! `DeserializeValue`: the row decoder ([`super::decode`]) and the pending
//! scanner ([`super::scanner`]) deserialize the raw integer and validate it in
//! a fallible post-step. A bad discriminator then classifies `Permanent`
//! (skip the row) or is skipped for forward-compatibility, rather than
//! becoming scylla's opaque `Terminal` `DeserializationError`, which would
//! tear the partition down over one bad row.

use crate::state::encoding::{PayloadEncoding, WalFormat};
use crate::state::{CollectionKindId, StateType};
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;

impl SerializeValue for StateType {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.as_i8().serialize(typ, writer)
    }
}

impl SerializeValue for CollectionKindId {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.as_i8().serialize(typ, writer)
    }
}

impl SerializeValue for PayloadEncoding {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.as_i16().serialize(typ, writer)
    }
}

impl SerializeValue for WalFormat {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.as_i16().serialize(typ, writer)
    }
}
