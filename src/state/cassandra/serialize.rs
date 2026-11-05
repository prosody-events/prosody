//! Write-only scylla `SerializeValue` impls for keyed-state discriminators,
//! the partition count, and the coordinate blob.
//!
//! Each persisted discriminator binds into a `SerializeRow` tuple through its
//! own `SerializeValue` impl, never as a driver primitive converted at the call
//! site. Each impl delegates through the discriminator's `From<_> for iN` impl,
//! like the [`TimerType`](crate::timers::TimerType) bridge in [`crate::cassandra`].
//!
//! These impls are serialize-only by design, like the
//! [`EventRef`](crate::state::EventRef) UDT bridge in [`super::udt`]. The cell
//! decoder (`cell::decode`) reads the raw integer and validates it in a
//! fallible step, so a bad discriminator classifies as `Permanent`. A
//! `DeserializeValue` impl would raise a `Terminal` driver error instead, and
//! that error stops the partition.
//!
//! [`PartitionCount`] and [`Coordinate`] are not discriminators, but they bind
//! the same way. A partition count binds as an `int`, and reads validate the
//! raw `i32` through `PartitionCount::try_from`. A coordinate binds as its
//! `blob`, alone or inside an `IN` list, and reads rebuild it from the raw
//! `Vec<u8>`.

use crate::state::StateType;
use crate::state::cassandra::cell::CellKind;
use crate::state::cassandra::cell::Encoding;
use crate::state::cell_key::Coordinate;
use crate::state_reader::PartitionCount;
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;

impl SerializeValue for CellKind {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i8::from(*self).serialize(typ, writer)
    }
}

impl SerializeValue for StateType {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i8::from(*self).serialize(typ, writer)
    }
}

impl SerializeValue for Encoding {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i16::from(*self).serialize(typ, writer)
    }
}

impl SerializeValue for Coordinate {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        self.as_bytes().serialize(typ, writer)
    }
}

impl SerializeValue for PartitionCount {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}
