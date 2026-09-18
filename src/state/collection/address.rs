//! Typed cell families and their encoded addresses.

use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::descriptor::{CellType, KeyOf};
use crate::state::order_codec::OrderedKeyCodec;
use std::marker::PhantomData;

/// An encoded address whose layout and cell type match its declared family.
/// Only `CellFamily::at` constructs one. Mutation commands consume it.
pub(crate) struct CellAddress<L, T> {
    pub(super) cell: CellKey,
    _type: PhantomData<fn() -> (L, T)>,
}

impl<L, T> CellAddress<L, T> {
    /// Borrows the encoded coordinate for collection metadata.
    pub(crate) fn coordinate(&self) -> &Coordinate {
        &self.cell.coordinate
    }
}

/// A declared section and cell type within layout `L`.
/// Commands require the operation's layout, so unrelated families cannot mix.
/// Tokens contain no session state. The layout macro declares them.
pub(crate) struct CellFamily<L, T> {
    section: Section,
    _type: PhantomData<fn() -> (L, T)>,
}

// Manual, so a family token does not inherit `L: Copy` / `T: Copy` bounds from
// a derive.
impl<L, T> Copy for CellFamily<L, T> {}

impl<L, T> Clone for CellFamily<L, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<L, T> CellFamily<L, T> {
    /// Declares the family at durable section `id`. Called only from generated
    /// layout code.
    pub(crate) const fn declare(id: i8) -> Self {
        Self {
            section: Section::new(id),
            _type: PhantomData,
        }
    }

    /// Encodes one key and binds its address to this family.
    pub(crate) fn at(self, key: &KeyOf<T>) -> CellAddress<L, T>
    where
        T: CellType,
    {
        CellAddress {
            cell: CellKey {
                section: self.section,
                coordinate: T::Key::encode(key),
            },
            _type: PhantomData,
        }
    }

    /// The durable section this family addresses.
    pub(crate) const fn section(self) -> Section {
        self.section
    }
}
