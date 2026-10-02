//! Scoped reads through owner or reader admission.

use super::batch::{contains_keys, encode_key, get_keys};
use super::{
    BorrowedKeyOf, CellBuffer, CellCodecError, CellFamily, CellStateError, CellType,
    CollectionRead, ContextOf, FromSession, Presence, ReadOperation, ResolvedOf, StateName,
    StateSession, Values, WritableStateSession, WriteOperation, resolve_cell, sealed,
};
use crate::state::cell_key::CellRef;
use std::future::Future;
use std::num::NonZeroUsize;

impl<'c, S: StateSession, L> CollectionRead for ReadOperation<'c, S, L> {
    type Layout = L;
    type Session = S;

    fn name(&self) -> &StateName {
        self.collection.name()
    }

    fn has_ttl(&self) -> bool {
        self.collection.def().ttl.is_some()
    }

    fn keyset_limit(&self) -> usize {
        self.collection.def().keyset_limit
    }

    fn capacity(&self) -> Option<NonZeroUsize> {
        self.collection.def().capacity
    }

    fn get_many<'a, 'op, T, I>(
        &'op mut self,
        family: CellFamily<L, T>,
        keys: I,
    ) -> impl Future<
        Output = Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>,
    > + Send
    + use<'a, 'op, 'c, S, L, T, I>
    where
        T: CellType,
        I: IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let Self { collection, inner } = self;
        get_keys::<S, L, T>(collection, inner, family.section(), &[], keys.into_iter())
    }

    fn contains<'a, T: CellType>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<bool, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'c, S, L, T> {
        let encoded = encode_key::<T::Key>(key);
        let Self { collection, inner } = self;
        async move {
            let buffer = encoded.map_err(CellStateError::Key)?;
            let cell = CellRef {
                section: family.section(),
                coordinate: &buffer,
            };
            Ok(<S::Engine as sealed::Reads<S, Presence>>::read_point(
                collection.session(),
                inner,
                collection.state_type(),
                collection.name(),
                cell,
            )
            .await?
            .is_some())
        }
    }

    fn contains_many<'a, 'op, T, I>(
        &'op mut self,
        family: CellFamily<L, T>,
        keys: I,
    ) -> impl Future<Output = Result<CellBuffer<bool>, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'op, 'c, S, L, T, I>
    where
        T: CellType,
        I: IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
    {
        let Self { collection, inner } = self;
        contains_keys::<S, L, T>(collection, inner, family.section(), &[], keys.into_iter())
    }

    fn get<'a, T>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'c, S, L, T>
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let encoded = encode_key::<T::Key>(key);
        let Self { collection, inner } = self;
        let session = collection.session();
        async move {
            let buffer = encoded.map_err(CellStateError::Key)?;
            let cell = CellRef {
                section: family.section(),
                coordinate: &buffer,
            };
            let bytes = <S::Engine as sealed::Reads<S, Values>>::read_point(
                session,
                inner,
                collection.state_type(),
                collection.name(),
                cell,
            )
            .await?;
            match bytes {
                Some(bytes) => Ok(Some(resolve_cell::<S, T>(session, bytes).await?)),
                None => Ok(None),
            }
        }
    }
}

impl<'c, S: WritableStateSession, L> CollectionRead for WriteOperation<'c, S, L> {
    type Layout = L;
    type Session = S;

    fn name(&self) -> &StateName {
        self.collection.name()
    }

    fn has_ttl(&self) -> bool {
        self.collection.def().ttl.is_some()
    }

    fn keyset_limit(&self) -> usize {
        self.collection.def().keyset_limit
    }

    fn capacity(&self) -> Option<NonZeroUsize> {
        self.collection.def().capacity
    }

    fn get_many<'a, 'op, T, I>(
        &'op mut self,
        family: CellFamily<L, T>,
        keys: I,
    ) -> impl Future<
        Output = Result<CellBuffer<Option<ResolvedOf<T>>>, CellStateError<CellCodecError<T>>>,
    > + Send
    + use<'a, 'op, 'c, S, L, T, I>
    where
        T: CellType,
        I: IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let Self {
            collection,
            inner,
            journal,
        } = self;
        get_keys::<S, L, T>(
            collection,
            inner,
            family.section(),
            journal,
            keys.into_iter(),
        )
    }

    fn contains<'a, T: CellType>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<bool, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'c, S, L, T> {
        let encoded = encode_key::<T::Key>(key);
        async move {
            let buffer = encoded.map_err(CellStateError::Key)?;
            let cell = CellRef {
                section: family.section(),
                coordinate: &buffer,
            };
            Ok(self.staged_or_read::<Presence>(cell).await?.is_some())
        }
    }

    fn contains_many<'a, 'op, T, I>(
        &'op mut self,
        family: CellFamily<L, T>,
        keys: I,
    ) -> impl Future<Output = Result<CellBuffer<bool>, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'op, 'c, S, L, T, I>
    where
        T: CellType,
        I: IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
    {
        let Self {
            collection,
            inner,
            journal,
        } = self;
        contains_keys::<S, L, T>(
            collection,
            inner,
            family.section(),
            journal,
            keys.into_iter(),
        )
    }

    fn get<'a, T>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<Option<ResolvedOf<T>>, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'a, 'c, S, L, T>
    where
        T: CellType,
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let encoded = encode_key::<T::Key>(key);
        async move {
            let buffer = encoded.map_err(CellStateError::Key)?;
            let cell = CellRef {
                section: family.section(),
                coordinate: &buffer,
            };
            let value = self.staged_or_read::<Values>(cell).await?;
            match value {
                Some(bytes) => Ok(Some(
                    resolve_cell::<S, T>(self.collection.session(), bytes).await?,
                )),
                None => Ok(None),
            }
        }
    }
}
