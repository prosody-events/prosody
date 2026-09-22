//! Scoped reads through owner or reader admission.

use super::batch::{batched, read_keys};
use super::{
    BorrowedKeyOf, CellBuffer, CellCodecError, CellFamily, CellStateError, CellType,
    CollectionRead, ContextOf, FromSession, Presence, ReadOperation, ResolvedOf, StateAccessError,
    StateName, StateSession, Values, WritableStateSession, WriteOperation, resolve_batch,
    resolve_cell, sealed,
};
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
        let section = family.section();
        let Self { collection, inner } = self;
        let session = collection.session();
        let keys = keys.into_iter();
        async move {
            let bytes = read_keys::<S, T, Values>(
                session,
                inner,
                collection.state_type(),
                collection.name(),
                section,
                keys,
            )
            .await?;
            resolve_batch::<S, T>(session, bytes).await
        }
    }

    fn contains<'a, T: CellType>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<bool, StateAccessError>> + Send + use<'a, 'c, S, L, T> {
        let cell = family.at(key).cell;
        let Self { collection, inner } = self;
        async move {
            Ok(<S::Engine as sealed::Reads<S, Presence>>::read_point(
                collection.session(),
                inner,
                collection.state_type(),
                collection.name(),
                &cell,
            )
            .await?
            .is_some())
        }
    }

    fn contains_many<'a, 'op, T, I>(
        &'op mut self,
        family: CellFamily<L, T>,
        keys: I,
    ) -> impl Future<Output = Result<CellBuffer<bool>, StateAccessError>>
    + Send
    + use<'a, 'op, 'c, S, L, T, I>
    where
        T: CellType,
        I: IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
    {
        let section = family.section();
        let Self { collection, inner } = self;
        let keys = keys.into_iter();
        async move {
            Ok(read_keys::<S, T, Presence>(
                collection.session(),
                inner,
                collection.state_type(),
                collection.name(),
                section,
                keys,
            )
            .await?
            .into_iter()
            .map(|value| value.is_some())
            .collect())
        }
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
        // The key is lowered before the async block, so only the owned
        // coordinate crosses the engine await.
        let cell = family.at(key).cell;
        let Self { collection, inner } = self;
        let session = collection.session();
        async move {
            let bytes = <S::Engine as sealed::Reads<S, Values>>::read_point(
                session,
                inner,
                collection.state_type(),
                collection.name(),
                &cell,
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
        let section = family.section();
        let slots = self.slots::<T, Values>(family, keys);
        let Self {
            collection, inner, ..
        } = self;
        let session = collection.session();
        async move {
            let bytes = batched::<S, Values>(
                session,
                &mut **inner,
                collection.state_type(),
                collection.name(),
                section,
                slots,
            )
            .await?;
            resolve_batch::<S, T>(session, bytes).await
        }
    }

    fn contains<'a, T: CellType>(
        &'a mut self,
        family: CellFamily<L, T>,
        key: &BorrowedKeyOf<T>,
    ) -> impl Future<Output = Result<bool, StateAccessError>> + Send + use<'a, 'c, S, L, T> {
        let cell = family.at(key).cell;
        async move { Ok(self.staged_or_read::<Presence>(&cell).await?.is_some()) }
    }

    fn contains_many<'a, 'op, T, I>(
        &'op mut self,
        family: CellFamily<L, T>,
        keys: I,
    ) -> impl Future<Output = Result<CellBuffer<bool>, StateAccessError>>
    + Send
    + use<'a, 'op, 'c, S, L, T, I>
    where
        T: CellType,
        I: IntoIterator<Item = &'a BorrowedKeyOf<T>, IntoIter: Send>,
    {
        let section = family.section();
        let slots = self.slots::<T, Presence>(family, keys);
        let Self {
            collection, inner, ..
        } = self;
        async move {
            Ok(batched::<S, Presence>(
                collection.session(),
                &mut **inner,
                collection.state_type(),
                collection.name(),
                section,
                slots,
            )
            .await?
            .into_iter()
            .map(|value| value.is_some())
            .collect())
        }
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
        let cell = family.at(key).cell;
        async move {
            match self.staged_or_read::<Values>(&cell).await? {
                Some(bytes) => Ok(Some(
                    resolve_cell::<S, T>(self.collection.session(), bytes).await?,
                )),
                None => Ok(None),
            }
        }
    }
}
