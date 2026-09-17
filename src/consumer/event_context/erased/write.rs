//! Codec and message-reference writes for erased collection handles.

use super::ErasedStateError;
use crate::codec::{Codec, ErasedStateCodec};
use crate::consumer::kafka_state::MessageCell;
use crate::consumer::message::ConsumerMessage;
use crate::loader::MessageLoader;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{
    CellCodecError, CellStateError, CellType, ContextOf, DequeHandle, DequeStateError, FromSession,
    MapHandle, MapStateError, ResolvedOf, ValueHandle,
};
use crate::state::order_codec::Utf8KeyCodec;
use std::future::Future;

pub(super) trait ErasedWrite: CellType + Sized {
    /// Rejects the JSON-null "absent" sentinel on the value families; a no-op
    /// for message cells (a `ConsumerMessage` is never the null sentinel).
    fn reject_null(item: &ResolvedOf<Self>) -> Result<(), ErasedStateError>;

    fn value_set<'a, S>(
        handle: &'a ValueHandle<S, Self>,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;

    fn map_set<'a, S>(
        handle: &'a MapHandle<S, Utf8KeyCodec, Self>,
        key: String,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), MapStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;

    fn deque_push_back<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;

    fn deque_push_front<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ResolvedOf<Self>,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>;
}

/// Owned write: a plain codec cell (JSON value, C# passthrough) — the write
/// value moves straight into the typed handle, no clone.
impl<C> ErasedWrite for C
where
    C: Codec,
    C::Payload: ErasedStateCodec,
{
    fn reject_null(item: &C::Payload) -> Result<(), ErasedStateError> {
        if item.is_absent_sentinel() {
            return Err(ErasedStateError::null_write());
        }
        Ok(())
    }

    fn value_set<'a, S>(
        handle: &'a ValueHandle<S, Self>,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), CellStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(item)
    }

    fn map_set<'a, S>(
        handle: &'a MapHandle<S, Utf8KeyCodec, Self>,
        key: String,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), MapStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(key, item)
    }

    fn deque_push_back<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_back(item)
    }

    fn deque_push_front<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: C::Payload,
    ) -> impl Future<Output = Result<(), DequeStateError<CellCodecError<Self>>>> + Send + 'a
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_front(item)
    }
}

/// Borrowed write: the Kafka message cell — the typed handle takes the message
/// by reference, so the future owns the item and lends it. No overlap with the
/// owned impl: [`MessageCell`] is a `WithResolver`, which does not impl
/// [`Codec`].
impl<L: MessageLoader + 'static> ErasedWrite for MessageCell<L> {
    fn reject_null(_item: &ConsumerMessage<L::Payload>) -> Result<(), ErasedStateError> {
        // A message ref is never the null sentinel.
        Ok(())
    }

    async fn value_set<'a, S>(
        handle: &'a ValueHandle<S, Self>,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), CellStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(&item).await
    }

    async fn map_set<'a, S>(
        handle: &'a MapHandle<S, Utf8KeyCodec, Self>,
        key: String,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), MapStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.set(key, &item).await
    }

    async fn deque_push_back<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), DequeStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_back(&item).await
    }

    async fn deque_push_front<'a, S>(
        handle: &'a DequeHandle<S, Self>,
        item: ConsumerMessage<L::Payload>,
    ) -> Result<(), DequeStateError<CellCodecError<Self>>>
    where
        S: WritableStateSession,
        for<'s> ContextOf<'s, Self>: FromSession<'s, S>,
    {
        handle.push_front(&item).await
    }
}
