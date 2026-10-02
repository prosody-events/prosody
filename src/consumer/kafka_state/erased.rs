//! Erased writes of Kafka message references.

use super::MessageCell;
use crate::consumer::message::ConsumerMessage;
use crate::loader::MessageLoader;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{
    CellCodecError, CellStateError, ContextOf, DequeHandle, DequeStateError, FromSession,
    MapHandle, MapStateError, ValueHandle,
};
use crate::state::erased::{ErasedStateError, ErasedWrite};
use crate::state::order_codec::Utf8KeyCodec;

/// Stores message references through the borrowed typed write methods.
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
        handle.set(&key, &item).await
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
