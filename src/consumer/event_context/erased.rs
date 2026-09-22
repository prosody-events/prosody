//! The event context adapter used by language clients.

use super::{
    BoxDequeState, BoxMapState, BoxSetState, BoxValueState, CompactDateTime, ConsumerMessage,
    DynClone, ErasedDeque, ErasedMap, ErasedSet, ErasedStateCodec, ErasedStateError, ErasedValue,
    Error, EventContext, EventContextError, Registered, StateSession, TimerType, Utf8KeyCodec,
    async_trait, deque_state, map_state, message_deque_state, message_map_state, message_state,
    set_state, value_state,
};

/// Object-safe boxed event context
pub type BoxEventContext<P> = Box<dyn DynEventContext<Payload = P>>;

/// Boxed error type for object-safe contexts.
pub type BoxEventContextError = Box<dyn EventContextError>;

impl Error for BoxEventContextError {}

/// Object-safe version of `EventContext` with boxed futures and errors.
///
/// Allows using `EventContext` trait objects where return types must be named.
///
/// # Object safety
///
/// Every method resolves to an object-safe shape: the timer ops are `async fn`
/// (boxed by `#[async_trait]`), `should_cancel` is a synchronous `bool`, and
/// the seven keyed-state vend methods are synchronous fallible `fn`s returning
/// a boxed erased handle (`Result<Box<dyn Dyn*State>, ErasedStateError>`).
#[async_trait]
pub trait DynEventContext: DynClone + Send + Sync + 'static {
    /// The message payload type events on this context carry; mirrors
    /// [`EventContext::Payload`] so `Box<dyn DynEventContext<Payload = P>>`
    /// keeps the payload nameable across the type-erased FFI boundary.
    type Payload: Send + Sync + 'static;

    /// Async wait for message cancellation signal (includes partition
    /// shutdown).
    async fn on_cancel(&self);

    /// Schedule a timer for the current key.
    ///
    /// # Errors
    ///
    /// Returns an error if scheduling fails.
    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule all existing timers and schedule a new one.
    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule a specific timer.
    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule all timers of the specified type.
    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), BoxEventContextError>;

    /// List scheduled execution times for the specified type.
    async fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, BoxEventContextError>;

    /// Synchronously check if message cancellation has been requested (includes
    /// partition shutdown).
    fn should_cancel(&self) -> bool;

    // Keyed-state vend methods — the FFI seam the bindings wrap. Each mints a
    // boxed erased handle
    // ([`DynValueState`]/[`DynMapState`]/[`DynSetState`]/[`DynDequeState`])
    // over the *same* typed `state(...)` path as the Rust API: the value
    // families recover the cell codec from the payload via [`ErasedStateCodec`]
    // (the blanket impl's `Self::Payload: ErasedStateCodec` bound restricts a
    // boxed context to the FFI payloads); the message families resolve through
    // the session's loader. Maps and sets always use `String` keys.
    //
    // Vending runs the access-time `verify_state_registration` check (an
    // unregistered or identity-mismatched name is a Permanent error), then
    // returns the bind-once handle. The attempt-epoch fence is inherited from
    // the typed cell interface the handle wraps — the erased seam adds no
    // fencing of its own. Errors carry a two-way `{Permanent, Transient}`
    // category and never `Terminal` (see [`ErasedStateError`]).

    /// Vends the erased handle for the named single-value collection.
    ///
    /// # Errors
    ///
    /// Returns a Permanent error for an unregistered or identity-mismatched
    /// name.
    fn value_state(&self, name: &str) -> Result<BoxValueState<Self::Payload>, ErasedStateError>;

    /// Vends the erased handle for the named `String`-keyed map collection.
    ///
    /// # Errors
    ///
    /// See [`value_state`](Self::value_state).
    fn map_state(&self, name: &str) -> Result<BoxMapState<Self::Payload>, ErasedStateError>;

    /// Vends the erased handle for the named `String`-keyed set collection.
    ///
    /// # Errors
    ///
    /// See [`value_state`](Self::value_state).
    fn set_state(&self, name: &str) -> Result<BoxSetState, ErasedStateError>;

    /// Vends the erased handle for the named deque collection.
    ///
    /// # Errors
    ///
    /// See [`value_state`](Self::value_state).
    fn deque_state(&self, name: &str) -> Result<BoxDequeState<Self::Payload>, ErasedStateError>;

    /// Vends the erased handle for the named single-value Kafka-message
    /// collection — its item is the full [`ConsumerMessage`], resolved through
    /// the consumer's loader.
    ///
    /// # Errors
    ///
    /// See [`value_state`](Self::value_state).
    fn message_value_state(
        &self,
        name: &str,
    ) -> Result<BoxValueState<ConsumerMessage<Self::Payload>>, ErasedStateError>;

    /// Vends the erased handle for the named `String`-keyed map of Kafka
    /// messages.
    ///
    /// # Errors
    ///
    /// See [`value_state`](Self::value_state).
    fn message_map_state(
        &self,
        name: &str,
    ) -> Result<BoxMapState<ConsumerMessage<Self::Payload>>, ErasedStateError>;

    /// Vends the erased handle for the named deque of Kafka messages.
    ///
    /// # Errors
    ///
    /// See [`value_state`](Self::value_state).
    fn message_deque_state(
        &self,
        name: &str,
    ) -> Result<BoxDequeState<ConsumerMessage<Self::Payload>>, ErasedStateError>;
}

dyn_clone::clone_trait_object!(<P> DynEventContext<Payload = P>);

#[async_trait]
impl<C> DynEventContext for C
where
    C: EventContext + Send + Sync + 'static,
    C::Error: Error + Send + Sync + 'static,
    // The keyed-state value ops recover the codec from the payload, so the
    // erased seam exists only for payloads that map to one. Every FFI payload
    // does; this is also why `EventContext::boxed` carries the same bound.
    C::Payload: ErasedStateCodec,
{
    type Payload = C::Payload;

    async fn on_cancel(&self) {
        EventContext::on_cancel(self).await;
    }

    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError> {
        EventContext::schedule(self, time, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError> {
        EventContext::clear_and_schedule(self, time, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError> {
        EventContext::unschedule(self, time, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), BoxEventContextError> {
        EventContext::clear_scheduled(self, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, BoxEventContextError> {
        EventContext::scheduled(self, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    fn should_cancel(&self) -> bool {
        EventContext::should_cancel(self)
    }

    fn value_state(&self, name: &str) -> Result<BoxValueState<Self::Payload>, ErasedStateError> {
        let handle = self
            .state(Registered::new(value_state::<
                <C::Payload as ErasedStateCodec>::Codec,
            >(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedValue::new(handle)))
    }

    fn map_state(&self, name: &str) -> Result<BoxMapState<Self::Payload>, ErasedStateError> {
        let handle = self
            .state(Registered::new(map_state::<
                Utf8KeyCodec,
                <C::Payload as ErasedStateCodec>::Codec,
            >(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedMap::new(handle)))
    }

    fn set_state(&self, name: &str) -> Result<BoxSetState, ErasedStateError> {
        let handle = self
            .state(Registered::new(set_state::<Utf8KeyCodec>(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedSet::new(handle)))
    }

    fn deque_state(&self, name: &str) -> Result<BoxDequeState<Self::Payload>, ErasedStateError> {
        let handle = self
            .state(Registered::new(deque_state::<
                <C::Payload as ErasedStateCodec>::Codec,
            >(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedDeque::new(handle)))
    }

    fn message_value_state(
        &self,
        name: &str,
    ) -> Result<BoxValueState<ConsumerMessage<Self::Payload>>, ErasedStateError> {
        let handle = self
            .state(Registered::new(message_state::<
                <C::State as StateSession>::Loader,
            >(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedValue::new(handle)))
    }

    fn message_map_state(
        &self,
        name: &str,
    ) -> Result<BoxMapState<ConsumerMessage<Self::Payload>>, ErasedStateError> {
        let handle = self
            .state(Registered::new(message_map_state::<
                Utf8KeyCodec,
                <C::State as StateSession>::Loader,
            >(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedMap::new(handle)))
    }

    fn message_deque_state(
        &self,
        name: &str,
    ) -> Result<BoxDequeState<ConsumerMessage<Self::Payload>>, ErasedStateError> {
        let handle = self
            .state(Registered::new(message_deque_state::<
                <C::State as StateSession>::Loader,
            >(name)))
            .map_err(|error| ErasedStateError::from_classified(&error))?;
        Ok(Box::new(ErasedDeque::new(handle)))
    }
}
