//! Tests for `TimerDeferContext` wrapper.
//!
//! Verifies that the context wrapper correctly unifies active and deferred
//! timer operations, delegating appropriately based on deferral state.

use super::*;
use crate::consumer::Keyed;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::event_context::TerminationSignals;
use crate::consumer::middleware::RepinProof;
use crate::consumer::middleware::defer::timer::context::TimerDeferContext;
use crate::consumer::middleware::defer::timer::store::TimerDeferStore;
use crate::consumer::middleware::defer::timer::store::memory::MemoryTimerDeferStore;
use crate::otel::SpanRelation;
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::tests::support::UnavailableState;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;

// ============================================================================
// KeyedMockContext - Context that implements Keyed trait
// ============================================================================

/// Mock context with `Keyed` trait for testing `TimerDeferContext`.
#[derive(Clone)]
pub struct KeyedMockContext {
    inner: MockContext,
    key: Key,
    /// Tracks timer times by type for verification.
    active_timers: Arc<Mutex<Vec<(CompactDateTime, TimerType)>>>,
}

impl KeyedMockContext {
    #[must_use]
    pub fn new(key: &str) -> Self {
        Self {
            inner: MockContext::new(),
            key: Arc::from(key),
            active_timers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Removes deferred timers without a timer operation.
    pub fn drop_deferred_timers(&self) {
        self.active_timers
            .lock()
            .retain(|(_, timer_type)| *timer_type != TimerType::DeferredTimer);
    }

    /// Returns all active Application timers for this context.
    #[must_use]
    pub fn active_application_timers(&self) -> Vec<CompactDateTime> {
        self.active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == TimerType::Application)
            .map(|(time, _)| *time)
            .collect()
    }

    /// Returns all active `DeferredTimer` timers for this context.
    #[must_use]
    pub fn active_deferred_timers(&self) -> Vec<CompactDateTime> {
        self.active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == TimerType::DeferredTimer)
            .map(|(time, _)| *time)
            .collect()
    }
}

impl Keyed for KeyedMockContext {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.key
    }
}

impl TerminationSignals for KeyedMockContext {
    fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    fn is_message_cancelled(&self) -> bool {
        self.inner.is_message_cancelled()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_shutdown()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_message_cancelled()
    }
}

impl EventContext for KeyedMockContext {
    type Error = Infallible;
    type Payload = serde_json::Value;
    type State = UnavailableState<serde_json::Value>;

    fn state<DESC>(
        &self,
        registered: Registered<DESC>,
    ) -> Result<DESC::Handle<Self::State>, StateAccessError>
    where
        DESC: StateDescriptor,
    {
        registered.descriptor().bind(&UnavailableState::new())
    }

    fn redispatch(&self, proof: RepinProof) -> Self {
        // Forward to the inner mock (compiler-enforced, like `state`); the
        // key/timer capture are cheap clones this wrapper owns.
        Self {
            inner: self.inner.redispatch(proof),
            key: self.key.clone(),
            active_timers: self.active_timers.clone(),
        }
    }

    fn should_cancel(&self) -> bool {
        self.inner.should_cancel()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_cancel()
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn uncancel(&self) {
        self.inner.uncancel();
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers.lock().push((time, timer_type));
        self.inner.schedule(time, timer_type)
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // Clear existing timers of this type, then add new one
        self.active_timers.lock().retain(|(_, t)| *t != timer_type);
        self.active_timers.lock().push((time, timer_type));
        self.inner.clear_and_schedule(time, timer_type)
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers
            .lock()
            .retain(|(t, tt)| !(*t == time && *tt == timer_type));
        self.inner.unschedule(time, timer_type)
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers.lock().retain(|(_, t)| *t != timer_type);
        self.inner.clear_scheduled(timer_type)
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        let times: Vec<CompactDateTime> = self
            .active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == timer_type)
            .map(|(time, _)| *time)
            .collect();
        ready(Ok(times))
    }
}

// ============================================================================
// Test Helper
// ============================================================================

struct ContextTestHarness {
    store: MemoryTimerDeferStore,
    inner_context: KeyedMockContext,
}

impl ContextTestHarness {
    fn new(key: &str) -> Self {
        let store = MemoryTimerDeferStore::new(SpanRelation::default());
        let inner_context = KeyedMockContext::new(key);
        Self {
            store,
            inner_context,
        }
    }

    fn create_wrapped_context(&self) -> TimerDeferContext<KeyedMockContext, MemoryTimerDeferStore> {
        TimerDeferContext::new(
            self.inner_context.clone(),
            self.store.clone(),
            self.inner_context.key.clone(),
        )
    }

    fn key(&self) -> &Key {
        &self.inner_context.key
    }

    async fn defer_timer(&self, time_secs: u32) -> color_eyre::Result<()> {
        let trigger = self.create_trigger(time_secs);
        self.store.defer_first_timer(&trigger).await?;
        Ok(())
    }

    async fn defer_additional_timer(&self, time_secs: u32) -> color_eyre::Result<()> {
        let trigger = self.create_trigger(time_secs);
        self.store.defer_additional_timer(&trigger).await?;
        Ok(())
    }

    fn create_trigger(&self, time_secs: u32) -> Trigger {
        let time = CompactDateTime::from(time_secs);
        Trigger::new(
            self.inner_context.key.clone(),
            time,
            TimerType::Application,
            tracing::Span::current(),
        )
    }

    async fn is_deferred(&self) -> color_eyre::Result<bool> {
        Ok(self.store.is_deferred(self.key()).await?.is_some())
    }

    /// Returns all deferred times for this key, sorted ascending.
    async fn deferred_times(&self) -> color_eyre::Result<Vec<CompactDateTime>> {
        Ok(self.store.deferred_times(self.key()).await?)
    }
}

mod errors;
mod timers;
