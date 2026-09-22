//! Descriptor access to the lifecycle and marker of an event session.

use super::sealed::{MarkerIdentity, MessageMarker};
use crate::consumer::event_context::EventContext;
use crate::state::CollectionKindId;
use crate::state::access::StateAccessError;
use crate::state::collection::StateSession;
use crate::state::descriptor::{
    DescriptorIdentity, Registered, SealedDescriptor, StateDescriptor, StructuralIdentity,
};
use crate::state::registry::CollectionDef;

/// Crate-private descriptor reaching a session through the one public
/// [`EventContext::state`] method — the sole state surface wrapper contexts
/// forward. Binding it yields the session itself (`Handle<S> = S`), so the
/// settlement boundary drives the full sealed [`StateLifecycle`] on it.
///
/// This is the **settlement surface**: the sealed [`StateLifecycle`] verbs.
/// It stays `pub(crate)` because the settle-module-private
/// [`SettlementAccess`](crate::consumer::middleware::settle) extension must
/// name it. Residual: no convenient crate-wide accessor exists (the old
/// `LifecycleAccessExt` is gone), so reaching this surface outside settle
/// requires writing `context.state(Registered::new(LifecycleAccess))` plus a
/// `use sealed::StateLifecycle` by hand — a deliberate, greppable act rather
/// than a one-call convenience. Dedup / defer-reload reach only the marker
/// identity, through the narrow [`MarkerHandle`].
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct LifecycleAccess;

impl DescriptorIdentity for LifecycleAccess {
    /// Inert: [`LifecycleAccess::bind`](StateDescriptor::bind) returns the
    /// session verbatim without validating registration, and `LifecycleAccess`
    /// is never registered, so neither `name` nor `structural_identity` is ever
    /// consulted. They exist only to satisfy the [`StateDescriptor`]
    /// supertrait.
    fn name(&self) -> &'static str {
        "\u{0}lifecycle"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            format_id: "\u{0}framework-lifecycle",
            resolver_id: None,
            key_format_id: "\u{0}framework-lifecycle",
        }
    }
}

impl SealedDescriptor for LifecycleAccess {}

impl StateDescriptor for LifecycleAccess {
    type Handle<S: StateSession> = S;

    /// Returns the session itself — the lifecycle tunnel binds no typed handle
    /// and validates no registration; the boundary drives the sealed
    /// [`StateLifecycle`] on the returned session. Every real caller binds a
    /// [`EventSession`], so the returned `S` carries the full lifecycle.
    fn bind<S: StateSession>(self, session: &S) -> Result<S, StateAccessError> {
        Ok(session.clone())
    }

    /// No-op: the lifecycle tunnel carries no operational settings, so it keeps
    /// the default [`collection_def`](StateDescriptor::collection_def) and the
    /// inherited fluent setters are unreachable no-ops.
    fn with_collection_def(self, _def: CollectionDef) -> Self {
        self
    }
}

/// The narrow marker-identity handle handed to the three
/// [`MarkerIdentity`] audiences (defer-reload set,
/// dedup read, settle read). Wraps a session clone but exposes **only** the
/// two marker methods — never the raw session, so it cannot reach the
/// settlement surface. This is the tunnel-narrowing enforcement: dedup and
/// defer-reload bind [`MarkerAccess`] and get one of these, so they cannot
/// import `sealed::StateLifecycle` and call `close_gate` / `finalize`.
pub(crate) struct MarkerHandle<S>(S);

impl<S: MarkerIdentity> MarkerHandle<S> {
    /// Sets the deferred-reload identity override — see
    /// [`MarkerIdentity::set_reload_marker`].
    pub(crate) fn set_reload_marker(&self, marker: MessageMarker) {
        self.0.set_reload_marker(marker);
    }

    /// The message commit-marker identity — see
    /// [`MarkerIdentity::message_marker`].
    pub(crate) fn message_marker(&self) -> Option<MessageMarker> {
        self.0.message_marker()
    }
}

/// Crate-private descriptor binding a session's marker-identity surface,
/// forwarded through the one public [`EventContext::state`] method exactly as
/// [`LifecycleAccess`] is. Binding yields a [`MarkerHandle`], never the raw
/// session, so the audience reaches only `set_reload_marker`/`message_marker`.
///
/// Deliberately a full sibling of `LifecycleAccess` rather than a shared
/// generic descriptor — the two distinct bound `Handle` types are the tunnel
/// split itself.
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarkerAccess;

impl DescriptorIdentity for MarkerAccess {
    /// Inert, exactly as [`LifecycleAccess`]: `MarkerAccess` is never
    /// registered, so neither field is consulted — they satisfy the
    /// [`StateDescriptor`] supertrait only.
    fn name(&self) -> &'static str {
        "\u{0}marker"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            format_id: "\u{0}framework-marker",
            resolver_id: None,
            key_format_id: "\u{0}framework-marker",
        }
    }
}

impl SealedDescriptor for MarkerAccess {}

impl StateDescriptor for MarkerAccess {
    type Handle<S: StateSession> = MarkerHandle<S>;

    /// Wraps the session in a [`MarkerHandle`], validating no registration —
    /// the marker tunnel carries no typed collection.
    fn bind<S: StateSession>(self, session: &S) -> Result<MarkerHandle<S>, StateAccessError> {
        Ok(MarkerHandle(session.clone()))
    }

    /// No-op: the marker tunnel carries no operational settings.
    fn with_collection_def(self, _def: CollectionDef) -> Self {
        self
    }
}

/// Crate-private extension giving the marker-identity audiences (defer-reload,
/// dedup, and settle) one-call access to their event's [`MarkerHandle`]
/// through the public [`EventContext::state`] method — the narrow replacement
/// for the deleted crate-wide `lifecycle()` accessor.
pub(crate) trait MarkerAccessExt: EventContext {
    /// Binds the event's marker-identity handle. Fails with
    /// [`StateAccessError`] only when the context is terminated;
    /// [`MarkerAccess`] is otherwise registration-independent.
    fn marker_identity(&self) -> Result<MarkerHandle<Self::State>, StateAccessError> {
        self.state(Registered::new(MarkerAccess))
    }
}

impl<C: EventContext> MarkerAccessExt for C {}
