//! Framework-internal engine authority: admission, the raw byte reads,
//! mutation replay, and the mid-handler durable pair.
//!
//! These traits carry `pub` only to keep the public session bounds above them
//! from tripping `private_bounds`. The module's own `pub(crate)` visibility is
//! the seal. Downstream code can project and bound `S::Engine`, but it cannot
//! name the traits. Their associated functions are therefore uncallable, and no
//! outside type can claim to have acquired owner admission.
//!
//! A private *supertrait* of a public trait would not seal a callable command,
//! because Rust permits that call through the public subtrait. Every command
//! that carries authority therefore lives one layer below anything a caller can
//! name.

use super::MutationJournal;
use crate::state::StateAccessError;
use crate::state::cell::{Presence, Projection, Values};
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::StructuralIdentity;
use crate::state::registry::CollectionDef;
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType, StoreOutcome};
use futures::Stream;
use std::future::Future;
use std::ops::DerefMut;

/// The engine a session type binds. The engine selection makes owner and
/// published-reader behavior a compile-time choice. No runtime branch
/// separates them.
pub trait Session: Sized {
    /// This session's engine.
    type Engine: ReadEngine<Self> + Reads<Self, Values> + Reads<Self, Presence>;
}

/// A session whose engine can also mutate. `WriteOperation` exists only
/// for these, so "mutate through read admission" is unrepresentable.
pub trait WritableSession: Session<Engine: WriteEngine<Self>> {}

/// Owns read admission, plan capture, and the dispatch fence.
pub trait ReadEngine<S: ?Sized> {
    /// The per-invocation state: the owner's gate permit, or the reader's
    /// operation-local source selection.
    type ReadInner<'a>: Send
    where
        S: 'a;

    /// The owned state a managed stream plan carries out of the invocation
    /// that built it. The owner plan keeps nothing, because each chunk
    /// reacquires admission. The reader plan keeps its selected source, so
    /// each chunk resumes on the source the planning command chose.
    type Plan: Clone + Send + Sync + 'static;

    /// Validates the collection named `name` against this engine's
    /// authority and returns its canonical name. The owner validates
    /// registration and structural identity against the registry. The
    /// published reader reuses the validation from its source acquisition.
    ///
    /// # Errors
    ///
    /// The engine's validation refusal. For the owner, that is an
    /// unregistered name or a structural-identity mismatch.
    fn verify_registration(
        session: &S,
        name: &'static str,
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError>;

    /// The collection's operational settings as this engine sees them. Each
    /// impl documents its source. The engine captures them once at bind, so
    /// every configuration query inside a scoped operation answers from one
    /// snapshot.
    fn collection_def(session: &S, state_type: StateType, name: &StateName) -> CollectionDef;

    /// Acquires this invocation's read state.
    fn begin_read(session: &S) -> impl Future<Output = Self::ReadInner<'_>> + Send;

    /// Freezes this invocation's state into the plan a managed stream
    /// driver runs on. Total: there is no unplannable invocation, so no
    /// driver carries an unreachable arm.
    fn capture(inner: &Self::ReadInner<'_>) -> Self::Plan;

    /// Re-enters an invocation under a captured plan for one coordinate
    /// chunk's admission. The owner reacquires the gate here. A coordinate
    /// stream therefore holds no gate across a yield.
    fn resume<'a>(
        session: &'a S,
        plan: &Self::Plan,
    ) -> impl Future<Output = Self::ReadInner<'a>> + Send;

    /// The per-emission fence a managed stream runs after every source
    /// completion, before the item or error escapes. Vacuous on the
    /// published reader, which has no attempt to leak past.
    ///
    /// # Errors
    ///
    /// [`StateAccessError::Terminated`] once the stream outlived its
    /// dispatch attempt.
    fn fence(session: &S) -> Result<(), StateAccessError>;
}

/// Reads one projection under the engine's admission and plan.
pub trait Reads<S: ?Sized, P: Projection>: ReadEngine<S> {
    /// Reads one projected cell and updates the invocation state.
    fn read_point(
        session: &S,
        inner: &mut Self::ReadInner<'_>,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<Option<P::Payload>, StateAccessError>> + Send;

    /// Reads an aligned batch and updates the invocation state.
    fn read_batch(
        session: &S,
        inner: &mut Self::ReadInner<'_>,
        state_type: StateType,
        name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<Option<P::Payload>>, StateAccessError>> + Send;

    /// Pages a durable range under a captured plan without the gate. This is
    /// the range driver's only lower hop and the one command that cannot
    /// repair.
    fn page<'a>(
        session: &'a S,
        plan: &'a Self::Plan,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, P::Payload), StateAccessError>> + Send + use<'a, Self, S, P>;
}

/// The write half of one engine: admission, the final fence, journal
/// replay, and the mid-handler durable pair.
pub trait WriteEngine<S: ?Sized>: ReadEngine<S> {
    /// The per-invocation write state. `DerefMut` to the read state is the
    /// one-way relation from write admission to the read admission it
    /// subsumes. A write operation therefore reuses the read driver unchanged,
    /// with no runtime variant and no inverse conversion.
    type WriteInner<'a>: DerefMut<Target = Self::ReadInner<'a>> + Send
    where
        S: 'a;

    /// Acquires this invocation's write state.
    ///
    /// # Errors
    ///
    /// The engine's admission refusal. For the owner, that is a stale
    /// attempt, a closed session, or termination.
    fn begin_write(
        session: &S,
    ) -> impl Future<Output = Result<Self::WriteInner<'_>, StateAccessError>> + Send;

    /// Rechecks admission at the end of the invocation, immediately before
    /// replay.
    ///
    /// # Errors
    ///
    /// As [`Self::begin_write`].
    fn validate_write(session: &S, inner: &Self::WriteInner<'_>) -> Result<(), StateAccessError>;

    /// Replays a validated journal into the event overlay. Synchronous and
    /// infallible by contract: there is no suspension point between the
    /// fence and the last staged mutation.
    fn apply(
        session: &S,
        state_type: StateType,
        name: &StateName,
        inner: &Self::WriteInner<'_>,
        journal: MutationJournal,
    );

    /// Durably commits the collection's buffered changes mid-invocation.
    ///
    /// # Errors
    ///
    /// Admission refusal, or a store failure.
    fn commit(
        session: &S,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;

    /// Discards the collection's buffered changes mid-invocation.
    fn rollback(
        session: &S,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = StoreOutcome> + Send;
}
