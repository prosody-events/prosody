//! Error types and aliases raised by the keyed-state middleware.

use super::context::KeyedStateContext;
use crate::consumer::event_context::BoxEventContextError;
use crate::consumer::middleware::FallibleHandler;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::oracle::CommitOracle;
use crate::state::pending::PendingIndexScanner;
use crate::state::value::{DurableWalStore, TransactionValueStoreError, ValueKind, ValueStore};
use crate::timers::datetime::CompactDateTimeError;
use std::error::Error;
use thiserror::Error;

/// Bundle of the constraints every keyed-state error component satisfies.
///
/// Folds the `ClassifyError + Error + Send + Sync + 'static` bound repeated
/// across [`KeyedStateMiddlewareError`] and its impls into one name. The
/// wrapped-handler error is bound separately because it need not be `Sync`.
pub trait MiddlewareErrorComponent: ClassifyError + Error + Send + Sync + 'static {}

impl<T> MiddlewareErrorComponent for T where T: ClassifyError + Error + Send + Sync + 'static {}

/// Errors raised by the middleware itself.
#[derive(Debug, Error)]
pub enum KeyedStateMiddlewareError<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr>
where
    InnerErr: ClassifyError + Error + Send + 'static,
    DirtyErr: MiddlewareErrorComponent,
    DurableErr: MiddlewareErrorComponent,
    ScannerErr: MiddlewareErrorComponent,
    OracleErr: MiddlewareErrorComponent,
{
    /// The wrapped handler returned an error.
    #[error("wrapped handler failed")]
    Inner(#[source] InnerErr),

    /// A durable Value store operation failed.
    #[error("keyed-state durable store failed")]
    Durable(#[source] DurableErr),

    /// A scanner pull failed.
    #[error("keyed-state pending scanner failed")]
    Scanner(#[source] ScannerErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),

    /// The dirty-store factory failed at partition assignment time.
    /// Surfaced on every dispatch for the affected partition until
    /// revocation.
    #[error("keyed-state dirty factory failed at partition assignment")]
    Factory(#[source] BoxedFactoryError),

    /// Scheduling or unscheduling the recovery timer failed.
    ///
    /// Carries the type-erased context error (`C::Error` is a per-method
    /// generic the impl-level `Error` cannot name), matching the defer
    /// middleware's `DeferError::Timer(BoxEventContextError)`.
    #[error("keyed-state recovery timer failed: {0:#}")]
    Timer(BoxEventContextError),

    /// The keyed-state transaction state machine refused the requested
    /// transition (e.g. sealing in direct mode).
    #[error("keyed-state transaction failed")]
    Transaction(#[source] TransactionValueStoreError<DirtyErr, DurableErr>),

    /// `CompactDateTime` arithmetic failed when computing the recovery
    /// fire time.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),
}

impl<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr> ClassifyError
    for KeyedStateMiddlewareError<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr>
where
    InnerErr: ClassifyError + Error + Send + 'static,
    DirtyErr: MiddlewareErrorComponent,
    DurableErr: MiddlewareErrorComponent,
    ScannerErr: MiddlewareErrorComponent,
    OracleErr: MiddlewareErrorComponent,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Inner(e) => e.classify_error(),
            Self::Durable(e) => e.classify_error(),
            Self::Scanner(e) => e.classify_error(),
            Self::Oracle(e) => e.classify_error(),
            Self::Factory(e) => e.classify_error(),
            Self::Timer(e) => e.classify_error(),
            Self::Transaction(e) => e.classify_error(),
            Self::DateTime(e) => e.classify_error(),
        }
    }
}

/// Factory error captured at partition assignment time and surfaced on
/// every dispatch for that partition until revocation.
///
/// The original `F::Error` is type-erased to a stable boxed shape so the
/// handler can clone it on each dispatch (the original `F::Error` is not
/// required to be `Clone`).
#[derive(Clone, Debug, Error)]
#[error("keyed-state factory error: {message}")]
pub struct BoxedFactoryError {
    message: String,
    category: ErrorCategory,
}

impl BoxedFactoryError {
    pub(super) fn new<E>(err: &E) -> Self
    where
        E: ClassifyError + Error + ?Sized,
    {
        Self {
            message: format!("{err}"),
            category: err.classify_error(),
        }
    }
}

impl ClassifyError for BoxedFactoryError {
    fn classify_error(&self) -> ErrorCategory {
        self.category
    }
}

pub(super) type MiddlewareError<T, D, Sc, O, S> = KeyedStateMiddlewareError<
    <T as FallibleHandler>::Error,
    <S as ValueStore>::Error,
    <D as DurableWalStore<ValueKind>>::Error,
    <Sc as PendingIndexScanner>::Error,
    <O as CommitOracle>::Error,
>;

/// The `build_context` result: the wrapped context or a fully-typed
/// middleware error. Named so the handler signature reads cleanly without a
/// `clippy::type_complexity` allow.
pub(super) type BuildContextResult<C, T, D, Sc, O, S> =
    Result<KeyedStateContext<C, D, S>, MiddlewareError<T, D, Sc, O, S>>;

/// Errors raised by the shared state-recovery sweep
/// `recover_pending_entries`.
///
/// The sweep only ever fails while scanning the pending index, reading or
/// mutating durable state, consulting the oracle, or clearing the recovery
/// timer. It never runs the inner handler, drives a transaction, or
/// computes a fire time, so those variants are absent — the production
/// caller lifts this into [`KeyedStateMiddlewareError`] via `?`.
#[derive(Debug, Error)]
pub(crate) enum RecoveryError<DurableErr, ScannerErr, OracleErr>
where
    DurableErr: Error + 'static,
    ScannerErr: Error + 'static,
    OracleErr: Error + 'static,
{
    /// A durable Value store operation failed.
    #[error("keyed-state durable store failed")]
    Durable(#[source] DurableErr),

    /// A scanner pull failed.
    #[error("keyed-state pending scanner failed")]
    Scanner(#[source] ScannerErr),

    /// The commit oracle failed.
    #[error("keyed-state commit oracle failed")]
    Oracle(#[source] OracleErr),

    /// Clearing the recovery timer failed (type-erased context error).
    #[error("keyed-state recovery timer failed: {0:#}")]
    Timer(BoxEventContextError),
}

impl<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr>
    From<RecoveryError<DurableErr, ScannerErr, OracleErr>>
    for KeyedStateMiddlewareError<InnerErr, DirtyErr, DurableErr, ScannerErr, OracleErr>
where
    InnerErr: ClassifyError + Error + Send + 'static,
    DirtyErr: MiddlewareErrorComponent,
    DurableErr: MiddlewareErrorComponent,
    ScannerErr: MiddlewareErrorComponent,
    OracleErr: MiddlewareErrorComponent,
{
    fn from(err: RecoveryError<DurableErr, ScannerErr, OracleErr>) -> Self {
        match err {
            RecoveryError::Durable(e) => Self::Durable(e),
            RecoveryError::Scanner(e) => Self::Scanner(e),
            RecoveryError::Oracle(e) => Self::Oracle(e),
            RecoveryError::Timer(e) => Self::Timer(e),
        }
    }
}
