//! Type-erased collection handles for the four language bindings.
//!
//! Each adapter delegates to a typed collection handle. Cursors preserve
//! demand-driven reads and attempt fences. Only this FFI layer uses trait
//! objects.
//!
//! Maps expose `entries()` and `keys()`. Sets expose `keys()`.
//! Deques expose `values()`. Each builder supports the typed query methods.
//! Call `stream()` to create an owned cursor. Its first pull starts the read.
//! Builders own their handles and bounds, so clients can store them.
//! Commit and rollback return the typed handle's [`StoreOutcome`].

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StoreOutcome;
use async_trait::async_trait;
use std::fmt::Display;
use thiserror::Error;

/// Keeps typed and erased method signatures separate.
pub(crate) struct Erased<T>(pub(crate) T);

/// Two-way error category for the FFI state seam.
///
/// `Terminal` is deliberately absent: the keyed-state layer never surfaces it
/// (owner posture — a lower-layer `Terminal` redelivers as `Transient`), so it
/// is structurally unrepresentable here even if a caller bypasses the boundary
/// fold on [`ErasedStateError`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ErasedCategory {
    /// Business-logic failure — do not retry (unregistered name, codec error,
    /// null-write rejection).
    Permanent,

    /// Transient failure — retry may succeed (store/loader hiccup, a
    /// terminated attempt, a folded lower-layer `Terminal`).
    Transient,
}

impl From<ErasedCategory> for ErrorCategory {
    fn from(category: ErasedCategory) -> Self {
        match category {
            ErasedCategory::Permanent => ErrorCategory::Permanent,
            ErasedCategory::Transient => ErrorCategory::Transient,
        }
    }
}

/// An erased state error with its classification and message.
/// Private fields keep classification consistent across language clients.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ErasedStateError {
    category: ErasedCategory,
    message: String,
}

impl ErasedStateError {
    /// The sole fold point from a typed error to the erased seam: any
    /// classification maps through, with `Terminal` folded to `Transient` (the
    /// state layer never surfaces `Terminal`).
    pub(crate) fn from_classified<E>(error: &E) -> Self
    where
        E: ClassifyError + Display,
    {
        let category = match error.classify_error() {
            ErrorCategory::Permanent => ErasedCategory::Permanent,
            ErrorCategory::Transient | ErrorCategory::Terminal => ErasedCategory::Transient,
        };
        Self {
            category,
            message: error.to_string(),
        }
    }

    /// A synthetic terminated-family error — a [`StateCursor`] used after
    /// `close()` or a failure. `Transient`, mirroring
    /// [`StateAccessError::Terminated`](crate::state::StateAccessError).
    fn terminated(message: &str) -> Self {
        Self {
            category: ErasedCategory::Transient,
            message: message.to_owned(),
        }
    }

    /// The `Permanent` rejection of a JSON-null value write, naming
    /// `clear`/`remove` as the way to express deletion.
    fn null_write() -> Self {
        Self {
            category: ErasedCategory::Permanent,
            message: "JSON null is not a storable value; use clear (value/deque) or remove (map) \
                      to delete an entry"
                .to_owned(),
        }
    }

    /// This error's category, as data for the bindings.
    #[must_use]
    pub fn category(&self) -> ErasedCategory {
        self.category
    }

    /// The rendered error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl ClassifyError for ErasedStateError {
    fn classify_error(&self) -> ErrorCategory {
        self.category.into()
    }
}

/// Erased single-value collection — the object-safe face of
/// [`crate::state::descriptor::ValueHandle`].
#[async_trait]
pub trait DynValueState<Item: Send + 'static>: Send + Sync {
    /// Reads the current value (`None` when absent/cleared).
    async fn get(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Buffers a write of `item`. Rejects the JSON-null sentinel (`Permanent`).
    async fn set(&self, item: Item) -> Result<(), ErasedStateError>;

    /// Buffers a clear of the value.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<StoreOutcome, ErasedStateError>;

    /// Discards buffered uncommitted ops. Infallible no-op on a terminated
    /// session.
    async fn rollback(&self) -> StoreOutcome;
}

/// Erased ordered map — the object-safe face of
/// [`crate::state::descriptor::MapHandle`], keys always `String`.
#[async_trait]
pub trait DynMapState<Item: Send + 'static>: Send + Sync {
    /// Reads `key`'s value (`None` when absent).
    async fn get(&self, key: String) -> Result<Option<Item>, ErasedStateError>;

    /// Whether a stored cell exists for `key`, without decoding its value or
    /// running the resolver (a presence read through the dirty overlay).
    async fn contains_key(&self, key: String) -> Result<bool, ErasedStateError>;

    /// Whether the map holds no live entries.
    async fn is_empty(&self) -> Result<bool, ErasedStateError>;

    /// Reads each key in input order as one aligned batch. Absent keys yield
    /// `None`, and duplicate keys retain their positions.
    async fn get_many(&self, keys: Vec<String>) -> Result<Vec<Option<Item>>, ErasedStateError>;

    /// Tests each key for presence in input order as one aligned batch.
    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError>;

    /// Inserts or overwrites `key`. Rejects the JSON-null sentinel
    /// (`Permanent`).
    async fn set(&self, key: String, item: Item) -> Result<(), ErasedStateError>;

    /// Removes `key`.
    async fn remove(&self, key: String) -> Result<(), ErasedStateError>;

    /// Removes every entry.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// A fluent query over the live entries in key order.
    fn entries(&self) -> ErasedKeyRead<(String, Item)>;

    /// A fluent query over the live entry **keys** in key order,
    /// without decoding or resolving any value (zero Kafka fetches for a
    /// message-backed map). A key is present even when its value is not.
    fn keys(&self) -> ErasedKeyRead<String>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<StoreOutcome, ErasedStateError>;

    /// Discards buffered uncommitted ops.
    async fn rollback(&self) -> StoreOutcome;
}

/// Erased presence-only ordered set with `String` keys.
#[async_trait]
pub trait DynSetState: Send + Sync {
    /// Tests whether `key` belongs to the set.
    async fn contains(&self, key: String) -> Result<bool, ErasedStateError>;

    /// Tests each key for membership in input order.
    async fn contains_many(&self, keys: Vec<String>) -> Result<Vec<bool>, ErasedStateError>;

    /// Reports whether the set has no live members.
    async fn is_empty(&self) -> Result<bool, ErasedStateError>;

    /// Inserts `key` into the set.
    async fn insert(&self, key: String) -> Result<(), ErasedStateError>;

    /// Removes `key` from the set.
    async fn remove(&self, key: String) -> Result<(), ErasedStateError>;

    /// Removes all members.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// Builds a fluent query over live keys.
    fn keys(&self) -> ErasedKeyRead<String>;

    /// Commits buffered set operations.
    async fn commit(&self) -> Result<StoreOutcome, ErasedStateError>;

    /// Discards buffered set operations.
    async fn rollback(&self) -> StoreOutcome;
}

/// Erased deque — the object-safe face of
/// [`crate::state::descriptor::DequeHandle`].
#[async_trait]
pub trait DynDequeState<Item: Send + 'static>: Send + Sync {
    /// The number of live elements.
    async fn len(&self) -> Result<usize, ErasedStateError>;

    /// Whether the deque holds no live elements.
    async fn is_empty(&self) -> Result<bool, ErasedStateError>;

    /// Reads the element at front-relative position `index` (`None` past the
    /// end).
    async fn get(&self, index: usize) -> Result<Option<Item>, ErasedStateError>;

    /// Appends at the back. Rejects the JSON-null sentinel (`Permanent`).
    async fn push_back(&self, item: Item) -> Result<(), ErasedStateError>;

    /// Prepends at the front. Rejects the JSON-null sentinel (`Permanent`).
    async fn push_front(&self, item: Item) -> Result<(), ErasedStateError>;

    /// Removes and returns the front element (`None` when empty).
    async fn pop_front(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Removes and returns the back element (`None` when empty).
    async fn pop_back(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Reads the front element without a length read (`None` when empty).
    async fn peek_front(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Reads the back element without a length read (`None` when empty).
    async fn peek_back(&self) -> Result<Option<Item>, ErasedStateError>;

    /// Removes every element.
    async fn clear(&self) -> Result<(), ErasedStateError>;

    /// A fluent query over the live elements in index order.
    fn values(&self) -> ErasedDequeRead<Item>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<StoreOutcome, ErasedStateError>;

    /// Discards buffered uncommitted ops.
    async fn rollback(&self) -> StoreOutcome;
}

/// Boxed erased single-value handle a vend method returns.
pub type BoxValueState<Item> = Box<dyn DynValueState<Item>>;

/// Boxed erased map handle a vend method returns.
pub type BoxMapState<Item> = Box<dyn DynMapState<Item>>;
/// Boxed erased set handle.
pub type BoxSetState = Box<dyn DynSetState>;

/// Boxed erased deque handle a vend method returns.
pub type BoxDequeState<Item> = Box<dyn DynDequeState<Item>>;

mod cursor;
mod deque;
mod map;
mod set;
mod value;
mod write;
pub use cursor::StateCursor;
pub(crate) use write::ErasedWrite;

mod query;
pub(crate) use query::read;
pub use query::{ErasedDequeRead, ErasedKeyRead, ErasedReadSource};
