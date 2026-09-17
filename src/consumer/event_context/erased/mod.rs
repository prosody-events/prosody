//! Type-erased collection handles for the four language bindings.
//!
//! Each adapter delegates to a typed collection handle. Cursors preserve
//! demand-driven reads and attempt fences. Only this FFI layer uses trait
//! objects.

use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cell_key::{Direction, ScanEdge};
use crate::state::descriptor::map::Query;
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use async_trait::async_trait;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::ops::Bound;
use thiserror::Error;

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

/// Erased scan constraints over edges of type `E`.
/// The default scans forward, unbounded, and without a limit.
#[derive(Clone, Debug)]
pub struct ScanConfig<E> {
    /// The scan direction.
    pub dir: Direction,
    /// The maximum number of present items.
    pub limit: Option<NonZeroUsize>,
    /// The inclusive, exclusive, or open range start.
    pub start: Bound<E>,
    /// The inclusive, exclusive, or open range end.
    pub end: Bound<E>,
}

/// Map and set scan constraints. The key edges follow the scan direction.
pub type KeyScanConfig = ScanConfig<String>;

/// Deque scan constraints. The position edges count from the front.
pub type DequeScanConfig = ScanConfig<u64>;

impl<E> Default for ScanConfig<E> {
    fn default() -> Self {
        Self {
            dir: Direction::Forward,
            limit: None,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl From<ErasedCategory> for ErrorCategory {
    fn from(category: ErasedCategory) -> Self {
        match category {
            ErasedCategory::Permanent => ErrorCategory::Permanent,
            ErasedCategory::Transient => ErrorCategory::Transient,
        }
    }
}

/// The error every erased state op and vend method returns.
///
/// Carries its classification as data ([`ErasedCategory`]) so the four
/// bindings can branch on it directly; [`ClassifyError`] also reaches it
/// through the box, so the binding handler-error bridges reclassify with zero
/// changes. Fields are private so no caller can mint an inconsistently
/// classified error — the two `pub(crate)` constructors are the only mints.
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
    async fn commit(&self) -> Result<(), ErasedStateError>;

    /// Discards buffered uncommitted ops. Infallible no-op on a terminated
    /// session.
    async fn rollback(&self);
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

    /// A demand-driven cursor over the live entries in key order.
    fn scan(&self, config: KeyScanConfig) -> BoxStateCursor<(String, Item)>;

    /// A demand-driven cursor over the live entry **keys** in key order,
    /// without decoding or resolving any value (zero Kafka fetches for a
    /// message-backed map). A key is present even when its value is not.
    fn keys(&self, config: KeyScanConfig) -> BoxStateCursor<String>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<(), ErasedStateError>;

    /// Discards buffered uncommitted ops.
    async fn rollback(&self);
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
    /// Returns a demand-driven cursor over live keys.
    fn keys(&self, config: KeyScanConfig) -> BoxStateCursor<String>;
    /// Commits buffered set operations.
    async fn commit(&self) -> Result<(), ErasedStateError>;
    /// Discards buffered set operations.
    async fn rollback(&self);
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

    /// A demand-driven cursor over the live elements in index order.
    fn scan(&self, config: DequeScanConfig) -> BoxStateCursor<Item>;

    /// Durably commits buffered ops mid-handler (at-least-once).
    async fn commit(&self) -> Result<(), ErasedStateError>;

    /// Discards buffered uncommitted ops.
    async fn rollback(&self);
}

/// Boxed erased single-value handle a vend method returns.
pub type BoxValueState<Item> = Box<dyn DynValueState<Item>>;

/// Boxed erased map handle a vend method returns.
pub type BoxMapState<Item> = Box<dyn DynMapState<Item>>;
/// Boxed erased set handle.
pub type BoxSetState = Box<dyn DynSetState>;

/// Boxed erased deque handle a vend method returns.
pub type BoxDequeState<Item> = Box<dyn DynDequeState<Item>>;

/// Boxed [`StateCursor`] a `scan` returns.
pub type BoxStateCursor<Item> = Box<StateCursor<Item>>;

fn bound_usize(bound: Bound<u64>) -> Bound<usize> {
    bound.map(|value| usize::try_from(value).unwrap_or(usize::MAX))
}

/// Encodes the shared map and set query bounds.
fn key_query(config: KeyScanConfig) -> Query {
    let edge = |bound: Bound<String>| match bound {
        Bound::Included(key) => ScanEdge::Included(Utf8KeyCodec::encode(&key)),
        Bound::Excluded(key) => ScanEdge::Excluded(Utf8KeyCodec::encode(&key)),
        Bound::Unbounded => ScanEdge::Unbounded,
    };
    Query {
        dir: config.dir,
        limit: config.limit,
        start: edge(config.start),
        end: edge(config.end),
    }
}

mod cursor;
mod deque;
mod map;
mod set;
mod value;
mod write;
pub use cursor::StateCursor;
pub(super) use deque::ErasedDeque;
pub(super) use map::ErasedMap;
pub(super) use set::ErasedSet;
pub(super) use value::ErasedValue;
