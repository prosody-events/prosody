//! Domain-specific locking abstraction for timer system coordination.
//!
//! This module provides [`SlabLock`], a specialized locking mechanism that
//! coordinates access between two distinct types of operations in the timer
//! system:
//!
//! - **Trigger operations**: Individual timer scheduling, querying, and
//!   processing that can occur concurrently
//! - **Slab operations**: Structural changes like loading time slabs, ownership
//!   transfers, and system state modifications that require exclusive access
//!
//! The abstraction ensures that multiple trigger operations can proceed
//! simultaneously while slab operations have exclusive access when needed,
//! optimizing performance for the common case of concurrent timer processing.

use educe::Educe;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A specialized lock that coordinates trigger and slab operations.
///
/// [`SlabLock`] provides two distinct locking modes tailored to the timer
/// system's access patterns:
///
/// - **Trigger locks**: Allow concurrent access for operations that work with
///   individual timers (scheduling, querying, processing)
/// - **Slab locks**: Provide exclusive access for operations that modify system
///   structure (loading slabs, ownership changes, cleanup)
///
/// This design optimizes for the common case where multiple trigger operations
/// can safely occur simultaneously, while ensuring structural operations have
/// the exclusive access they require.
///
/// # Concurrency Model
///
/// - Multiple trigger operations can execute concurrently
/// - Slab operations are mutually exclusive with all other operations
/// - The lock is shared across threads via [`Clone`]
#[derive(Educe)]
#[educe(Clone(bound()), Debug)]
pub struct SlabLock<T> {
    #[educe(Debug(ignore))]
    inner: Arc<RwLock<T>>,
}

/// Guard providing concurrent access for trigger operations.
///
/// This guard allows multiple trigger operations to proceed simultaneously,
/// such as scheduling new timers, querying timer states, or processing
/// individual timer events. The underlying data cannot be modified through
/// this guard.
#[derive(Educe)]
#[educe(Debug)]
pub struct SlabLockTriggerGuard<'a, T>(#[educe(Debug(ignore))] RwLockReadGuard<'a, T>);

impl<T> Deref for SlabLockTriggerGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Guard providing exclusive access for slab operations.
///
/// This guard ensures exclusive access for operations that modify the timer
/// system's structure, such as loading new time slabs, transferring slab
/// ownership, or performing system-wide cleanup. No other operations can
/// proceed while this guard is held.
#[derive(Educe)]
#[educe(Debug)]
pub struct SlabLockSlabGuard<'a, T>(#[educe(Debug(ignore))] RwLockWriteGuard<'a, T>);

impl<T> Deref for SlabLockSlabGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for SlabLockSlabGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> SlabLock<T> {
    /// Creates a new slab lock containing the given value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to protect with this lock
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(value)),
        }
    }

    /// Acquires concurrent access for trigger operations.
    ///
    /// This method returns a guard that allows concurrent access with other
    /// trigger operations. Use this for operations that work with individual
    /// timers without modifying the overall system structure.
    pub async fn trigger_lock(&self) -> SlabLockTriggerGuard<T> {
        SlabLockTriggerGuard(self.inner.read().await)
    }

    /// Acquires exclusive access for slab operations.
    ///
    /// This method returns a guard that provides exclusive access to the
    /// protected data. Use this for operations that modify the timer system's
    /// structure, such as loading slabs or changing ownership.
    pub async fn slab_lock(&self) -> SlabLockSlabGuard<T> {
        SlabLockSlabGuard(self.inner.write().await)
    }
}
