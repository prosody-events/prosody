use crate::timers::slab::SlabId;
use educe::Educe;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Educe)]
#[educe(Clone(bound()), Debug)]
pub struct RangeLock<T> {
    #[educe(Debug(ignore))]
    range: Arc<RwLock<ContiguousRange<T>>>,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct RangeLockTriggerGuard<'a, T>(
    #[educe(Debug(ignore))] RwLockReadGuard<'a, ContiguousRange<T>>,
);

impl<T> Deref for RangeLockTriggerGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

impl<T> RangeLockTriggerGuard<'_, T> {
    pub fn is_owned(&self, slab_id: SlabId) -> bool {
        self.0.is_owned(slab_id)
    }
}

#[derive(Educe)]
#[educe(Debug)]
pub struct RangeLockSlabGuard<'a, T>(
    #[educe(Debug(ignore))] RwLockWriteGuard<'a, ContiguousRange<T>>,
);

impl<T> Deref for RangeLockSlabGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

impl<T> DerefMut for RangeLockSlabGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.value
    }
}

impl<T> RangeLockSlabGuard<'_, T> {
    pub fn value(&mut self) -> &mut T {
        &mut self.0.value
    }

    pub fn extend_ownership(&mut self, new_max: SlabId) {
        self.0.extend_ownership(new_max);
    }
}

impl<T> RangeLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            range: Arc::new(RwLock::new(ContiguousRange::new(value))),
        }
    }

    pub async fn trigger_lock(&self) -> RangeLockTriggerGuard<T> {
        RangeLockTriggerGuard(self.range.read().await)
    }

    pub async fn slab_lock(&self) -> RangeLockSlabGuard<T> {
        RangeLockSlabGuard(self.range.write().await)
    }
}

#[derive(Clone, Debug)]
struct ContiguousRange<T> {
    value: T,
    max_owned: Option<SlabId>,
}

impl<T> ContiguousRange<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            max_owned: None,
        }
    }

    pub fn is_owned(&self, slab_id: SlabId) -> bool {
        self.max_owned.filter(|max| slab_id <= *max).is_some()
    }

    pub fn extend_ownership(&mut self, new_max: SlabId) {
        if self
            .max_owned
            .is_some_and(|current_max| current_max > new_max)
        {
            return;
        }

        self.max_owned = Some(new_max);
    }
}
