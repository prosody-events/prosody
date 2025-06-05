use educe::Educe;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Educe)]
#[educe(Clone(bound()), Debug)]
pub struct SlabLock<T> {
    #[educe(Debug(ignore))]
    inner: Arc<RwLock<T>>,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct SlabLockTriggerGuard<'a, T>(#[educe(Debug(ignore))] RwLockReadGuard<'a, T>);

impl<T> Deref for SlabLockTriggerGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(value)),
        }
    }

    pub async fn trigger_lock(&self) -> SlabLockTriggerGuard<T> {
        SlabLockTriggerGuard(self.inner.read().await)
    }

    pub async fn slab_lock(&self) -> SlabLockSlabGuard<T> {
        SlabLockSlabGuard(self.inner.write().await)
    }
}
