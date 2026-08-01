//! Pooled, thread-local serialize buffer.
//!
//! Hot serialize paths (the Kafka producer's record encoding, the keyed-state
//! cell encoding a collection's `set` command performs) borrow a reusable
//! `Vec<u8>` instead of
//! allocating a fresh one per call. [`SerializeBufGuard::acquire`] takes the
//! per-thread buffer; on drop the guard clears it and returns it, keeping the
//! larger of the two capacities so steady-state encoding stops allocating.

use std::cell::RefCell;
use std::mem::take;
use std::ops::{Deref, DerefMut};

thread_local! {
    static SERIALIZE_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// RAII guard that takes the per-thread serialize buffer for the duration of a
/// serialize and returns it on drop, preserving its capacity for reuse.
///
/// `acquire` releases the `RefCell` borrow immediately (it `take`s the buffer
/// out), so a nested acquire on the same thread cannot panic — the inner call
/// simply gets a fresh empty buffer and returns the larger one on drop.
pub(crate) struct SerializeBufGuard {
    buf: Vec<u8>,
}

impl SerializeBufGuard {
    pub(crate) fn acquire() -> Self {
        Self {
            buf: SERIALIZE_BUF.with_borrow_mut(take),
        }
    }
}

impl Drop for SerializeBufGuard {
    fn drop(&mut self) {
        let mut buf = take(&mut self.buf);
        buf.clear();
        SERIALIZE_BUF.with_borrow_mut(|tls| {
            if buf.capacity() > tls.capacity() {
                *tls = buf;
            }
        });
    }
}

impl Deref for SerializeBufGuard {
    type Target = Vec<u8>;

    fn deref(&self) -> &Vec<u8> {
        &self.buf
    }
}

impl DerefMut for SerializeBufGuard {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

#[cfg(test)]
mod tests;
