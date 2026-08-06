//! The partition-manager sweep, and the proof that it ran.

use super::Managers;
use futures::StreamExt;
use futures::future::ready;
use futures::stream::FuturesUnordered;

/// Proof that the partition-manager sweep finished.
///
/// [`drain_managers`] mints this after the last manager stops, and the field is
/// private to this module. So a step that demands this value cannot be written
/// ahead of the sweep, and no caller can fabricate the proof.
///
/// The proof names no map on purpose. Carrying the swept map's identity would
/// add a type parameter to both the sweep and the step that spends the proof,
/// to refuse a caller that does not exist: one map is in scope at the one call
/// site, and nothing else in the crate mints or spends a `Swept`.
pub(in crate::consumer) struct Swept(());

/// Shuts down every partition manager the final revoke left behind.
///
/// rdkafka skips its close-poll loop when the queue cannot be closed, and the
/// final revoke never dispatches then. Each retained manager holds a handler
/// clone, so this drain is what bounds the peer teardown that follows. After a
/// normal revoke the map is already empty.
pub(in crate::consumer) async fn drain_managers<P: Send + 'static>(
    managers: &Managers<P>,
) -> Swept {
    let draining: FuturesUnordered<_> = managers
        .write()
        .drain()
        .map(|(_, manager)| manager.shutdown())
        .collect();
    draining.for_each(|_| ready(())).await;
    Swept(())
}
