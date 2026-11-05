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

/// Shuts down every partition manager in `managers`.
///
/// The poll loop runs this sweep before it drops the client, because each
/// partition queue holds the client. The consumer runs it again after the
/// poll loop joins. That second sweep finds the map empty unless the poll loop
/// failed. Each retained manager holds a handler clone, so this drain bounds
/// consumer shutdown.
pub(in crate::consumer) async fn drain_managers(managers: &Managers) -> Swept {
    let draining: FuturesUnordered<_> = managers
        .write()
        .drain()
        .map(|(_, manager)| manager.shutdown())
        .collect();
    draining.for_each(|_| ready(())).await;
    Swept(())
}
