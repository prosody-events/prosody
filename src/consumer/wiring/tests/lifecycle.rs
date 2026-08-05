//! What a consumer does about the peer runtime at startup and at shutdown.

use super::{
    Event, EventLog, RecordingBackend, RecordingDirectory, consumer_config, peer_config,
    retain_manager, start,
};
use crate::consumer::Managers;
use crate::consumer::error::{ConsumerError, PeerInitError};
use crate::consumer::wiring::peer::{NoPeer, prepare_requester};
use crate::heartbeat::HeartbeatRegistry;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use parking_lot::Mutex;
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;

/// A consumer that carries no peer configuration starts, runs and stops
/// exactly as it did before the peer runtime existed: it touches no directory,
/// and its shutdown reports success.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_without_a_peer_starts_and_stops() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let config = consumer_config("peer-lifecycle-none")?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let consumer = start(&config, managers, heartbeats, Arc::clone(&log), NoPeer).await?;

    consumer.shutdown().await?;

    // The poll task drops the provider, and nothing else records anything. An
    // equality rather than a predicate: a predicate over an empty log would
    // pass without the consumer ever running.
    assert_eq!(*log.lock(), vec![Event::ProviderDropped]);
    Ok(())
}

/// The peer runtime outlives the handlers. Shutdown joins the poll loop, which
/// drops the client and therefore the handler provider, then sweeps the
/// partition manager the final revoke left behind, and only then lets the
/// coordinator deregister this node.
///
/// The sweep is what bounds the peer teardown, so its position between the two
/// is asserted rather than its occurrence.
#[tokio::test(flavor = "multi_thread")]
async fn peer_teardown_follows_the_poll_loop_and_the_sweep() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let directory = RecordingDirectory::new(Arc::clone(&log), false);
    let backend = RecordingBackend {
        directory: directory.clone(),
    };
    let config = consumer_config("peer-lifecycle-order")?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let attachment = prepare_requester(&peer, &backend, Arc::clone(&managers), &heartbeats).await?;
    let consumer = start(
        &config,
        Arc::clone(&managers),
        heartbeats,
        Arc::clone(&log),
        attachment,
    )
    .await?;
    assert!(
        log.lock()
            .iter()
            .any(|event| matches!(event, Event::Registered { .. })),
        "startup must register the peer"
    );
    retain_manager(&config, &managers, Arc::clone(&log))?;

    consumer.shutdown().await?;

    let events = log.lock();
    let position = |wanted: &Event| events.iter().position(|event| event == wanted);
    let provider = position(&Event::ProviderDropped)
        .ok_or_else(|| eyre!("the poll loop kept its provider"))?;
    let swept = position(&Event::ManagerSwept)
        .ok_or_else(|| eyre!("shutdown did not sweep the retained partition manager"))?;
    let deregistered = position(&Event::Deregistered)
        .ok_or_else(|| eyre!("peer teardown did not deregister the node"))?;
    ensure!(provider < swept, "the sweep preceded the poll loop");
    ensure!(swept < deregistered, "peer teardown preceded the sweep");
    ensure!(
        events
            .iter()
            .filter(|event| matches!(event, Event::Deregistered))
            .count()
            == 1,
        "peer teardown must deregister exactly once"
    );
    assert_eq!(events.last(), Some(&Event::Deregistered));
    Ok(())
}

/// Only the caller that takes the runtime state runs the teardown. A clone
/// whose sibling already shut the consumer down touches nothing shared.
///
/// The partition managers are the shared state that would be lost: a losing
/// caller that swept them would tear down machinery a live poll loop is still
/// feeding, and would break the one-manager-per-partition rule when a rebalance
/// lands in that window.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_shutdown_sweeps_nothing() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let config = consumer_config("peer-lifecycle-second")?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let consumer = start(
        &config,
        Arc::clone(&managers),
        heartbeats,
        Arc::clone(&log),
        NoPeer,
    )
    .await?;
    let loser = consumer.clone();

    consumer.shutdown().await?;
    // Retained after the winner finished, so only the loser could sweep it.
    retain_manager(&config, &managers, Arc::clone(&log))?;
    loser.shutdown().await?;

    assert_eq!(
        managers.read().len(),
        1,
        "the losing shutdown drained the shared partition managers"
    );
    assert_eq!(*log.lock(), vec![Event::ProviderDropped]);
    Ok(())
}

/// A first directory write that applied and then failed is rolled back, and
/// the served listener is released.
///
/// The current-thread flavour is deliberate. The listener task cannot run
/// between the abandon that joins it and the synchronous rebind below, so the
/// rebind observes the release rather than a race.
#[tokio::test]
async fn failed_activation_rolls_back_and_releases_the_listener() -> Result<()> {
    let log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let directory = RecordingDirectory::new(Arc::clone(&log), true);
    let backend = RecordingBackend {
        directory: directory.clone(),
    };
    let config = consumer_config("peer-lifecycle-rollback")?;
    let managers: Arc<Managers<Value>> = Arc::default();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let attachment = prepare_requester(&peer, &backend, Arc::clone(&managers), &heartbeats).await?;

    let error = start(&config, managers, heartbeats, Arc::clone(&log), attachment)
        .await
        .err()
        .ok_or_else(|| eyre!("activation succeeded despite the scripted failure"))?;
    assert!(
        matches!(error, ConsumerError::Peer(PeerInitError::Directory { .. })),
        "activation returned the wrong error: {error:#}"
    );

    let events = log.lock();
    let (port, held) = events
        .iter()
        .find_map(|event| match event {
            Event::RegisterFailed { port, port_held } => Some((*port, *port_held)),
            _ => None,
        })
        .ok_or_else(|| eyre!("the directory did not record the failed registration"))?;
    assert!(
        held,
        "the peer listener did not hold its port during registration"
    );
    assert!(
        matches!(
            events.as_slice(),
            [
                Event::RegisterFailed { .. },
                Event::Deregistered,
                Event::ProviderDropped
            ]
        ),
        "activation rollback events were {events:?}"
    );
    drop(events);
    assert_eq!(directory.inner.len(), 0);
    let rebound = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    drop(rebound);
    Ok(())
}
