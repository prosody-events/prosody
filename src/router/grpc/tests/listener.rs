//! What the listener itself bounds: the port it publishes, how many
//! connections it holds, and what it discloses.

use super::{FRAME_CAP, Harness, transport};
use crate::response::frame::FrameCap;
use crate::router::directory::tests::support::{directory, store};
use crate::router::grpc::codec::ClientFrameCodec;
use crate::router::grpc::conn::admitted;
use crate::router::grpc::{BoundListener, TRANSPORT, TransportConfiguration};
use crate::router::loopback::HANG_GUARD;
use crate::router::runtime::{PeerRuntime, RouterConfiguration};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use futures::StreamExt;
use std::net::Ipv4Addr;
use std::pin::pin;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::sleep;
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Endpoint as Dialled;
use tonic::{Code, Request};
use validator::Validate;

/// The Cassandra contact point the routed-address probe aims at.
const CONTACT: &str = "localhost:9042";

/// The reflection method a generic client calls to learn what a port serves.
const REFLECTION: &str = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";

/// A listener bound to port zero publishes the port the operating system
/// assigned, because that is the only port registration can read.
#[test]
fn a_registration_publishes_the_port_the_listener_bound() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let bound = BoundListener::bind(&transport(FRAME_CAP)?).await?;
        let expected = bound.address().port();
        let directory =
            directory(RouterConfiguration::default().registration_ttl.duration()).await?;
        let runtime = PeerRuntime::start(
            store().await?.clone(),
            &bound,
            CONTACT,
            &RouterConfiguration::default(),
            None,
        )
        .await?;
        let outcome = async {
            let published = directory
                .read(runtime.node())
                .await?
                .ok_or_else(|| eyre!("a started runtime must already resolve"))?;
            ensure!(
                published.direct.port != 0,
                "a registration must never publish the request for a port"
            );
            ensure!(
                published.direct.port == expected,
                "the published port must be the one the listener bound"
            );
            Ok(())
        }
        .await;
        let shutdown = runtime.shutdown().await;
        outcome.and(shutdown.map_err(Into::into))
    })
}

/// A connection over the cap is refused and counted, and nothing waits for a
/// permit.
///
/// The admission stream is driven directly: a second tonic channel would leave
/// both the refusal and the count to reconnect timing, and this suite asserts
/// on real signals only — the refused socket's own end of file, or a second
/// admission that must never arrive.
#[test]
fn a_connection_over_the_cap_is_refused_and_counted() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let refused = TRANSPORT.refused_connections();
        let (admissions, mut admitted_rx) = unbounded_channel();
        let holder = tokio::spawn(async move {
            let mut connections = pin!(admitted(listener, 1));
            // Every admitted connection is held for the whole test, so its
            // permit cannot be released and make a later admission legal.
            let mut held = Vec::new();
            while let Some(Ok(connection)) = connections.next().await {
                held.push(connection);
                if admissions.send(()).is_err() {
                    return;
                }
            }
        });
        let first = TcpStream::connect(address).await?;
        admitted_rx
            .recv()
            .await
            .ok_or_else(|| eyre!("the first connection was never admitted"))?;
        let mut second = TcpStream::connect(address).await?;
        let mut byte = [0u8; 1];
        let outcome = select! {
            read = second.read(&mut byte) => {
                ensure!(read? == 0, "a refused connection must be closed, not answered");
                ensure!(
                    TRANSPORT.refused_connections() == refused + 1,
                    "a refused connection must be counted"
                );
                Ok(())
            }
            _ = admitted_rx.recv() => {
                bail!("the cap admitted a connection while its only permit was held")
            }
            () = sleep(HANG_GUARD) => bail!("the refused connection neither closed nor was admitted"),
        };
        holder.abort();
        drop(first);
        outcome
    })
}

/// Reflection publishes the peer schema only where an operator asked for it.
#[test]
fn reflection_is_served_only_when_it_is_configured() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        for (reflection, served) in [(true, false), (false, true)] {
            let harness = Harness::with(Ok(TransportConfiguration {
                reflection,
                ..transport(FRAME_CAP)?
            }))
            .await?;
            let answered = reflect(harness.address.port).await;
            harness.stop().await?;
            ensure!(
                (answered? == Code::Unimplemented) == served,
                "reflection enabled = {reflection} must not answer UNIMPLEMENTED = {served}"
            );
        }
        Ok(())
    })
}

/// A cap of zero is refused before a listener can be built with it. Zero is not
/// "no limit" on either field: h2 reads a stream cap of zero as "open no
/// streams", and a connection cap of zero admits nothing, so a listener built
/// with either would be wedged rather than unbounded.
#[test]
fn a_transport_configuration_with_no_cap_is_refused() -> Result<()> {
    let refused = TransportConfiguration::builder()
        .max_concurrent_streams(0_u32)
        .build()?;
    ensure!(
        refused.validate().is_err(),
        "a stream cap of zero must be refused"
    );
    let refused = TransportConfiguration::builder()
        .max_connections(0_usize)
        .build()?;
    ensure!(
        refused.validate().is_err(),
        "a connection cap of zero must be refused"
    );
    Ok(())
}

/// Opens the reflection method with no request messages and reports the status
/// the port answered. `UNIMPLEMENTED` means the service is not routed there.
async fn reflect(port: u16) -> Result<Code> {
    let channel = Dialled::from_shared(format!("http://127.0.0.1:{port}"))?.connect_lazy();
    let mut client = Grpc::new(channel).max_decoding_message_size(FrameCap::MAX_BYTES);
    let request = Request::new(tokio_stream::iter(Vec::new()));
    if let Err(error) = client.ready().await {
        bail!("the reflection channel never became ready: {error:#}");
    }
    let outcome = client
        .streaming(
            request,
            PathAndQuery::from_static(REFLECTION),
            ClientFrameCodec::new(0),
        )
        .await;
    Ok(match outcome {
        Ok(_) => Code::Ok,
        Err(status) => status.code(),
    })
}
