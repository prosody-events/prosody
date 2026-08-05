//! What the listener itself bounds: the port it publishes, how many
//! connections it holds, and what it discloses.

use super::{FRAME_CAP, Harness, transport};
use crate::requester::config::RequesterConfiguration;
use crate::response::frame::FrameCap;
use crate::router::directory::tests::support::{directory, store};
use crate::router::fleet::config::FleetConfiguration;
use crate::router::grpc::codec::ClientFrameCodec;
use crate::router::grpc::conn::admitted;
use crate::router::grpc::{BoundListener, TRANSPORT, TransportConfiguration};
use crate::router::loopback::{HANG_GUARD, TestHealth};
use crate::router::runtime::{PeerInputs, PeerRuntime, RouterConfiguration};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{bail, ensure, eyre};
use futures::StreamExt;
use std::net::Ipv4Addr;
use std::pin::pin;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::{pause, resume, sleep};
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Endpoint as Dialled;
use tonic::{Code, Request};
use validator::Validate;

/// The Cassandra contact point the routed-address probe aims at.
const CONTACT: &str = "localhost:9042";

/// The reflection method a generic client calls to learn what a port serves.
const REFLECTION: &str = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";

/// The HTTP/2 client preface, and the empty SETTINGS frame that must follow it.
/// Together they are everything a client owes before a server settles.
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00";

/// The HTTP/2 frame type of a SETTINGS frame.
const SETTINGS_FRAME: u8 = 0x04;

/// The flag that marks a SETTINGS frame as an acknowledgement rather than an
/// announcement.
const SETTINGS_ACK: u8 = 0x01;

/// The SETTINGS parameter that carries the stream cap.
const MAX_CONCURRENT_STREAMS: u16 = 0x03;

/// The SETTINGS parameter that carries the per-stream receive window.
const INITIAL_WINDOW_SIZE: u16 = 0x04;

/// Bytes in one HTTP/2 frame header.
const FRAME_HEADER_BYTES: usize = 9;

/// Bytes in one SETTINGS entry: a two-byte identifier and a four-byte value.
const SETTING_BYTES: usize = 6;

/// The stream cap this suite configures. Equal to no default, so an advertised
/// cap can only have come from the configuration.
const STREAM_CAP: u32 = 3;

/// The frame ceiling the announced-caps case configures. Above what one stream
/// buffers by default and unequal to any library default, so an announced
/// window can only have come from this ceiling.
const WINDOW_CAP: usize = 32 * 1024;

/// Connections and streams that fit the receive budget at the smallest frame
/// ceiling only if a stream is assumed to buffer that ceiling and nothing more.
const FLOOR_CONNECTIONS: usize = 2_048;

/// Streams beside [`FLOOR_CONNECTIONS`].
const FLOOR_STREAMS: u32 = 64;

/// Connections and streams whose single-copy product is inside the receive
/// budget but whose peak is not.
const PEAK_CONNECTIONS: usize = 256;

/// Streams beside [`PEAK_CONNECTIONS`].
const PEAK_STREAMS: u32 = 8;

/// One stream per connection, so a connection buffers far less than HTTP/2
/// grants it anyway.
const SPARSE_STREAMS: u32 = 1;

/// How long the silence case waits for a connection the listener must close.
///
/// Far past the listener's own deadline, so under paused time that deadline
/// always comes first. It is the hang guard, never the assertion: a connection
/// that is never closed fails this test instead of hanging it.
const SILENCE_GUARD: Duration = Duration::from_hours(1);

/// What one connection read out of the server's opening SETTINGS frame.
struct Announced {
    streams: Option<u32>,
    window: Option<u32>,
}

/// A listener bound to port zero publishes the port the operating system
/// assigned, because that is the only port registration can read.
#[test]
fn a_registration_publishes_the_port_the_listener_bound() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let bound = BoundListener::bind(&transport(FRAME_CAP)?).await?;
        let expected = bound.address().port();
        let router = RouterConfiguration::default();
        // The response ceiling matches this suite's frame ceiling: `start`
        // refuses a process that would admit a response its own listener could
        // not carry.
        let requester = RequesterConfiguration {
            max_response_bytes: FRAME_CAP,
            ..RequesterConfiguration::default()
        };
        let directory = directory(router.registration_ttl.duration()).await?;
        let runtime = PeerRuntime::start(PeerInputs {
            store: store().await?.clone(),
            listener: bound,
            health: TestHealth::new(true, true),
            contact: CONTACT,
            group: None,
            router: &router,
            fleet: FleetConfiguration::default(),
            requester: &requester,
        })
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
        let shutdown = runtime.shutdown(|| async {}).await;
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

/// A connection that falls silent holds nothing, whether it ever spoke or not.
///
/// The permit is the resource: a peer that connects and never completes the
/// HTTP/2 handshake would otherwise hold one for the life of the process, and
/// enough of them would close the port to every real peer. One byte must not
/// buy that, which is why the case that speaks once is here beside the case
/// that never speaks. Time is paused only for the wait the deadline must end,
/// so the deadline is the only thing that can complete that read.
#[tokio::test]
async fn a_connection_that_falls_silent_is_closed() -> Result<()> {
    init_test_logging();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let mut connections = pin!(admitted(listener, 2));
    for spoke in [false, true] {
        let mut peer = TcpStream::connect(address).await?;
        let Some(Ok(mut connection)) = connections.next().await else {
            bail!("the silent connection was never admitted");
        };
        let mut byte = [0u8; 1];
        if spoke {
            peer.write_all(b"P").await?;
            ensure!(
                connection.read(&mut byte).await? == 1,
                "the byte the peer sent must be read"
            );
        }
        // Every step above waits for a byte the kernel carries, so it runs in
        // real time: a clock that may jump would reach the listener's deadline
        // while the byte was still in flight, and the read would fail for a
        // silence that never happened. Nothing below waits for the network, so
        // the clock is free to jump to the deadline here.
        pause();
        let closed = select! {
            read = connection.read(&mut byte) => read,
            () = sleep(SILENCE_GUARD) => {
                bail!("the silent connection was never closed (spoke = {spoke})")
            }
        };
        resume();
        ensure!(
            closed.is_err(),
            "a connection that sends nothing more must be closed, not held (spoke = {spoke})"
        );
        drop(peer);
    }
    Ok(())
}

/// The caps an operator configured are the caps the listener serves under: it
/// announces the stream cap and a receive window from its own frame ceiling to
/// every peer that connects, and it holds no more connections than it was
/// given.
///
/// All three are read off a real socket rather than off the configuration, so a
/// cap that never reaches the server is a red test. The window matters most
/// here, because the transport leaves it at a megabyte per stream when nothing
/// sets it and no configuration field reaches that default. The first
/// connection is the one that reads the announcement, which is also what proves
/// it holds the only permit while the second one is refused.
#[test]
fn the_listener_serves_the_caps_it_was_configured_with() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::with(Ok(TransportConfiguration {
            max_connections: 1,
            max_concurrent_streams: STREAM_CAP,
            ..transport(WINDOW_CAP)?
        }))
        .await?;
        let port = harness.address.port;
        let refused = TRANSPORT.refused_connections();
        let outcome = async {
            let (held, announced) = select! {
                settled = settled(port) => settled?,
                () = sleep(HANG_GUARD) => bail!("the listener announced no settings"),
            };
            ensure!(
                announced.streams == Some(STREAM_CAP),
                "the listener must announce the configured stream cap, not {:?}",
                announced.streams
            );
            ensure!(
                announced.window == Some(u32::try_from(WINDOW_CAP)?),
                "the listener must announce a receive window from its own frame ceiling, not {:?}",
                announced.window
            );
            let mut over_the_cap = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
            let mut byte = [0u8; 1];
            let read = select! {
                read = over_the_cap.read(&mut byte) => read?,
                () = sleep(HANG_GUARD) => bail!("the connection over the cap was neither closed nor served"),
            };
            ensure!(read == 0, "a connection over the cap must be closed, not answered");
            ensure!(
                TRANSPORT.refused_connections() == refused + 1,
                "a connection over the cap must be counted"
            );
            drop(held);
            Ok(())
        }
        .await;
        let stopped = harness.stop().await;
        outcome.and(stopped)
    })
}

/// Reflection publishes the peer schema only where an operator asked for it.
#[test]
fn reflection_is_served_only_when_it_is_configured() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        for (reflection, unimplemented) in [(true, false), (false, true)] {
            let harness = Harness::with(Ok(TransportConfiguration {
                reflection,
                ..transport(FRAME_CAP)?
            }))
            .await?;
            let answered = reflect(harness.address.port).await;
            harness.stop().await?;
            ensure!(
                (answered? == Code::Unimplemented) == unimplemented,
                "reflection enabled = {reflection} must answer UNIMPLEMENTED = {unimplemented}"
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

/// Caps that are each inside their own range can still ask for more memory than
/// one listener may hold, so the product is refused. The defaults are inside
/// that budget, which is what makes a listener with no configuration usable.
///
/// Each case names one term of the peak the budget is checked against, and each
/// fits without that term: the copies a stream holds beyond the one it
/// assembles, the window HTTP/2 grants a connection whatever its streams
/// buffer, and the floor under what one stream buffers.
#[test]
fn a_transport_configuration_over_the_receive_budget_is_refused() -> Result<()> {
    let refused = TransportConfiguration::builder()
        .max_connections(super::super::MAX_CONNECTIONS)
        .max_concurrent_streams(super::super::MAX_STREAMS)
        .frame_cap(FrameCap::MAX)
        .build()?;
    ensure!(
        refused.validate().is_err(),
        "caps whose product is over the receive budget must be refused"
    );
    let peaked = TransportConfiguration::builder()
        .max_connections(PEAK_CONNECTIONS)
        .max_concurrent_streams(PEAK_STREAMS)
        .frame_cap(FrameCap::DEFAULT)
        .build()?;
    ensure!(
        peaked.validate().is_err(),
        "caps that fit only while a stream is counted once must be refused"
    );
    let sparse = TransportConfiguration::builder()
        .max_connections(super::super::MAX_CONNECTIONS)
        .max_concurrent_streams(SPARSE_STREAMS)
        .frame_cap(FrameCap::new(FrameCap::MIN_BYTES)?)
        .build()?;
    ensure!(
        sparse.validate().is_err(),
        "a connection holds the window HTTP/2 grants it, however little it buffers"
    );
    let floor = TransportConfiguration::builder()
        .max_connections(FLOOR_CONNECTIONS)
        .max_concurrent_streams(FLOOR_STREAMS)
        .frame_cap(FrameCap::new(FrameCap::MIN_BYTES)?)
        .build()?;
    ensure!(
        floor.validate().is_err(),
        "a frame ceiling under what one stream buffers must not buy more streams"
    );
    ensure!(
        TransportConfiguration::default().validate().is_ok(),
        "the default caps must be inside the receive budget"
    );
    Ok(())
}

/// Connects, completes the client's half of the HTTP/2 handshake, and reports
/// what the server announced together with the connection that read it. The
/// connection is returned so a caller can hold the permit it took.
async fn settled(port: u16) -> Result<(TcpStream, Announced)> {
    let mut socket = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    socket.write_all(PREFACE).await?;
    loop {
        let mut head = [0u8; FRAME_HEADER_BYTES];
        socket.read_exact(&mut head).await?;
        let (length, kind, flags) = frame_head(head)?;
        let mut payload = vec![0u8; length];
        socket.read_exact(&mut payload).await?;
        if kind == SETTINGS_FRAME && flags & SETTINGS_ACK == 0 {
            return Ok((
                socket,
                Announced {
                    streams: setting(&payload, MAX_CONCURRENT_STREAMS),
                    window: setting(&payload, INITIAL_WINDOW_SIZE),
                },
            ));
        }
    }
}

/// The payload length, type and flags one frame header states.
fn frame_head(head: [u8; FRAME_HEADER_BYTES]) -> Result<(usize, u8, u8)> {
    let [high, middle, low, kind, flags, ..] = head;
    let length = usize::try_from(u32::from_be_bytes([0, high, middle, low]))?;
    Ok((length, kind, flags))
}

/// The value one SETTINGS payload announced for `wanted`, when it announced it.
fn setting(payload: &[u8], wanted: u16) -> Option<u32> {
    payload.chunks_exact(SETTING_BYTES).find_map(|entry| {
        let (id, value) = entry.split_at_checked(2)?;
        let id = u16::from_be_bytes(id.try_into().ok()?);
        let value = u32::from_be_bytes(value.try_into().ok()?);
        (id == wanted).then_some(value)
    })
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
