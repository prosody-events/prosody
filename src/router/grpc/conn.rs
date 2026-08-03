//! Admission at the peer port: how many connections one listener holds at once.
//!
//! The per-message ceiling bounds one frame. Without a bound on connections an
//! unlimited number of maximum-size frames can be in flight before any registry
//! admission is consulted, so the listener refuses a connection over the cap
//! rather than queueing it.

use super::TRANSPORT;
use async_stream::stream;
use futures::Stream;
use std::convert::Infallible;
use std::io::{IoSlice, Result as IoResult};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::sleep;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::warn;

/// How long the listener waits before it accepts again after a failed accept.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// One connection the listener admitted.
///
/// It holds its permit for as long as the connection lives, so the count of
/// live permits is the count of live connections and needs no bookkeeping of
/// its own.
pub(super) struct Admitted {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl Connected for Admitted {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.inner.connect_info()
    }
}

impl AsyncRead for Admitted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        Pin::new(&mut self.inner).poll_read(context, buf)
    }
}

impl AsyncWrite for Admitted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buf)
    }

    /// Forwarded, like every other method here: a wrapper that answered for
    /// itself would report no vectored support and make the writer above it
    /// copy every frame into one buffer before it writes.
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(context, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

/// Accepts connections, up to `max` of them at a time.
///
/// A connection over the cap is closed at once and counted, so nothing
/// unbounded waits for a permit. The permit is released when the connection's
/// task ends and drops it. A peer that dies without a FIN leaves that task with
/// nothing to end it, which is what the listener's keepalive is for: it closes
/// a connection whose peer stopped answering, and the permit returns with it.
///
/// The stream never yields an error. tonic reads one as an accept failure and
/// keeps looping, so a persistent failure would spin the accept path; a failed
/// accept is logged and retried after a fixed pause instead.
pub(super) fn admitted(
    listener: TcpListener,
    max: usize,
) -> impl Stream<Item = Result<Admitted, Infallible>> {
    let permits = Arc::new(Semaphore::new(max));
    stream! {
        loop {
            match listener.accept().await {
                Ok((inner, peer)) => {
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        TRANSPORT.record_refused_connection();
                        warn!(%peer, max, "the peer listener refused a connection over its cap");
                        drop(inner);
                        continue;
                    };
                    // tonic ignores its own `tcp_nodelay` under a supplied
                    // stream, so the listener sets it here or Nagle delays
                    // every frame this connection carries.
                    if let Err(error) = inner.set_nodelay(true) {
                        warn!(%error, %peer, "the peer listener could not disable Nagle on a connection");
                    }
                    yield Ok(Admitted { inner, _permit: permit });
                }
                Err(error) => {
                    warn!(%error, "the peer listener could not accept a connection");
                    sleep(ACCEPT_BACKOFF).await;
                }
            }
        }
    }
}
