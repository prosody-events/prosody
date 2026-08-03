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
use std::future::Future;
use std::io::{Error as IoError, ErrorKind, IoSlice, Result as IoResult};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep, sleep};
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::warn;

/// How long the listener waits before it accepts again after a failed accept.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// How long a connection may send nothing before the listener closes it.
///
/// This is the backstop under the HTTP/2 keepalive, not a copy of it: the
/// keepalive can only bound a connection that already exists, so a peer that
/// connects and never completes the HTTP/2 handshake would otherwise hold its
/// admission permit for the life of the process. A peer that did complete it
/// answers the listener's ping once every
/// [`KEEPALIVE_INTERVAL`](super::KEEPALIVE_INTERVAL), and that answer is a
/// read, so two intervals always contain one and this deadline never reaches a
/// live connection.
const SILENCE_TIMEOUT: Duration = super::KEEPALIVE_INTERVAL.saturating_mul(2);

/// One connection the listener admitted.
///
/// It holds its permit for as long as the connection lives, so the count of
/// live permits is the count of live connections and needs no bookkeeping of
/// its own. What ends a connection nobody ends is [`SILENCE_TIMEOUT`].
pub(super) struct Admitted {
    inner: TcpStream,
    /// Fires once per [`SILENCE_TIMEOUT`], and closes the connection at the
    /// first expiry that finds nothing was read since the one before.
    silence: Pin<Box<Sleep>>,
    /// Whether anything was read in this period. Cleared at every expiry, so
    /// the deadline is reset once a period rather than once a read.
    spoke: bool,
    _permit: OwnedSemaphorePermit,
}

impl Connected for Admitted {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.inner.connect_info()
    }
}

impl AsyncRead for Admitted {
    /// Reads, and closes a connection that has sent nothing for
    /// [`SILENCE_TIMEOUT`].
    ///
    /// The deadline is polled here rather than on a task of its own because
    /// this is where the connection waits: the server holds a read open for the
    /// whole connection, which is what registers the deadline's waker. The
    /// socket is polled first, so bytes that already arrived are never thrown
    /// away for a deadline that expired while the task waited its turn.
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let admitted = self.get_mut();
        let read = buf.filled().len();
        let polled = Pin::new(&mut admitted.inner).poll_read(context, buf);
        if polled.is_ready() {
            admitted.spoke |= buf.filled().len() > read;
            return polled;
        }
        // The reset always lands in the future, so this runs at most twice.
        while admitted.silence.as_mut().poll(context).is_ready() {
            if !admitted.spoke {
                return Poll::Ready(Err(IoError::new(
                    ErrorKind::TimedOut,
                    "the peer sent nothing for the silence timeout",
                )));
            }
            admitted.spoke = false;
            admitted
                .silence
                .as_mut()
                .reset(Instant::now() + SILENCE_TIMEOUT);
        }
        Poll::Pending
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
/// task ends and drops it. A peer that stops answering — because it died
/// without a FIN, or because it never spoke HTTP/2 at all — is closed by the
/// listener's keepalive or by [`SILENCE_TIMEOUT`] under it.
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
                    yield Ok(Admitted {
                        inner,
                        silence: Box::pin(sleep(SILENCE_TIMEOUT)),
                        spoke: false,
                        _permit: permit,
                    });
                }
                Err(error) => {
                    warn!(%error, "the peer listener could not accept a connection");
                    sleep(ACCEPT_BACKOFF).await;
                }
            }
        }
    }
}
