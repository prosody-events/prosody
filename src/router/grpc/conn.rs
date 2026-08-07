//! Accepted peer connections for the Tonic server.

use async_stream::stream;
use futures::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;
use tracing::warn;

const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Accepts sockets and lets Tonic control their HTTP/2 work.
pub(super) fn incoming(listener: TcpListener) -> impl Stream<Item = Result<TcpStream, Infallible>> {
    stream! {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    if let Err(error) = stream.set_nodelay(true) {
                        warn!(%error, %peer, "the peer listener could not disable Nagle on a connection");
                    }
                    yield Ok(stream);
                }
                Err(error) => {
                    warn!(%error, "the peer listener could not accept a connection");
                    sleep(ACCEPT_BACKOFF).await;
                }
            }
        }
    }
}
