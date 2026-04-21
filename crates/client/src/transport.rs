//! Transport abstraction for the client.
//!
//! Provides a single boxed async reader+writer, masking the UDS / Named Pipe
//! distinction. Broker-side has its own listener-side transport in the broker
//! crate.

use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

use tokimo_bus_protocol::BusError;

use crate::config::Endpoint;

/// Boxed full-duplex stream used by the client.
pub type ClientTransport = Pin<Box<dyn AsyncStream + Send>>;

/// Combined `AsyncRead + AsyncWrite` trait object helper.
pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncStream for T {}

/// Establish a single connection to the broker.
pub async fn connect(endpoint: &Endpoint) -> Result<ClientTransport, BusError> {
    match endpoint {
        Endpoint::UnixSocket(path) => {
            let stream = tokio::net::UnixStream::connect(path).await?;
            Ok(Box::pin(stream))
        }
        #[cfg(windows)]
        Endpoint::NamedPipe(name) => {
            // Retry briefly to tolerate races with the broker creating the pipe.
            use std::time::Duration;
            use tokio::net::windows::named_pipe::ClientOptions;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match ClientOptions::new().open(name) {
                    Ok(p) => return Ok(Box::pin(p)),
                    Err(e)
                        if e.raw_os_error() == Some(231) // ERROR_PIPE_BUSY
                            && std::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}
