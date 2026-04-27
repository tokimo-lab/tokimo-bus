//! Transport abstraction for the client.
//!
//! Provides a single boxed async reader+writer, masking the UDS / Named Pipe
//! distinction. Broker-side has its own listener-side transport in the broker
//! crate.

use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

use tokimo_bus_protocol::{BusError, BusStream, DataPlaneSocket};

use crate::config::Endpoint;

/// Boxed full-duplex stream used by the client.
pub type ClientTransport = Pin<Box<dyn AsyncStream + Send>>;

/// Combined `AsyncRead + AsyncWrite` trait object helper.
pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncStream for T {}

/// Establish a single connection to the broker.
pub async fn connect(endpoint: &Endpoint) -> Result<ClientTransport, BusError> {
    let socket = endpoint_to_socket(endpoint)?;
    let stream = BusStream::connect(&socket).await?;
    Ok(Box::pin(stream))
}

/// Convert an Endpoint to a DataPlaneSocket for the transport layer.
fn endpoint_to_socket(endpoint: &Endpoint) -> Result<DataPlaneSocket, BusError> {
    match endpoint {
        Endpoint::UnixSocket(path) => Ok(DataPlaneSocket::Unix {
            path: path.to_string_lossy().into_owned(),
        }),
        #[cfg(windows)]
        Endpoint::NamedPipe(name) => {
            // Strip the \\.\pipe\ prefix if present
            let bare_name = name.strip_prefix(r"\\.\pipe\").unwrap_or(name).to_string();
            Ok(DataPlaneSocket::NamedPipe { name: bare_name })
        }
    }
}
