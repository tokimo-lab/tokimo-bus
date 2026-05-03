//! Embedded broker for tokimo-bus.
//!
//! Typical usage inside `tokimo-server`:
//!
//! ```no_run
//! # async fn run() -> Result<(), tokimo_bus_protocol::BusError> {
//! use tokimo_bus_broker::{Broker, BrokerConfig};
//! use tokimo_bus_protocol::DataPlaneSocket;
//!
//! let broker = Broker::new(BrokerConfig::default());
//! // Register a known service's auth token before spawning the app process.
//! broker.issue_token("helloworld");
//!
//! // Cross-platform: use listen() with the right DataPlaneSocket variant.
//! // broker.listen(DataPlaneSocket::Unix { path: "/run/tokimo-bus.sock".into() }).await?;
//! // broker.listen(DataPlaneSocket::NamedPipe { name: "tokimo-bus".into() }).await?;
//!
//! // Later, from an HTTP handler:
//! let result = broker
//!     .call("helloworld", "echo", b"{}".to_vec(), Default::default())
//!     .await?;
//! # let _ = result;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod broker;
mod registry;
mod session;

pub use broker::{Broker, BrokerConfig, LocalCallFuture, LocalServiceHandler};
pub use registry::{Registry, ServiceEntry};
pub use tokimo_bus_protocol as protocol;
