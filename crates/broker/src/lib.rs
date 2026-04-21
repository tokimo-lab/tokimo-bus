//! Embedded broker for tokimo-bus.
//!
//! Typical usage inside `tokimo-server`:
//!
//! ```no_run
//! # async fn run() -> Result<(), tokimo_bus_protocol::BusError> {
//! use tokimo_bus_broker::{Broker, BrokerConfig};
//!
//! let broker = Broker::new(BrokerConfig::default());
//! // Register a known service's auth token before spawning the app process.
//! broker.issue_token("helloworld");
//! broker.listen_unix("/run/tokimo-bus.sock").await?;
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

pub use broker::{Broker, BrokerConfig};
pub use registry::{Registry, ServiceEntry};
pub use tokimo_bus_protocol as protocol;
