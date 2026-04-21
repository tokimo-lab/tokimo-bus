//! App-side SDK for tokimo-bus.
//!
//! Typical app main loop:
//!
//! ```no_run
//! # async fn run() -> Result<(), tokimo_bus_protocol::BusError> {
//! use tokimo_bus_client::{BusClient, ClientConfig};
//! use tokimo_bus_protocol::MethodDecl;
//!
//! let client = BusClient::builder(ClientConfig::from_env()?)
//!     .service("helloworld", env!("CARGO_PKG_VERSION"))
//!     .method(MethodDecl {
//!         name: "echo".into(),
//!         requires_auth: false,
//!         streaming: false,
//!         description: Some("Echo payload back".into()),
//!     })
//!     .on_invoke("echo", |req| async move {
//!         // `req.payload` is opaque rmp bytes; decode with your own types.
//!         Ok(req.payload)
//!     })
//!     .build()
//!     .await?;
//!
//! // Emit a periodic event
//! client.publish("helloworld.heartbeat", b"tick".to_vec()).await?;
//!
//! client.run_until_shutdown().await;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod builder;
mod client;
mod config;
mod transport;

pub use builder::{BusClientBuilder, InvokeRequest};
pub use client::BusClient;
pub use config::ClientConfig;
pub use tokimo_bus_protocol as protocol;
pub use transport::{ClientTransport, connect as connect_transport};
