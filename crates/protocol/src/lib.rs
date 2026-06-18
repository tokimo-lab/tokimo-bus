//! Wire protocol for tokimo-bus.
//!
//! This crate is pure types + a length-prefixed `rmp-serde` frame codec. It is
//! shared between:
//!
//! - **apps** (via [`tokimo-bus-client`]) that send `Hello`, receive `Invoke`,
//!   and emit `Response` / `Event` / `Log` frames;
//! - **the broker** (via [`tokimo-bus-broker`]) embedded in `tokimo-server`
//!   that accepts app connections and routes frames between them and HTTP
//!   callers.
//!
//! Everything here is transport-agnostic: the same [`BusFrame`] encoding is
//! used over Unix domain sockets, Windows Named Pipes, or (future) TCP.
//!
//! ## Wire format
//!
//! ```text
//! ┌────────────┬──────────────────────────┐
//! │ u32 BE len │ rmp-serde encoded frame  │
//! └────────────┴──────────────────────────┘
//! ```
//!
//! `len` is the length in bytes of the rmp payload, capped at
//! [`MAX_FRAME_BYTES`] to defend against malformed senders.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod frame;
pub mod task_local;
pub mod transport;

#[cfg(feature = "axum")]
mod axum_listener;

pub use codec::{MAX_FRAME_BYTES, read_frame, read_frame_opt, write_frame};
pub use frame::{
    BusError, BusFrame, CallerCtx, DataPlaneSocket, Event, HelloAck, HelloRequest, HttpMethod, Invoke, MethodDecl,
    ProtocolVersion, Response, StreamChunk, StreamClose, StreamOpen,
};
pub use transport::{BusListener, BusStream, app_socket, cleanup};
