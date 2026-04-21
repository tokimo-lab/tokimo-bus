//! Wire frame definitions.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Semver-ish protocol version. Broker rejects `Hello` with mismatched major.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtocolVersion {
    /// Incompatible wire changes bump this.
    pub major: u16,
    /// Backwards-compatible additions bump this.
    pub minor: u16,
}

impl ProtocolVersion {
    /// Version implemented by this crate.
    pub const CURRENT: Self = Self { major: 1, minor: 0 };
}

/// Declaration of a single method exposed by an app.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodDecl {
    /// Fully qualified within the service, e.g. `"library.scan"`.
    pub name: String,
    /// If `true`, broker rejects `Invoke` whose [`CallerCtx::user_id`] is `None`.
    #[serde(default)]
    pub requires_auth: bool,
    /// If `true`, method uses `StreamOpen`/`StreamChunk`/`StreamClose`.
    #[serde(default)]
    pub streaming: bool,
    /// Human-readable one-liner. Optional.
    #[serde(default)]
    pub description: Option<String>,
}

/// First frame an app sends after connecting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloRequest {
    pub protocol: ProtocolVersion,
    /// Service name, must be unique in the broker.
    pub service: String,
    pub version: String,
    pub pid: u32,
    /// One-time token handed out via env `TOKIMO_BUS_TOKEN` when broker
    /// spawned this process. Prevents rogue processes from impersonating a
    /// known service.
    pub auth_token: String,
    /// Full method catalog.
    pub methods: Vec<MethodDecl>,
    /// Topics this app intends to publish (informational).
    #[serde(default)]
    pub events: Vec<String>,
}

/// Broker's acknowledgement of a successful `Hello`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    /// Unique id assigned to this connection.
    pub client_id: u64,
    /// How often the app should expect `Ping` from the broker.
    #[serde(with = "serde_duration_millis")]
    pub heartbeat_interval: Duration,
}

/// Identity + tracing context, injected by `tokimo-server` **after** auth.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CallerCtx {
    /// `None` = public / unauthenticated request.
    pub user_id: Option<String>,
    /// X-Request-Id propagation.
    pub request_id: String,
    /// Optional workspace slug for multi-tenant deployments.
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Unary request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoke {
    pub req_id: u64,
    pub method: String,
    /// Opaque rmp-serde payload; semantics defined by the app.
    pub payload: Vec<u8>,
    pub caller: CallerCtx,
}

/// Unary response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub req_id: u64,
    pub result: Result<Vec<u8>, BusError>,
}

/// Opens a bidirectional stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOpen {
    pub req_id: u64,
    pub method: String,
    pub caller: CallerCtx,
    /// Optional initial payload.
    #[serde(default)]
    pub initial: Vec<u8>,
}

/// One chunk of a streaming call; may flow in either direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    pub req_id: u64,
    pub data: Vec<u8>,
}

/// Terminates a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamClose {
    pub req_id: u64,
    /// `None` = graceful.
    pub error: Option<BusError>,
}

/// Pub-sub event frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Hierarchical dot-separated topic, e.g. `"video.library.scanned"`.
    pub topic: String,
    pub payload: Vec<u8>,
    /// Publishing service name. Filled by the broker on fan-out.
    #[serde(default)]
    pub from: Option<String>,
}

/// The single multiplexed frame type carried on every connection.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BusFrame {
    // ── control plane ──────────────────────────────────────────────────
    Hello(HelloRequest),
    HelloAck(HelloAck),
    /// Broker → app heartbeat. App replies with `Pong`.
    Ping,
    Pong,
    /// Broker asks the app to exit gracefully.
    Shutdown,

    // ── unary RPC ──────────────────────────────────────────────────────
    Invoke(Invoke),
    Response(Response),

    // ── bidirectional streaming ────────────────────────────────────────
    StreamOpen(StreamOpen),
    StreamChunk(StreamChunk),
    StreamClose(StreamClose),

    // ── pub / sub ──────────────────────────────────────────────────────
    /// App → broker publish.
    Publish(Event),
    /// App → broker subscription registration. Prefix match on topic.
    Subscribe {
        /// Topic prefix (exact equality or dot-separated parent match).
        topic_prefix: String,
    },
    /// App → broker subscription removal.
    Unsubscribe {
        /// Topic prefix previously passed to [`BusFrame::Subscribe`].
        topic_prefix: String,
    },
    /// Broker → app fan-out.
    Event(Event),
}

/// Structured bus-level error.
#[allow(missing_docs)]
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum BusError {
    /// Underlying transport I/O failure.
    #[error("I/O error: {0}")]
    Io(String),

    /// Frame could not be encoded / decoded as rmp-serde.
    #[error("codec: {0}")]
    Codec(String),

    /// Incoming frame size exceeded [`crate::MAX_FRAME_BYTES`].
    #[error("frame too large: {size} bytes > {max}")]
    FrameTooLarge {
        /// Observed frame size in bytes.
        size: u64,
        /// Configured maximum.
        max: u32,
    },

    /// Peer closed the connection cleanly.
    #[error("connection closed")]
    ConnectionClosed,

    /// Broker / client major versions differ.
    #[error("protocol mismatch: broker={broker:?}, client={client:?}")]
    ProtocolMismatch {
        /// Broker's advertised version.
        broker: ProtocolVersion,
        /// Client's advertised version.
        client: ProtocolVersion,
    },

    /// `Hello.auth_token` did not match what the broker handed out.
    #[error("invalid spawn token")]
    InvalidAuthToken,

    /// Caller addressed a service name the broker has no registration for.
    #[error("service `{0}` not registered")]
    ServiceNotFound(String),

    /// Caller addressed a method the service did not declare.
    #[error("method `{service}.{method}` not found")]
    MethodNotFound {
        /// Service name.
        service: String,
        /// Method name.
        method: String,
    },

    /// Method requires auth but caller is unauthenticated.
    #[error("method `{service}.{method}` requires authentication")]
    Unauthorized {
        /// Service name.
        service: String,
        /// Method name.
        method: String,
    },

    /// Call exceeded its configured timeout.
    #[error("call timed out after {ms} ms")]
    Timeout {
        /// Elapsed milliseconds.
        ms: u64,
    },

    /// Caller-side validation error.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Anything else on the broker side.
    #[error("internal error: {0}")]
    Internal(String),

    /// App-level error payload; semantics defined by the service.
    #[error("app error: {0}")]
    App(String),
}

impl From<std::io::Error> for BusError {
    fn from(e: std::io::Error) -> Self {
        BusError::Io(e.to_string())
    }
}

// Serde helper so `Duration` round-trips through MessagePack as a single
// unsigned integer (milliseconds) instead of the default `{secs,nanos}` map.
mod serde_duration_millis {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}
