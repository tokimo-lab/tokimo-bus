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

/// HTTP verb used when the server exposes this method as a typed REST route
/// at `POST|GET|…/api/apps/<service>/<method>`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    /// `GET` — also matches `HEAD`.
    Get,
    /// `POST` — default for RPC-style methods.
    #[default]
    Post,
    /// `PUT`.
    Put,
    /// `PATCH`.
    Patch,
    /// `DELETE`.
    Delete,
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
    /// HTTP verb used by the server's generated typed route. Defaults to `POST`.
    #[serde(default)]
    pub http_method: HttpMethod,
    /// URL path tail appended after `/api/apps/<service>/`. Defaults to `name`
    /// (with `.` → `/` so `"library.scan"` becomes `library/scan`).
    #[serde(default)]
    pub path: Option<String>,
    /// Human-readable one-liner. Optional.
    #[serde(default)]
    pub description: Option<String>,
}

/// Data-plane endpoint (reverse-proxied by the server at
/// `/api/apps/<service>/data/*path`). Absent → app does not serve large bodies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DataPlaneSocket {
    /// Unix domain socket path. Used on Linux, macOS, and modern Windows.
    Unix {
        /// Absolute filesystem path to the UDS.
        path: String,
    },
    /// Windows Named Pipe (`\\.\pipe\<name>`). Fallback for older Windows.
    NamedPipe {
        /// Pipe name without the `\\.\pipe\` prefix.
        name: String,
    },
}

impl DataPlaneSocket {
    /// Human-readable identifier for logging.
    pub fn display_name(&self) -> String {
        match self {
            Self::Unix { path } => path.clone(),
            Self::NamedPipe { name } => format!("pipe://{name}"),
        }
    }
}

/// First frame an app sends after connecting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloRequest {
    /// Protocol version spoken by the app.
    pub protocol: ProtocolVersion,
    /// Service name, must be unique in the broker.
    pub service: String,
    /// App version string (e.g. crate `CARGO_PKG_VERSION`).
    pub version: String,
    /// OS process id of the app.
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
    /// Data-plane socket for large-body HTTP reverse-proxy (video streaming,
    /// file downloads, uploads). `None` = control plane only (pure CRUD apps).
    #[serde(default)]
    pub data_plane: Option<DataPlaneSocket>,
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
    /// Identifies the app that originated this call (e.g. `"video"`).
    /// Stamped by the broker for app-originated requests; `None` for HTTP-origin requests.
    #[serde(default)]
    pub caller_app_id: Option<String>,
}

/// Unary request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoke {
    /// Correlates with the [`Response::req_id`].
    pub req_id: u64,
    /// Fully-qualified method name (e.g. `"items.list"`).
    pub method: String,
    /// Opaque rmp-serde payload; semantics defined by the app.
    pub payload: Vec<u8>,
    /// Identity + tracing context, injected by the server.
    pub caller: CallerCtx,
}

/// Unary response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Matches the originating [`Invoke::req_id`].
    pub req_id: u64,
    /// `Ok(payload)` on success, `Err` on app-reported failure.
    pub result: Result<Vec<u8>, BusError>,
}

/// Opens a bidirectional stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOpen {
    /// Correlates all frames in this stream.
    pub req_id: u64,
    /// Fully-qualified method name.
    pub method: String,
    /// Identity + tracing context.
    pub caller: CallerCtx,
    /// Optional initial payload.
    #[serde(default)]
    pub initial: Vec<u8>,
}

/// One chunk of a streaming call; may flow in either direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    /// Correlates with the opening [`StreamOpen::req_id`].
    pub req_id: u64,
    /// Raw chunk bytes.
    pub data: Vec<u8>,
}

/// Terminates a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamClose {
    /// Correlates with the opening [`StreamOpen::req_id`].
    pub req_id: u64,
    /// `None` = graceful.
    pub error: Option<BusError>,
}

/// Pub-sub event frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Hierarchical dot-separated topic, e.g. `"video.library.scanned"`.
    pub topic: String,
    /// Opaque rmp-serde payload.
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
