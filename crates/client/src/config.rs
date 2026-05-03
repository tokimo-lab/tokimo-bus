//! Configuration resolution for an app connecting to the bus.

use std::{env, path::PathBuf, time::Duration};

use tokimo_bus_protocol::BusError;

/// Runtime configuration for [`crate::BusClient`].
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Path to the broker's UDS / Named Pipe endpoint.
    pub endpoint: Endpoint,
    /// One-time token the broker injected via env `TOKIMO_BUS_TOKEN`.
    /// Treated as opaque; app never reads this directly.
    pub auth_token: String,
    /// When set, client will auto-reconnect on connection loss using
    /// exponential backoff up to this cap. `None` disables auto-reconnect
    /// (the app exits when the broker goes away).
    pub reconnect_max_backoff: Option<Duration>,
    /// Idle-exit threshold measured in the app: if no `Invoke` / `StreamOpen`
    /// arrives for this long, `run_until_shutdown` returns so the app can
    /// gracefully exit. `None` = never idle-exit. Independent from broker's
    /// own idle tracking.
    pub idle_exit: Option<Duration>,
}

/// A broker endpoint; platform- and transport-agnostic.
#[derive(Debug, Clone)]
pub enum Endpoint {
    /// Unix domain socket or (on Windows 10 1803+) `AF_UNIX` path.
    UnixSocket(PathBuf),
    /// Windows Named Pipe, e.g. `\\.\pipe\tokimo-bus`.
    #[cfg(windows)]
    NamedPipe(String),
}

impl ClientConfig {
    /// Resolve from environment variables injected by the broker when it
    /// spawned this process.
    ///
    /// | Env var              | Required | Meaning                                    |
    /// |----------------------|----------|--------------------------------------------|
    /// | `TOKIMO_BUS_SOCKET`  | yes      | Path to UDS (or `pipe://...` on Windows)   |
    /// | `TOKIMO_BUS_TOKEN`   | yes      | Spawn auth token                           |
    /// | `TOKIMO_BUS_IDLE_MS` | no       | Idle-exit threshold in ms; `0` = disable   |
    pub fn from_env() -> Result<Self, BusError> {
        let endpoint_raw = env::var("TOKIMO_BUS_SOCKET")
            .map_err(|_| BusError::BadRequest("env TOKIMO_BUS_SOCKET is required".into()))?;
        let auth_token = env::var("TOKIMO_BUS_TOKEN")
            .map_err(|_| BusError::BadRequest("env TOKIMO_BUS_TOKEN is required".into()))?;
        let idle_ms = env::var("TOKIMO_BUS_IDLE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let idle_exit = if idle_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(idle_ms))
        };

        let endpoint = parse_endpoint(&endpoint_raw)?;

        Ok(Self {
            endpoint,
            auth_token,
            reconnect_max_backoff: Some(Duration::from_secs(30)),
            idle_exit,
        })
    }

    /// Construct an explicit UDS config (useful for tests).
    pub fn unix<P: Into<PathBuf>>(path: P, token: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::UnixSocket(path.into()),
            auth_token: token.into(),
            reconnect_max_backoff: None,
            idle_exit: None,
        }
    }

    /// Construct an explicit Named Pipe config (useful for tests on Windows).
    #[cfg(windows)]
    pub fn named_pipe(name: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: Endpoint::NamedPipe(format!(r"\\.\pipe\{}", name.into())),
            auth_token: token.into(),
            reconnect_max_backoff: None,
            idle_exit: None,
        }
    }
}

fn parse_endpoint(raw: &str) -> Result<Endpoint, BusError> {
    #[cfg(windows)]
    if let Some(pipe) = raw.strip_prefix("pipe://") {
        return Ok(Endpoint::NamedPipe(format!(r"\\.\pipe\{pipe}")));
    }
    #[cfg(windows)]
    if raw.starts_with(r"\\.\pipe\") {
        return Ok(Endpoint::NamedPipe(raw.to_string()));
    }
    Ok(Endpoint::UnixSocket(PathBuf::from(raw)))
}
