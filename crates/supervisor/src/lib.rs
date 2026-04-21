//! Process supervisor: the `procd` equivalent.
//!
//! Responsible for spawning app processes, restarting them on crash, and
//! (for on-demand apps) letting them exit when idle and respawning on the
//! next call.
//!
//! ```no_run
//! # async fn run() -> Result<(), tokimo_bus_protocol::BusError> {
//! use std::sync::Arc;
//! use std::time::Duration;
//! use tokimo_bus_broker::{Broker, BrokerConfig};
//! use tokimo_bus_supervisor::{AppSpec, Lifecycle, Supervisor};
//!
//! let broker = Broker::new(BrokerConfig::default());
//! broker.listen_unix("/run/tokimo-bus.sock").await?;
//!
//! let sup = Supervisor::new(broker.clone(), "/run/tokimo-bus.sock");
//! sup.register(AppSpec {
//!     service: "helloworld".into(),
//!     binary: "/usr/local/bin/tokimo-app-helloworld".into(),
//!     args: vec![],
//!     env: vec![],
//!     lifecycle: Lifecycle::OnDemand {
//!         idle_timeout: Duration::from_secs(300),
//!     },
//! });
//! sup.start_all_resident().await;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::{process::Child, sync::Mutex as AsyncMutex};
use tracing::{info, warn};

use tokimo_bus_broker::Broker;
use tokimo_bus_protocol::BusError;

/// Lifecycle policy for a supervised app.
#[derive(Debug, Clone)]
pub enum Lifecycle {
    /// Started at broker startup; respawned on crash with exponential backoff.
    Resident,
    /// Spawned on first call to this service. Exits itself after being idle
    /// for `idle_timeout` (the app reads the threshold from env
    /// `TOKIMO_BUS_IDLE_MS`).
    OnDemand {
        /// Idle threshold propagated to the app.
        idle_timeout: Duration,
    },
}

/// Definition of a supervised app.
#[derive(Debug, Clone)]
pub struct AppSpec {
    /// Service name (must match [`tokimo_bus_protocol::HelloRequest::service`]).
    pub service: String,
    /// Path to the app binary.
    pub binary: PathBuf,
    /// Extra CLI arguments.
    pub args: Vec<String>,
    /// Extra env vars (on top of `TOKIMO_BUS_SOCKET` / `TOKIMO_BUS_TOKEN`).
    pub env: Vec<(String, String)>,
    /// When / how to start.
    pub lifecycle: Lifecycle,
}

struct AppState {
    spec: AppSpec,
    child: AsyncMutex<Option<Child>>,
    backoff: Mutex<Backoff>,
    last_spawn: Mutex<Option<Instant>>,
}

/// Simple exponential backoff (1s → 2s → 4s → … → 5 min).
struct Backoff {
    next: Duration,
}

impl Backoff {
    const fn new() -> Self {
        Self {
            next: Duration::from_secs(1),
        }
    }
    fn bump(&mut self) -> Duration {
        let d = self.next;
        self.next = (self.next * 2).min(Duration::from_secs(300));
        d
    }
    fn reset(&mut self) {
        self.next = Duration::from_secs(1);
    }
}

/// Process supervisor.
pub struct Supervisor {
    broker: Arc<Broker>,
    socket_path: PathBuf,
    apps: DashMap<String, Arc<AppState>>,
}

impl Supervisor {
    /// Construct a supervisor.
    #[must_use]
    pub fn new(broker: Arc<Broker>, socket_path: impl Into<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            broker,
            socket_path: socket_path.into(),
            apps: DashMap::new(),
        })
    }

    /// Register an app. Does not start it; call [`Self::start_all_resident`]
    /// or rely on the first [`Self::ensure_up`] call.
    pub fn register(&self, spec: AppSpec) {
        let state = Arc::new(AppState {
            spec: spec.clone(),
            child: AsyncMutex::new(None),
            backoff: Mutex::new(Backoff::new()),
            last_spawn: Mutex::new(None),
        });
        self.apps.insert(spec.service, state);
    }

    /// Start every resident app and install crash-restart loops. OnDemand
    /// apps remain dormant until `ensure_up` is called.
    pub async fn start_all_resident(self: &Arc<Self>) {
        for kv in self.apps.iter() {
            if matches!(kv.value().spec.lifecycle, Lifecycle::Resident) {
                let me = self.clone();
                let service = kv.key().clone();
                tokio::spawn(async move {
                    me.run_resident(service).await;
                });
            }
        }
    }

    /// Ensure the app for `service` is running; no-op if already up.
    pub async fn ensure_up(self: &Arc<Self>, service: &str) -> Result<(), BusError> {
        let state = self
            .apps
            .get(service)
            .ok_or_else(|| BusError::ServiceNotFound(service.to_string()))?
            .clone();

        let mut child_guard = state.child.lock().await;
        if let Some(child) = child_guard.as_mut() {
            if child.try_wait().map_err(BusError::from)?.is_none() {
                return Ok(()); // already running
            }
        }
        self.spawn_once(&state, &mut child_guard).await
    }

    async fn run_resident(self: Arc<Self>, service: String) {
        loop {
            let Some(state) = self.apps.get(&service).map(|v| v.clone()) else {
                return;
            };
            {
                let mut guard = state.child.lock().await;
                if let Err(e) = self.spawn_once(&state, &mut guard).await {
                    warn!(service = %service, error = %e, "supervisor: spawn failed");
                    let wait = state.backoff.lock().bump();
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }

            // Wait for child exit, then loop to respawn.
            let status = {
                let mut guard = state.child.lock().await;
                match guard.as_mut() {
                    Some(c) => c.wait().await,
                    None => return,
                }
            };
            match status {
                Ok(s) if s.success() => {
                    info!(service = %service, "supervisor: child exited cleanly");
                    state.backoff.lock().reset();
                }
                Ok(s) => {
                    warn!(service = %service, status = ?s, "supervisor: child exited with error");
                }
                Err(e) => warn!(service = %service, error = %e, "supervisor: wait failed"),
            }
            let wait = state.backoff.lock().bump();
            tokio::time::sleep(wait).await;
        }
    }

    async fn spawn_once(
        self: &Arc<Self>,
        state: &Arc<AppState>,
        child_guard: &mut tokio::sync::MutexGuard<'_, Option<Child>>,
    ) -> Result<(), BusError> {
        let token = self.broker.issue_token(&state.spec.service);

        let idle_ms: u64 = match state.spec.lifecycle {
            Lifecycle::OnDemand { idle_timeout } => idle_timeout.as_millis() as u64,
            Lifecycle::Resident => 0,
        };

        let mut cmd = tokio::process::Command::new(&state.spec.binary);
        cmd.args(&state.spec.args)
            .env("TOKIMO_BUS_SOCKET", &self.socket_path)
            .env("TOKIMO_BUS_TOKEN", token)
            .env("TOKIMO_BUS_IDLE_MS", idle_ms.to_string())
            .kill_on_drop(true);
        for (k, v) in &state.spec.env {
            cmd.env(k, v);
        }

        let child = cmd.spawn().map_err(BusError::from)?;
        info!(
            service = %state.spec.service,
            pid = child.id(),
            "supervisor: spawned",
        );
        *state.last_spawn.lock() = Some(Instant::now());
        **child_guard = Some(child);
        Ok(())
    }
}
