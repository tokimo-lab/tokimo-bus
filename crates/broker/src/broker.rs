//! Top-level [`Broker`] facade.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::{info, warn};

use tokimo_bus_protocol::{
    BusError, BusFrame, CallerCtx, Event, Invoke, Response,
};

use crate::registry::Registry;

/// Broker tunables.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// How often broker sends `Ping` to connected apps.
    pub heartbeat_interval: Duration,
    /// Default timeout applied to [`Broker::call`] when none is given.
    pub default_call_timeout: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(15),
            default_call_timeout: Duration::from_secs(30),
        }
    }
}

/// Broker runtime, embeddable in `tokimo-server`.
pub struct Broker {
    config: BrokerConfig,
    registry: Arc<Registry>,
    next_client_id: AtomicU64,
    /// Spawn tokens handed out before `Command::spawn`. Consumed (single-use)
    /// on successful `Hello`.
    tokens: DashMap<String, String>,
    /// Per-service pending outbound-call tables, keyed by (service, generation).
    sessions: DashMap<(String, u64), SessionHandle>,
    /// Topic-prefix subscriptions: subscriber_service → set of prefixes.
    subscriptions: Mutex<HashMap<String, HashSet<String>>>,
}

#[derive(Clone)]
struct SessionHandle {
    pending_out: Arc<DashMap<u64, oneshot::Sender<Response>>>,
    next_req_id: Arc<AtomicU64>,
}

impl Broker {
    /// Construct a broker.
    #[must_use]
    pub fn new(config: BrokerConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            registry: Arc::new(Registry::default()),
            next_client_id: AtomicU64::new(1),
            tokens: DashMap::new(),
            sessions: DashMap::new(),
            subscriptions: Mutex::new(HashMap::new()),
        })
    }

    /// Generate (and remember) a spawn token for a service. Pass this via
    /// env `TOKIMO_BUS_TOKEN` when spawning the process.
    pub fn issue_token(self: &Arc<Self>, service: &str) -> String {
        let token = random_token();
        self.tokens.insert(service.to_string(), token.clone());
        token
    }

    /// One-shot token check used by the handshake.
    pub(crate) fn consume_token(&self, service: &str, token: &str) -> bool {
        match self.tokens.remove(service) {
            Some((_, expected)) => expected == token,
            None => false,
        }
    }

    /// The service registry.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Broker configuration.
    #[must_use]
    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    /// Listen on a Unix domain socket (Linux / macOS / Windows 10 1803+).
    pub async fn listen_unix<P: AsRef<Path>>(
        self: &Arc<Self>,
        path: P,
    ) -> Result<(), BusError> {
        let path = path.as_ref();
        // Remove stale socket from a previous crash.
        let _ = tokio::fs::remove_file(path).await;
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let listener = tokio::net::UnixListener::bind(path)?;
        info!(path = %path.display(), "bus-broker: listening on UDS");

        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let peer = format!("{addr:?}");
                        let me = me.clone();
                        tokio::spawn(async move {
                            crate::session::serve(me, stream, peer).await;
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "bus-broker: accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
        Ok(())
    }

    /// Perform an HTTP-originated unary call to `service.method`.
    ///
    /// Panics on `default_call_timeout` if the app does not respond.
    pub async fn call(
        self: &Arc<Self>,
        service: &str,
        method: &str,
        payload: Vec<u8>,
        caller: CallerCtx,
    ) -> Result<Vec<u8>, BusError> {
        self.call_with_timeout(service, method, payload, caller, self.config.default_call_timeout)
            .await
    }

    /// Like [`Self::call`] but with an explicit timeout.
    pub async fn call_with_timeout(
        self: &Arc<Self>,
        service: &str,
        method: &str,
        payload: Vec<u8>,
        caller: CallerCtx,
        timeout: Duration,
    ) -> Result<Vec<u8>, BusError> {
        let entry = self
            .registry
            .get(service)
            .ok_or_else(|| BusError::ServiceNotFound(service.to_string()))?;

        let decl = entry
            .methods
            .iter()
            .find(|m| m.name == method)
            .ok_or_else(|| BusError::MethodNotFound {
                service: service.to_string(),
                method: method.to_string(),
            })?;

        if decl.requires_auth && caller.user_id.is_none() {
            return Err(BusError::Unauthorized {
                service: service.to_string(),
                method: method.to_string(),
            });
        }

        let handle = self
            .sessions
            .get(&(service.to_string(), entry.generation))
            .map(|h| h.clone())
            .ok_or_else(|| BusError::ServiceNotFound(service.to_string()))?;

        let req_id = handle.next_req_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        handle.pending_out.insert(req_id, tx);

        let frame = BusFrame::Invoke(Invoke {
            req_id,
            method: method.to_string(),
            payload,
            caller,
        });
        entry
            .tx
            .send(frame)
            .map_err(|_| BusError::ServiceNotFound(service.to_string()))?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(resp)) => resp.result,
            Ok(Err(_)) => Err(BusError::ConnectionClosed),
            Err(_) => {
                handle.pending_out.remove(&req_id);
                Err(BusError::Timeout {
                    ms: timeout.as_millis() as u64,
                })
            }
        }
    }

    /// Ask an app to shut down gracefully. Broker does *not* wait; the
    /// session task will clean up when the socket closes.
    pub fn shutdown_service(&self, service: &str) -> Result<(), BusError> {
        let entry = self
            .registry
            .get(service)
            .ok_or_else(|| BusError::ServiceNotFound(service.to_string()))?;
        entry
            .tx
            .send(BusFrame::Shutdown)
            .map_err(|_| BusError::ConnectionClosed)
    }

    // ── pub / sub ──────────────────────────────────────────────────────

    pub(crate) fn subscribe(&self, service: String, prefix: String) {
        let mut subs = self.subscriptions.lock();
        subs.entry(service).or_default().insert(prefix);
    }

    pub(crate) fn unsubscribe(&self, service: &str, prefix: &str) {
        let mut subs = self.subscriptions.lock();
        if let Some(s) = subs.get_mut(service) {
            s.remove(prefix);
        }
    }

    pub(crate) fn fanout(&self, from_service: &str, mut event: Event) {
        event.from = Some(from_service.to_string());
        let targets: Vec<String> = {
            let subs = self.subscriptions.lock();
            subs.iter()
                .filter(|(svc, _)| svc.as_str() != from_service)
                .filter(|(_, prefixes)| prefixes.iter().any(|p| event.topic.starts_with(p)))
                .map(|(svc, _)| svc.clone())
                .collect()
        };
        for subscriber in targets {
            if let Some(entry) = self.registry.get(&subscriber) {
                let _ = entry.tx.send(BusFrame::Event(event.clone()));
            }
        }
    }

    // ── session attach (used by session.rs) ────────────────────────────

    pub(crate) fn next_client_id(&self) -> u64 {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn attach_session(
        &self,
        service: &str,
        generation: u64,
        pending_out: Arc<DashMap<u64, oneshot::Sender<Response>>>,
        next_req_id: Arc<AtomicU64>,
    ) {
        self.sessions.insert(
            (service.to_string(), generation),
            SessionHandle {
                pending_out,
                next_req_id,
            },
        );
    }

    pub(crate) fn detach_session(&self, service: &str, generation: u64) {
        self.sessions.remove(&(service.to_string(), generation));
    }
}

fn random_token() -> String {
    // Non-crypto-grade token adequate for single-machine spawn pairing;
    // real defense is the UDS's filesystem permission.
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id();
    let ctr = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{ns:016x}{pid:08x}{ctr:016x}")
}
