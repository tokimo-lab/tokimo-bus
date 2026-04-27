//! Top-level [`Broker`] facade.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::Path,
    pin::Pin,
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

use tokimo_bus_protocol::{BusError, BusFrame, CallerCtx, Event, Invoke, MethodDecl, Response};

/// Future returned by a [`LocalServiceHandler`].
pub type LocalCallFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, BusError>> + Send + 'static>>;

/// In-process handler for a "virtual" service that lives inside the broker
/// host (e.g. tokimo-server) instead of a separate process.
///
/// Used as a transitional bridge while individual apps are still being
/// extracted into their own binaries: an app can already call
/// `bus.call("notification_center", "notify", ...)` even though
/// `notification_center` is still part of the main server.
pub type LocalServiceHandler = Arc<dyn Fn(String, Vec<u8>, CallerCtx) -> LocalCallFuture + Send + Sync>;

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
    /// In-process "virtual" services. Looked up before the session registry,
    /// so the broker host (e.g. tokimo-server) can expose its own handlers
    /// under a service name and let connected apps call them via the normal
    /// `bus.call(service, method, …)` protocol.
    local_services: DashMap<String, LocalServiceHandler>,
    /// Method catalog for local services — required for HTTP typed-route
    /// dispatch (verb + auth validation).
    local_service_methods: DashMap<String, Arc<Vec<MethodDecl>>>,
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
            local_services: DashMap::new(),
            local_service_methods: DashMap::new(),
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
    pub async fn listen_unix<P: AsRef<Path>>(self: &Arc<Self>, path: P) -> Result<(), BusError> {
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

    /// Register an in-process handler for a *virtual* service with an
    /// explicit method catalog. Used by `tokimo-server` so local services
    /// (e.g. `notification_center`) can be dispatched from HTTP typed routes
    /// with full per-method verb + auth validation, identical to real apps.
    pub fn register_local_service(
        self: &Arc<Self>,
        service: impl Into<String>,
        methods: Vec<MethodDecl>,
        handler: LocalServiceHandler,
    ) {
        let name = service.into();
        info!(service = %name, methods = methods.len(), "bus-broker: registering local service");
        self.local_service_methods.insert(name.clone(), Arc::new(methods));
        self.local_services.insert(name, handler);
    }

    /// Lookup helper used by tests / introspection.
    #[must_use]
    pub fn has_local_service(&self, service: &str) -> bool {
        self.local_services.contains_key(service)
    }

    /// Method catalog for a local service, if registered.
    #[must_use]
    pub fn local_service_methods(&self, service: &str) -> Option<Arc<Vec<MethodDecl>>> {
        self.local_service_methods.get(service).map(|m| m.clone())
    }

    /// Names of all registered local services (used for HTTP route expansion).
    #[must_use]
    pub fn local_service_names(&self) -> Vec<String> {
        self.local_services.iter().map(|e| e.key().clone()).collect()
    }

    /// Method catalog for a remote (subprocess) service in the registry.
    #[must_use]
    pub fn registry_methods(&self, service: &str) -> Option<Arc<Vec<MethodDecl>>> {
        self.registry.get(service).map(|e| e.methods)
    }

    /// Data-plane socket declared by a remote (subprocess) service, if any.
    #[must_use]
    pub fn registry_data_plane(&self, service: &str) -> Option<tokimo_bus_protocol::DataPlaneSocket> {
        self.registry.get(service).and_then(|e| e.data_plane)
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
        // ── 1. Local (in-process) services take precedence ──
        if let Some(handler) = self.local_services.get(service).map(|h| h.clone()) {
            // Validate against method catalog (matches remote-app semantics).
            if let Some(methods) = self.local_service_methods.get(service) {
                let decl = methods
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
            }
            let fut = handler(method.to_string(), payload, caller);
            return match tokio::time::timeout(timeout, fut).await {
                Ok(res) => res,
                Err(_) => Err(BusError::Timeout {
                    ms: timeout.as_millis() as u64,
                }),
            };
        }

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
    use std::sync::atomic::AtomicU64;
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id();
    let ctr = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{ns:016x}{pid:08x}{ctr:016x}")
}
