//! [`BusClient`]: connection, frame dispatch, invocation, pub/sub.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncWrite, split},
    sync::{Notify, mpsc, oneshot},
};
use tracing::{debug, info, warn};

use tokimo_bus_protocol::{
    BusError, BusFrame, CallerCtx, Event, HelloRequest, Invoke, ProtocolVersion, Response, read_frame_opt, write_frame,
};

use crate::{
    builder::{BusClientBuilder, InvokeHandler, InvokeRequest},
    config::ClientConfig,
    transport::connect,
};

/// Connected bus client. Spawns background reader + writer tasks; invocation
/// and publish methods are cheap `Arc`-wrapped handles.
pub struct BusClient {
    /// Service name this client registered.
    service: String,
    /// Next outbound `req_id` (for `invoke` to other services).
    next_req_id: AtomicU64,
    /// Outbound frame channel; writer task drains this.
    tx: mpsc::UnboundedSender<BusFrame>,
    /// Pending outbound calls awaiting `Response`.
    pending: Arc<DashMap<u64, oneshot::Sender<Response>>>,
    /// Registered invoke handlers.
    handlers: Arc<HashMap<String, InvokeHandler>>,
    /// Shutdown signal (either broker-initiated or caller-initiated).
    shutdown: Arc<Notify>,
    shutdown_fired: AtomicBool,
    /// Last time we observed inbound `Invoke` / `StreamOpen` activity. Used
    /// for app-side idle-exit tracking.
    last_activity: Mutex<Instant>,
    /// Idle-exit threshold.
    idle_exit: Option<Duration>,
}

impl BusClient {
    pub(crate) async fn connect_with_builder(b: BusClientBuilder) -> Result<Arc<Self>, BusError> {
        let (service, version) = b
            .service
            .ok_or_else(|| BusError::BadRequest("service() was not called".into()))?;

        // Sanity: every handler must have a matching MethodDecl.
        for name in b.handlers.keys() {
            if !b.methods.iter().any(|m| &m.name == name) {
                return Err(BusError::BadRequest(format!(
                    "handler registered for `{name}` but no MethodDecl declared it"
                )));
            }
        }

        let hello = HelloRequest {
            protocol: ProtocolVersion::CURRENT,
            service: service.clone(),
            version,
            pid: std::process::id(),
            auth_token: b.cfg.auth_token.clone(),
            methods: b.methods,
            events: b.events,
            data_plane: b.data_plane,
        };

        let (client, runner) = Self::establish(b.cfg, hello, b.handlers).await?;
        tokio::spawn(runner);
        Ok(client)
    }

    /// Internal: connect, handshake, spawn reader/writer, return handle +
    /// future that drives the connection until shutdown.
    async fn establish(
        cfg: ClientConfig,
        hello: HelloRequest,
        handlers: HashMap<String, InvokeHandler>,
    ) -> Result<(Arc<Self>, impl std::future::Future<Output = ()> + Send + 'static), BusError> {
        // Retry initial transport connect if reconnect_max_backoff is configured.
        // Only retry on transient endpoint availability errors (ConnectionRefused/NotFound
        // on Unix, pipe busy on Windows) before we send Hello to avoid consuming spawn tokens.
        let mut stream = if let Some(max_backoff) = cfg.reconnect_max_backoff {
            let deadline = Instant::now() + max_backoff;
            let mut backoff_ms = 50;
            loop {
                match connect(&cfg.endpoint).await {
                    Ok(s) => break s,
                    Err(BusError::Io(msg)) if is_transient_connect_msg(&msg) && Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(1000); // cap at 1s per retry
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        } else {
            connect(&cfg.endpoint).await?
        };
        write_frame(&mut stream, &BusFrame::Hello(hello.clone())).await?;

        // First reply must be HelloAck or a Response carrying a BusError.
        let ack = match read_frame_opt::<_, BusFrame>(&mut stream).await? {
            Some(BusFrame::HelloAck(a)) => a,
            Some(BusFrame::Response(r)) => {
                let err = r
                    .result
                    .err()
                    .unwrap_or(BusError::Internal("broker rejected Hello without explaining".into()));
                return Err(err);
            }
            Some(other) => {
                return Err(BusError::Internal(format!("expected HelloAck, got {other:?}")));
            }
            None => return Err(BusError::ConnectionClosed),
        };
        info!(service = %hello.service, client_id = ack.client_id, "bus: handshake ok");

        let (tx, rx) = mpsc::unbounded_channel::<BusFrame>();
        let client = Arc::new(BusClient {
            service: hello.service.clone(),
            next_req_id: AtomicU64::new(1),
            tx,
            pending: Arc::new(DashMap::new()),
            handlers: Arc::new(handlers),
            shutdown: Arc::new(Notify::new()),
            shutdown_fired: AtomicBool::new(false),
            last_activity: Mutex::new(Instant::now()),
            idle_exit: cfg.idle_exit,
        });

        let runner = run_connection(client.clone(), stream, rx, ack.heartbeat_interval);
        Ok((client, runner))
    }

    /// Name of this service.
    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service
    }

    /// 构造 CallerCtx，自动从 task-local 读取 user_id。
    ///
    /// 主服务器 proxy 会注入 `x-tokimo-user-id` header，auth_middleware
    /// 将其存入 task-local，此方法自动读取。app handler 调用此方法即可，
    /// 无需关心 auth 细节。
    ///
    /// 如果不在 HTTP 请求上下文中调用（如 CLI 模式），user_id 为 None。
    pub fn auto_caller(&self, app_id: &str) -> CallerCtx {
        CallerCtx {
            user_id: tokimo_bus_protocol::task_local::current_user_id(),
            request_id: uuid::Uuid::new_v4().to_string(),
            workspace: None,
            caller_app_id: Some(app_id.to_string()),
        }
    }

    /// Invoke another service's method. Future resolves when `Response` is
    /// received.
    pub async fn invoke(
        &self,
        service: &str,
        method: &str,
        payload: Vec<u8>,
        caller: CallerCtx,
    ) -> Result<Vec<u8>, BusError> {
        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.pending.insert(req_id, reply_tx);

        // Outbound Invoke is addressed to another service via a
        // namespaced method name `service.method`. Broker does the routing.
        let frame = BusFrame::Invoke(Invoke {
            req_id,
            method: format!("{service}.{method}"),
            payload,
            caller,
        });
        self.tx.send(frame).map_err(|_| BusError::ConnectionClosed)?;

        match reply_rx.await {
            Ok(resp) => resp.result,
            Err(_) => Err(BusError::ConnectionClosed),
        }
    }

    /// Publish an event.
    pub async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), BusError> {
        self.tx
            .send(BusFrame::Publish(Event {
                topic: topic.to_string(),
                payload,
                from: None,
            }))
            .map_err(|_| BusError::ConnectionClosed)
    }

    /// Subscribe to events matching `topic_prefix`. Delivery is not yet
    /// exposed here — wire up a channel in a future revision.
    pub fn subscribe(&self, topic_prefix: &str) -> Result<(), BusError> {
        self.tx
            .send(BusFrame::Subscribe {
                topic_prefix: topic_prefix.to_string(),
            })
            .map_err(|_| BusError::ConnectionClosed)
    }

    /// Request graceful shutdown of this client.
    pub fn shutdown(&self) {
        if !self.shutdown_fired.swap(true, Ordering::SeqCst) {
            self.shutdown.notify_waiters();
        }
    }

    /// Block until the broker sends `Shutdown`, the connection dies, or
    /// the configured idle timeout elapses.
    pub async fn run_until_shutdown(self: &Arc<Self>) {
        let notify = self.shutdown.clone();
        let idle = self.idle_exit;
        let me = self.clone();

        tokio::select! {
            () = notify.notified() => {},
            () = async move {
                // Idle-exit poll loop; no-op if idle_exit is None.
                let Some(thresh) = idle else {
                    std::future::pending::<()>().await;
                    return;
                };
                loop {
                    tokio::time::sleep(thresh / 4).await;
                    let last = *me.last_activity.lock();
                    if last.elapsed() >= thresh {
                        info!(service = %me.service, "bus: idle threshold reached, exiting");
                        return;
                    }
                }
            } => {},
        }
    }

    fn record_activity(&self) {
        *self.last_activity.lock() = Instant::now();
    }
}

/// Drives the connection: splits the stream, spawns reader + writer, waits.
async fn run_connection<S>(
    client: Arc<BusClient>,
    stream: S,
    mut rx: mpsc::UnboundedReceiver<BusFrame>,
    _heartbeat: Duration,
) where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    // We split so writer can continue even while reader is blocked on a long
    // frame. `split()` works on any `AsyncRead + AsyncWrite` regardless of
    // whether the underlying type supports explicit half-close.
    let (mut reader, mut writer) = split(Box::pin(stream));
    let writer_client = client.clone();

    let writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let Err(e) = write_frame(&mut writer, &frame).await {
                warn!(service = %writer_client.service, error = %e, "bus: writer failed");
                break;
            }
        }
        debug!(service = %writer_client.service, "bus: writer task ended");
    });

    // Reader loop
    loop {
        match read_frame_opt::<_, BusFrame>(&mut reader).await {
            Ok(Some(frame)) => {
                handle_inbound(&client, frame).await;
            }
            Ok(None) => {
                info!(service = %client.service, "bus: broker closed connection");
                break;
            }
            Err(e) => {
                warn!(service = %client.service, error = %e, "bus: reader error");
                break;
            }
        }
    }

    client.shutdown();
    writer_task.abort();
}

async fn handle_inbound(client: &Arc<BusClient>, frame: BusFrame) {
    match frame {
        BusFrame::Ping => {
            let _ = client.tx.send(BusFrame::Pong);
        }
        BusFrame::Shutdown => {
            info!(service = %client.service, "bus: broker requested shutdown");
            client.shutdown();
        }
        BusFrame::Invoke(inv) => {
            client.record_activity();
            // IMPORTANT: spawn so the inbound reader keeps draining frames.
            // Handlers may themselves call `client.invoke(...)`, whose Response
            // arrives via this same inbound loop — awaiting here would deadlock.
            let c = client.clone();
            tokio::spawn(async move { dispatch_invoke(c, inv).await });
        }
        BusFrame::Response(resp) => {
            if let Some((_, tx)) = client.pending.remove(&resp.req_id) {
                let _ = tx.send(resp);
            } else {
                warn!(req_id = resp.req_id, "bus: response for unknown req_id");
            }
        }
        BusFrame::Event(_) => {
            // Subscription delivery not yet surfaced; stub for now.
        }
        BusFrame::StreamOpen(_) | BusFrame::StreamChunk(_) | BusFrame::StreamClose(_) => {
            warn!("bus: streaming not yet implemented on client side");
        }
        BusFrame::HelloAck(_) | BusFrame::Hello(_) | BusFrame::Pong => {
            // Unexpected on app side post-handshake.
        }
        BusFrame::Publish(_) | BusFrame::Subscribe { .. } | BusFrame::Unsubscribe { .. } => {
            // These travel app → broker only.
        }
    }
}

async fn dispatch_invoke(client: Arc<BusClient>, inv: Invoke) {
    let handler = client.handlers.get(&inv.method).cloned();
    let req_id = inv.req_id;
    let result = match handler {
        Some(h) => {
            let req = InvokeRequest {
                payload: inv.payload,
                caller: inv.caller,
            };
            let user_id = req.caller.user_id.clone();
            tokimo_bus_protocol::task_local::scope_user_id(user_id, h(req)).await
        }
        None => Err(BusError::MethodNotFound {
            service: client.service.clone(),
            method: inv.method,
        }),
    };
    let _ = client.tx.send(BusFrame::Response(Response { req_id, result }));
}

/// Returns true if the I/O error message indicates a transient endpoint
/// availability failure that should be retried on initial connect (Unix
/// ConnectionRefused/NotFound, Windows pipe busy).
fn is_transient_connect_msg(msg: &str) -> bool {
    msg.contains("Connection refused") || msg.contains("No such file or directory")
}
