//! Per-connection session: reads frames, dispatches, writes outbound.

use std::sync::{Arc, atomic::AtomicU64};

use dashmap::DashMap;
use tokio::{
    io::{AsyncRead, AsyncWrite, split},
    sync::{mpsc, oneshot},
};
use tracing::{debug, info, warn};

use tokimo_bus_protocol::{BusError, BusFrame, HelloAck, ProtocolVersion, Response, read_frame_opt, write_frame};

use crate::{broker::Broker, registry::ServiceEntry};

/// Handle a freshly-accepted connection. Runs until the stream closes.
pub(crate) async fn serve<S>(broker: Arc<Broker>, stream: S, peer_addr: String)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut reader, mut writer) = split(Box::pin(stream));

    // ── handshake ──────────────────────────────────────────────────────
    let hello: BusFrame = match read_frame_opt(&mut reader).await {
        Ok(Some(f)) => f,
        Ok(None) => return,
        Err(e) => {
            warn!(peer = %peer_addr, error = %e, "bus-broker: bad hello frame");
            return;
        }
    };
    let hello = match hello {
        BusFrame::Hello(h) => h,
        other => {
            warn!(peer = %peer_addr, ?other, "bus-broker: expected Hello");
            return;
        }
    };

    if hello.protocol.major != ProtocolVersion::CURRENT.major {
        let _ = write_frame(
            &mut writer,
            &BusFrame::Response(Response {
                req_id: 0,
                result: Err(BusError::ProtocolMismatch {
                    broker: ProtocolVersion::CURRENT,
                    client: hello.protocol,
                }),
            }),
        )
        .await;
        return;
    }

    if !broker.consume_token(&hello.service, &hello.auth_token) {
        warn!(peer = %peer_addr, service = %hello.service, "bus-broker: bad spawn token");
        let _ = write_frame(
            &mut writer,
            &BusFrame::Response(Response {
                req_id: 0,
                result: Err(BusError::InvalidAuthToken),
            }),
        )
        .await;
        return;
    }

    let client_id = broker.next_client_id();
    let (tx, rx) = mpsc::unbounded_channel::<BusFrame>();
    let entry = ServiceEntry {
        service: hello.service.clone(),
        generation: 0, // set by insert()
        tx: tx.clone(),
        methods: Arc::new(hello.methods.clone()),
        pid: hello.pid,
        data_plane: hello.data_plane.clone(),
    };
    let generation = broker.registry().insert(entry);

    info!(
        peer = %peer_addr,
        service = %hello.service,
        pid = hello.pid,
        client_id,
        generation,
        "bus-broker: service registered",
    );

    if let Err(e) = write_frame(
        &mut writer,
        &BusFrame::HelloAck(HelloAck {
            client_id,
            heartbeat_interval: broker.config().heartbeat_interval,
        }),
    )
    .await
    {
        warn!(service = %hello.service, error = %e, "bus-broker: ack write failed");
        broker.registry().remove_if(&hello.service, generation);
        return;
    }

    // ── main loop ──────────────────────────────────────────────────────
    let writer_task = tokio::spawn(async move {
        let mut rx = rx;
        while let Some(f) = rx.recv().await {
            if let Err(e) = write_frame(&mut writer, &f).await {
                debug!(error = %e, "bus-broker: writer ended");
                break;
            }
        }
    });

    let pending_out = Arc::new(DashMap::<u64, oneshot::Sender<Response>>::new());
    let next_req_id = Arc::new(AtomicU64::new(1));

    // Register this session with the broker so `call()` can route through it.
    broker.attach_session(&hello.service, generation, pending_out.clone(), next_req_id.clone());

    loop {
        match read_frame_opt::<_, BusFrame>(&mut reader).await {
            Ok(Some(BusFrame::Pong)) => {}
            Ok(Some(BusFrame::Response(resp))) => {
                if let Some((_, waker)) = pending_out.remove(&resp.req_id) {
                    let _ = waker.send(resp);
                }
            }
            Ok(Some(BusFrame::Publish(ev))) => {
                broker.fanout(&hello.service, ev);
            }
            Ok(Some(BusFrame::Subscribe { topic_prefix })) => {
                broker.subscribe(hello.service.clone(), topic_prefix);
            }
            Ok(Some(BusFrame::Unsubscribe { topic_prefix })) => {
                broker.unsubscribe(&hello.service, &topic_prefix);
            }
            Ok(Some(BusFrame::Invoke(inv))) => {
                // App-originated call: parse `service.method`, route via broker, reply to caller.
                let broker = Arc::clone(&broker);
                let tx = tx.clone();
                let originator = hello.service.clone();
                tokio::spawn(async move {
                    let (target_service, target_method) = match inv.method.split_once('.') {
                        Some((s, m)) => (s.to_string(), m.to_string()),
                        None => {
                            let _ = tx.send(BusFrame::Response(Response {
                                req_id: inv.req_id,
                                result: Err(BusError::BadRequest(format!(
                                    "invoke method must be `service.method`, got `{}`",
                                    inv.method
                                ))),
                            }));
                            return;
                        }
                    };
                    let result = broker
                        .call(&target_service, &target_method, inv.payload, inv.caller)
                        .await;
                    debug!(
                        from = %originator,
                        target = %format!("{target_service}.{target_method}"),
                        ok = result.is_ok(),
                        "bus-broker: app-originated invoke routed",
                    );
                    let _ = tx.send(BusFrame::Response(Response {
                        req_id: inv.req_id,
                        result,
                    }));
                });
            }
            Ok(Some(other)) => {
                debug!(?other, "bus-broker: ignoring frame");
            }
            Ok(None) => {
                info!(service = %hello.service, "bus-broker: connection closed");
                break;
            }
            Err(e) => {
                warn!(service = %hello.service, error = %e, "bus-broker: read error");
                break;
            }
        }
    }

    broker.detach_session(&hello.service, generation);
    broker.registry().remove_if(&hello.service, generation);
    writer_task.abort();
}
