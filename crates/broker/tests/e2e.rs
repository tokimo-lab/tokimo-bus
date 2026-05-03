//! End-to-end test: in-process broker + in-process client over a real transport.

use std::time::Duration;

use tokimo_bus_broker::{Broker, BrokerConfig};
use tokimo_bus_client::{BusClient, ClientConfig};
use tokimo_bus_protocol::{CallerCtx, DataPlaneSocket, HttpMethod, MethodDecl};

/// Create a platform-appropriate `DataPlaneSocket` for testing.
fn test_socket() -> DataPlaneSocket {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    #[cfg(unix)]
    {
        let dir = tempdir();
        DataPlaneSocket::Unix {
            path: dir.join("tokimo-bus.sock").to_string_lossy().into_owned(),
        }
    }
    #[cfg(windows)]
    {
        DataPlaneSocket::NamedPipe {
            name: format!("tokimo-test-{}-{n}", std::process::id()),
        }
    }
}

/// Create a platform-appropriate `ClientConfig` for testing.
fn test_client_config(socket: &DataPlaneSocket, token: String) -> ClientConfig {
    match socket {
        #[cfg(unix)]
        DataPlaneSocket::Unix { path } => ClientConfig::unix(path.clone(), token),
        #[cfg(windows)]
        DataPlaneSocket::NamedPipe { name } => ClientConfig::named_pipe(name.clone(), token),
        _ => unimplemented!("unsupported platform/transport combination"),
    }
}

#[tokio::test]
async fn echo_roundtrip() {
    let socket = test_socket();

    let broker = Broker::new(BrokerConfig {
        heartbeat_interval: Duration::from_secs(60),
        default_call_timeout: Duration::from_secs(5),
    });
    broker.listen(socket.clone()).await.unwrap();

    // Normally the supervisor would issue this; emulate here.
    let token = broker.issue_token("helloworld");

    let cfg = test_client_config(&socket, token);
    let client = BusClient::builder(cfg)
        .service("helloworld", "0.1.0")
        .method(MethodDecl {
            name: "echo".into(),
            requires_auth: false,
            streaming: false,
            http_method: HttpMethod::Post,
            path: None,
            description: None,
        })
        .on_invoke("echo", |req| async move { Ok(req.payload) })
        .build()
        .await
        .expect("client handshake");

    // Give the session time to fully register.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let out = broker
        .call("helloworld", "echo", b"hello".to_vec(), CallerCtx::default())
        .await
        .expect("call");
    assert_eq!(out, b"hello");

    // Unknown service
    let err = broker
        .call("unknown", "x", vec![], CallerCtx::default())
        .await
        .unwrap_err();
    assert!(matches!(err, tokimo_bus_protocol::BusError::ServiceNotFound(_)));

    // Unknown method
    let err = broker
        .call("helloworld", "nope", vec![], CallerCtx::default())
        .await
        .unwrap_err();
    assert!(matches!(err, tokimo_bus_protocol::BusError::MethodNotFound { .. }));

    client.shutdown();
}

#[tokio::test]
async fn bad_token_rejected() {
    let socket = test_socket();

    let broker = Broker::new(BrokerConfig::default());
    broker.listen(socket.clone()).await.unwrap();
    // Do NOT issue a token; any token will be rejected.

    let cfg = test_client_config(&socket, "bogus-token".to_string());
    let err = match BusClient::builder(cfg)
        .service("helloworld", "0.1.0")
        .method(MethodDecl {
            name: "echo".into(),
            requires_auth: false,
            streaming: false,
            http_method: HttpMethod::Post,
            path: None,
            description: None,
        })
        .on_invoke("echo", |req| async move { Ok(req.payload) })
        .build()
        .await
    {
        Ok(_) => panic!("handshake should fail"),
        Err(e) => e,
    };
    assert!(matches!(err, tokimo_bus_protocol::BusError::InvalidAuthToken));
}

#[cfg(unix)]
fn tempdir() -> std::path::PathBuf {
    // Use a random-ish subdir in /tmp; cleanup is best-effort (short sockets
    // path matters more than cleanup on Linux where UNIX_PATH_MAX ≈ 108).
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("tb-{ns}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn local_service_handler() {
    use std::sync::Arc;
    let broker = Broker::new(BrokerConfig::default());

    broker.register_local_service(
        "notification_center",
        vec![MethodDecl {
            name: "notify".into(),
            requires_auth: false,
            streaming: false,
            http_method: HttpMethod::Post,
            path: None,
            description: None,
        }],
        Arc::new(|method, payload, _caller| {
            Box::pin(async move {
                assert_eq!(method, "notify");
                let mut out = b"ack:".to_vec();
                out.extend_from_slice(&payload);
                Ok(out)
            })
        }),
    );

    assert!(broker.has_local_service("notification_center"));

    let resp = broker
        .call("notification_center", "notify", b"hello".to_vec(), CallerCtx::default())
        .await
        .unwrap();
    assert_eq!(resp, b"ack:hello");
}
