//! End-to-end test: in-process broker + in-process client over a real UDS.

use std::time::Duration;

use tokimo_bus_broker::{Broker, BrokerConfig};
use tokimo_bus_client::{BusClient, ClientConfig};
use tokimo_bus_protocol::{CallerCtx, MethodDecl};

#[tokio::test]
async fn echo_roundtrip() {
    let dir = tempdir();
    let sock = dir.join("tokimo-bus.sock");

    let broker = Broker::new(BrokerConfig {
        heartbeat_interval: Duration::from_secs(60),
        default_call_timeout: Duration::from_secs(5),
    });
    broker.listen_unix(&sock).await.unwrap();

    // Normally the supervisor would issue this; emulate here.
    let token = broker.issue_token("helloworld");

    let cfg = ClientConfig::unix(sock.clone(), token);
    let client = BusClient::builder(cfg)
        .service("helloworld", "0.1.0")
        .method(MethodDecl {
            name: "echo".into(),
            requires_auth: false,
            streaming: false,
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
    assert!(matches!(
        err,
        tokimo_bus_protocol::BusError::ServiceNotFound(_)
    ));

    // Unknown method
    let err = broker
        .call("helloworld", "nope", vec![], CallerCtx::default())
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tokimo_bus_protocol::BusError::MethodNotFound { .. }
    ));

    client.shutdown();
}

#[tokio::test]
async fn bad_token_rejected() {
    let dir = tempdir();
    let sock = dir.join("tokimo-bus.sock");

    let broker = Broker::new(BrokerConfig::default());
    broker.listen_unix(&sock).await.unwrap();
    // Do NOT issue a token; any token will be rejected.

    let cfg = ClientConfig::unix(sock, "bogus-token");
    let err = match BusClient::builder(cfg)
        .service("helloworld", "0.1.0")
        .method(MethodDecl {
            name: "echo".into(),
            requires_auth: false,
            streaming: false,
            description: None,
        })
        .on_invoke("echo", |req| async move { Ok(req.payload) })
        .build()
        .await
    {
        Ok(_) => panic!("handshake should fail"),
        Err(e) => e,
    };
    assert!(matches!(
        err,
        tokimo_bus_protocol::BusError::InvalidAuthToken
    ));
}

fn tempdir() -> std::path::PathBuf {
    // Use a random-ish subdir in /tmp; cleanup is best-effort (short sockets
    // path matters more than cleanup on Linux where UNIX_PATH_MAX ≈ 108).
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("tb-{ns}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
