//! Builder for [`crate::BusClient`].

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use tokimo_bus_protocol::{BusError, CallerCtx, MethodDecl};

use crate::{client::BusClient, config::ClientConfig};

/// Data handed to a method handler registered via
/// [`BusClientBuilder::on_invoke`].
#[derive(Debug, Clone)]
pub struct InvokeRequest {
    /// Opaque rmp-serde bytes; decode with your own types.
    pub payload: Vec<u8>,
    /// Identity / trace context injected by `tokimo-server`.
    pub caller: CallerCtx,
}

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub(crate) type InvokeHandler = Arc<
    dyn Fn(InvokeRequest) -> BoxFuture<'static, Result<Vec<u8>, BusError>> + Send + Sync,
>;

/// Fluent builder; see crate-level docs for a full example.
pub struct BusClientBuilder {
    pub(crate) cfg: ClientConfig,
    pub(crate) service: Option<(String, String)>, // (name, version)
    pub(crate) methods: Vec<MethodDecl>,
    pub(crate) handlers: HashMap<String, InvokeHandler>,
    pub(crate) events: Vec<String>,
}

impl BusClientBuilder {
    pub(crate) fn new(cfg: ClientConfig) -> Self {
        Self {
            cfg,
            service: None,
            methods: Vec::new(),
            handlers: HashMap::new(),
            events: Vec::new(),
        }
    }

    /// Set the service name (must be unique in the broker) and build version.
    #[must_use]
    pub fn service(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.service = Some((name.into(), version.into()));
        self
    }

    /// Declare a method. Must be called before [`Self::on_invoke`] for the
    /// same method name.
    #[must_use]
    pub fn method(mut self, decl: MethodDecl) -> Self {
        self.methods.push(decl);
        self
    }

    /// Register an async handler for a declared method. Ignored if no
    /// [`MethodDecl`] exists yet with this name (the connect will fail).
    #[must_use]
    pub fn on_invoke<F, Fut>(mut self, method: impl Into<String>, handler: F) -> Self
    where
        F: Fn(InvokeRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, BusError>> + Send + 'static,
    {
        let handler: InvokeHandler = Arc::new(move |req| Box::pin(handler(req)));
        self.handlers.insert(method.into(), handler);
        self
    }

    /// Declare a topic this app will publish. Informational only — `publish`
    /// works with any topic.
    #[must_use]
    pub fn publishes(mut self, topic: impl Into<String>) -> Self {
        self.events.push(topic.into());
        self
    }

    /// Connect to the broker, send the `Hello`, and return a ready client.
    pub async fn build(self) -> Result<Arc<BusClient>, BusError> {
        BusClient::connect_with_builder(self).await
    }
}

/// Entry point on [`BusClient`].
impl BusClient {
    /// Start building a client.
    #[must_use]
    pub fn builder(cfg: ClientConfig) -> BusClientBuilder {
        BusClientBuilder::new(cfg)
    }
}
