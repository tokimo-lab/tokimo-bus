//! 请求级 user_id 自动注入 — 基于 tokio task-local。
//!
//! 主服务器 data_plane proxy 在转发请求给 sidecar 时会注入 `x-tokimo-user-id` header。
//! 本模块将其提取到 task-local，供 bus client 构造 CallerCtx 时自动读取。
//!
//! ## 使用方式
//!
//! ```rust,no_run
//! // app_server.rs — 注册一次 middleware
//! Router::new()
//!     .route("/analyze", post(handler))
//!     .layer(axum::middleware::from_fn(
//!         tokimo_bus_protocol::task_local::auth_middleware,
//!     ))
//! ```
//!
//! ```rust,ignore
//! // handler — 完全无感知 auth
//! pub async fn handler(State(state): State<Arc<AppCtx>>) -> ... {
//!     let client = state.client.get().unwrap();
//!     let job = jobs::create(client, client.auto_caller("my-app"), req).await?;
//! }
//! ```

use std::cell::RefCell;

tokio::task_local! {
    static USER_ID: RefCell<Option<String>>;
}

/// 从 task-local 获取当前请求的 user_id。
///
/// 在 handler 或 bus client 内部调用，无需传参。
/// 如果不在 HTTP 请求上下文中调用（如 CLI 模式），返回 `None`。
pub fn current_user_id() -> Option<String> {
    USER_ID.try_with(|ctx| ctx.borrow().clone()).ok().flatten()
}

/// 设置 task-local user_id（供非 axum 环境使用，如 bus invoke handler）。
pub fn set_user_id(user_id: Option<String>) {
    let _ = USER_ID.try_with(|ctx| {
        *ctx.borrow_mut() = user_id;
    });
}

/// Axum 中间件：从 `x-tokimo-user-id` header 提取 user_id 存入 task-local。
///
/// 每个 HTTP 请求在独立 tokio task 中执行，task-local 天然隔离，无需加锁。
/// 请求结束后 scope 自动结束，值被 drop。
#[cfg(feature = "axum")]
pub async fn auth_middleware(req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    let user_id = req
        .headers()
        .get("x-tokimo-user-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    USER_ID
        .scope(RefCell::new(user_id), async { next.run(req).await })
        .await
}
