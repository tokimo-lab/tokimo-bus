//! Tokimo app 认证库：Axum extractor + DB helpers + task-local auth

/// Axum extractor（从 X-Tokimo-User-Id header 提取用户信息）
#[cfg(feature = "axum")]
pub mod axum;

/// DB helpers（connect_db / verify_token / VerifiedUser）
#[cfg(feature = "cli-db")]
pub mod db;

/// 请求级 auth 中间件（task-local 自动注入 user_id）
pub mod task_local {
    pub use tokimo_bus_protocol::task_local::*;
}

#[cfg(feature = "axum")]
pub use self::axum::TokimoUser;
