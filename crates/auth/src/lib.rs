//! Tokimo app 认证库：Axum extractor + CLI client

/// Axum extractor（从 X-Tokimo-User-Id header 提取用户信息）
#[cfg(feature = "axum")]
pub mod axum;

/// CLI client（自动注入 app_id + Bearer token 的 HTTP 客户端）
#[cfg(any(feature = "cli", feature = "cli-db"))]
pub mod cli;

#[cfg(feature = "axum")]
pub use self::axum::TokimoUser;

#[cfg(feature = "cli")]
pub use self::cli::{CliError, Client, Credentials, TokimoAuthArgs};

#[cfg(feature = "cli-db")]
pub use cli::db;
