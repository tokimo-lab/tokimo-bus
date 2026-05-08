//! Tokimo app 认证库：Axum extractor + DB helpers

/// Axum extractor（从 X-Tokimo-User-Id header 提取用户信息）
#[cfg(feature = "axum")]
pub mod axum;

/// DB helpers（connect_db / verify_token / VerifiedUser）
#[cfg(feature = "cli-db")]
pub mod db;

#[cfg(feature = "axum")]
pub use self::axum::TokimoUser;
