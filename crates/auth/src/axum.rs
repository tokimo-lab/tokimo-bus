use ::axum::{
    Json,
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use serde_json::{Value, json};

/// Tokimo 用户信息（从 X-Tokimo-User-Id header 提取）
#[derive(Debug, Clone)]
pub struct TokimoUser {
    /// 用户 ID（主 server 认证后通过 header 传递）
    pub user_id: String,
}

impl<S> FromRequestParts<S> for TokimoUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<Value>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(user_id) = parts
            .headers
            .get("x-tokimo-user-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
        else {
            return Err(unauthorized());
        };

        Ok(Self {
            user_id: user_id.to_owned(),
        })
    }
}

fn unauthorized() -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "error": "missing X-Tokimo-User-Id (not authenticated by main server)",
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::axum::http::Request;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    fn request_parts(user_id: Option<&str>) -> Parts {
        let mut builder = Request::builder();
        if let Some(user_id) = user_id {
            builder = builder.header("x-tokimo-user-id", user_id);
        }

        let (parts, ()) = builder.body(()).expect("mock request should build").into_parts();
        parts
    }

    fn block_on_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);

        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("extractor future should be ready"),
        }
    }

    #[test]
    fn extracts_user_id_from_header() {
        let mut parts = request_parts(Some("user-1"));

        let user =
            block_on_ready(TokimoUser::from_request_parts(&mut parts, &())).expect("user id should be extracted");

        assert_eq!(user.user_id, "user-1");
    }

    #[test]
    fn rejects_missing_header() {
        let mut parts = request_parts(None);

        let (status, Json(body)) =
            block_on_ready(TokimoUser::from_request_parts(&mut parts, &())).expect_err("missing header should reject");

        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            body,
            json!({
                "error": "missing X-Tokimo-User-Id (not authenticated by main server)",
            })
        );
    }
}
