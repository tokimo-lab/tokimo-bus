use clap::Args;
use reqwest::Response;
use serde::{Serialize, de::DeserializeOwned};

/// Tokimo 主 server 认证参数（CLI 参数 + 环境变量）
#[derive(Debug, Clone, Args)]
pub struct TokimoAuthArgs {
    /// Tokimo API token (mm_xxx). 在主 server 的设置页 → API Keys 创建。
    #[arg(long = "tokimo-token", env = "TOKIMO_TOKEN", global = true, hide_env_values = true)]
    pub token: Option<String>,
    /// Tokimo 主 server URL，默认 http://localhost:5678
    #[arg(long = "tokimo-server", env = "TOKIMO_SERVER_URL", global = true)]
    pub server: Option<String>,
}

/// 认证凭据（解析后的 server URL + token）
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Tokimo 主 server 的完整 URL
    pub server_url: String,
    /// API token（mm_xxx 格式）
    pub token: String,
}

impl Credentials {
    /// 从 CLI 参数解析凭据（校验 token 非空，设置默认 server）
    pub fn resolve(args: &TokimoAuthArgs) -> Result<Self, CliError> {
        let token = args
            .token
            .clone()
            .filter(|token| !token.trim().is_empty())
            .ok_or(CliError::MissingToken)?;
        let server_url = args
            .server
            .clone()
            .unwrap_or_else(|| "http://localhost:5678".to_owned());

        Ok(Self { server_url, token })
    }
}

/// CLI 错误类型
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("missing --tokimo-token (or TOKIMO_TOKEN env). 去主 server 设置页 → API Keys 创建一个 mm_xxx token。")]
    MissingToken,
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server returned {status}: {body}")]
    Server { status: u16, body: String },
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Tokimo app API client（自动注入 app_id + Bearer token）
#[derive(Debug, Clone)]
pub struct Client {
    app_id: String,
    base_url: String,
    http: reqwest::Client,
    token: String,
}

impl Client {
    /// 创建新 client（base_url = {server}/api/apps/{app_id}）
    pub fn new(app_id: impl Into<String>, credentials: Credentials) -> Self {
        let app_id = app_id.into();
        let server = credentials.server_url.trim_end_matches('/');
        let base_url = format!("{server}/api/apps/{app_id}");

        Self {
            app_id,
            base_url,
            http: reqwest::Client::new(),
            token: credentials.token,
        }
    }

    /// 返回此 client 绑定的 app_id
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// GET 请求（自动反序列化 JSON 响应）
    pub async fn get<T>(&self, path: &str) -> Result<T, CliError>
    where
        T: DeserializeOwned,
    {
        let response = self.http.get(self.url(path)).bearer_auth(&self.token).send().await?;

        handle_resp(response).await
    }

    /// POST 请求（body 序列化为 JSON，响应反序列化）
    pub async fn post<B, T>(&self, path: &str, body: &B) -> Result<T, CliError>
    where
        B: Serialize,
        T: DeserializeOwned,
    {
        let response = self
            .http
            .post(self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?;

        handle_resp(response).await
    }

    /// PUT 请求（body 序列化为 JSON，响应反序列化）
    pub async fn put<B, T>(&self, path: &str, body: &B) -> Result<T, CliError>
    where
        B: Serialize,
        T: DeserializeOwned,
    {
        let response = self
            .http
            .put(self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await?;

        handle_resp(response).await
    }

    /// DELETE 请求（无响应 body）
    pub async fn delete(&self, path: &str) -> Result<(), CliError> {
        let response = self.http.delete(self.url(path)).bearer_auth(&self.token).send().await?;

        handle_empty_resp(response).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }
}

async fn handle_resp<T>(response: Response) -> Result<T, CliError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        return Err(server_error(status.as_u16(), response).await);
    }

    let bytes = response.bytes().await?;
    serde_json::from_slice(&bytes).map_err(CliError::Json)
}

async fn handle_empty_resp(response: Response) -> Result<(), CliError> {
    let status = response.status();
    if !status.is_success() {
        return Err(server_error(status.as_u16(), response).await);
    }

    Ok(())
}

async fn server_error(status: u16, response: Response) -> CliError {
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => format!("failed to read error body: {error}"),
    };

    CliError::Server { status, body }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_requires_token() {
        let args = TokimoAuthArgs {
            token: None,
            server: None,
        };

        let error = Credentials::resolve(&args).expect_err("missing token should fail");

        assert!(matches!(error, CliError::MissingToken));
    }

    #[test]
    fn client_url_trims_server_and_path_slashes() {
        let credentials = Credentials {
            server_url: "http://localhost:5678/".to_owned(),
            token: "token".to_owned(),
        };
        let client = Client::new("helloworld", credentials);

        assert_eq!(client.app_id(), "helloworld");
        assert_eq!(
            client.url("/items/1"),
            "http://localhost:5678/api/apps/helloworld/items/1"
        );
    }
}
