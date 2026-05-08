use clap::Args;

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
    pub fn resolve(args: &TokimoAuthArgs) -> anyhow::Result<Self> {
        let token = args.token.clone().filter(|t| !t.trim().is_empty()).ok_or_else(|| {
            anyhow::anyhow!(
                "missing --tokimo-token (or TOKIMO_TOKEN env). \
                     去主 server 设置页 → API Keys 创建一个 mm_xxx token。"
            )
        })?;
        let server_url = args
            .server
            .clone()
            .unwrap_or_else(|| "http://localhost:5678".to_owned());

        Ok(Self { server_url, token })
    }
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

        assert!(Credentials::resolve(&args).is_err());
    }

    #[test]
    fn resolve_defaults_server_url() {
        let args = TokimoAuthArgs {
            token: Some("mm_test".to_owned()),
            server: None,
        };

        let creds = Credentials::resolve(&args).unwrap();
        assert_eq!(creds.server_url, "http://localhost:5678");
        assert_eq!(creds.token, "mm_test");
    }
}
