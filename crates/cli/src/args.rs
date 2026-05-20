use clap::Args;

/// Tokimo CLI 认证参数（API token + 环境变量）。
///
/// CLI 直接连数据库（通过 `DATABASE_URL`），用 token 校验用户身份。
/// 不依赖主 server HTTP 进程运行。
#[derive(Debug, Clone, Args)]
pub struct TokimoAuthArgs {
    /// Tokimo API token (mm_xxx). 在主 server 的设置页 → API Keys 创建。
    #[arg(long = "tokimo-token", env = "TOKIMO_TOKEN", global = true, hide_env_values = true)]
    pub token: Option<String>,
}

/// 认证凭据（解析后的 token）。
#[derive(Debug, Clone)]
pub struct Credentials {
    /// API token（mm_xxx 格式）
    pub token: String,
}

impl Credentials {
    /// 从 CLI 参数解析凭据（校验 token 非空）。
    pub fn resolve(args: &TokimoAuthArgs) -> anyhow::Result<Self> {
        let token = args.token.clone().filter(|t| !t.trim().is_empty()).ok_or_else(|| {
            anyhow::anyhow!(
                "missing --tokimo-token (or TOKIMO_TOKEN env). \
                     去主 server 设置页 → API Keys 创建一个 mm_xxx token。"
            )
        })?;

        Ok(Self { token })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_requires_token() {
        let args = TokimoAuthArgs { token: None };

        assert!(Credentials::resolve(&args).is_err());
    }

    #[test]
    fn resolve_returns_token() {
        let args = TokimoAuthArgs {
            token: Some("mm_test".to_owned()),
        };

        let creds = Credentials::resolve(&args).unwrap();
        assert_eq!(creds.token, "mm_test");
    }
}
