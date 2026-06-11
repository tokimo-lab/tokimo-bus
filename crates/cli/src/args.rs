use clap::Args;

/// Common CLI authentication parameters.
///
/// The CLI connects directly to the database (via `DATABASE_URL`) to verify
/// user identity; it does not depend on the main server HTTP process.
#[derive(Debug, Clone, Args)]
pub struct TokimoAuthArgs {
    /// Tokimo API token (mm_xxx). Create one in the main server Settings → API Keys.
    #[arg(
        long = "tokimo-token",
        env = "TOKIMO_TOKEN",
        global = true,
        hide_env_values = true,
        hide = true
    )]
    pub token: Option<String>,
}

/// Resolved authentication credentials.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// API token (mm_xxx format)
    pub token: String,
}

impl Credentials {
    /// Parse credentials from CLI args (validates token is non-empty).
    pub fn resolve(args: &TokimoAuthArgs) -> anyhow::Result<Self> {
        let token = args.token.clone().filter(|t| !t.trim().is_empty()).ok_or_else(|| {
            anyhow::anyhow!(
                "missing --tokimo-token (or TOKIMO_TOKEN env). \
                     Create one in the main server Settings → API Keys."
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
