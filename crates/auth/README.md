# tokimo-app-auth

`tokimo-app-auth` 是 Tokimo 三方 App 的认证辅助 crate，提供两类能力：

- `axum` feature：从主服务转发的请求头中提取当前 Tokimo 用户。
- `cli` feature：为 App CLI 统一解析 Tokimo token / server，并封装带 Bearer Token 的 HTTP 客户端。

## Axum 用法

```rust
use tokimo_app_auth::TokimoUser;

async fn handler(TokimoUser { user_id }: TokimoUser) -> String {
    user_id
}
```

Extractor 会读取 `X-Tokimo-User-Id` 请求头；缺失或为空时返回 `401` JSON 错误。

## CLI 用法

```rust
use clap::Parser;
use tokimo_app_auth::{Client, Credentials, TokimoAuthArgs};

#[derive(Parser)]
struct Args {
    #[command(flatten)]
    auth: TokimoAuthArgs,
}

fn build_client(args: &Args) -> Result<Client, tokimo_app_auth::CliError> {
    let creds = Credentials::resolve(&args.auth)?;
    Ok(Client::new("helloworld", creds))
}
```

`TokimoAuthArgs` 支持：

- `--tokimo-token` / `TOKIMO_TOKEN`：必填认证 token。
- `--tokimo-server` / `TOKIMO_SERVER_URL`：Tokimo 主服务地址，默认 `http://localhost:5678`。

## Features

| feature | 默认启用 | 说明 |
| --- | --- | --- |
| `axum` | 是 | 启用 `TokimoUser` extractor。 |
| `cli` | 是 | 启用 `TokimoAuthArgs`、`Credentials`、`Client` 和 `CliError`。 |
