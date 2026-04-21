# tokimo-bus

Multi-process IPC bus for [Tokimo](https://github.com/tokimo-lab/tokimo), inspired by
OpenWrt [**ubus**](https://openwrt.org/docs/techref/ubus) + [**procd**](https://openwrt.org/docs/techref/procd).

Foundation for splitting the monolithic `tokimo-server` into per-app processes:
each app runs in its own OS process, registers methods with the broker, and the
server proxies `HTTP → bus call → app` transparently.

## Why

- **Isolation**: one app's panic or memory leak no longer takes down the server
- **Independent upgrade**: redeploy a single app binary, supervisor hot-swaps it
- **Independent scaling**: heavy model apps (AI / perception) get their own process,
  with idle exit to reclaim RAM back to the OS
- **Third-party app ecosystem**: any binary that speaks the bus protocol can plug in

## Architecture

```
                          HTTP / WebSocket
                                 │
                ┌────────────────▼────────────────┐
                │  tokimo-server (main process)   │
                │  ┌───────────────────────────┐  │
                │  │ Axum router (auth / CORS) │  │
                │  │ HTTP → bus adapter        │  │
                │  └──────────────┬────────────┘  │
                │  ┌──────────────▼────────────┐  │
                │  │ tokimo-bus-broker         │◀─┼─── UDS / Named Pipe
                │  │ · Service registry        │  │    tokimo-bus.sock
                │  │ · Invoke router           │  │
                │  │ · Event pub/sub           │  │
                │  │ · AppSupervisor (procd)   │  │
                │  └───────────────────────────┘  │
                └────────────────┬────────────────┘
                                 │ framed rmp-serde
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
  ┌─────────────┐        ┌──────────────┐        ┌──────────────┐
  │ app-video   │        │ app-terminal │  ...   │ app-helloworld
  │ (own proc)  │        │ (own proc)   │        │ (reference)  │
  └─────────────┘        └──────────────┘        └──────────────┘
```

## Crates

| Crate | Purpose | Depended on by |
|---|---|---|
| [`tokimo-bus-protocol`](./crates/protocol) | Wire types + length-prefixed `rmp-serde` frame codec | everything |
| [`tokimo-bus-client`](./crates/client) | App-side SDK: connect, register methods, call, pub/sub, auto-reconnect | each app (`tokimo-app-*`) |
| [`tokimo-bus-broker`](./crates/broker) | Server-side broker: registry, Invoke router, event bus | `tokimo-server` |
| [`tokimo-bus-supervisor`](./crates/supervisor) | `procd` equivalent: spawn / idle-exit / respawn / backoff | `tokimo-server` |

Apps pull only `protocol + client` (small, platform-independent). Server pulls
all four.

## OpenWrt parallels

| OpenWrt        | tokimo-bus                         |
|----------------|------------------------------------|
| `ubusd`        | `tokimo-bus-broker` (embedded in server) |
| `procd`        | `tokimo-bus-supervisor`            |
| `/var/run/ubus.sock` | `$DATA_LOCAL_PATH/tokimo-bus.sock` |
| blobmsg        | `rmp-serde`                        |
| `ubus call`    | `BusClient::call(service, method, payload)` |
| `ubus listen`  | `BusClient::subscribe(topic_prefix)` |
| ACL files      | `MethodDecl { requires_auth }` + `CallerCtx` injected by server |

## Supported platforms

- **Linux** — UDS (`AF_UNIX`)
- **macOS** — UDS (`AF_UNIX`)
- **Windows 10 1803+ / 11** — UDS (native `AF_UNIX`) with Named Pipe fallback

All transports use the same `rmp-serde` framed protocol; choice of transport is
runtime-configured via `AnyTransport`.

## Example

See [`tokimo-app-helloworld`](https://github.com/tokimo-lab/tokimo-app-helloworld)
for a minimal reference app: connects to the bus, registers an `echo` method,
emits a periodic event, and handles graceful shutdown.

## Status

Phase 1 — foundation crates. Not yet integrated into `tokimo-server`. APIs may
change until 0.2.

## License

MIT OR Apache-2.0
