# wasmCloud Host

Custom wasmCloud **washlet** binary with a SurrealDB host plugin (`seamlezz:surrealdb/call@0.2.0`). Runs cluster-connected workloads and exposes SurrealDB query + live subscribe to guest components.

## Workspace

| Crate | Binary / lib | Purpose |
|-------|----------------|---------|
| `wasmcloud-host-runtime` | `wasmcloud-host` | NATS-connected cluster host (`ClusterHostBuilder`) |
| `wasmcloud-plugin-surrealdb` | — | `HostPlugin` for SurrealDB |

Guest components: [`surrealdb-component-sdk`](https://github.com/Seamlezz/surrealdb-wasi-component).

## Prerequisites

- Rust **1.91+** (see `rust-version` in root `Cargo.toml`)
- [NATS](https://nats.io/) reachable from the host
- wasmCloud operator or compatible scheduler on the same NATS cluster

## Run locally

```bash
cargo build --release -p wasmcloud-host-runtime
./target/release/wasmcloud-host \
  --scheduler-nats-url=nats://127.0.0.1:4222 \
  --data-nats-url=nats://127.0.0.1:4222 \
  --host-group=default
```

Same settings via env: `SCHEDULER_NATS_URL`, `DATA_NATS_URL`, `HOST_GROUP`. Full flags: `wasmcloud-host --help`.

## SurrealDB on workloads

Add a `hostInterfaces` entry for `seamlezz:surrealdb/call@0.2.0` with:

| Key | Required | Example |
|-----|----------|---------|
| `url` | yes | `memory`, `ws://127.0.0.1:8000` |
| `namespace` | yes | `dev` |
| `database` | yes | `app` |
| `username` | no | root user |
| `password` | no | required when `username` is set |

## Configuration

| Flag | Env | Default |
|------|-----|---------|
| `--host-group` | `HOST_GROUP` | `default` |
| `--scheduler-nats-url` | `SCHEDULER_NATS_URL` | `nats://127.0.0.1:4222` |
| `--data-nats-url` | `DATA_NATS_URL` | `nats://127.0.0.1:4222` |
| `--http-addr` | `HTTP_ADDR` | `0.0.0.0:8080` |
| `--host-name` | `HOST_NAME` | — |
| `--environment` | `WASMCLOUD_HOST_ENVIRONMENT` | — |
| `--oci-cache-dir` | `OCI_CACHE_DIR` | — |
| `--wasip3` | `WASIP3` | `true` |
| `--wasi-otel` | `WASI_OTEL` | `true` |

WASI P3 and OpenTelemetry stay on unless disabled with `--wasip3=false` / `--wasi-otel=false`.

## Container image

Published to **`ghcr.io/seamlezz/wasmcloud-host`** (`linux/amd64`, `linux/arm64`). Tags: workspace version from `Cargo.toml` and `latest`.

CI (`.github/workflows/publish-runtime.yml`) publishes on push to `main` when `[workspace.package].version` changes. **workflow_dispatch** rebuilds the current version without a bump.

Local build ([Dagger CLI](https://docs.dagger.io/install)):

```bash
dagger call runtime-image --platform=linux/amd64 with-exec --args=/usr/local/bin/wasmcloud-host stdout
```

Publish (needs `write:packages` token):

```bash
export GITHUB_TOKEN=ghp_...
dagger call build-and-push \
  --registry=ghcr.io \
  --image=seamlezz/wasmcloud-host \
  --tag=0.1.0 \
  --username=YOUR_GH_USER \
  --password=env://GITHUB_TOKEN \
  --include-latest=true
```

## Development

Refresh vendored WIT after contract changes (`wkg.lock` and `wit/deps/` are committed):

```bash
cd crates/plugins/surrealdb
wkg get seamlezz:surrealdb@0.2.0 --format wit -o wit/deps/seamlezz-surrealdb-0.2.0/package.wit
```

```bash
cargo test --workspace
```

## License

[Unlicense](LICENSE)
