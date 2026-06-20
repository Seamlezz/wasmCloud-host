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
| `--log-level` | — | `info` |
| `--verbose` / `-v` | — | `false` |
| `--otel-debug` | `WASMCLOUD_OTEL_DEBUG` | `false` |
| `--oci-cache-dir` | `OCI_CACHE_DIR` | — |
| `--wasip3` | `WASIP3` | `true` |
| `--wasi-otel` | `WASI_OTEL` | `true` |

WASI P3 and OpenTelemetry stay on unless disabled with `--wasip3=false` / `--wasi-otel=false`.

## Observability

The host exports traces, logs, and metrics via OTLP (gRPC) when any `OTEL_*` environment variable is set. Without `OTEL_*`, logs go to stderr only (`RUST_LOG` filters apply).

### Local Jaeger

```bash
docker run -d --name jaeger \
  -p 4317:4317 -p 16686:16686 \
  jaegertracing/all-in-one:latest

export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_SERVICE_NAME=wasmcloud-host
export RUST_LOG=info,wasmcloud_plugin_surrealdb=debug

./target/release/wasmcloud-host --host-group=dev
```

Open http://localhost:16686 to inspect traces. Guest workload telemetry (via `wasi:otel`) exports separately with `service.name=wasi-otel`.

### Environment variables

| Variable | Purpose |
|----------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC collector endpoint (traces, logs, metrics) |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Traces-only endpoint override |
| `OTEL_SERVICE_NAME` | Host `service.name` resource attribute |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes (comma-separated `key=value`) |
| `OTEL_TRACES_SAMPLER` | Trace sampling policy |
| `WASMCLOUD_OTEL_DEBUG` | Sets host observability logging to debug and enables verbose runtime targets |
| `RUST_LOG` | Log filter (overrides `--log-level` when set) |

Map host identity into resource attributes:

| Host flag / env | Suggested OTEL attribute |
|-----------------|--------------------------|
| `--host-group` / `HOST_GROUP` | `wasmcloud.host.group` |
| `--host-name` / `HOST_NAME` | `wasmcloud.host.name` |
| `--environment` / `WASMCLOUD_HOST_ENVIRONMENT` | `deployment.environment` |

Example:

```bash
export OTEL_RESOURCE_ATTRIBUTES="wasmcloud.host.group=prod,wasmcloud.host.name=host-1,deployment.environment=prod"
```

SurrealDB host calls emit spans with `db.system=surrealdb` and `db.operation` (`query`, `subscribe`, `cancel`). Query text and param values are not logged.

See [docs/observability.md](docs/observability.md) for the full design.

## Container image

Published to **`ghcr.io/seamlezz/wasmcloud-host`** (`linux/amd64`, `linux/arm64`). Tags: workspace version from `Cargo.toml` and `latest`. Per-platform images are also tagged as `<version>-amd64` and `<version>-arm64`.

CI (`.github/workflows/publish-runtime.yml`) builds and publishes each platform natively, then combines them into the version and `latest` multi-arch manifests. Publishing runs on push to `main` when the version tag does not already exist in GHCR. **workflow_dispatch** forces a republish.

### Local development

Read the current runtime version:

```bash
dagger call runtime-version
```

Check whether CI would publish (requires `packages: read` token):

```bash
dagger call needs-publish \
  --registry=ghcr.io \
  --image=seamlezz/wasmcloud-host \
  --username=YOUR_GH_USER \
  --password=env://GITHUB_TOKEN
```

Publish one platform (same as a CI matrix leg):

```bash
dagger call publish-platform \
  --platform=linux/amd64 \
  --registry=ghcr.io \
  --image=seamlezz/wasmcloud-host \
  --username=YOUR_GH_USER \
  --password=env://GITHUB_TOKEN
```

Publish multi-arch locally (builds both platforms in one graph):

```bash
dagger call publish \
  --registry=ghcr.io \
  --image=seamlezz/wasmcloud-host \
  --tag=0.1.0 \
  --username=YOUR_GH_USER \
  --password=env://GITHUB_TOKEN \
  --include-latest=true
```

Build a single-platform image and inspect it:

```bash
dagger call build --platform=linux/amd64 with-exec --args=/usr/local/bin/wasmcloud-host stdout
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
