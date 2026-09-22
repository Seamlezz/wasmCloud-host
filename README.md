# wasmCloud Host

Custom wasmCloud **washlet** binary with a SurrealDB host plugin (`seamlezz:surrealdb/call@0.4.0`). Runs cluster-connected workloads and exposes SurrealDB query + live subscribe to guest components.

## Workspace

| Crate | Binary / lib | Purpose |
|-------|----------------|---------|
| `wasmcloud-host-runtime` | `wasmcloud-host` | NATS-connected cluster host (upstream `HostCommand`) |
| `wasmcloud-plugin-surrealdb` | — | `HostPlugin` for SurrealDB |

Guest components: [`surrealdb-component-sdk`](https://github.com/Seamlezz/surrealdb-wasi-component).

## Prerequisites

- Rust **1.95+** (see `rust-version` in root `Cargo.toml`)
- [NATS](https://nats.io/) reachable from the host
- wasmCloud operator or compatible scheduler on the same NATS cluster

## Run locally

```bash
cargo build --release -p wasmcloud-host-runtime
./target/release/wasmcloud-host host \
  --scheduler-nats-url=nats://127.0.0.1:4222 \
  --data-nats-url=nats://127.0.0.1:4222 \
  --host-group=default
```

The binary delegates host lifecycle, resource limits, probes, startup retries, and graceful shutdown to wasmCloud 2.9.0. The SurrealDB plugin is registered before host configuration is validated. Full flags: `wasmcloud-host host --help`.

## SurrealDB on workloads

Add a `hostInterfaces` entry for `seamlezz:surrealdb/call@0.4.0` with:

| Key | Required | Example |
|-----|----------|---------|
| `url` | yes | `memory`, `http://127.0.0.1:8000`, `ws://127.0.0.1:8000`, `wss://db.example.com` |
| `namespace` | yes | `dev` |
| `database` | yes | `app` |
| `username` | no | root user |
| `password` | no | required when `username` is set |

## Configuration

Host flags require the `host` subcommand: `wasmcloud-host host [flags]`. Global flags include `--log-level`, `--verbose`, `--otel-debug`, `--meters`, and `--user-config`.

Use `--user-config /path/to/wash.toml` for host controlled plugin bindings. The operator chart supplies this file. Native messaging uses `wasmcloud:nats@0.1.0`, including Core NATS and JetStream. Workload interfaces declare `core-subscriptions`; host bindings own credentials and subject grants.

Scheduler and data connections accept `--scheduler-nats-creds` and `--data-nats-creds`, or `SCHEDULER_NATS_CREDENTIALS` and `DATA_NATS_CREDENTIALS`. The native NATS plugin independently receives its credential path through its host binding `creds` setting. TLS flags follow upstream `wash host --help`.

Auth callout uses the ordinary host subject grant `$SYS.REQ.USER.AUTH`. The fork removes the special `$SYS` prohibition. No custom identity selector or binding is required. Set `workloadConfig = "deny"` so workloads cannot replace host credentials or widen subject grants.

Use `--meters duration` (default), `--meters fuel`, or `--meters off` instead of the removed `--enable-fuel-meters` flag. Run `wasmcloud-host --help` for the accepted value syntax.

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

./target/release/wasmcloud-host host --host-group=dev
```

Open http://localhost:16686 to inspect traces. Guest telemetry through `wasi:otel` preserves component span identifiers and timestamps. By default, each component supplies `service.name`, with workload namespace and identity attached as resource attributes.

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
| `--host-group` | `wasmcloud.host.group` |
| `--host-name` | `wasmcloud.host.name` |
| `--environment` / `WASMCLOUD_HOST_ENVIRONMENT` | `deployment.environment` |

Example:

```bash
export OTEL_RESOURCE_ATTRIBUTES="wasmcloud.host.group=prod,wasmcloud.host.name=host-1,deployment.environment=prod"
```

SurrealDB host calls emit spans with `db.system.name=surrealdb` and `db.operation.name` (`query`, `subscribe`, `subscribe.stream`, `cancel`). Each call accepts an optional explicit W3C trace context; when supplied, the host uses it as the span parent. Callers may omit it when they do not need explicit propagation. Raw query text and parameter values are not recorded; spans include only query length and parameter count.

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
dagger call build --platform=linux/amd64 with-exec --args=/usr/local/bin/wasmcloud-host,host,--help stdout
```

## Development

Refresh vendored WIT after contract changes (`wkg.lock` and `wit/deps/` are committed):

```bash
cd crates/plugins/surrealdb
wkg get seamlezz:surrealdb@0.4.0 --format wit -o wit/deps/seamlezz-surrealdb-0.4.0/package.wit
```

```bash
dagger check
```

## License

[Unlicense](LICENSE)
