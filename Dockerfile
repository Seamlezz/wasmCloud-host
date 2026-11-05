FROM cgr.dev/chainguard/rust:latest-dev AS builder
USER 0
RUN apk add --no-cache protoc protobuf protobuf-dev
WORKDIR /workspace
ENV CARGO_HOME=/usr/local/cargo
COPY . .
RUN cargo build --release -p wasmcloud-host-runtime --bin wasmcloud-host

FROM cgr.dev/chainguard/wolfi-base
COPY --from=builder --chmod=755 /workspace/target/release/wasmcloud-host /usr/local/bin/wasmcloud-host
ENV RUST_LOG=info
ENV OTEL_SERVICE_NAME=wasmcloud-host
ENTRYPOINT ["/usr/local/bin/wasmcloud-host"]
