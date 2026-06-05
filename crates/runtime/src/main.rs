use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context as _;
use clap::Parser;
use tracing::info;
use wash_runtime::{
    engine::Engine,
    host::{
        HostConfig,
        http::{DynamicRouter, HttpServer},
    },
    plugin::{
        wasi_blobstore::NatsBlobstore, wasi_config::DynamicConfig, wasi_keyvalue::NatsKeyValue,
        wasi_logging::TracingLogger, wasi_otel::WasiOtel, wasmcloud_messaging::NatsMessaging,
    },
    washlet::{ClusterHostBuilder, NatsConnectionOptions, connect_nats, run_cluster_host},
};
use wasmcloud_plugin_surrealdb::WasmcloudSurrealdb;

#[derive(Debug, Parser)]
#[command(
    name = "wasmcloud-host",
    about = "wasmCloud cluster host with SurrealDB plugin"
)]
struct Args {
    #[arg(long = "host-group", default_value = "default", env = "HOST_GROUP")]
    host_group: String,

    #[arg(
        long = "scheduler-nats-url",
        default_value = "nats://127.0.0.1:4222",
        env = "SCHEDULER_NATS_URL"
    )]
    scheduler_nats_url: String,

    #[arg(
        long = "data-nats-url",
        default_value = "nats://127.0.0.1:4222",
        env = "DATA_NATS_URL"
    )]
    data_nats_url: String,

    #[arg(long = "host-name", env = "HOST_NAME")]
    host_name: Option<String>,

    #[arg(long = "environment", env = "WASMCLOUD_HOST_ENVIRONMENT")]
    environment: Option<String>,

    #[arg(long = "http-addr", default_value = "0.0.0.0:8080", env = "HTTP_ADDR")]
    http_addr: SocketAddr,

    #[arg(
        long = "allow-insecure-registries",
        default_value_t = false,
        env = "ALLOW_INSECURE_REGISTRIES"
    )]
    allow_insecure_registries: bool,

    #[arg(
        long = "registry-pull-timeout",
        value_parser = humantime::parse_duration,
        default_value = "30s",
        env = "REGISTRY_PULL_TIMEOUT"
    )]
    registry_pull_timeout: Duration,

    #[arg(long = "oci-cache-dir", env = "OCI_CACHE_DIR")]
    oci_cache_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    wash_runtime::init_crypto();

    let args = Args::parse();

    let scheduler_nats = connect_nats(
        args.scheduler_nats_url.clone(),
        NatsConnectionOptions::default(),
    )
    .await
    .context("failed to connect to scheduler NATS")?;

    let data_nats = connect_nats(args.data_nats_url.clone(), NatsConnectionOptions::default())
        .await
        .context("failed to connect to data NATS")?;
    let data_nats = Arc::new(data_nats);

    let host_config = HostConfig {
        allow_oci_insecure: args.allow_insecure_registries,
        oci_pull_timeout: Some(args.registry_pull_timeout),
        oci_cache_dir: args.oci_cache_dir,
    };

    let engine = Engine::builder()
        .with_pooling_allocator(true)
        .with_wasip3(true)
        .build()
        .context("failed to build engine")?;

    let http = HttpServer::new(DynamicRouter::default(), args.http_addr)
        .await
        .context("failed to start HTTP server")?;

    let mut builder = ClusterHostBuilder::default()
        .with_engine(engine)
        .with_host_config(host_config)
        .with_nats_client(Arc::new(scheduler_nats))
        .with_host_group(args.host_group.clone())
        .with_plugin(Arc::new(
            DynamicConfig::builder().copy_environment(true).build(),
        ))?
        .with_plugin(Arc::new(TracingLogger::default()))?
        .with_plugin(Arc::new(WasiOtel::default()))?
        .with_plugin(Arc::new(NatsBlobstore::new(&data_nats)))?
        .with_plugin(Arc::new(NatsMessaging::new(data_nats.clone())))?
        .with_plugin(Arc::new(NatsKeyValue::new(&data_nats)))?
        .with_plugin(Arc::new(WasmcloudSurrealdb::new()))?
        .with_http_handler(Arc::new(http));

    if let Some(name) = args.host_name {
        builder = builder.with_host_name(name);
    }
    if let Some(environment) = args.environment {
        builder = builder.with_environment(environment);
    }

    let cluster_host = builder.build().context("failed to build cluster host")?;

    info!(
        host_group = %args.host_group,
        http = %args.http_addr,
        "wasmcloud-host started (washlet)"
    );

    let shutdown = run_cluster_host(cluster_host)
        .await
        .context("failed to start cluster host")?;

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;
        tokio::select! {
            res = tokio::signal::ctrl_c() => res.context("failed to listen for SIGINT")?,
            _ = sigterm.recv() => info!("Received SIGTERM, shutting down..."),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for shutdown signal")?;
    }

    info!("Stopping host...");
    shutdown.await?;
    info!("Host stopped");
    Ok(())
}
