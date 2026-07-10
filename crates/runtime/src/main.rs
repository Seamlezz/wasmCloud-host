use std::{io::IsTerminal, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context as _;
use clap::{Parser, Subcommand};
use tracing::{Level, info};
use wash_runtime::{
    engine::{Engine, WasmProposal},
    host::{
        HostConfig,
        http::{DynamicRouter, HttpServer, TlsConfig},
    },
    observability::{self, Meters},
    plugin::{
        wasi_blobstore::NatsBlobstore, wasi_config::DynamicConfig, wasi_keyvalue::NatsKeyValue,
        wasi_logging::TracingLogger, wasi_otel::WasiOtel, wasmcloud_messaging::NatsMessaging,
    },
    washlet::{ClusterHostBuilder, NatsConnectionOptions, run_cluster_host},
};
use wasmcloud_plugin_surrealdb::WasmcloudSurrealdb;

#[derive(Debug, Parser)]
#[command(
    name = "wasmcloud-host",
    about = "wasmCloud cluster host with SurrealDB plugin"
)]
struct Cli {
    #[arg(short = 'l', long = "log-level", default_value_t = Level::INFO, global = true)]
    log_level: Level,

    #[arg(long, short = 'v', default_value_t = false, global = true)]
    verbose: bool,

    #[arg(
        long = "otel-debug",
        default_value_t = false,
        env = "WASMCLOUD_OTEL_DEBUG",
        global = true
    )]
    otel_debug: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the wasmCloud host
    Host(HostArgs),
}

#[derive(Debug, clap::Args)]
struct HostArgs {
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
        env = "REGISTRY_PULL_TIMEOUT",
    )]
    registry_pull_timeout: Duration,

    #[arg(
        long = "enable-fuel-meters",
        env = "WASMCLOUD_ENABLE_FUEL_METERS",
        default_value_t = false
    )]
    enable_fuel_meters: bool,

    #[arg(long = "oci-cache-dir", env = "OCI_CACHE_DIR")]
    oci_cache_dir: Option<PathBuf>,

    #[arg(long = "scheduler-nats-creds", env = "SCHEDULER_NATS_CREDENTIALS")]
    scheduler_nats_creds: Option<PathBuf>,

    #[arg(long = "data-nats-creds", env = "DATA_NATS_CREDENTIALS")]
    data_nats_creds: Option<PathBuf>,

    // Per-connection scheduler NATS TLS args
    #[arg(long = "scheduler-nats-tls-ca", env = "SCHEDULER_NATS_TLS_CA")]
    scheduler_nats_tls_ca: Option<PathBuf>,

    #[arg(long = "scheduler-nats-tls-cert", env = "SCHEDULER_NATS_TLS_CERT")]
    scheduler_nats_tls_cert: Option<PathBuf>,

    #[arg(long = "scheduler-nats-tls-key", env = "SCHEDULER_NATS_TLS_KEY")]
    scheduler_nats_tls_key: Option<PathBuf>,

    // Per-connection data NATS TLS args
    #[arg(long = "data-nats-tls-ca", env = "DATA_NATS_TLS_CA")]
    data_nats_tls_ca: Option<PathBuf>,

    #[arg(long = "data-nats-tls-cert", env = "DATA_NATS_TLS_CERT")]
    data_nats_tls_cert: Option<PathBuf>,

    #[arg(long = "data-nats-tls-key", env = "DATA_NATS_TLS_KEY")]
    data_nats_tls_key: Option<PathBuf>,

    // HTTP TLS args
    #[arg(long = "tls-cert-path", env = "TLS_CERT_PATH")]
    tls_cert_path: Option<PathBuf>,

    #[arg(long = "tls-key-path", env = "TLS_KEY_PATH")]
    tls_key_path: Option<PathBuf>,

    // Engine configuration
    #[arg(
        long = "wasm-proposal",
        env = "WASMCLOUD_WASM_PROPOSALS",
        value_delimiter = ',',
        num_args = 0..,
    )]
    wasm_proposals: Vec<WasmProposal>,

    #[arg(
        long = "max-instances",
        env = "WASMCLOUD_MAX_INSTANCES",
        default_value_t = 1000
    )]
    max_instances: u32,

    #[arg(
        long = "compilation-cache-size",
        env = "WASMCLOUD_COMPILATION_CACHE_SIZE",
        default_value_t = 100
    )]
    compilation_cache_size: u64,

    #[arg(
        long = "compilation-cache-ttl",
        env = "WASMCLOUD_COMPILATION_CACHE_TTL",
        value_parser = humantime::parse_duration,
        default_value = "600s"
    )]
    compilation_cache_ttl: Duration,
}

fn build_scheduler_nats_options(args: &HostArgs) -> NatsConnectionOptions {
    NatsConnectionOptions {
        tls_ca: args.scheduler_nats_tls_ca.clone(),
        tls_cert: args.scheduler_nats_tls_cert.clone(),
        tls_key: args.scheduler_nats_tls_key.clone(),
        ..Default::default()
    }
}

fn build_data_nats_options(args: &HostArgs) -> NatsConnectionOptions {
    NatsConnectionOptions {
        tls_ca: args.data_nats_tls_ca.clone(),
        tls_cert: args.data_nats_tls_cert.clone(),
        tls_key: args.data_nats_tls_key.clone(),
        ..Default::default()
    }
}

async fn connect_nats_with_creds(
    url: String,
    options: NatsConnectionOptions,
    creds: Option<PathBuf>,
) -> anyhow::Result<async_nats::Client> {
    let mut opts = async_nats::ConnectOptions::new();

    if let Some(timeout) = options.request_timeout {
        opts = opts.request_timeout(Some(timeout));
    }

    if let Some(ca_path) = options.tls_ca {
        opts = opts.add_root_certificates(ca_path);
    }

    if options.tls_first {
        opts = opts.tls_first();
    }

    if let (Some(cert_path), Some(key_path)) = (options.tls_cert, options.tls_key) {
        opts = opts.add_client_certificate(cert_path, key_path);
    }

    if let Some(creds_path) = creds {
        opts = opts
            .credentials_file(creds_path)
            .await
            .context("failed to load NATS credentials file")?;
    }

    opts.connect(url).await.context("failed to connect to NATS")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let Command::Host(args) = cli.command;

    let observability_log_level = if cli.otel_debug {
        Level::DEBUG
    } else {
        cli.log_level
    };
    let observability_verbose = cli.verbose || cli.otel_debug;

    let otel_shutdown = observability::initialize_observability(
        observability_log_level,
        std::io::stderr().is_terminal(),
        observability_verbose,
    )?;

    wash_runtime::init_crypto();

    let scheduler_nats_options = build_scheduler_nats_options(&args);
    let data_nats_options = build_data_nats_options(&args);

    let scheduler_nats = connect_nats_with_creds(
        args.scheduler_nats_url.clone(),
        scheduler_nats_options,
        args.scheduler_nats_creds.clone(),
    )
    .await
    .context("failed to connect to scheduler NATS")?;

    let data_nats = connect_nats_with_creds(
        args.data_nats_url.clone(),
        data_nats_options,
        args.data_nats_creds.clone(),
    )
    .await
    .context("failed to connect to data NATS")?;
    let data_nats = Arc::new(data_nats);

    let host_config = HostConfig {
        allow_oci_insecure: args.allow_insecure_registries,
        oci_pull_timeout: Some(args.registry_pull_timeout),
        oci_cache_dir: args.oci_cache_dir,
    };

    let enable_fuel_meters = args.enable_fuel_meters;

    let mut engine_builder = Engine::builder()
        .with_pooling_allocator(true)
        .with_fuel_consumption(enable_fuel_meters)
        .with_max_instances(args.max_instances)
        .with_compilation_cache(args.compilation_cache_size, args.compilation_cache_ttl);

    for proposal in &args.wasm_proposals {
        engine_builder = engine_builder.with_wasm_proposal(*proposal);
    }

    let engine = engine_builder.build().context("failed to build engine")?;

    let http = match (&args.tls_cert_path, &args.tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let tls_config = TlsConfig::new(cert_path, key_path);
            HttpServer::new_with_tls(DynamicRouter::default(), args.http_addr, tls_config)
                .await
                .context("failed to start HTTPS server")?
        }
        _ => HttpServer::new(DynamicRouter::default(), args.http_addr)
            .await
            .context("failed to start HTTP server")?,
    };

    let mut builder = ClusterHostBuilder::default()
        .with_engine(engine)
        .with_host_config(host_config)
        .with_nats_client(Arc::new(scheduler_nats))
        .with_host_group(args.host_group.clone())
        .with_meters(Meters::new(enable_fuel_meters))
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
    otel_shutdown();
    Ok(())
}
