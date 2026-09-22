use std::{io::IsTerminal, path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use tracing::Level;
use wash::cli::{CliContextBuilder, host::HostCommand};
use wash_runtime::observability::{self, MeterKind};
use wasmcloud_plugin_surrealdb::WasmcloudSurrealdb;

#[derive(Parser)]
#[command(name = "wasmcloud-host", about = "wasmCloud host with SurrealDB")]
struct Cli {
    #[arg(long, short = 'l', default_value_t = Level::INFO, global = true)]
    log_level: Level,
    #[arg(long, short = 'v', global = true)]
    verbose: bool,
    #[arg(long, env = "WASMCLOUD_OTEL_DEBUG", global = true)]
    otel_debug: bool,
    #[arg(long = "user-config", global = true)]
    config: Option<PathBuf>,
    #[arg(long, default_value = "duration", global = true)]
    meters: MeterKind,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Host(Box<HostCommand>),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let shutdown = observability::initialize_observability(
        if cli.otel_debug {
            Level::DEBUG
        } else {
            cli.log_level
        },
        std::io::stderr().is_terminal(),
        cli.verbose || cli.otel_debug,
    )?;
    let mut context = CliContextBuilder::default()
        .non_interactive(true)
        .meters(cli.meters);
    if let Some(config) = cli.config {
        context = context.config(config);
    }
    let context = context.build().await?;
    let Command::Host(command) = cli.command;
    let result = command
        .handle_with_plugins(&context, vec![Arc::new(WasmcloudSurrealdb::new())])
        .await;
    shutdown();
    result.map(|_| ())
}
