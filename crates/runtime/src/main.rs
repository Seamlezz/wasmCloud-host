use std::{io::IsTerminal, path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use tracing::Level;
use wash::cli::{CliContextBuilder, host::HostCommand};
use wash_runtime::observability::{self, MeterKind};
use wasmcloud_plugin_surrealdb::{WORKER_STACK_SIZE, WasmcloudSurrealdb, runtime_builder};

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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let runtime = runtime_builder().build()?;
    // `block_on` polls the host on the calling thread, so that thread gets the
    // worker stack SurrealDB needs too, whatever the main thread was given.
    let host = std::thread::Builder::new()
        .name("main".to_string())
        .stack_size(WORKER_STACK_SIZE)
        .spawn(move || runtime.block_on(run(cli)))?;
    host.join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
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
