use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use wide_quoter::pump_recorder::{run_pump_recorder, PumpRecorderConfig};
use wide_quoter::telemetry::install_tls_crypto_provider;

#[derive(Debug, Parser)]
#[command(
    name = "pump-record",
    about = "Record raw live PUMP books, trades, and PumpSwap state for lead/lag research"
)]
struct Args {
    /// Requested final NDJSON path. Incomplete runs retain a .part artifact and manifest.
    #[arg(long)]
    output: PathBuf,
    /// Timed capture length after every required source becomes ready.
    #[arg(long, default_value_t = 3_600)]
    duration_seconds: u64,
    /// Maximum time to wait for all live sources before failing.
    #[arg(long, default_value_t = 90)]
    warmup_timeout_seconds: u64,
    #[arg(long, default_value_t = 1_000)]
    flush_interval_ms: u64,
    #[arg(long, default_value_t = 65_536)]
    channel_capacity: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,wide_quoter::pump_recorder=debug")),
        )
        .try_init()
        .map_err(|error| anyhow!("install tracing subscriber: {error}"))?;
    install_tls_crypto_provider()?;

    let config = PumpRecorderConfig {
        output_path: args.output,
        duration: Duration::from_secs(args.duration_seconds),
        warmup_timeout: Duration::from_secs(args.warmup_timeout_seconds),
        flush_interval: Duration::from_millis(args.flush_interval_ms),
        channel_capacity: args.channel_capacity,
    };
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });

    let summary = run_pump_recorder(config, cancellation)
        .await
        .context("record PUMP market data")?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
