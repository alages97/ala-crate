use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use wide_quoter::live_market_data::{run_live_market_data, LiveMarketDataConfig};
use wide_quoter::telemetry::{install_metrics_listener, install_tls_crypto_provider};

#[derive(Debug, Parser)]
#[command(
    name = "live-md",
    about = "Live public BBO collector for Binance, Bybit, Hyperliquid, and Lighter"
)]
struct Args {
    #[arg(long, default_value = "config/live-market-data.toml")]
    config: PathBuf,
    /// Validate configuration without opening listeners or exchange connections.
    #[arg(long)]
    validate_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = LiveMarketDataConfig::load(&args.config)
        .with_context(|| format!("load {}", args.config.display()))?;
    if args.validate_only {
        println!(
            "valid live market-data configuration: {}",
            args.config.display()
        );
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(config.log_filter.clone())
                .map_err(|error| anyhow!("invalid log_filter: {error}"))?,
        )
        .try_init()
        .map_err(|error| anyhow!("install tracing subscriber: {error}"))?;
    // Keep bridged dependency logs below TRACE. Raw WebSocket frame tracing is
    // noisy and can make production logs unexpectedly large.
    log::set_max_level(log::LevelFilter::Debug);
    install_tls_crypto_provider()?;
    install_metrics_listener(config.metrics_socket_address()?)?;

    let cancellation = CancellationToken::new();
    let service = run_live_market_data(config, cancellation.clone());
    tokio::pin!(service);
    tokio::select! {
        result = &mut service => result.context("live market-data service"),
        signal = shutdown_signal() => {
            signal?;
            tracing::info!("shutdown signal received");
            cancellation.cancel();
            service.await.context("shut down live market-data service")
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C")?,
        _ = terminate.recv() => {},
    }
    Ok(())
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")
}
