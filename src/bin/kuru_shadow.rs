use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use wide_quoter::kuru_shadow::{run_kuru_shadow, KuruShadowConfig};
use wide_quoter::telemetry::{install_metrics_listener, install_tls_crypto_provider};

#[derive(Debug, Parser)]
#[command(
    name = "kuru-shadow",
    about = "Prove block-pinned Kuru/counterparty round-trip quotes without broadcasting"
)]
struct Args {
    /// TOML configuration file.
    #[arg(long, default_value = "config/kuru-monad-shadow.toml")]
    config: PathBuf,

    /// Override the config's create-new NDJSON output path.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Probe one current Monad block and exit.
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,wide_quoter::kuru_shadow=debug")),
        )
        .try_init()
        .map_err(|error| anyhow!("install tracing subscriber: {error}"))?;
    install_tls_crypto_provider()?;

    let config = KuruShadowConfig::from_path(&args.config)
        .with_context(|| format!("load Kuru shadow config {}", args.config.display()))?;
    if let Some(address) = config.metrics_bind {
        install_metrics_listener(address)?;
    }

    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });

    let summary = run_kuru_shadow(config, args.output, args.once, cancellation)
        .await
        .context("run Kuru/counterparty shadow prover")?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
