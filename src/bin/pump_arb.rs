use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use serde::Deserialize;

use wide_quoter::backtest::{
    fetch_pump_historical_data, run_pump_arb, write_pump_arb_report, write_pump_dataset,
    PumpArbConfig, PumpDataConfig, PumpDataInterval,
};

#[derive(Debug, Parser)]
#[command(
    name = "pump-arb",
    about = "Fetch live PUMP history and run a candle-based cross-venue basis screen"
)]
struct Args {
    /// Versioned data, cost, and strategy configuration.
    #[arg(long, default_value = "config/pump-arb.toml")]
    config: PathBuf,

    /// Override the configured output directory.
    #[arg(long)]
    output_directory: Option<PathBuf>,

    /// Override the configured candle interval.
    #[arg(long, value_enum)]
    interval: Option<IntervalArg>,

    /// Override the configured number of bars (maximum 5,000).
    #[arg(long)]
    bars: Option<u32>,

    /// Override the delay between GeckoTerminal pages (milliseconds).
    #[arg(long)]
    gecko_page_delay_ms: Option<u64>,

    /// Fetch and normalize history without running the simulator.
    #[arg(long)]
    fetch_only: bool,

    /// Parse and validate configuration without making network requests.
    #[arg(long)]
    validate_only: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum IntervalArg {
    #[value(name = "1m")]
    OneMinute,
    #[value(name = "4h")]
    FourHours,
}

impl From<IntervalArg> for PumpDataInterval {
    fn from(value: IntervalArg) -> Self {
        match value {
            IntervalArg::OneMinute => Self::OneMinute,
            IntervalArg::FourHours => Self::FourHours,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    data: PumpDataConfig,
    strategy: PumpArbConfig,
    output: OutputConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputConfig {
    directory: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let text = fs::read_to_string(&args.config)
        .with_context(|| format!("read configuration {}", args.config.display()))?;
    let mut config: FileConfig = toml::from_str(&text)
        .with_context(|| format!("parse configuration {}", args.config.display()))?;
    if let Some(interval) = args.interval {
        config.data.interval = interval.into();
    }
    if let Some(bars) = args.bars {
        config.data.bars = bars;
    }
    if let Some(delay_ms) = args.gecko_page_delay_ms {
        config.data.gecko_page_delay_ms = delay_ms;
    }
    config
        .data
        .validate()
        .context("validate PUMP data config")?;
    config
        .strategy
        .validate()
        .context("validate PUMP strategy config")?;

    if args.validate_only {
        println!("configuration is valid: {}", args.config.display());
        return Ok(());
    }

    let config_directory = args.config.parent().unwrap_or_else(|| Path::new("."));
    let output_directory = args
        .output_directory
        .unwrap_or_else(|| resolve_config_path(config_directory, &config.output.directory));
    println!(
        "fetching {} completed {} bars for canonical PUMP across PumpSwap, Hyperliquid UPUMP/PUMP, and Lighter PUMP",
        config.data.bars, config.data.interval
    );
    let dataset = fetch_pump_historical_data(&config.data)
        .await
        .context("fetch and align live PUMP history")?;
    let data_paths = write_pump_dataset(&dataset, &output_directory)
        .with_context(|| format!("write normalized data to {}", output_directory.display()))?;

    println!(
        "aligned {} contiguous bars: {}..{} (UPUMP @188 treated as 1:1 PUMP)",
        dataset.frames.len(),
        dataset.manifest.aligned_start_ms,
        dataset.manifest.aligned_end_ms_exclusive,
    );
    println!("normalized: {}", data_paths.normalized_csv.display());
    println!("sources:    {}", data_paths.source_manifest_json.display());
    if args.fetch_only {
        return Ok(());
    }

    let result = run_pump_arb(&config.strategy, &dataset.frames)
        .context("run PUMP cross-venue basis screen")?;
    let report_paths = write_pump_arb_report(&result, &output_directory)
        .with_context(|| format!("write reports to {}", output_directory.display()))?;
    println!(
        "completed {} trades: net_pnl={} final_equity={} max_signal_net_edge_bps={}",
        result.summary.trades,
        result.summary.net_pnl,
        result.summary.final_equity,
        result.summary.maximum_signal_net_edge_bps,
    );
    println!("summary:    {}", report_paths.summary_json.display());
    println!("equity:     {}", report_paths.equity_csv.display());
    println!("trades:     {}", report_paths.trades_csv.display());
    println!(
        "warning: candle closes and next opens are a persistent-basis screen, not synchronized executable bid/ask proof"
    );
    Ok(())
}

fn resolve_config_path(config_directory: &Path, configured_path: &Path) -> PathBuf {
    if configured_path.is_absolute() {
        configured_path.to_owned()
    } else {
        config_directory.join(configured_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_relative_output_from_config_directory() {
        assert_eq!(
            resolve_config_path(Path::new("config"), Path::new("../output/pump")),
            PathBuf::from("config/../output/pump")
        );
    }

    #[test]
    fn maps_cli_intervals_to_data_intervals() {
        assert_eq!(
            PumpDataInterval::from(IntervalArg::FourHours),
            PumpDataInterval::FourHours
        );
    }
}
