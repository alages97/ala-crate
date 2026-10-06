use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;

use wide_quoter::backtest::{
    load_bars_csv, write_report, BacktestConfig, BacktestEngine, BuiltInStrategyConfig,
    CrossSectionalMomentum,
};

#[derive(Debug, Parser)]
#[command(
    name = "backtest",
    about = "Run an offline, causality-safe strategy backtest"
)]
struct Args {
    /// Versioned backtest configuration.
    #[arg(long, default_value = "config/backtest-momentum.toml")]
    config: PathBuf,

    /// Override the configured output directory. Relative paths use the current directory.
    #[arg(long)]
    output_directory: Option<PathBuf>,

    /// Parse and validate configuration without reading data or writing output.
    #[arg(long)]
    validate_only: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = BacktestConfig::load(&args.config)
        .with_context(|| format!("load backtest config {}", args.config.display()))?;

    if args.validate_only {
        println!("configuration is valid: {}", args.config.display());
        return Ok(());
    }

    let config_directory = args.config.parent().unwrap_or_else(|| Path::new("."));
    let data_path = resolve_config_path(config_directory, &config.data.path);
    let output_directory = args
        .output_directory
        .unwrap_or_else(|| resolve_config_path(config_directory, &config.output.directory));

    let batches = load_bars_csv(
        &data_path,
        config.data.interval_ms,
        config.data.require_contiguous_batches,
    )
    .with_context(|| format!("load historical bars from {}", data_path.display()))?;
    let engine = BacktestEngine::new(config.engine.clone()).context("configure backtest engine")?;

    let result = match config.strategy {
        BuiltInStrategyConfig::Momentum(strategy_config) => {
            let mut strategy = CrossSectionalMomentum::new(strategy_config)
                .context("configure momentum strategy")?;
            engine.run(&mut strategy, &batches)?
        }
    };
    let paths = write_report(&result, &output_directory)
        .with_context(|| format!("write reports to {}", output_directory.display()))?;

    println!(
        "completed {} batches and {} trades: final_equity={} total_return={} close_to_close_max_drawdown={}",
        result.summary.batches,
        result.summary.trades,
        result.summary.final_equity,
        result.summary.total_return,
        result.summary.close_to_close_max_drawdown,
    );
    println!("summary: {}", paths.summary_json.display());
    println!("equity:  {}", paths.equity_csv.display());
    println!("trades:  {}", paths.trades_csv.display());

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
    fn resolves_configured_relative_paths_from_the_config_directory() {
        assert_eq!(
            resolve_config_path(Path::new("config"), Path::new("../data/bars.csv")),
            PathBuf::from("config/../data/bars.csv")
        );
    }

    #[test]
    fn leaves_absolute_paths_unchanged() {
        let absolute = if cfg!(windows) {
            PathBuf::from(r"C:\data\bars.csv")
        } else {
            PathBuf::from("/data/bars.csv")
        };
        assert_eq!(
            resolve_config_path(Path::new("config"), &absolute),
            absolute
        );
    }
}
