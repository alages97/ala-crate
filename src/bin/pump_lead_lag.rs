use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use wide_quoter::pump_lead_lag::{
    analyze_pump_capture, write_pump_lead_lag_report, PumpLeadLagConfig,
};

#[derive(Debug, Parser)]
#[command(
    name = "pump-lead-lag",
    about = "Analyze causal sub-second lead/lag in a raw PUMP capture"
)]
struct Args {
    /// Completed NDJSON file produced by pump-record.
    #[arg(long)]
    input: PathBuf,
    /// Directory for summary.json, lead_lag.csv, and common_shocks.csv.
    #[arg(long, default_value = "backtest-output/pump-lead-lag")]
    output_directory: PathBuf,
    /// Maximum age of every as-of quote endpoint.
    #[arg(long, default_value_t = 1_000)]
    max_quote_age_ms: u64,
    /// Ignore initiating mid-price moves smaller than this many basis points.
    #[arg(long, default_value_t = 0.0)]
    min_leader_move_bps: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = PumpLeadLagConfig {
        max_quote_age_ms: args.max_quote_age_ms,
        min_leader_move_bps: args.min_leader_move_bps,
        ..PumpLeadLagConfig::default()
    };
    let report = analyze_pump_capture(&args.input, &config)
        .with_context(|| format!("analyze PUMP capture {}", args.input.display()))?;
    let paths = write_pump_lead_lag_report(&report, &args.output_directory).with_context(|| {
        format!(
            "write PUMP lead/lag report to {}",
            args.output_directory.display()
        )
    })?;

    println!(
        "capture={} records={} decoded_mid_events={} markets={} metric_rows={}",
        report.capture_id,
        report.input_records,
        report.decoded_mid_events,
        report.markets.len(),
        report.metrics.len()
    );
    for market in &report.markets {
        println!(
            "market={} events={} price_changes={}",
            market.market, market.events, market.price_changes
        );
    }
    println!("summary={}", paths.summary_json.display());
    println!("metrics={}", paths.metrics_csv.display());
    println!("common_shocks={}", paths.common_shocks_csv.display());
    println!(
        "warning: results use public-feed arrival times and mid prices; they are not executable-fill proof"
    );
    Ok(())
}
