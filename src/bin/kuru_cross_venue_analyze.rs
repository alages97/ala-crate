use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};

use wide_quoter::kuru_cross_venue_analysis::{
    analyze_kuru_cross_venue, format_kuru_cross_venue_text, KuruCrossVenueAnalysisConfig,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "kuru-cross-venue-analyze",
    about = "Join pinned Kuru proofs to causal publisher-time Hyperliquid/Lighter BBOs and measure gross basis"
)]
struct Args {
    /// Kuru shadow-prover NDJSON capture.
    #[arg(long)]
    kuru_input: PathBuf,

    /// Normalized live-md BBO NDJSON containing Hyperliquid and/or Lighter MON.
    #[arg(long)]
    bbo_input: PathBuf,

    /// Native symbol to select from the BBO stream.
    #[arg(long, default_value = "MON")]
    symbol: String,

    /// Maximum publisher age at Kuru proof completion, inclusive.
    #[arg(long, default_value_t = 500)]
    max_bbo_age_ms: u64,

    /// Gross entry-basis threshold to count, in basis points.
    #[arg(long, default_value = "5")]
    threshold_bps: String,

    /// Kuru base-token decimals.
    #[arg(long, default_value_t = 18)]
    base_decimals: u32,

    /// Kuru quote-token decimals.
    #[arg(long, default_value_t = 6)]
    quote_decimals: u32,

    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = KuruCrossVenueAnalysisConfig {
        symbol: args.symbol,
        max_bbo_age_ms: args.max_bbo_age_ms,
        threshold_bps: args.threshold_bps,
        base_decimals: args.base_decimals,
        quote_decimals: args.quote_decimals,
    };
    let report = analyze_kuru_cross_venue(&args.kuru_input, &args.bbo_input, &config)
        .with_context(|| {
            format!(
                "analyze Kuru {} against BBO {}",
                args.kuru_input.display(),
                args.bbo_input.display()
            )
        })?;
    match args.format {
        OutputFormat::Text => print!("{}", format_kuru_cross_venue_text(&report)),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }
    Ok(())
}
