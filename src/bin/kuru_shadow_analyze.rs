use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};

use wide_quoter::kuru_shadow_analysis::{
    analyze_kuru_shadow_capture, format_kuru_shadow_analysis_text, KuruShadowAnalysisConfig,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "kuru-shadow-analyze",
    about = "Summarize edge, proof validity, persistence, and latency in a Kuru shadow capture"
)]
struct Args {
    /// Kuru shadow NDJSON capture; complete and in-progress files are accepted.
    #[arg(long)]
    input: PathBuf,

    /// Re-screen all recorded edges at this gross-bps threshold.
    #[arg(long)]
    threshold_bps: Option<String>,

    /// Human-readable table or complete machine-readable report.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let report = analyze_kuru_shadow_capture(
        &args.input,
        &KuruShadowAnalysisConfig {
            threshold_bps: args.threshold_bps,
        },
    )
    .with_context(|| format!("analyze Kuru shadow capture {}", args.input.display()))?;
    match args.format {
        OutputFormat::Text => print!("{}", format_kuru_shadow_analysis_text(&report)),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }
    Ok(())
}
