use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::types::{Side, Venue};

use super::types::PortfolioSnapshot;

/// Aggregate, accounting-reconciled outcome of one deterministic run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BacktestSummary {
    pub strategy: String,
    pub start_timestamp_ms: u64,
    pub end_timestamp_ms: u64,
    pub batches: usize,
    pub trades: usize,
    #[serde(with = "rust_decimal::serde::str")]
    pub initial_collateral: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub final_collateral: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub final_equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub total_return: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close_to_close_max_drawdown: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fees: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_pnl: Decimal,
    /// Informational attribution versus the unadjusted execution price.
    /// Slippage is already embedded in realized/unrealized PnL and must not be
    /// deducted from equity a second time.
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_cost: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EquityRecord {
    pub timestamp_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub collateral: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub gross_exposure: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub net_exposure: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub cumulative_fees: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub cumulative_funding_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub cumulative_slippage_cost: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeReason {
    Rebalance,
    EndLiquidation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradeRecord {
    pub sequence: u64,
    pub decision_batch_index: usize,
    pub fill_batch_index: usize,
    pub decision_timestamp_ms: u64,
    pub fill_timestamp_ms: u64,
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reference_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fill_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fee: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_cost: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub post_trade_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub post_trade_average_entry_price: Decimal,
    pub reason: TradeReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BacktestResult {
    pub summary: BacktestSummary,
    pub equity_curve: Vec<EquityRecord>,
    pub trades: Vec<TradeRecord>,
    pub final_portfolio: PortfolioSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportPaths {
    pub summary_json: PathBuf,
    pub equity_csv: PathBuf,
    pub trades_csv: PathBuf,
}

#[derive(Debug, Error)]
pub enum ReportError {
    #[error("report I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize report JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("serialize report CSV: {0}")]
    Csv(#[from] csv::Error),
}

/// Write the compact JSON summary plus deterministic equity and trade ledgers.
/// Existing files with the same names are replaced.
pub fn write_report(
    result: &BacktestResult,
    output_directory: impl AsRef<Path>,
) -> Result<ReportPaths, ReportError> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;

    let paths = ReportPaths {
        summary_json: output_directory.join("summary.json"),
        equity_csv: output_directory.join("equity.csv"),
        trades_csv: output_directory.join("trades.csv"),
    };

    let mut summary = BufWriter::new(File::create(&paths.summary_json)?);
    serde_json::to_writer_pretty(&mut summary, &result.summary)?;
    summary.write_all(b"\n")?;
    summary.flush()?;

    let mut equity = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&paths.equity_csv)?;
    equity.write_record([
        "timestamp_ms",
        "collateral",
        "equity",
        "gross_exposure",
        "net_exposure",
        "realized_pnl",
        "unrealized_pnl",
        "cumulative_fees",
        "cumulative_funding_pnl",
        "cumulative_slippage_cost",
    ])?;
    for record in &result.equity_curve {
        equity.serialize(record)?;
    }
    equity.flush()?;

    let mut trades = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&paths.trades_csv)?;
    trades.write_record([
        "sequence",
        "decision_batch_index",
        "fill_batch_index",
        "decision_timestamp_ms",
        "fill_timestamp_ms",
        "venue",
        "symbol",
        "side",
        "quantity",
        "reference_price",
        "fill_price",
        "notional",
        "fee",
        "slippage_cost",
        "realized_pnl",
        "post_trade_quantity",
        "post_trade_average_entry_price",
        "reason",
    ])?;
    for record in &result.trades {
        trades.serialize(record)?;
    }
    trades.flush()?;

    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("valid decimal")
    }

    #[test]
    fn writer_emits_all_three_artifacts() {
        let result = BacktestResult {
            summary: BacktestSummary {
                strategy: "test".to_owned(),
                start_timestamp_ms: 1,
                end_timestamp_ms: 2,
                batches: 2,
                trades: 0,
                initial_collateral: decimal("1000"),
                final_collateral: decimal("1000"),
                final_equity: decimal("1000"),
                total_return: Decimal::ZERO,
                close_to_close_max_drawdown: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                fees: Decimal::ZERO,
                funding_pnl: Decimal::ZERO,
                slippage_cost: Decimal::ZERO,
            },
            equity_curve: vec![EquityRecord {
                timestamp_ms: 2,
                collateral: decimal("1000"),
                equity: decimal("1000"),
                gross_exposure: Decimal::ZERO,
                net_exposure: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                cumulative_fees: Decimal::ZERO,
                cumulative_funding_pnl: Decimal::ZERO,
                cumulative_slippage_cost: Decimal::ZERO,
            }],
            trades: Vec::new(),
            final_portfolio: PortfolioSnapshot {
                timestamp_ms: 2,
                collateral: decimal("1000"),
                equity: decimal("1000"),
                gross_exposure: Decimal::ZERO,
                net_exposure: Decimal::ZERO,
                positions: Vec::new(),
            },
        };

        let directory = tempfile::tempdir().expect("temporary directory");
        let paths = write_report(&result, directory.path()).expect("write report");
        assert!(paths.summary_json.is_file());
        assert!(paths.equity_csv.is_file());
        assert!(paths.trades_csv.is_file());

        let json = std::fs::read_to_string(paths.summary_json).expect("read summary");
        assert!(json.contains("\"strategy\": \"test\""));
        let equity = std::fs::read_to_string(paths.equity_csv).expect("read equity");
        assert!(equity.starts_with("timestamp_ms,collateral,equity"));
        let trades = std::fs::read_to_string(paths.trades_csv).expect("read trades");
        assert!(trades.starts_with("sequence,decision_batch_index,fill_batch_index"));
    }
}
