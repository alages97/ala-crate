//! Specialized event loop for testing PUMP spot/perpetual basis trades.
//!
//! The generic bar engine is intentionally instrument-neutral, but it models a
//! single linear-perpetual account and one global execution-cost schedule.  A
//! cross-venue spot/perp trade needs two simultaneous legs, venue-specific
//! costs, and an explicit prohibition on shorting unborrowed spot.  This module
//! keeps the same causal contract (completed-bar signal, next-open execution)
//! while modelling those details directly.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const BASIS_POINTS: u32 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PumpLeg {
    SolanaSpot,
    /// Hyperliquid `@188` / UPUMP, the 1:1 PUMP spot representation. Its
    /// observed market price is retained so temporary tracking error is real data.
    HyperliquidSpot,
    HyperliquidPerp,
    LighterPerp,
}

impl PumpLeg {
    pub const fn is_spot(self) -> bool {
        matches!(self, Self::SolanaSpot | Self::HyperliquidSpot)
    }

    pub const fn is_perp(self) -> bool {
        !self.is_spot()
    }
}

impl std::fmt::Display for PumpLeg {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::SolanaSpot => "solana_spot",
            Self::HyperliquidSpot => "hyperliquid_upump_spot",
            Self::HyperliquidPerp => "hyperliquid_perp",
            Self::LighterPerp => "lighter_perp",
        };
        formatter.write_str(label)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpBar {
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub open: Decimal,
    pub close: Decimal,
    pub volume_usd: Decimal,
    /// Effective funding cash rate for this bar. Positive means longs pay.
    /// Spot legs must leave this as `None`.
    pub funding_rate: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpFrame {
    pub index: usize,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub bars: BTreeMap<PumpLeg, PumpBar>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PumpLegCost {
    #[serde(with = "rust_decimal::serde::str")]
    pub taker_fee_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_bps: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PumpArbConfig {
    #[serde(with = "rust_decimal::serde::str")]
    pub initial_capital: Decimal,
    /// USD notional on the long leg. The short uses the same token quantity.
    #[serde(with = "rust_decimal::serde::str")]
    pub trade_notional_usd: Decimal,
    /// Common token-size increment for both legs. PUMP venues currently use one token.
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity_step: Decimal,
    /// Minimum signal basis remaining after the exit target and estimated
    /// round-trip fees/slippage.
    #[serde(with = "rust_decimal::serde::str")]
    pub min_net_entry_edge_bps: Decimal,
    /// Exit after a completed bar reaches this raw short-over-long basis.
    #[serde(with = "rust_decimal::serde::str")]
    pub exit_basis_bps: Decimal,
    pub max_hold_bars: usize,
    pub cooldown_bars: usize,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_bar_volume_usd: Decimal,
    pub allow_solana_spot: bool,
    pub allow_hyperliquid_spot: bool,
    pub allow_perp_perp: bool,
    pub costs: BTreeMap<PumpLeg, PumpLegCost>,
}

impl PumpArbConfig {
    pub fn validate(&self) -> Result<(), PumpArbError> {
        if self.initial_capital <= Decimal::ZERO {
            return invalid_config("initial_capital must be positive");
        }
        if self.trade_notional_usd <= Decimal::ZERO {
            return invalid_config("trade_notional_usd must be positive");
        }
        if self.quantity_step <= Decimal::ZERO {
            return invalid_config("quantity_step must be positive");
        }
        if self.trade_notional_usd > self.initial_capital {
            return invalid_config("trade_notional_usd cannot exceed initial_capital");
        }
        if self.min_net_entry_edge_bps < Decimal::ZERO {
            return invalid_config("min_net_entry_edge_bps must be non-negative");
        }
        if self.max_hold_bars == 0 {
            return invalid_config("max_hold_bars must be positive");
        }
        if self.min_bar_volume_usd < Decimal::ZERO {
            return invalid_config("min_bar_volume_usd must be non-negative");
        }
        if !self.allow_solana_spot && !self.allow_hyperliquid_spot && !self.allow_perp_perp {
            return invalid_config("at least one route family must be enabled");
        }
        for leg in [
            PumpLeg::SolanaSpot,
            PumpLeg::HyperliquidSpot,
            PumpLeg::HyperliquidPerp,
            PumpLeg::LighterPerp,
        ] {
            let cost = self.costs.get(&leg).ok_or_else(|| {
                PumpArbError::InvalidConfig(format!("missing execution costs for {leg}"))
            })?;
            for (name, value) in [
                ("taker_fee_bps", cost.taker_fee_bps),
                ("slippage_bps", cost.slippage_bps),
            ] {
                if value < Decimal::ZERO || value >= Decimal::from(BASIS_POINTS) {
                    return invalid_config(format!(
                        "{leg}.{name} must be non-negative and below 10,000"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct PumpRoute {
    pub long: PumpLeg,
    pub short: PumpLeg,
}

impl std::fmt::Display for PumpRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "long_{}__short_{}", self.long, self.short)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PumpExitReason {
    Convergence,
    MaxHold,
    EndOfData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PumpArbTrade {
    pub sequence: usize,
    pub route: PumpRoute,
    pub entry_signal_time_ms: u64,
    pub entry_fill_time_ms: u64,
    pub exit_signal_time_ms: u64,
    pub exit_fill_time_ms: u64,
    pub hold_bars: usize,
    pub exit_reason: PumpExitReason,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub signal_basis_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub signal_net_edge_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub entry_basis_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub exit_basis_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub gross_price_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fees: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_cost: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub net_pnl: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PumpArbEquityRecord {
    pub timestamp_ms: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PumpArbSummary {
    pub start_timestamp_ms: u64,
    pub end_timestamp_ms: u64,
    pub frames: usize,
    pub qualified_signals: usize,
    pub trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    #[serde(with = "rust_decimal::serde::str")]
    pub initial_capital: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub final_equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub net_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub gross_price_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fees: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_cost: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub total_return: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close_to_close_max_drawdown: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub maximum_signal_net_edge_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub average_hold_bars: Decimal,
    pub route_trade_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PumpArbResult {
    pub summary: PumpArbSummary,
    pub trades: Vec<PumpArbTrade>,
    pub equity_curve: Vec<PumpArbEquityRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PumpArbReportPaths {
    pub summary_json: PathBuf,
    pub trades_csv: PathBuf,
    pub equity_csv: PathBuf,
}

#[derive(Serialize)]
struct PumpArbTradeCsv {
    sequence: usize,
    route: String,
    long_leg: String,
    short_leg: String,
    entry_signal_time_ms: u64,
    entry_fill_time_ms: u64,
    exit_signal_time_ms: u64,
    exit_fill_time_ms: u64,
    hold_bars: usize,
    exit_reason: PumpExitReason,
    quantity: String,
    signal_basis_bps: String,
    signal_net_edge_bps: String,
    entry_basis_bps: String,
    exit_basis_bps: String,
    gross_price_pnl: String,
    funding_pnl: String,
    fees: String,
    slippage_cost: String,
    net_pnl: String,
}

impl From<&PumpArbTrade> for PumpArbTradeCsv {
    fn from(trade: &PumpArbTrade) -> Self {
        Self {
            sequence: trade.sequence,
            route: trade.route.to_string(),
            long_leg: trade.route.long.to_string(),
            short_leg: trade.route.short.to_string(),
            entry_signal_time_ms: trade.entry_signal_time_ms,
            entry_fill_time_ms: trade.entry_fill_time_ms,
            exit_signal_time_ms: trade.exit_signal_time_ms,
            exit_fill_time_ms: trade.exit_fill_time_ms,
            hold_bars: trade.hold_bars,
            exit_reason: trade.exit_reason,
            quantity: trade.quantity.to_string(),
            signal_basis_bps: trade.signal_basis_bps.to_string(),
            signal_net_edge_bps: trade.signal_net_edge_bps.to_string(),
            entry_basis_bps: trade.entry_basis_bps.to_string(),
            exit_basis_bps: trade.exit_basis_bps.to_string(),
            gross_price_pnl: trade.gross_price_pnl.to_string(),
            funding_pnl: trade.funding_pnl.to_string(),
            fees: trade.fees.to_string(),
            slippage_cost: trade.slippage_cost.to_string(),
            net_pnl: trade.net_pnl.to_string(),
        }
    }
}

#[derive(Debug, Error)]
pub enum PumpArbError {
    #[error("invalid PUMP arb configuration: {0}")]
    InvalidConfig(String),
    #[error("PUMP arb input contains no frames")]
    EmptyData,
    #[error("invalid PUMP frame {index}: {reason}")]
    InvalidFrame { index: usize, reason: String },
    #[error("missing {leg} bar needed to execute {action} in frame {index}")]
    MissingExecutionBar {
        index: usize,
        leg: PumpLeg,
        action: &'static str,
    },
    #[error(
        "trade_notional_usd is too small to buy one quantity step ({quantity_step}) at {price}"
    )]
    TradeBelowQuantityStep {
        quantity_step: Decimal,
        price: Decimal,
    },
    #[error("decimal arithmetic failed while {0}")]
    Arithmetic(&'static str),
    #[error("PUMP arb report I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize PUMP arb JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("serialize PUMP arb CSV: {0}")]
    Csv(#[from] csv::Error),
}

#[derive(Debug, Clone)]
struct EntrySignal {
    route: PumpRoute,
    signal_time_ms: u64,
    basis_bps: Decimal,
    net_edge_bps: Decimal,
}

#[derive(Debug, Clone)]
enum PendingAction {
    Enter(EntrySignal),
    Exit {
        signal_time_ms: u64,
        reason: PumpExitReason,
    },
}

#[derive(Debug, Clone)]
struct OpenPosition {
    route: PumpRoute,
    entry_signal_time_ms: u64,
    entry_fill_time_ms: u64,
    entry_frame_index: usize,
    quantity: Decimal,
    signal_basis_bps: Decimal,
    signal_net_edge_bps: Decimal,
    entry_long_reference: Decimal,
    entry_short_reference: Decimal,
    entry_long_fill: Decimal,
    entry_short_fill: Decimal,
    entry_fees: Decimal,
    slippage_cost: Decimal,
    funding_pnl: Decimal,
}

#[derive(Debug, Clone, Copy)]
enum FillSide {
    Buy,
    Sell,
}

pub fn run_pump_arb(
    config: &PumpArbConfig,
    frames: &[PumpFrame],
) -> Result<PumpArbResult, PumpArbError> {
    config.validate()?;
    validate_frames(frames)?;

    let mut pending = None;
    let mut position: Option<OpenPosition> = None;
    let mut realized_pnl = Decimal::ZERO;
    let mut trades = Vec::new();
    let mut equity_curve = Vec::with_capacity(frames.len());
    let mut qualified_signals = 0_usize;
    let mut maximum_signal_net_edge_bps: Option<Decimal> = None;
    let mut next_entry_index = 0_usize;

    for frame in frames {
        if let Some(action) = pending.take() {
            match action {
                PendingAction::Enter(signal) => {
                    debug_assert!(position.is_none());
                    position = Some(open_position(config, frame, signal)?);
                }
                PendingAction::Exit {
                    signal_time_ms,
                    reason,
                } => {
                    let open = position.take().expect("exit is queued only while open");
                    let trade = close_position(
                        config,
                        frame,
                        open,
                        signal_time_ms,
                        reason,
                        false,
                        trades.len() + 1,
                    )?;
                    realized_pnl = checked_add(realized_pnl, trade.net_pnl, "realizing trade PnL")?;
                    next_entry_index = frame
                        .index
                        .checked_add(config.cooldown_bars)
                        .ok_or(PumpArbError::Arithmetic("advancing cooldown"))?;
                    trades.push(trade);
                }
            }
        }

        if let Some(open) = position.as_mut() {
            settle_funding(open, frame)?;
        }

        let unrealized = match &position {
            Some(open) => mark_open_position(open, frame)?,
            None => Decimal::ZERO,
        };
        let equity = checked_add(
            config.initial_capital,
            checked_add(realized_pnl, unrealized, "marking cumulative PnL")?,
            "marking equity",
        )?;
        equity_curve.push(PumpArbEquityRecord {
            timestamp_ms: frame.close_time_ms,
            equity,
            realized_pnl,
            unrealized_pnl: unrealized,
        });

        if let Some(open) = &position {
            let basis = route_basis_at_close(open.route, frame)?;
            let hold_bars = frame
                .index
                .checked_sub(open.entry_frame_index)
                .and_then(|value| value.checked_add(1))
                .ok_or(PumpArbError::Arithmetic("calculating holding period"))?;
            let reason = if basis <= config.exit_basis_bps {
                Some(PumpExitReason::Convergence)
            } else if hold_bars >= config.max_hold_bars {
                Some(PumpExitReason::MaxHold)
            } else {
                None
            };
            if let Some(reason) = reason {
                pending = Some(PendingAction::Exit {
                    signal_time_ms: frame.close_time_ms,
                    reason,
                });
            }
        } else if frame.index >= next_entry_index {
            if let Some(signal) = best_entry_signal(config, frame)? {
                maximum_signal_net_edge_bps = Some(
                    maximum_signal_net_edge_bps.map_or(signal.net_edge_bps, |maximum| {
                        maximum.max(signal.net_edge_bps)
                    }),
                );
                if signal.net_edge_bps >= config.min_net_entry_edge_bps {
                    qualified_signals = qualified_signals
                        .checked_add(1)
                        .ok_or(PumpArbError::Arithmetic("counting qualified signals"))?;
                    pending = Some(PendingAction::Enter(signal));
                }
            }
        }
    }

    if let Some(open) = position.take() {
        let last = frames.last().expect("validated non-empty frames");
        let trade = close_position(
            config,
            last,
            open,
            last.close_time_ms,
            PumpExitReason::EndOfData,
            true,
            trades.len() + 1,
        )?;
        realized_pnl = checked_add(realized_pnl, trade.net_pnl, "realizing terminal PnL")?;
        trades.push(trade);
        if let Some(last_point) = equity_curve.last_mut() {
            last_point.equity = checked_add(
                config.initial_capital,
                realized_pnl,
                "marking terminal equity",
            )?;
            last_point.realized_pnl = realized_pnl;
            last_point.unrealized_pnl = Decimal::ZERO;
        }
    }

    let final_equity = checked_add(
        config.initial_capital,
        realized_pnl,
        "calculating final equity",
    )?;
    let total_return = final_equity
        .checked_sub(config.initial_capital)
        .ok_or(PumpArbError::Arithmetic(
            "calculating total return numerator",
        ))?
        .checked_div(config.initial_capital)
        .ok_or(PumpArbError::Arithmetic("calculating total return"))?;
    let winning_trades = trades
        .iter()
        .filter(|trade| trade.net_pnl > Decimal::ZERO)
        .count();
    let losing_trades = trades
        .iter()
        .filter(|trade| trade.net_pnl < Decimal::ZERO)
        .count();
    let gross_price_pnl = sum_trade_field(&trades, |trade| trade.gross_price_pnl)?;
    let funding_pnl = sum_trade_field(&trades, |trade| trade.funding_pnl)?;
    let fees = sum_trade_field(&trades, |trade| trade.fees)?;
    let slippage_cost = sum_trade_field(&trades, |trade| trade.slippage_cost)?;
    let average_hold_bars = if trades.is_empty() {
        Decimal::ZERO
    } else {
        let total = trades.iter().try_fold(0_u64, |sum, trade| {
            let hold = u64::try_from(trade.hold_bars)
                .map_err(|_| PumpArbError::Arithmetic("converting hold bars"))?;
            sum.checked_add(hold)
                .ok_or(PumpArbError::Arithmetic("summing hold bars"))
        })?;
        Decimal::from(total)
            .checked_div(Decimal::from(trades.len() as u64))
            .ok_or(PumpArbError::Arithmetic("averaging hold bars"))?
    };
    let mut route_trade_counts = BTreeMap::new();
    for trade in &trades {
        *route_trade_counts
            .entry(trade.route.to_string())
            .or_insert(0) += 1;
    }

    Ok(PumpArbResult {
        summary: PumpArbSummary {
            start_timestamp_ms: frames.first().expect("non-empty").open_time_ms,
            end_timestamp_ms: frames.last().expect("non-empty").close_time_ms,
            frames: frames.len(),
            qualified_signals,
            trades: trades.len(),
            winning_trades,
            losing_trades,
            initial_capital: config.initial_capital,
            final_equity,
            net_pnl: realized_pnl,
            gross_price_pnl,
            funding_pnl,
            fees,
            slippage_cost,
            total_return,
            close_to_close_max_drawdown: max_drawdown(&equity_curve)?,
            maximum_signal_net_edge_bps: maximum_signal_net_edge_bps.unwrap_or(Decimal::ZERO),
            average_hold_bars,
            route_trade_counts,
        },
        trades,
        equity_curve,
    })
}

pub fn write_pump_arb_report(
    result: &PumpArbResult,
    output_directory: impl AsRef<Path>,
) -> Result<PumpArbReportPaths, PumpArbError> {
    let output_directory = output_directory.as_ref();
    fs::create_dir_all(output_directory)?;
    let paths = PumpArbReportPaths {
        summary_json: output_directory.join("summary.json"),
        trades_csv: output_directory.join("trades.csv"),
        equity_csv: output_directory.join("equity.csv"),
    };

    let mut summary = BufWriter::new(File::create(&paths.summary_json)?);
    serde_json::to_writer_pretty(&mut summary, &result.summary)?;
    summary.write_all(b"\n")?;
    summary.flush()?;

    let mut trades = csv::Writer::from_path(&paths.trades_csv)?;
    if result.trades.is_empty() {
        trades.write_record([
            "sequence",
            "route",
            "long_leg",
            "short_leg",
            "entry_signal_time_ms",
            "entry_fill_time_ms",
            "exit_signal_time_ms",
            "exit_fill_time_ms",
            "hold_bars",
            "exit_reason",
            "quantity",
            "signal_basis_bps",
            "signal_net_edge_bps",
            "entry_basis_bps",
            "exit_basis_bps",
            "gross_price_pnl",
            "funding_pnl",
            "fees",
            "slippage_cost",
            "net_pnl",
        ])?;
    } else {
        for trade in &result.trades {
            trades.serialize(PumpArbTradeCsv::from(trade))?;
        }
    }
    trades.flush()?;

    let mut equity = csv::Writer::from_path(&paths.equity_csv)?;
    for point in &result.equity_curve {
        equity.serialize(point)?;
    }
    equity.flush()?;
    Ok(paths)
}

fn open_position(
    config: &PumpArbConfig,
    frame: &PumpFrame,
    signal: EntrySignal,
) -> Result<OpenPosition, PumpArbError> {
    let long_bar = execution_bar(frame, signal.route.long, "entry")?;
    let short_bar = execution_bar(frame, signal.route.short, "entry")?;
    let long_fill = slipped_price(config, signal.route.long, long_bar.open, FillSide::Buy)?;
    let short_fill = slipped_price(config, signal.route.short, short_bar.open, FillSide::Sell)?;
    let unrounded_quantity = config
        .trade_notional_usd
        .checked_div(long_fill)
        .ok_or(PumpArbError::Arithmetic("sizing entry quantity"))?;
    let quantity = unrounded_quantity
        .checked_div(config.quantity_step)
        .ok_or(PumpArbError::Arithmetic("rounding entry quantity"))?
        .floor()
        .checked_mul(config.quantity_step)
        .ok_or(PumpArbError::Arithmetic("rounding entry quantity"))?;
    if quantity <= Decimal::ZERO {
        return Err(PumpArbError::TradeBelowQuantityStep {
            quantity_step: config.quantity_step,
            price: long_fill,
        });
    }
    let long_notional = checked_mul(quantity, long_fill, "calculating entry long notional")?;
    let short_notional = checked_mul(quantity, short_fill, "calculating entry short notional")?;
    let entry_fees = checked_add(
        fee(config, signal.route.long, long_notional)?,
        fee(config, signal.route.short, short_notional)?,
        "adding entry fees",
    )?;
    let slippage_cost = checked_add(
        checked_mul(
            quantity,
            checked_sub(long_fill, long_bar.open, "entry long slippage")?,
            "costing entry long slippage",
        )?,
        checked_mul(
            quantity,
            checked_sub(short_bar.open, short_fill, "entry short slippage")?,
            "costing entry short slippage",
        )?,
        "adding entry slippage",
    )?;

    Ok(OpenPosition {
        route: signal.route,
        entry_signal_time_ms: signal.signal_time_ms,
        entry_fill_time_ms: frame.open_time_ms,
        entry_frame_index: frame.index,
        quantity,
        signal_basis_bps: signal.basis_bps,
        signal_net_edge_bps: signal.net_edge_bps,
        entry_long_reference: long_bar.open,
        entry_short_reference: short_bar.open,
        entry_long_fill: long_fill,
        entry_short_fill: short_fill,
        entry_fees,
        slippage_cost,
        funding_pnl: Decimal::ZERO,
    })
}

#[allow(clippy::too_many_arguments)]
fn close_position(
    config: &PumpArbConfig,
    frame: &PumpFrame,
    position: OpenPosition,
    signal_time_ms: u64,
    reason: PumpExitReason,
    at_close: bool,
    sequence: usize,
) -> Result<PumpArbTrade, PumpArbError> {
    let long_bar = execution_bar(frame, position.route.long, "exit")?;
    let short_bar = execution_bar(frame, position.route.short, "exit")?;
    let long_reference = if at_close {
        long_bar.close
    } else {
        long_bar.open
    };
    let short_reference = if at_close {
        short_bar.close
    } else {
        short_bar.open
    };
    let long_fill = slipped_price(config, position.route.long, long_reference, FillSide::Sell)?;
    let short_fill = slipped_price(config, position.route.short, short_reference, FillSide::Buy)?;
    let long_pnl = checked_mul(
        position.quantity,
        checked_sub(
            long_fill,
            position.entry_long_fill,
            "closing long price change",
        )?,
        "calculating long PnL",
    )?;
    let short_pnl = checked_mul(
        position.quantity,
        checked_sub(
            position.entry_short_fill,
            short_fill,
            "closing short price change",
        )?,
        "calculating short PnL",
    )?;
    let execution_price_pnl = checked_add(long_pnl, short_pnl, "adding leg PnL")?;
    let reference_long_pnl = checked_mul(
        position.quantity,
        checked_sub(
            long_reference,
            position.entry_long_reference,
            "closing reference long price change",
        )?,
        "calculating reference long PnL",
    )?;
    let reference_short_pnl = checked_mul(
        position.quantity,
        checked_sub(
            position.entry_short_reference,
            short_reference,
            "closing reference short price change",
        )?,
        "calculating reference short PnL",
    )?;
    let gross_price_pnl = checked_add(
        reference_long_pnl,
        reference_short_pnl,
        "adding reference leg PnL",
    )?;
    let exit_fees = checked_add(
        fee(
            config,
            position.route.long,
            checked_mul(position.quantity, long_fill, "exit long notional")?,
        )?,
        fee(
            config,
            position.route.short,
            checked_mul(position.quantity, short_fill, "exit short notional")?,
        )?,
        "adding exit fees",
    )?;
    let fees = checked_add(position.entry_fees, exit_fees, "adding round-trip fees")?;
    let exit_slippage = checked_add(
        checked_mul(
            position.quantity,
            checked_sub(long_reference, long_fill, "exit long slippage")?,
            "costing exit long slippage",
        )?,
        checked_mul(
            position.quantity,
            checked_sub(short_fill, short_reference, "exit short slippage")?,
            "costing exit short slippage",
        )?,
        "adding exit slippage",
    )?;
    let slippage_cost = checked_add(
        position.slippage_cost,
        exit_slippage,
        "adding round-trip slippage",
    )?;
    let pnl_after_fees = checked_sub(
        checked_add(
            gross_price_pnl,
            position.funding_pnl,
            "adding funding to price PnL",
        )?,
        fees,
        "deducting fees",
    )?;
    let net_pnl = checked_sub(pnl_after_fees, slippage_cost, "deducting slippage")?;
    debug_assert_eq!(
        execution_price_pnl,
        checked_sub(gross_price_pnl, slippage_cost, "checking execution PnL")?
    );
    let hold_bars = frame
        .index
        .checked_sub(position.entry_frame_index)
        .and_then(|value| value.checked_add(usize::from(at_close)))
        .ok_or(PumpArbError::Arithmetic("calculating closed hold bars"))?
        .max(1);

    Ok(PumpArbTrade {
        sequence,
        route: position.route,
        entry_signal_time_ms: position.entry_signal_time_ms,
        entry_fill_time_ms: position.entry_fill_time_ms,
        exit_signal_time_ms: signal_time_ms,
        exit_fill_time_ms: if at_close {
            frame.close_time_ms
        } else {
            frame.open_time_ms
        },
        hold_bars,
        exit_reason: reason,
        quantity: position.quantity,
        signal_basis_bps: position.signal_basis_bps,
        signal_net_edge_bps: position.signal_net_edge_bps,
        entry_basis_bps: basis_bps(
            position.entry_short_reference,
            position.entry_long_reference,
        )?,
        exit_basis_bps: basis_bps(short_reference, long_reference)?,
        gross_price_pnl,
        funding_pnl: position.funding_pnl,
        fees,
        slippage_cost,
        net_pnl,
    })
}

fn settle_funding(position: &mut OpenPosition, frame: &PumpFrame) -> Result<(), PumpArbError> {
    for (leg, direction) in [
        (position.route.long, -Decimal::ONE),
        (position.route.short, Decimal::ONE),
    ] {
        if !leg.is_perp() {
            continue;
        }
        let bar = execution_bar(frame, leg, "funding")?;
        let rate = bar.funding_rate.unwrap_or(Decimal::ZERO);
        let payment = checked_mul(
            checked_mul(position.quantity, bar.close, "funding notional")?,
            checked_mul(rate, direction, "funding direction")?,
            "funding payment",
        )?;
        position.funding_pnl = checked_add(
            position.funding_pnl,
            payment,
            "accumulating position funding",
        )?;
    }
    if position.funding_pnl.is_zero() {
        position.funding_pnl = Decimal::ZERO;
    }
    Ok(())
}

fn mark_open_position(position: &OpenPosition, frame: &PumpFrame) -> Result<Decimal, PumpArbError> {
    let long = execution_bar(frame, position.route.long, "mark")?;
    let short = execution_bar(frame, position.route.short, "mark")?;
    let long_pnl = checked_mul(
        position.quantity,
        checked_sub(long.close, position.entry_long_fill, "marking long")?,
        "marking long PnL",
    )?;
    let short_pnl = checked_mul(
        position.quantity,
        checked_sub(position.entry_short_fill, short.close, "marking short")?,
        "marking short PnL",
    )?;
    checked_sub(
        checked_add(
            checked_add(long_pnl, short_pnl, "marking price PnL")?,
            position.funding_pnl,
            "marking funding PnL",
        )?,
        position.entry_fees,
        "marking entry fees",
    )
}

fn best_entry_signal(
    config: &PumpArbConfig,
    frame: &PumpFrame,
) -> Result<Option<EntrySignal>, PumpArbError> {
    let mut longs = Vec::new();
    if config.allow_solana_spot {
        longs.push(PumpLeg::SolanaSpot);
    }
    if config.allow_hyperliquid_spot {
        longs.push(PumpLeg::HyperliquidSpot);
    }
    if config.allow_perp_perp {
        longs.extend([PumpLeg::HyperliquidPerp, PumpLeg::LighterPerp]);
    }

    let mut best: Option<EntrySignal> = None;
    for long in longs {
        for short in [PumpLeg::HyperliquidPerp, PumpLeg::LighterPerp] {
            if long == short || (long.is_perp() && !config.allow_perp_perp) {
                continue;
            }
            let Some(long_bar) = frame.bars.get(&long) else {
                continue;
            };
            let Some(short_bar) = frame.bars.get(&short) else {
                continue;
            };
            if long_bar.volume_usd < config.min_bar_volume_usd
                || short_bar.volume_usd < config.min_bar_volume_usd
            {
                continue;
            }
            let basis = basis_bps(short_bar.close, long_bar.close)?;
            let cost = estimated_round_trip_cost_bps(config, PumpRoute { long, short })?;
            let convergence =
                checked_sub(basis, config.exit_basis_bps, "estimating basis convergence")?;
            let net_edge = checked_sub(convergence, cost, "estimating net entry edge")?;
            let candidate = EntrySignal {
                route: PumpRoute { long, short },
                signal_time_ms: frame.close_time_ms,
                basis_bps: basis,
                net_edge_bps: net_edge,
            };
            if best.as_ref().is_none_or(|current| {
                candidate.net_edge_bps > current.net_edge_bps
                    || (candidate.net_edge_bps == current.net_edge_bps
                        && candidate.route < current.route)
            }) {
                best = Some(candidate);
            }
        }
    }
    Ok(best)
}

fn estimated_round_trip_cost_bps(
    config: &PumpArbConfig,
    route: PumpRoute,
) -> Result<Decimal, PumpArbError> {
    let long = config.costs[&route.long];
    let short = config.costs[&route.short];
    checked_mul(
        checked_add(
            checked_add(long.taker_fee_bps, long.slippage_bps, "long estimated cost")?,
            checked_add(
                short.taker_fee_bps,
                short.slippage_bps,
                "short estimated cost",
            )?,
            "one-way estimated cost",
        )?,
        Decimal::TWO,
        "round-trip estimated cost",
    )
}

fn route_basis_at_close(route: PumpRoute, frame: &PumpFrame) -> Result<Decimal, PumpArbError> {
    let long = execution_bar(frame, route.long, "basis mark")?;
    let short = execution_bar(frame, route.short, "basis mark")?;
    basis_bps(short.close, long.close)
}

fn basis_bps(short_price: Decimal, long_price: Decimal) -> Result<Decimal, PumpArbError> {
    checked_mul(
        short_price
            .checked_div(long_price)
            .ok_or(PumpArbError::Arithmetic("calculating basis ratio"))?
            .checked_sub(Decimal::ONE)
            .ok_or(PumpArbError::Arithmetic("calculating basis spread"))?,
        Decimal::from(BASIS_POINTS),
        "converting basis to bps",
    )
}

fn slipped_price(
    config: &PumpArbConfig,
    leg: PumpLeg,
    reference: Decimal,
    side: FillSide,
) -> Result<Decimal, PumpArbError> {
    let fraction = config.costs[&leg]
        .slippage_bps
        .checked_div(Decimal::from(BASIS_POINTS))
        .ok_or(PumpArbError::Arithmetic("calculating slippage fraction"))?;
    let multiplier = match side {
        FillSide::Buy => checked_add(Decimal::ONE, fraction, "buy slippage multiplier")?,
        FillSide::Sell => checked_sub(Decimal::ONE, fraction, "sell slippage multiplier")?,
    };
    checked_mul(reference, multiplier, "applying slippage")
}

fn fee(config: &PumpArbConfig, leg: PumpLeg, notional: Decimal) -> Result<Decimal, PumpArbError> {
    let fraction = config.costs[&leg]
        .taker_fee_bps
        .checked_div(Decimal::from(BASIS_POINTS))
        .ok_or(PumpArbError::Arithmetic("calculating fee fraction"))?;
    checked_mul(notional.abs(), fraction, "calculating fee")
}

fn execution_bar<'a>(
    frame: &'a PumpFrame,
    leg: PumpLeg,
    action: &'static str,
) -> Result<&'a PumpBar, PumpArbError> {
    frame
        .bars
        .get(&leg)
        .ok_or(PumpArbError::MissingExecutionBar {
            index: frame.index,
            leg,
            action,
        })
}

fn validate_frames(frames: &[PumpFrame]) -> Result<(), PumpArbError> {
    if frames.is_empty() {
        return Err(PumpArbError::EmptyData);
    }
    for (offset, frame) in frames.iter().enumerate() {
        if frame.open_time_ms >= frame.close_time_ms {
            return invalid_frame(frame, "open time must precede close time");
        }
        if let Some(previous) = offset.checked_sub(1).map(|index| &frames[index]) {
            let expected = previous
                .index
                .checked_add(1)
                .ok_or(PumpArbError::Arithmetic("validating frame index"))?;
            if frame.index != expected {
                return invalid_frame(frame, "indices must be consecutive");
            }
            if frame.open_time_ms != previous.close_time_ms {
                return invalid_frame(frame, "frames must be contiguous");
            }
        }
        for (leg, bar) in &frame.bars {
            if bar.open_time_ms != frame.open_time_ms || bar.close_time_ms != frame.close_time_ms {
                return invalid_frame(frame, format!("{leg} interval does not match frame"));
            }
            if bar.open <= Decimal::ZERO || bar.close <= Decimal::ZERO {
                return invalid_frame(frame, format!("{leg} prices must be positive"));
            }
            if bar.volume_usd < Decimal::ZERO {
                return invalid_frame(frame, format!("{leg} volume must be non-negative"));
            }
            if leg.is_spot() && bar.funding_rate.is_some() {
                return invalid_frame(frame, format!("{leg} spot bar cannot carry funding"));
            }
        }
    }
    Ok(())
}

fn max_drawdown(equity_curve: &[PumpArbEquityRecord]) -> Result<Decimal, PumpArbError> {
    let mut peak = Decimal::ZERO;
    let mut maximum = Decimal::ZERO;
    for point in equity_curve {
        peak = peak.max(point.equity);
        if peak > Decimal::ZERO {
            let drawdown = checked_sub(peak, point.equity, "drawdown numerator")?
                .checked_div(peak)
                .ok_or(PumpArbError::Arithmetic("calculating drawdown"))?;
            maximum = maximum.max(drawdown);
        }
    }
    Ok(maximum)
}

fn sum_trade_field(
    trades: &[PumpArbTrade],
    field: impl Fn(&PumpArbTrade) -> Decimal,
) -> Result<Decimal, PumpArbError> {
    trades.iter().try_fold(Decimal::ZERO, |sum, trade| {
        checked_add(sum, field(trade), "summing trade field")
    })
}

fn invalid_config<T>(message: impl Into<String>) -> Result<T, PumpArbError> {
    Err(PumpArbError::InvalidConfig(message.into()))
}

fn invalid_frame<T>(frame: &PumpFrame, reason: impl Into<String>) -> Result<T, PumpArbError> {
    Err(PumpArbError::InvalidFrame {
        index: frame.index,
        reason: reason.into(),
    })
}

fn checked_add(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, PumpArbError> {
    left.checked_add(right)
        .ok_or(PumpArbError::Arithmetic(context))
}

fn checked_sub(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, PumpArbError> {
    left.checked_sub(right)
        .ok_or(PumpArbError::Arithmetic(context))
}

fn checked_mul(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, PumpArbError> {
    left.checked_mul(right)
        .ok_or(PumpArbError::Arithmetic(context))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(value: &str) -> Decimal {
        value.parse().expect("valid decimal")
    }

    fn config() -> PumpArbConfig {
        let mut costs = BTreeMap::new();
        for leg in [
            PumpLeg::SolanaSpot,
            PumpLeg::HyperliquidSpot,
            PumpLeg::HyperliquidPerp,
            PumpLeg::LighterPerp,
        ] {
            costs.insert(
                leg,
                PumpLegCost {
                    taker_fee_bps: Decimal::ZERO,
                    slippage_bps: Decimal::ZERO,
                },
            );
        }
        PumpArbConfig {
            initial_capital: d("10000"),
            trade_notional_usd: d("1000"),
            quantity_step: Decimal::ONE,
            min_net_entry_edge_bps: d("10"),
            exit_basis_bps: d("1"),
            max_hold_bars: 10,
            cooldown_bars: 0,
            min_bar_volume_usd: d("100"),
            allow_solana_spot: true,
            allow_hyperliquid_spot: true,
            allow_perp_perp: true,
            costs,
        }
    }

    fn frame(index: usize, prices: [(PumpLeg, &str, &str); 4]) -> PumpFrame {
        let open_time_ms = index as u64 * 60_000;
        let close_time_ms = open_time_ms + 60_000;
        let bars = prices
            .into_iter()
            .map(|(leg, open, close)| {
                (
                    leg,
                    PumpBar {
                        open_time_ms,
                        close_time_ms,
                        open: d(open),
                        close: d(close),
                        volume_usd: d("100000"),
                        funding_rate: leg.is_perp().then_some(Decimal::ZERO),
                    },
                )
            })
            .collect();
        PumpFrame {
            index,
            open_time_ms,
            close_time_ms,
            bars,
        }
    }

    fn prices(
        spot: &'static str,
        hl: &'static str,
        lighter: &'static str,
    ) -> [(PumpLeg, &'static str, &'static str); 4] {
        [
            (PumpLeg::SolanaSpot, spot, spot),
            (PumpLeg::HyperliquidSpot, spot, spot),
            (PumpLeg::HyperliquidPerp, hl, hl),
            (PumpLeg::LighterPerp, lighter, lighter),
        ]
    }

    #[test]
    fn signal_uses_close_and_entry_uses_only_next_open() {
        let frames = vec![
            frame(0, prices("1", "1.02", "1.01")),
            frame(
                1,
                [
                    (PumpLeg::SolanaSpot, "1.10", "1.10"),
                    (PumpLeg::HyperliquidSpot, "1.10", "1.10"),
                    (PumpLeg::HyperliquidPerp, "1.11", "1.11"),
                    (PumpLeg::LighterPerp, "1.10", "1.10"),
                ],
            ),
        ];
        let result = run_pump_arb(&config(), &frames).expect("run");
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].entry_fill_time_ms, 60_000);
        assert_eq!(
            result.trades[0].entry_basis_bps,
            d("90.90909090909090909090909100")
        );
    }

    #[test]
    fn spot_is_never_selected_as_the_short_leg() {
        let frames = vec![
            frame(
                0,
                [
                    (PumpLeg::SolanaSpot, "1.20", "1.20"),
                    (PumpLeg::HyperliquidSpot, "1.30", "1.30"),
                    (PumpLeg::HyperliquidPerp, "1", "1"),
                    (PumpLeg::LighterPerp, "1.02", "1.02"),
                ],
            ),
            frame(1, prices("1", "1", "1")),
        ];
        let result = run_pump_arb(&config(), &frames).expect("run");
        assert!(result
            .trades
            .iter()
            .all(|trade| trade.route.short.is_perp()));
    }

    #[test]
    fn route_selection_uses_net_edge_after_venue_costs() {
        let mut configured = config();
        configured
            .costs
            .get_mut(&PumpLeg::SolanaSpot)
            .expect("cost")
            .taker_fee_bps = d("100");
        let frames = vec![
            frame(
                0,
                [
                    (PumpLeg::SolanaSpot, "1", "1"),
                    (PumpLeg::HyperliquidSpot, "1.005", "1.005"),
                    (PumpLeg::HyperliquidPerp, "1.02", "1.02"),
                    (PumpLeg::LighterPerp, "1.01", "1.01"),
                ],
            ),
            frame(1, prices("1", "1", "1")),
        ];
        let result = run_pump_arb(&configured, &frames).expect("run");
        assert_eq!(result.trades[0].route.long, PumpLeg::HyperliquidSpot);
        assert_eq!(result.trades[0].route.short, PumpLeg::HyperliquidPerp);
    }

    #[test]
    fn fees_and_slippage_can_turn_raw_convergence_into_a_loss() {
        let mut configured = config();
        configured.min_net_entry_edge_bps = Decimal::ZERO;
        for cost in configured.costs.values_mut() {
            cost.taker_fee_bps = d("5");
            cost.slippage_bps = d("5");
        }
        let frames = vec![
            frame(0, prices("1", "1.005", "1.002")),
            frame(1, prices("1", "1", "1")),
        ];
        let result = run_pump_arb(&configured, &frames).expect("run");
        assert!(result.trades[0].gross_price_pnl >= Decimal::ZERO);
        assert!(result.trades[0].slippage_cost > Decimal::ZERO);
        assert!(result.trades[0].fees > Decimal::ZERO);
        assert!(result.trades[0].net_pnl < Decimal::ZERO);
    }

    #[test]
    fn positive_funding_is_received_by_the_short_perp() {
        let mut frames = vec![
            frame(0, prices("1", "1.02", "1.01")),
            frame(1, prices("1", "1.02", "1.01")),
        ];
        frames[1]
            .bars
            .get_mut(&PumpLeg::HyperliquidPerp)
            .expect("bar")
            .funding_rate = Some(d("0.001"));
        let result = run_pump_arb(&config(), &frames).expect("run");
        assert!(result.trades[0].funding_pnl > Decimal::ZERO);
    }

    #[test]
    fn missing_execution_leg_fails_closed() {
        let mut frames = vec![
            frame(0, prices("1", "1.02", "1.01")),
            frame(1, prices("1", "1", "1")),
        ];
        frames[1].bars.remove(&PumpLeg::HyperliquidPerp);
        let error = run_pump_arb(&config(), &frames).expect_err("missing fill leg");
        assert!(matches!(error, PumpArbError::MissingExecutionBar { .. }));
    }

    #[test]
    fn entry_quantity_is_rounded_down_to_the_common_lot() {
        let mut configured = config();
        configured.trade_notional_usd = d("1000.9");
        configured.quantity_step = d("1");
        let frames = vec![
            frame(0, prices("3", "3.06", "3.03")),
            frame(1, prices("3", "3", "3")),
        ];

        let result = run_pump_arb(&configured, &frames).expect("run");

        assert_eq!(result.trades[0].quantity, d("333"));
    }

    #[test]
    fn trade_pnl_separates_reference_move_from_slippage_once() {
        let mut configured = config();
        configured.min_net_entry_edge_bps = Decimal::ZERO;
        for cost in configured.costs.values_mut() {
            cost.taker_fee_bps = d("4");
            cost.slippage_bps = d("3");
        }
        let frames = vec![
            frame(0, prices("1", "1.01", "1.005")),
            frame(1, prices("1", "1", "1")),
        ];

        let result = run_pump_arb(&configured, &frames).expect("run");
        let trade = &result.trades[0];
        assert_eq!(
            trade.net_pnl,
            trade.gross_price_pnl + trade.funding_pnl - trade.fees - trade.slippage_cost
        );
    }

    #[test]
    fn non_consecutive_frame_indices_are_rejected() {
        let frames = vec![
            frame(0, prices("1", "1.02", "1.01")),
            frame(2, prices("1", "1", "1")),
        ];

        let error = run_pump_arb(&config(), &frames).expect_err("index gap");

        assert!(matches!(error, PumpArbError::InvalidFrame { .. }));
    }

    #[test]
    fn report_flattens_the_two_leg_route_into_csv_columns() {
        let frames = vec![
            frame(0, prices("1", "1.02", "1.01")),
            frame(1, prices("1", "1", "1")),
        ];
        let result = run_pump_arb(&config(), &frames).expect("run");
        let directory = tempfile::tempdir().expect("tempdir");

        let paths = write_pump_arb_report(&result, directory.path()).expect("report");
        let csv = fs::read_to_string(paths.trades_csv).expect("read trades");

        assert!(csv.starts_with("sequence,route,long_leg,short_leg,"));
        assert!(csv.contains("hyperliquid_perp"));
    }

    #[test]
    fn signal_edge_reserves_the_configured_exit_basis() {
        let mut configured = config();
        configured.min_net_entry_edge_bps = Decimal::ZERO;
        configured.exit_basis_bps = d("25");
        for cost in configured.costs.values_mut() {
            cost.taker_fee_bps = d("7.5");
        }
        let frames = vec![
            frame(0, prices("1", "1.005", "1")),
            frame(1, prices("1", "1", "1")),
        ];

        let result = run_pump_arb(&configured, &frames).expect("run");

        assert!(result.trades.is_empty());
        assert!(result.summary.maximum_signal_net_edge_bps < Decimal::ZERO);
    }
}
