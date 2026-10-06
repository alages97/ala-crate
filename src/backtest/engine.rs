use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::Side;

use super::report::{BacktestResult, BacktestSummary, EquityRecord, TradeReason, TradeRecord};
use super::types::{
    Bar, BarBatch, MarketId, PortfolioSnapshot, PositionSnapshot, Strategy, StrategyContext,
    StrategyDecision, StrategyError, TargetBook,
};

const BASIS_POINTS: u32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    #[serde(with = "rust_decimal::serde::str")]
    pub initial_collateral: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub fee_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub slippage_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_gross_leverage: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_abs_net_weight: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_asset_weight: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub min_order_notional: Decimal,
    pub apply_funding: bool,
    pub require_funding_for_open_positions: bool,
    pub liquidate_at_end: bool,
}

impl EngineConfig {
    pub fn validate(&self) -> Result<(), BacktestError> {
        if self.initial_collateral <= Decimal::ZERO {
            return Err(BacktestError::InvalidConfig(
                "initial_collateral must be positive".to_owned(),
            ));
        }
        if self.fee_bps < Decimal::ZERO || self.fee_bps >= Decimal::from(BASIS_POINTS) {
            return Err(BacktestError::InvalidConfig(
                "fee_bps must be non-negative and less than 10,000".to_owned(),
            ));
        }
        if self.slippage_bps < Decimal::ZERO || self.slippage_bps >= Decimal::from(BASIS_POINTS) {
            return Err(BacktestError::InvalidConfig(
                "slippage_bps must be non-negative and less than 10,000".to_owned(),
            ));
        }
        if self.max_gross_leverage <= Decimal::ZERO {
            return Err(BacktestError::InvalidConfig(
                "max_gross_leverage must be positive".to_owned(),
            ));
        }
        if self.max_abs_net_weight < Decimal::ZERO
            || self.max_abs_net_weight > self.max_gross_leverage
        {
            return Err(BacktestError::InvalidConfig(
                "max_abs_net_weight must be non-negative and no greater than max_gross_leverage"
                    .to_owned(),
            ));
        }
        if self.max_asset_weight <= Decimal::ZERO || self.max_asset_weight > self.max_gross_leverage
        {
            return Err(BacktestError::InvalidConfig(
                "max_asset_weight must be positive and no greater than max_gross_leverage"
                    .to_owned(),
            ));
        }
        if self.min_order_notional < Decimal::ZERO {
            return Err(BacktestError::InvalidConfig(
                "min_order_notional must be non-negative".to_owned(),
            ));
        }
        if self.require_funding_for_open_positions && !self.apply_funding {
            return Err(BacktestError::InvalidConfig(
                "require_funding_for_open_positions requires apply_funding=true".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BacktestError {
    #[error("invalid engine configuration: {0}")]
    InvalidConfig(String),
    #[error("the backtest has no bar batches")]
    EmptyData,
    #[error("invalid batch {batch_index}: {reason}")]
    InvalidBatch { batch_index: usize, reason: String },
    #[error("strategy failed: {0}")]
    Strategy(#[from] StrategyError),
    #[error("invalid target book at timestamp {timestamp_ms}: {reason}")]
    InvalidTarget { timestamp_ms: u64, reason: String },
    #[error("projected open portfolio violates risk in batch {batch_index}: {reason}")]
    ProjectedRisk { batch_index: usize, reason: String },
    #[error(
        "target for {market:?}, decided in batch {decision_batch_index}, has no next-open bar in batch {fill_batch_index}"
    )]
    MissingNextOpenBar {
        market: MarketId,
        decision_batch_index: usize,
        fill_batch_index: usize,
    },
    #[error("open position {market:?} has no bar in batch {batch_index}")]
    MissingOpenPositionBar {
        market: MarketId,
        batch_index: usize,
    },
    #[error("funding is missing for open position {market:?} in batch {batch_index}")]
    MissingFunding {
        market: MarketId,
        batch_index: usize,
    },
    #[error(
        "batch {decision_batch_index} data became available at {decision_timestamp_ms}, after batch {fill_batch_index} opened at {open_timestamp_ms}"
    )]
    DecisionAfterNextOpen {
        decision_batch_index: usize,
        fill_batch_index: usize,
        decision_timestamp_ms: u64,
        open_timestamp_ms: u64,
    },
    #[error("decimal arithmetic failed while {0}")]
    Arithmetic(&'static str),
    #[error("equity is non-positive at timestamp {timestamp_ms}: {equity}")]
    NonPositiveEquity { timestamp_ms: u64, equity: Decimal },
    #[error("accounting invariant failed: expected equity {expected}, observed {observed}")]
    AccountingInvariant {
        expected: Decimal,
        observed: Decimal,
    },
}

#[derive(Debug, Clone)]
pub struct BacktestEngine {
    config: EngineConfig,
}

impl BacktestEngine {
    pub fn new(config: EngineConfig) -> Result<Self, BacktestError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub const fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn run<S: Strategy>(
        &self,
        strategy: &mut S,
        batches: &[BarBatch],
    ) -> Result<BacktestResult, BacktestError> {
        if batches.is_empty() {
            return Err(BacktestError::EmptyData);
        }

        // Strategies always receive markets in canonical order, even if a
        // caller constructed batches without going through the CSV loader.
        let mut batches = batches.to_vec();
        for batch in &mut batches {
            batch
                .bars
                .sort_by(|left, right| left.market.cmp(&right.market));
        }
        validate_batches(&batches)?;

        let strategy_name = strategy.name().to_owned();
        let mut account = Account::new(self.config.initial_collateral);
        let mut pending: Option<PendingTargets> = None;
        let mut equity_curve = Vec::with_capacity(batches.len());
        let mut trades = Vec::new();
        let mut next_trade_sequence = 1_u64;

        for batch in &batches {
            let bars = bars_by_market(batch);
            require_bars_for_open_positions(&account, &bars, batch.index)?;

            // The open is the first causal price available for this execution
            // phase. Revalue the old book before sizing or applying any order:
            // a gap can invalidate both account solvency and yesterday's
            // otherwise-valid target weights.
            account.mark_positions_at_open(&bars)?;
            let open_portfolio = account.snapshot(batch.open_time_ms)?;
            require_positive_equity(&open_portfolio)?;

            if let Some(targets) = pending.take() {
                if targets.decision_timestamp_ms > batch.open_time_ms {
                    return Err(BacktestError::DecisionAfterNextOpen {
                        decision_batch_index: targets.decision_batch_index,
                        fill_batch_index: batch.index,
                        decision_timestamp_ms: targets.decision_timestamp_ms,
                        open_timestamp_ms: batch.open_time_ms,
                    });
                }
                execute_targets(
                    &self.config,
                    &mut account,
                    &targets,
                    batch,
                    &bars,
                    open_portfolio.equity,
                    &mut trades,
                    &mut next_trade_sequence,
                )?;
            } else {
                validate_projected_exposure(
                    &self.config,
                    &account
                        .positions
                        .iter()
                        .map(|(market, position)| (market.clone(), position.quantity))
                        .collect(),
                    &bars,
                    open_portfolio.equity,
                    batch.index,
                )?;
            }

            // New positions initially carry their slipped fill as an entry
            // mark. Restore the unbiased open before the post-fill solvency
            // check, then advance to the completed close for strategy input.
            account.mark_positions_at_open(&bars)?;
            let post_fill_open = account.snapshot(batch.open_time_ms)?;
            require_positive_equity(&post_fill_open)?;
            account.mark_positions_at_close(&bars)?;
            if self.config.apply_funding {
                account.settle_funding(
                    &bars,
                    batch.index,
                    self.config.require_funding_for_open_positions,
                )?;
            }

            let portfolio = account.snapshot(batch.available_at_ms)?;
            require_positive_equity(&portfolio)?;
            equity_curve.push(account.equity_record(&portfolio)?);

            match strategy.on_bar_close(StrategyContext {
                batch,
                portfolio: &portfolio,
            })? {
                StrategyDecision::Hold => {}
                StrategyDecision::Rebalance(book) => {
                    validate_target_book(&self.config, &book, batch)?;
                    pending = Some(PendingTargets {
                        book,
                        decision_batch_index: batch.index,
                        decision_timestamp_ms: batch.available_at_ms,
                        // Quantity rules are instrument metadata known with
                        // the decision batch. Reading them from the fill bar
                        // would leak a field whose row is available only at
                        // that later bar's close.
                        quantity_steps: batch
                            .bars
                            .iter()
                            .filter_map(|bar| {
                                bar.quantity_step.map(|step| (bar.market.clone(), step))
                            })
                            .collect(),
                    });
                }
            }
        }

        let last_batch = batches.last().expect("non-empty checked above");
        if self.config.liquidate_at_end && !account.positions.is_empty() {
            let bars = bars_by_market(last_batch);
            liquidate_at_end(
                &self.config,
                &mut account,
                last_batch,
                &bars,
                &mut trades,
                &mut next_trade_sequence,
            )?;
            let final_snapshot = account.snapshot(last_batch.available_at_ms)?;
            require_positive_equity(&final_snapshot)?;
            if let Some(last) = equity_curve.last_mut() {
                *last = account.equity_record(&final_snapshot)?;
            }
        }

        let final_portfolio = account.snapshot(last_batch.available_at_ms)?;
        require_positive_equity(&final_portfolio)?;
        let unrealized_pnl = sum_unrealized(&final_portfolio)?;
        account.reconcile(final_portfolio.equity, unrealized_pnl)?;

        let total_return = checked_sub(
            final_portfolio.equity,
            self.config.initial_collateral,
            "calculating total return numerator",
        )?
        .checked_div(self.config.initial_collateral)
        .ok_or(BacktestError::Arithmetic("calculating total return"))?;
        let close_to_close_max_drawdown = calculate_max_drawdown(&equity_curve)?;

        let summary = BacktestSummary {
            strategy: strategy_name,
            start_timestamp_ms: batches
                .first()
                .expect("non-empty checked above")
                .available_at_ms,
            end_timestamp_ms: last_batch.available_at_ms,
            batches: batches.len(),
            trades: trades.len(),
            initial_collateral: self.config.initial_collateral,
            final_collateral: final_portfolio.collateral,
            final_equity: final_portfolio.equity,
            total_return,
            close_to_close_max_drawdown,
            realized_pnl: account.realized_pnl,
            unrealized_pnl,
            fees: account.fees,
            funding_pnl: account.funding_pnl,
            slippage_cost: account.slippage_cost,
        };

        Ok(BacktestResult {
            summary,
            equity_curve,
            trades,
            final_portfolio,
        })
    }
}

#[derive(Debug, Clone)]
struct PendingTargets {
    book: TargetBook,
    decision_batch_index: usize,
    decision_timestamp_ms: u64,
    quantity_steps: BTreeMap<MarketId, Decimal>,
}

#[derive(Debug, Clone)]
struct Position {
    quantity: Decimal,
    average_entry_price: Decimal,
    mark_price: Decimal,
}

#[derive(Debug)]
struct Account {
    initial_collateral: Decimal,
    collateral: Decimal,
    positions: BTreeMap<MarketId, Position>,
    realized_pnl: Decimal,
    fees: Decimal,
    funding_pnl: Decimal,
    slippage_cost: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct FillOutcome {
    realized_pnl: Decimal,
    post_quantity: Decimal,
    post_average_entry_price: Decimal,
}

impl Account {
    fn new(initial_collateral: Decimal) -> Self {
        Self {
            initial_collateral,
            collateral: initial_collateral,
            positions: BTreeMap::new(),
            realized_pnl: Decimal::ZERO,
            fees: Decimal::ZERO,
            funding_pnl: Decimal::ZERO,
            slippage_cost: Decimal::ZERO,
        }
    }

    fn apply_fill(
        &mut self,
        market: &MarketId,
        quantity_delta: Decimal,
        fill_price: Decimal,
        fee: Decimal,
        slippage_cost: Decimal,
    ) -> Result<FillOutcome, BacktestError> {
        if quantity_delta == Decimal::ZERO {
            return Ok(FillOutcome {
                realized_pnl: Decimal::ZERO,
                post_quantity: self
                    .positions
                    .get(market)
                    .map_or(Decimal::ZERO, |position| position.quantity),
                post_average_entry_price: self
                    .positions
                    .get(market)
                    .map_or(Decimal::ZERO, |position| position.average_entry_price),
            });
        }

        let current = self.positions.get(market).cloned().unwrap_or(Position {
            quantity: Decimal::ZERO,
            average_entry_price: Decimal::ZERO,
            mark_price: fill_price,
        });
        let new_quantity = checked_add(
            current.quantity,
            quantity_delta,
            "updating position quantity",
        )?;
        let same_direction = (current.quantity > Decimal::ZERO && quantity_delta > Decimal::ZERO)
            || (current.quantity < Decimal::ZERO && quantity_delta < Decimal::ZERO);

        let (new_average, realized_pnl) = if current.quantity == Decimal::ZERO {
            (fill_price, Decimal::ZERO)
        } else if same_direction {
            let old_cost = checked_mul(
                current.quantity.abs(),
                current.average_entry_price,
                "weighting existing entry price",
            )?;
            let added_cost = checked_mul(
                quantity_delta.abs(),
                fill_price,
                "weighting added entry price",
            )?;
            let total_cost = checked_add(old_cost, added_cost, "adding entry-price weights")?;
            let average = total_cost
                .checked_div(new_quantity.abs())
                .ok_or(BacktestError::Arithmetic("averaging entry price"))?;
            (average, Decimal::ZERO)
        } else {
            let closed_quantity = current.quantity.abs().min(quantity_delta.abs());
            let price_change = checked_sub(
                fill_price,
                current.average_entry_price,
                "calculating realized price change",
            )?;
            let direction = if current.quantity > Decimal::ZERO {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            let realized = checked_mul(
                checked_mul(price_change, closed_quantity, "calculating realized PnL")?,
                direction,
                "applying realized PnL direction",
            )?;
            let average = if new_quantity == Decimal::ZERO {
                Decimal::ZERO
            } else if (new_quantity > Decimal::ZERO) == (current.quantity > Decimal::ZERO) {
                current.average_entry_price
            } else {
                fill_price
            };
            (average, realized)
        };

        self.realized_pnl =
            checked_add(self.realized_pnl, realized_pnl, "accumulating realized PnL")?;
        self.fees = checked_add(self.fees, fee, "accumulating fees")?;
        self.slippage_cost = checked_add(
            self.slippage_cost,
            slippage_cost,
            "accumulating slippage cost",
        )?;
        self.collateral = checked_sub(
            checked_add(self.collateral, realized_pnl, "crediting realized PnL")?,
            fee,
            "deducting fee",
        )?;

        if new_quantity == Decimal::ZERO {
            self.positions.remove(market);
        } else {
            self.positions.insert(
                market.clone(),
                Position {
                    quantity: new_quantity,
                    average_entry_price: new_average,
                    mark_price: current.mark_price,
                },
            );
        }

        Ok(FillOutcome {
            realized_pnl,
            post_quantity: new_quantity,
            post_average_entry_price: new_average,
        })
    }

    fn mark_positions_at_open(
        &mut self,
        bars: &BTreeMap<MarketId, &Bar>,
    ) -> Result<(), BacktestError> {
        self.mark_positions_with(bars, |bar| bar.open)
    }

    fn mark_positions_at_close(
        &mut self,
        bars: &BTreeMap<MarketId, &Bar>,
    ) -> Result<(), BacktestError> {
        self.mark_positions_with(bars, Bar::valuation_price)
    }

    fn mark_positions_with(
        &mut self,
        bars: &BTreeMap<MarketId, &Bar>,
        price: impl Fn(&Bar) -> Decimal,
    ) -> Result<(), BacktestError> {
        for (market, position) in &mut self.positions {
            let bar = bars
                .get(market)
                .expect("open-position bar presence checked before marking");
            let mark = price(bar);
            if mark <= Decimal::ZERO {
                return Err(BacktestError::Arithmetic(
                    "marking a position at a non-positive price",
                ));
            }
            position.mark_price = mark;
        }
        Ok(())
    }

    fn settle_funding(
        &mut self,
        bars: &BTreeMap<MarketId, &Bar>,
        batch_index: usize,
        require_funding: bool,
    ) -> Result<(), BacktestError> {
        let mut net_payment = Decimal::ZERO;
        for (market, position) in &self.positions {
            let bar = bars
                .get(market)
                .expect("open-position bar presence checked before funding");
            let rate = match bar.funding_rate {
                Some(rate) => rate,
                None if require_funding => {
                    return Err(BacktestError::MissingFunding {
                        market: market.clone(),
                        batch_index,
                    });
                }
                None => Decimal::ZERO,
            };
            // Positive rate: positive (long) quantity pays, negative quantity
            // receives. Rate is already the aggregate rate for this bar.
            let signed_notional = checked_mul(
                position.quantity,
                position.mark_price,
                "calculating funding notional",
            )?;
            let payment = -checked_mul(signed_notional, rate, "calculating funding payment")?;
            net_payment = checked_add(net_payment, payment, "aggregating funding payments")?;
        }
        // rust_decimal preserves the sign bit on zero. Canonicalize it so
        // deterministic JSON/CSV reports never expose a misleading `-0` PnL.
        if net_payment.is_zero() {
            net_payment = Decimal::ZERO;
        }
        self.collateral = checked_add(self.collateral, net_payment, "settling funding")?;
        self.funding_pnl = checked_add(self.funding_pnl, net_payment, "accumulating funding PnL")?;
        if self.funding_pnl.is_zero() {
            self.funding_pnl = Decimal::ZERO;
        }
        Ok(())
    }

    fn snapshot(&self, timestamp_ms: u64) -> Result<PortfolioSnapshot, BacktestError> {
        let mut positions = Vec::with_capacity(self.positions.len());
        let mut unrealized_total = Decimal::ZERO;
        let mut gross_exposure = Decimal::ZERO;
        let mut net_exposure = Decimal::ZERO;

        for (market, position) in &self.positions {
            let price_change = checked_sub(
                position.mark_price,
                position.average_entry_price,
                "calculating unrealized price change",
            )?;
            let unrealized = checked_mul(
                price_change,
                position.quantity,
                "calculating unrealized PnL",
            )?;
            let notional = checked_mul(
                position.quantity,
                position.mark_price,
                "calculating position notional",
            )?;
            unrealized_total =
                checked_add(unrealized_total, unrealized, "aggregating unrealized PnL")?;
            gross_exposure =
                checked_add(gross_exposure, notional.abs(), "aggregating gross exposure")?;
            net_exposure = checked_add(net_exposure, notional, "aggregating net exposure")?;
            positions.push(PositionSnapshot {
                market: market.clone(),
                quantity: position.quantity,
                average_entry_price: position.average_entry_price,
                mark_price: position.mark_price,
                unrealized_pnl: unrealized,
                notional,
            });
        }

        let equity = checked_add(self.collateral, unrealized_total, "calculating equity")?;
        Ok(PortfolioSnapshot {
            timestamp_ms,
            collateral: self.collateral,
            equity,
            gross_exposure,
            net_exposure,
            positions,
        })
    }

    fn equity_record(&self, portfolio: &PortfolioSnapshot) -> Result<EquityRecord, BacktestError> {
        let unrealized_pnl = sum_unrealized(portfolio)?;
        Ok(EquityRecord {
            timestamp_ms: portfolio.timestamp_ms,
            collateral: portfolio.collateral,
            equity: portfolio.equity,
            gross_exposure: portfolio.gross_exposure,
            net_exposure: portfolio.net_exposure,
            realized_pnl: self.realized_pnl,
            unrealized_pnl,
            cumulative_fees: self.fees,
            cumulative_funding_pnl: self.funding_pnl,
            cumulative_slippage_cost: self.slippage_cost,
        })
    }

    fn reconcile(
        &self,
        observed_equity: Decimal,
        unrealized_pnl: Decimal,
    ) -> Result<(), BacktestError> {
        let expected = checked_add(
            checked_sub(
                checked_add(
                    checked_add(
                        self.initial_collateral,
                        self.realized_pnl,
                        "reconciling realized PnL",
                    )?,
                    self.funding_pnl,
                    "reconciling funding PnL",
                )?,
                self.fees,
                "reconciling fees",
            )?,
            unrealized_pnl,
            "reconciling unrealized PnL",
        )?;
        if expected != observed_equity {
            return Err(BacktestError::AccountingInvariant {
                expected,
                observed: observed_equity,
            });
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn execute_targets(
    config: &EngineConfig,
    account: &mut Account,
    pending: &PendingTargets,
    batch: &BarBatch,
    bars: &BTreeMap<MarketId, &Bar>,
    open_equity: Decimal,
    trades: &mut Vec<TradeRecord>,
    next_trade_sequence: &mut u64,
) -> Result<(), BacktestError> {
    let mut markets: BTreeSet<MarketId> = account.positions.keys().cloned().collect();
    markets.extend(pending.book.targets.keys().cloned());

    // Resolve the complete order set and its post-trade quantities before
    // mutating the account. Limits use the causally known, pre-trade open
    // equity and unadjusted open marks. Fees and adverse slippage remain real
    // PnL (and receive a separate post-fill solvency check) but do not silently
    // rescale the strategy's requested target weights.
    let mut orders = Vec::new();
    let mut projected_quantities = BTreeMap::new();
    for market in markets {
        let target_weight = pending
            .book
            .targets
            .get(&market)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let current_quantity = account
            .positions
            .get(&market)
            .map_or(Decimal::ZERO, |position| position.quantity);
        let bar = match bars.get(&market) {
            Some(bar) => *bar,
            None if target_weight == Decimal::ZERO && current_quantity == Decimal::ZERO => {
                continue;
            }
            None => {
                return Err(BacktestError::MissingNextOpenBar {
                    market,
                    decision_batch_index: pending.decision_batch_index,
                    fill_batch_index: batch.index,
                });
            }
        };

        let target_notional =
            checked_mul(target_weight, open_equity, "calculating target notional")?;
        let raw_target_quantity = target_notional
            .checked_div(bar.open)
            .ok_or(BacktestError::Arithmetic("calculating target quantity"))?;
        let target_quantity = round_toward_zero(
            raw_target_quantity,
            pending.quantity_steps.get(&market).copied(),
        )?;
        let quantity_delta = checked_sub(
            target_quantity,
            current_quantity,
            "calculating rebalance quantity",
        )?;
        if quantity_delta == Decimal::ZERO {
            if target_quantity != Decimal::ZERO {
                projected_quantities.insert(market, target_quantity);
            }
            continue;
        }

        let reference_notional =
            checked_mul(quantity_delta.abs(), bar.open, "calculating order notional")?;
        // A complete target book must always be able to close dust. The floor
        // applies only while the desired post-trade position is non-flat.
        if target_quantity != Decimal::ZERO && reference_notional < config.min_order_notional {
            if current_quantity != Decimal::ZERO {
                projected_quantities.insert(market, current_quantity);
            }
            continue;
        }
        if target_quantity != Decimal::ZERO {
            projected_quantities.insert(market.clone(), target_quantity);
        }
        orders.push(PlannedOrder {
            market,
            quantity_delta,
            reference_price: bar.open,
        });
    }

    validate_projected_exposure(
        config,
        &projected_quantities,
        bars,
        open_equity,
        batch.index,
    )?;

    for order in orders {
        execute_fill(
            config,
            account,
            &order.market,
            order.quantity_delta,
            order.reference_price,
            pending.decision_batch_index,
            batch.index,
            pending.decision_timestamp_ms,
            batch.open_time_ms,
            TradeReason::Rebalance,
            trades,
            next_trade_sequence,
        )?;
    }
    Ok(())
}

#[derive(Debug)]
struct PlannedOrder {
    market: MarketId,
    quantity_delta: Decimal,
    reference_price: Decimal,
}

fn validate_projected_exposure(
    config: &EngineConfig,
    quantities: &BTreeMap<MarketId, Decimal>,
    bars: &BTreeMap<MarketId, &Bar>,
    open_equity: Decimal,
    batch_index: usize,
) -> Result<(), BacktestError> {
    let maximum_name_notional = checked_mul(
        config.max_asset_weight,
        open_equity,
        "calculating projected per-name limit",
    )?;
    let maximum_gross_notional = checked_mul(
        config.max_gross_leverage,
        open_equity,
        "calculating projected gross limit",
    )?;
    let maximum_net_notional = checked_mul(
        config.max_abs_net_weight,
        open_equity,
        "calculating projected net limit",
    )?;
    let mut gross = Decimal::ZERO;
    let mut net = Decimal::ZERO;

    for (market, quantity) in quantities {
        if *quantity == Decimal::ZERO {
            continue;
        }
        let bar = bars
            .get(market)
            .ok_or_else(|| BacktestError::ProjectedRisk {
                batch_index,
                reason: format!("{}:{} has no open price", market.venue, market.symbol),
            })?;
        let notional = checked_mul(*quantity, bar.open, "projecting open notional")?;
        if notional.abs() > maximum_name_notional {
            return projected_risk(
                batch_index,
                format!(
                    "{}:{} notional {} exceeds per-name limit {}",
                    market.venue,
                    market.symbol,
                    notional.abs(),
                    maximum_name_notional
                ),
            );
        }
        gross = checked_add(gross, notional.abs(), "projecting gross exposure")?;
        net = checked_add(net, notional, "projecting net exposure")?;
    }

    if gross > maximum_gross_notional {
        return projected_risk(
            batch_index,
            format!("gross notional {gross} exceeds limit {maximum_gross_notional}"),
        );
    }
    if net.abs() > maximum_net_notional {
        return projected_risk(
            batch_index,
            format!(
                "absolute net notional {} exceeds limit {maximum_net_notional}",
                net.abs()
            ),
        );
    }
    Ok(())
}

fn projected_risk<T>(batch_index: usize, reason: String) -> Result<T, BacktestError> {
    Err(BacktestError::ProjectedRisk {
        batch_index,
        reason,
    })
}

fn liquidate_at_end(
    config: &EngineConfig,
    account: &mut Account,
    batch: &BarBatch,
    bars: &BTreeMap<MarketId, &Bar>,
    trades: &mut Vec<TradeRecord>,
    next_trade_sequence: &mut u64,
) -> Result<(), BacktestError> {
    let positions: Vec<(MarketId, Decimal)> = account
        .positions
        .iter()
        .map(|(market, position)| (market.clone(), position.quantity))
        .collect();
    for (market, quantity) in positions {
        let bar = bars
            .get(&market)
            .ok_or_else(|| BacktestError::MissingOpenPositionBar {
                market: market.clone(),
                batch_index: batch.index,
            })?;
        execute_fill(
            config,
            account,
            &market,
            -quantity,
            bar.close,
            batch.index,
            batch.index,
            // End liquidation is an exogenous run policy committed before
            // the final interval, not a strategy decision made after seeing
            // that interval. This makes the close fill causal even when the
            // completed research row is published after the close.
            batch.open_time_ms,
            batch.close_time_ms,
            TradeReason::EndLiquidation,
            trades,
            next_trade_sequence,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_fill(
    config: &EngineConfig,
    account: &mut Account,
    market: &MarketId,
    quantity_delta: Decimal,
    reference_price: Decimal,
    decision_batch_index: usize,
    fill_batch_index: usize,
    decision_timestamp_ms: u64,
    fill_timestamp_ms: u64,
    reason: TradeReason,
    trades: &mut Vec<TradeRecord>,
    next_trade_sequence: &mut u64,
) -> Result<(), BacktestError> {
    let side = if quantity_delta > Decimal::ZERO {
        Side::Buy
    } else {
        Side::Sell
    };
    let slippage_fraction = config
        .slippage_bps
        .checked_div(Decimal::from(BASIS_POINTS))
        .ok_or(BacktestError::Arithmetic("calculating slippage fraction"))?;
    let multiplier = match side {
        Side::Buy => checked_add(
            Decimal::ONE,
            slippage_fraction,
            "calculating buy slippage multiplier",
        )?,
        Side::Sell => checked_sub(
            Decimal::ONE,
            slippage_fraction,
            "calculating sell slippage multiplier",
        )?,
    };
    let fill_price = checked_mul(
        reference_price,
        multiplier,
        "calculating slipped fill price",
    )?;
    let notional = checked_mul(
        quantity_delta.abs(),
        fill_price,
        "calculating fill notional",
    )?;
    let fee_fraction = config
        .fee_bps
        .checked_div(Decimal::from(BASIS_POINTS))
        .ok_or(BacktestError::Arithmetic("calculating fee fraction"))?;
    let fee = checked_mul(notional, fee_fraction, "calculating fill fee")?;
    let slippage_per_unit = match side {
        Side::Buy => checked_sub(fill_price, reference_price, "calculating buy slippage")?,
        Side::Sell => checked_sub(reference_price, fill_price, "calculating sell slippage")?,
    };
    let slippage_cost = checked_mul(
        quantity_delta.abs(),
        slippage_per_unit,
        "calculating slippage cost",
    )?;
    let outcome = account.apply_fill(market, quantity_delta, fill_price, fee, slippage_cost)?;

    let sequence = *next_trade_sequence;
    *next_trade_sequence = next_trade_sequence
        .checked_add(1)
        .ok_or(BacktestError::Arithmetic("allocating trade sequence"))?;
    trades.push(TradeRecord {
        sequence,
        decision_batch_index,
        fill_batch_index,
        decision_timestamp_ms,
        fill_timestamp_ms,
        venue: market.venue,
        symbol: market.symbol.clone(),
        side,
        quantity: quantity_delta.abs(),
        reference_price,
        fill_price,
        notional,
        fee,
        slippage_cost,
        realized_pnl: outcome.realized_pnl,
        post_trade_quantity: outcome.post_quantity,
        post_trade_average_entry_price: outcome.post_average_entry_price,
        reason,
    });
    Ok(())
}

fn validate_target_book(
    config: &EngineConfig,
    book: &TargetBook,
    batch: &BarBatch,
) -> Result<(), BacktestError> {
    let available: BTreeSet<&MarketId> = batch.bars.iter().map(|bar| &bar.market).collect();
    let mut gross = Decimal::ZERO;
    let mut net = Decimal::ZERO;
    for (market, weight) in &book.targets {
        if market.symbol.trim().is_empty() {
            return invalid_target(batch, "market symbol must not be empty");
        }
        if *weight != Decimal::ZERO && !available.contains(market) {
            return invalid_target(
                batch,
                format!(
                    "non-zero target for {}:{} is not in the decision batch",
                    market.venue, market.symbol
                ),
            );
        }
        if weight.abs() > config.max_asset_weight {
            return invalid_target(
                batch,
                format!(
                    "target for {}:{} exceeds max_asset_weight",
                    market.venue, market.symbol
                ),
            );
        }
        gross = checked_add(gross, weight.abs(), "validating target gross leverage")?;
        net = checked_add(net, *weight, "validating target net weight")?;
    }
    if gross > config.max_gross_leverage {
        return invalid_target(batch, "target gross leverage exceeds max_gross_leverage");
    }
    if net.abs() > config.max_abs_net_weight {
        return invalid_target(
            batch,
            "absolute target net weight exceeds max_abs_net_weight",
        );
    }
    Ok(())
}

fn invalid_target<T>(batch: &BarBatch, reason: impl Into<String>) -> Result<T, BacktestError> {
    Err(BacktestError::InvalidTarget {
        timestamp_ms: batch.available_at_ms,
        reason: reason.into(),
    })
}

fn validate_batches(batches: &[BarBatch]) -> Result<(), BacktestError> {
    for (offset, batch) in batches.iter().enumerate() {
        if batch.bars.is_empty() {
            return invalid_batch(batch, "batch must contain at least one bar");
        }
        if batch.open_time_ms >= batch.close_time_ms {
            return invalid_batch(batch, "open_time_ms must be before close_time_ms");
        }
        if batch.available_at_ms < batch.close_time_ms {
            return invalid_batch(batch, "available_at_ms must not precede close_time_ms");
        }
        if let Some(previous) = offset.checked_sub(1).map(|index| &batches[index]) {
            if batch.index <= previous.index {
                return invalid_batch(batch, "batch indexes must increase strictly");
            }
            if batch.open_time_ms < previous.close_time_ms {
                return invalid_batch(batch, "bar batches must not overlap");
            }
            if batch.close_time_ms <= previous.close_time_ms {
                return invalid_batch(batch, "close times must increase strictly");
            }
            if batch.available_at_ms <= previous.available_at_ms {
                return invalid_batch(batch, "availability times must increase strictly");
            }
        }

        let mut previous_market: Option<&MarketId> = None;
        for bar in &batch.bars {
            if let Some(previous) = previous_market {
                if previous == &bar.market {
                    return invalid_batch(batch, "duplicate market in batch");
                }
            }
            previous_market = Some(&bar.market);
            validate_bar(batch, bar)?;
        }
    }
    Ok(())
}

fn validate_bar(batch: &BarBatch, bar: &Bar) -> Result<(), BacktestError> {
    if bar.market.symbol.trim().is_empty() {
        return invalid_batch(batch, "bar market symbol must not be empty");
    }
    if bar.open_time_ms != batch.open_time_ms || bar.close_time_ms != batch.close_time_ms {
        return invalid_batch(batch, "bar interval does not match its batch");
    }
    if bar.available_at_ms < bar.close_time_ms || bar.available_at_ms > batch.available_at_ms {
        return invalid_batch(batch, "bar availability is inconsistent with its batch");
    }
    if bar.open <= Decimal::ZERO
        || bar.high <= Decimal::ZERO
        || bar.low <= Decimal::ZERO
        || bar.close <= Decimal::ZERO
    {
        return invalid_batch(batch, "OHLC prices must be positive");
    }
    if bar.low > bar.high
        || bar.open < bar.low
        || bar.open > bar.high
        || bar.close < bar.low
        || bar.close > bar.high
    {
        return invalid_batch(batch, "OHLC prices are inconsistent");
    }
    for (name, value) in [
        ("volume_usd", bar.volume_usd),
        ("open_interest_usd", bar.open_interest_usd),
        ("market_cap_usd", bar.market_cap_usd),
    ] {
        if value.is_some_and(|value| value < Decimal::ZERO) {
            return invalid_batch(batch, format!("{name} must be non-negative"));
        }
    }
    if bar.mark_price.is_some_and(|value| value <= Decimal::ZERO) {
        return invalid_batch(batch, "mark_price must be positive when present");
    }
    if bar
        .quantity_step
        .is_some_and(|value| value <= Decimal::ZERO)
    {
        return invalid_batch(batch, "quantity_step must be positive when present");
    }
    Ok(())
}

fn invalid_batch<T>(batch: &BarBatch, reason: impl Into<String>) -> Result<T, BacktestError> {
    Err(BacktestError::InvalidBatch {
        batch_index: batch.index,
        reason: reason.into(),
    })
}

fn bars_by_market(batch: &BarBatch) -> BTreeMap<MarketId, &Bar> {
    batch
        .bars
        .iter()
        .map(|bar| (bar.market.clone(), bar))
        .collect()
}

fn require_bars_for_open_positions(
    account: &Account,
    bars: &BTreeMap<MarketId, &Bar>,
    batch_index: usize,
) -> Result<(), BacktestError> {
    for market in account.positions.keys() {
        if !bars.contains_key(market) {
            return Err(BacktestError::MissingOpenPositionBar {
                market: market.clone(),
                batch_index,
            });
        }
    }
    Ok(())
}

fn round_toward_zero(quantity: Decimal, step: Option<Decimal>) -> Result<Decimal, BacktestError> {
    let Some(step) = step else {
        return Ok(quantity);
    };
    let units = quantity
        .checked_div(step)
        .ok_or(BacktestError::Arithmetic("dividing by quantity step"))?
        .trunc();
    checked_mul(units, step, "rounding quantity to step")
}

fn sum_unrealized(portfolio: &PortfolioSnapshot) -> Result<Decimal, BacktestError> {
    portfolio
        .positions
        .iter()
        .try_fold(Decimal::ZERO, |sum, position| {
            checked_add(sum, position.unrealized_pnl, "summing final unrealized PnL")
        })
}

fn require_positive_equity(portfolio: &PortfolioSnapshot) -> Result<(), BacktestError> {
    if portfolio.equity <= Decimal::ZERO {
        Err(BacktestError::NonPositiveEquity {
            timestamp_ms: portfolio.timestamp_ms,
            equity: portfolio.equity,
        })
    } else {
        Ok(())
    }
}

fn calculate_max_drawdown(equity_curve: &[EquityRecord]) -> Result<Decimal, BacktestError> {
    let mut peak = Decimal::ZERO;
    let mut maximum = Decimal::ZERO;
    for point in equity_curve {
        if point.equity > peak {
            peak = point.equity;
        }
        if peak > Decimal::ZERO {
            let drawdown = checked_sub(peak, point.equity, "calculating drawdown numerator")?
                .checked_div(peak)
                .ok_or(BacktestError::Arithmetic("calculating drawdown"))?;
            if drawdown > maximum {
                maximum = drawdown;
            }
        }
    }
    Ok(maximum)
}

fn checked_add(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, BacktestError> {
    left.checked_add(right)
        .ok_or(BacktestError::Arithmetic(context))
}

fn checked_sub(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, BacktestError> {
    left.checked_sub(right)
        .ok_or(BacktestError::Arithmetic(context))
}

fn checked_mul(
    left: Decimal,
    right: Decimal,
    context: &'static str,
) -> Result<Decimal, BacktestError> {
    left.checked_mul(right)
        .ok_or(BacktestError::Arithmetic(context))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Venue;

    struct ScriptedStrategy {
        decisions: std::collections::VecDeque<StrategyDecision>,
    }

    impl ScriptedStrategy {
        fn new(decisions: impl IntoIterator<Item = StrategyDecision>) -> Self {
            Self {
                decisions: decisions.into_iter().collect(),
            }
        }
    }

    impl Strategy for ScriptedStrategy {
        fn name(&self) -> &str {
            "scripted"
        }

        fn on_bar_close(
            &mut self,
            _context: StrategyContext<'_>,
        ) -> Result<StrategyDecision, StrategyError> {
            Ok(self.decisions.pop_front().unwrap_or(StrategyDecision::Hold))
        }
    }

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("valid decimal")
    }

    fn market(symbol: &str) -> MarketId {
        MarketId::new(Venue::Hyperliquid, symbol)
    }

    fn bar(symbol: &str, open_time_ms: u64, open: &str, close: &str) -> Bar {
        let open_price = decimal(open);
        let close_price = decimal(close);
        Bar {
            market: market(symbol),
            open_time_ms,
            close_time_ms: open_time_ms + 100,
            available_at_ms: open_time_ms + 100,
            open: open_price,
            high: open_price.max(close_price),
            low: open_price.min(close_price),
            close: close_price,
            volume_usd: None,
            open_interest_usd: None,
            market_cap_usd: None,
            mark_price: None,
            funding_rate: Some(Decimal::ZERO),
            quantity_step: None,
        }
    }

    fn batch(index: usize, open_time_ms: u64, bars: Vec<Bar>) -> BarBatch {
        BarBatch {
            index,
            open_time_ms,
            close_time_ms: open_time_ms + 100,
            available_at_ms: open_time_ms + 100,
            bars,
        }
    }

    fn config() -> EngineConfig {
        EngineConfig {
            initial_collateral: decimal("1000"),
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            max_gross_leverage: decimal("2"),
            max_abs_net_weight: decimal("2"),
            max_asset_weight: decimal("2"),
            min_order_notional: Decimal::ZERO,
            apply_funding: true,
            require_funding_for_open_positions: true,
            liquidate_at_end: false,
        }
    }

    fn target(symbol: &str, weight: &str) -> StrategyDecision {
        let mut book = TargetBook::flat();
        book.insert(market(symbol), decimal(weight));
        StrategyDecision::Rebalance(book)
    }

    #[test]
    fn decision_never_fills_on_its_own_bar() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "101", "101")]),
        ];
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let result = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("successful run");

        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].decision_batch_index, 0);
        assert_eq!(result.trades[0].fill_batch_index, 1);
        assert_eq!(result.trades[0].reference_price, decimal("101"));
    }

    #[test]
    fn long_and_short_reductions_realize_correct_pnl() {
        let batches = vec![
            batch(
                0,
                0,
                vec![bar("LONG", 0, "100", "100"), bar("SHORT", 0, "100", "100")],
            ),
            batch(
                1,
                100,
                vec![
                    bar("LONG", 100, "100", "110"),
                    bar("SHORT", 100, "100", "90"),
                ],
            ),
            batch(
                2,
                200,
                vec![
                    bar("LONG", 200, "120", "120"),
                    bar("SHORT", 200, "80", "80"),
                ],
            ),
        ];
        let mut opening = TargetBook::flat();
        opening.insert(market("LONG"), decimal("0.5"));
        opening.insert(market("SHORT"), decimal("-0.5"));
        let mut strategy = ScriptedStrategy::new([
            StrategyDecision::Rebalance(opening),
            StrategyDecision::Rebalance(TargetBook::flat()),
        ]);
        let result = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("successful run");

        assert_eq!(result.summary.realized_pnl, decimal("200"));
        assert_eq!(result.summary.final_equity, decimal("1200"));
        assert!(result.final_portfolio.positions.is_empty());
    }

    #[test]
    fn positive_funding_makes_longs_pay_and_shorts_receive() {
        let mut long_bar = bar("LONG", 100, "100", "100");
        long_bar.funding_rate = Some(decimal("0.01"));
        let mut short_bar = bar("SHORT", 100, "100", "100");
        short_bar.funding_rate = Some(decimal("0.01"));
        let batches = vec![
            batch(
                0,
                0,
                vec![bar("LONG", 0, "100", "100"), bar("SHORT", 0, "100", "100")],
            ),
            batch(1, 100, vec![long_bar, short_bar]),
        ];
        let mut opening = TargetBook::flat();
        opening.insert(market("LONG"), decimal("0.5"));
        opening.insert(market("SHORT"), decimal("-0.25"));
        let mut strategy = ScriptedStrategy::new([StrategyDecision::Rebalance(opening)]);
        let result = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("successful run");

        assert_eq!(result.summary.funding_pnl, decimal("-2.5"));
        assert_eq!(result.summary.final_equity, decimal("997.5"));
    }

    #[test]
    fn fees_and_adverse_slippage_are_charged_once() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "100")]),
        ];
        let mut configured = config();
        configured.fee_bps = decimal("10");
        configured.slippage_bps = decimal("100");
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let result = BacktestEngine::new(configured)
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("successful run");

        assert_eq!(result.trades[0].fill_price, decimal("101"));
        assert_eq!(result.summary.slippage_cost, decimal("5"));
        assert_eq!(result.summary.fees, decimal("0.505"));
        assert_eq!(result.summary.final_equity, decimal("994.495"));
    }

    #[test]
    fn average_entry_survives_add_reduce_and_flip_exactly() {
        let id = market("BTC");
        let mut account = Account::new(decimal("1000"));
        account
            .apply_fill(
                &id,
                decimal("2"),
                decimal("100"),
                Decimal::ZERO,
                Decimal::ZERO,
            )
            .expect("open long");
        let added = account
            .apply_fill(
                &id,
                Decimal::ONE,
                decimal("130"),
                Decimal::ZERO,
                Decimal::ZERO,
            )
            .expect("add long");
        assert_eq!(added.post_quantity, decimal("3"));
        assert_eq!(added.post_average_entry_price, decimal("110"));

        let reduced = account
            .apply_fill(
                &id,
                -Decimal::ONE,
                decimal("140"),
                Decimal::ZERO,
                Decimal::ZERO,
            )
            .expect("reduce long");
        assert_eq!(reduced.realized_pnl, decimal("30"));
        assert_eq!(reduced.post_quantity, decimal("2"));
        assert_eq!(reduced.post_average_entry_price, decimal("110"));

        let flipped = account
            .apply_fill(
                &id,
                decimal("-3"),
                decimal("90"),
                Decimal::ZERO,
                Decimal::ZERO,
            )
            .expect("flip short");
        assert_eq!(flipped.realized_pnl, decimal("-40"));
        assert_eq!(flipped.post_quantity, decimal("-1"));
        assert_eq!(flipped.post_average_entry_price, decimal("90"));
        assert_eq!(account.realized_pnl, decimal("-10"));
        assert_eq!(account.collateral, decimal("990"));
    }

    #[test]
    fn execution_uses_quantity_step_known_at_decision_time() {
        let mut decision_bar = bar("BTC", 0, "100", "100");
        decision_bar.quantity_step = Some(Decimal::ONE);
        let mut future_bar = bar("BTC", 100, "100", "100");
        // This new value is not available until the fill bar closes and must
        // not affect an order already submitted by the prior decision.
        future_bar.quantity_step = Some(decimal("10"));
        let batches = vec![
            batch(0, 0, vec![decision_bar]),
            batch(1, 100, vec![future_bar]),
        ];
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.55")]);
        let result = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("successful run");

        assert_eq!(result.trades[0].quantity, decimal("5"));
        assert_eq!(result.final_portfolio.positions[0].quantity, decimal("5"));
    }

    #[test]
    fn changing_the_future_cannot_change_the_completed_prefix() {
        let prefix_batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "110")]),
        ];
        let mut full_batches = prefix_batches.clone();
        full_batches.push(batch(2, 200, vec![bar("BTC", 200, "999", "999")]));

        let mut prefix_strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let prefix = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut prefix_strategy, &prefix_batches)
            .expect("prefix run");
        let mut full_strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let full = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut full_strategy, &full_batches)
            .expect("full run");

        assert_eq!(prefix.equity_curve, full.equity_curve[..2]);
        assert_eq!(prefix.trades, full.trades);
    }

    #[test]
    fn target_books_over_a_risk_limit_are_rejected() {
        let batches = vec![batch(0, 0, vec![bar("BTC", 0, "100", "100")])];
        let mut configured = config();
        configured.max_asset_weight = decimal("0.25");
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let error = BacktestEngine::new(configured)
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect_err("oversized target must fail");
        assert!(matches!(error, BacktestError::InvalidTarget { .. }));
    }

    #[test]
    fn an_insolvent_overnight_gap_fails_before_a_queued_exit_can_fill() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "100")]),
            batch(2, 200, vec![bar("BTC", 200, "50", "50")]),
        ];
        let mut configured = config();
        configured.max_gross_leverage = decimal("2");
        configured.max_asset_weight = decimal("2");
        configured.max_abs_net_weight = decimal("2");
        let mut strategy = ScriptedStrategy::new([
            target("BTC", "2"),
            StrategyDecision::Rebalance(TargetBook::flat()),
        ]);
        let error = BacktestEngine::new(configured)
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect_err("the gap wipes out collateral before the exit open");

        assert_eq!(
            error,
            BacktestError::NonPositiveEquity {
                timestamp_ms: 200,
                equity: Decimal::ZERO,
            }
        );
    }

    #[test]
    fn next_open_targets_use_open_equity_instead_of_stale_close_equity() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "200")]),
            batch(2, 200, vec![bar("BTC", 200, "50", "50")]),
        ];
        let mut configured = config();
        configured.max_gross_leverage = Decimal::ONE;
        configured.max_asset_weight = Decimal::ONE;
        configured.max_abs_net_weight = Decimal::ONE;
        let mut strategy = ScriptedStrategy::new([target("BTC", "1"), target("BTC", "1")]);
        let result = BacktestEngine::new(configured)
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("open-equity sizing prevents accidental leverage");

        // Batch 1 closes at equity 2,000, but the batch 2 gap reduces causal
        // open equity to 500. A stale-equity implementation would target 40
        // units at 50; the correct open-equity target remains 10 units.
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.final_portfolio.positions[0].quantity, decimal("10"));
        assert_eq!(result.summary.final_equity, decimal("500"));
    }

    #[test]
    fn min_notional_skips_are_included_in_projected_open_risk() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "100")]),
            batch(2, 200, vec![bar("BTC", 200, "75", "75")]),
        ];
        let mut configured = config();
        configured.max_gross_leverage = decimal("2");
        configured.max_asset_weight = decimal("2");
        configured.max_abs_net_weight = decimal("2");
        configured.min_order_notional = decimal("1000");
        let mut strategy = ScriptedStrategy::new([target("BTC", "2"), target("BTC", "1.9")]);
        let error = BacktestEngine::new(configured)
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect_err("skipped reduction leaves projected leverage above the hard limit");

        assert!(matches!(
            error,
            BacktestError::ProjectedRisk { batch_index: 2, .. }
        ));
    }

    #[test]
    fn optional_end_liquidation_closes_at_the_last_known_close() {
        let mut batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "110")]),
        ];
        // The aggregate row is delayed, but terminal liquidation was selected
        // in configuration before the run. Its timestamps must remain the
        // interval open/close rather than pretending the close fill happened
        // only after this later publication time.
        batches[1].available_at_ms = 250;
        let mut configured = config();
        configured.liquidate_at_end = true;
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let result = BacktestEngine::new(configured)
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect("successful run");

        assert_eq!(result.trades.len(), 2);
        assert_eq!(result.trades[1].reason, TradeReason::EndLiquidation);
        assert_eq!(result.trades[1].reference_price, decimal("110"));
        assert_eq!(result.trades[1].decision_timestamp_ms, 100);
        assert_eq!(result.trades[1].fill_timestamp_ms, 200);
        assert!(result.final_portfolio.positions.is_empty());
        assert_eq!(result.summary.realized_pnl, decimal("50"));
        assert_eq!(result.summary.final_equity, decimal("1050"));
    }

    #[test]
    fn a_target_without_a_next_open_fails_closed() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("ETH", 100, "100", "100")]),
        ];
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let error = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect_err("missing next open must fail");
        assert!(matches!(error, BacktestError::MissingNextOpenBar { .. }));
    }

    #[test]
    fn a_missing_mark_for_an_open_position_fails_closed() {
        let batches = vec![
            batch(0, 0, vec![bar("BTC", 0, "100", "100")]),
            batch(1, 100, vec![bar("BTC", 100, "100", "100")]),
            batch(2, 200, vec![bar("ETH", 200, "100", "100")]),
        ];
        let mut strategy = ScriptedStrategy::new([target("BTC", "0.5")]);
        let error = BacktestEngine::new(config())
            .expect("valid engine")
            .run(&mut strategy, &batches)
            .expect_err("missing held-market bar must fail");
        assert!(matches!(
            error,
            BacktestError::MissingOpenPositionBar { .. }
        ));
    }
}
