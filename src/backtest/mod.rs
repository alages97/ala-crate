//! Deterministic, synchronous research backtesting for linear USD perpetuals.
//!
//! This subsystem is intentionally isolated from the live OMS and gateways.
//! Strategies see a completed cross-sectional bar only after its declared
//! availability time, and their target books can execute no earlier than the
//! next bar open.

pub mod config;
pub mod data;
pub mod engine;
pub mod indicators;
pub mod momentum;
pub mod pump_arb;
pub mod pump_data;
pub mod report;
pub mod types;

pub use config::{
    BacktestConfig, BacktestConfigError, BacktestDataConfig, BacktestOutputConfig,
    BuiltInStrategyConfig,
};
pub use data::{load_bars_csv, DataError};
pub use engine::{BacktestEngine, BacktestError, EngineConfig};
pub use momentum::{CrossSectionalMomentum, MomentumConfig};
pub use pump_arb::{
    run_pump_arb, write_pump_arb_report, PumpArbConfig, PumpArbEquityRecord, PumpArbError,
    PumpArbReportPaths, PumpArbResult, PumpArbSummary, PumpArbTrade, PumpBar, PumpExitReason,
    PumpFrame, PumpLeg, PumpLegCost, PumpRoute,
};
pub use pump_data::{
    align_longest_contiguous_window, fetch_pump_historical_data, write_pump_dataset,
    PumpDataConfig, PumpDataError, PumpDataInterval, PumpDatasetPaths, PumpHistoricalDataset,
    PumpSourceManifest, PumpSourceManifestEntry, GECKO_TERMINAL_API_URL, HYPERLIQUID_INFO_URL,
    HYPERLIQUID_PERP_COIN, HYPERLIQUID_SPOT_COIN, LIGHTER_API_URL, LIGHTER_MARKET_ID,
    PUMPSWAP_POOL, PUMP_MINT, PUMP_ONE_MINUTE_MS,
};
pub use report::{
    write_report, BacktestResult, BacktestSummary, EquityRecord, ReportError, ReportPaths,
    TradeReason, TradeRecord,
};
pub use types::{
    Bar, BarBatch, MarketId, PortfolioSnapshot, PositionSnapshot, Strategy, StrategyContext,
    StrategyDecision, StrategyError, TargetBook,
};
