# Backtesting

The `backtest` binary is an offline, synchronous research tool. It does not load
API keys, connect to an exchange, or reuse the live order manager. Strategies
produce complete target portfolios from completed bars; the simulator applies
those targets no earlier than the next bar open.

## Causal event order

For each cross-sectional bar batch `i` the engine performs these phases in
order:

1. Mark existing positions at the new opens and execute targets produced after
   batch `i-1` closed.
2. Mark positions at batch `i` closes and apply the aggregate funding rate
   declared on each row.
3. Expose the complete batch to the strategy once, after every row's
   `available_at_ms`.
4. Queue a complete target book for batch `i+1`; omitted markets mean flat.
5. Record close equity, exposure, costs, and drawdown.

The execution model never inspects the next bar's high, low, close, or volume
when filling at its open. A position or pending target whose market disappears
from the next batch fails the run instead of being priced with stale data.
The strategy still observes the final completed batch, but its last decision has
no following open and is not executed. Append a terminal execution bar when that
signal matters. `liquidate_at_end` is a separate, exogenous final-close policy
scheduled from the final bar's open; it is not the final strategy decision.

## Input CSV

Version 1 uses one long-format RFC 4180 CSV with this exact header:

```text
venue,symbol,open_time_ms,close_time_ms,available_at_ms,open,high,low,close,volume_usd,open_interest_usd,market_cap_usd,mark_price,funding_rate,quantity_step
```

- Timestamps are UTC Unix milliseconds. Bars use the half-open interval
  `[open_time_ms, close_time_ms)` and must match `data.interval_ms`.
- Decimal fields are ordinary base-10 strings, never binary floats or
  scientific notation.
- `volume_usd`, `open_interest_usd`, `market_cap_usd`, `mark_price`,
  `funding_rate`, and `quantity_step` may be empty. A configured universe gate
  treats its missing value as ineligible, not zero.
- `available_at_ms` is when all fields in the row were actually knowable and
  must be at or after the close. The next execution open must not precede the
  prior decision's availability time.
- `mark_price`, when present, values positions and funding; otherwise candle
  close is an explicit proxy.
- `funding_rate` is one effective cash rate for the whole bar; positive means
  longs pay. The engine applies it once to the end-of-bar valuation notional.
  When a bar spans multiple venue settlements, the dataset producer must derive
  an effective rate from the actual timestamped cashflows and their marks, not
  simply use the last observation or sum raw rates at changing mark prices.
- `quantity_step`, when present, is decision-time metadata used to round the
  following rebalance toward zero; the executor never reads a future bar's step.

Rows can arrive in any file order, but duplicate `(venue, symbol,
open_time_ms)` keys, conflicting intervals, malformed OHLC, negative attributes,
or irregular batches are rejected. Ranking order is deterministic by venue and
symbol after loading.

## Running

Validate the configuration without loading data:

```bash
cargo run --locked --bin backtest -- \
  --config config/backtest-momentum.toml \
  --validate-only
```

Run a backtest after placing the configured CSV:

```bash
cargo run --locked --release --bin backtest -- \
  --config config/backtest-momentum.toml
```

The output directory receives `summary.json`, `equity.csv`, and `trades.csv`.
The summary separates fees and funding from price PnL and includes total return
and close-to-close maximum drawdown. The equity and trade ledgers expose per-bar
exposure and exact traded notionals for further analysis. Annualized statistics
are deferred until the report schema has an explicit calendar and annualization
convention.

## First strategy: cross-sectional momentum

The bundled strategy is a research hypothesis, not a claimed edge. It first
requires point-in-time market-cap, open-interest, and average-volume gates. It
then ranks eligible Hyperliquid perps using short- and long-horizon returns, the
fast/slow EMA gap, and Wilder RSI. Long candidates must satisfy
`close > fast EMA > slow EMA` and the configured strong-RSI band; shorts use the
inverse trend and weak-RSI band. The highest and lowest scores receive equal
weights inside separately configured long and short gross budgets.

The supplied configuration is intended for daily bars: 7/30-day momentum,
EMA20/50, RSI14, daily rebalancing, and a 50% long / 50% short book. If either
side lacks the configured minimum breadth the target book is flat rather than
silently becoming directional.

## Adding another strategy

Implement the synchronous `backtest::Strategy` trait and return either `Hold`
or a complete `TargetBook` from each completed `BarBatch`. The engine owns data
ordering, next-open fills, accounting, funding, costs, and risk checks, so a
strategy only needs to maintain its causal rolling state and produce signed
equity weights. Add a variant to `BuiltInStrategyConfig` and the `backtest`
binary's match when the strategy should also be selectable from TOML.

## Bias controls and known limits

- Universe membership, OI, volume, and market cap must be historical
  point-in-time observations. Applying today's values or today's surviving
  symbol list to old bars creates lookahead and survivorship bias.
- Hyperliquid does not provide historical market cap. Join a separate provider
  with stable asset identifiers and observation timestamps; never join solely
  on a mutable ticker. Disable the gate if valid point-in-time data is absent.
- New listings naturally remain ineligible until indicator warm-up completes.
  Delisted markets must have a final executable bar; an open position cannot
  disappear from the dataset.
- Version 1 models linear USD perpetuals, full market fills, fixed bps costs,
  and one uniform bar interval. It does not model order-book depth, partial
  fills, queue position, portfolio margin, venue liquidation tiers, or
  cross-margin contagion.
- The single effective funding-rate field cannot represent a position change at
  the same instant as a funding event without an explicit boundary convention.
  A later adapter should replay timestamped venue funding cashflows separately
  from strategy bars.
- Equity and drawdown are sampled at completed bar closes. Intrabar highs and
  lows are validated but do not drive margin calls, liquidation, or drawdown;
  daily-bar results can therefore understate intraday risk substantially.
- Version 1 reports do not embed the input/config digest and the three output
  files are replaced sequentially rather than as one transaction. Use a fresh
  output directory and archive the exact config and frozen input beside any
  result used for comparison.
- Results should be evaluated out of sample and under fee/slippage stress.
  Parameter searches need walk-forward splits; the engine deliberately does
  not label an in-sample optimum as evidence of profitability.

Useful follow-on strategies include Donchian/EMA time-series trend following,
BTC-beta-residual momentum, price-plus-OI confirmation, funding-aware momentum,
cross-sectional funding carry, and carefully capacity-limited liquidation
mean-reversion.

## PUMP cross-venue basis screen

`pump-arb` is a specialized two-leg simulator built on the same completed-bar,
next-open causal convention. It downloads public mainnet history for these
fixed identifiers:

- native Solana PUMP mint
  `pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn` from PumpSwap pool
  `2uF4Xh61rDwxnG9woyxsVQP7zuA6kLFpb3NvnRQeoiSd`;
- Hyperliquid UPUMP spot, API coin `@188`;
- Hyperliquid PUMP perpetual;
- Lighter PUMP perpetual, market ID 45.

UPUMP is the one-for-one HyperCore representation of native PUMP. The
simulator therefore hedges the same integer token quantity on both legs, but it
never replaces UPUMP's observed price with the native price. A temporary
UPUMP/PUMP tracking spread remains part of the measured data.

Only funded long-spot/short-perpetual routes are allowed; the engine never
assumes an unmodelled spot borrow. Either direction is allowed between the two
perpetuals. A signal is the short-over-long close basis less the configured exit
basis and estimated round-trip fees and slippage. It executes at the following
bar open, exits after convergence or the maximum hold, applies timestamped
hourly funding with positive rates paid by longs, and rounds both legs down to
the common integer-PUMP lot.

Run the recent and coarse screens with:

```bash
cargo run --release --locked --bin pump-arb -- \
  --config config/pump-arb.toml

cargo run --release --locked --bin pump-arb -- \
  --config config/pump-arb-4h.toml
```

The one-minute configuration requests the latest 5,000 bars. The four-hour
configuration requests 1,080 bars, or 180 days, matching the public on-chain
history limit. The exact intersection may be shorter when a venue has no candle
for an interval; missing bars are never forward-filled. Each output directory
contains `normalized.csv`, `source_manifest.json`, `summary.json`,
`equity.csv`, and `trades.csv`. Funding endpoints are paginated independently
of candles, and settlements at time `T` belong to the right-closed bar ending
at `T`, not to a position first opened at `T`.

The bundled costs assume current tier-zero Hyperliquid taker fees, a Standard
Lighter account, the current 30 bps fee of the noncanonical PumpSwap PUMP/USDC
pool, and explicit per-leg slippage. PumpSwap charged 25 bps during part of the
four-hour history, so the static 30 bps setting is conservative there. Change
the configuration for the actual account tier and run fee/slippage sensitivity
tests before interpreting a result.

This remains a persistent-basis screen, not executable-arbitrage proof. Public
candles do not contain synchronized bid/ask depth, AMM reserve state, partial
fills, transaction ordering, observation latency, Lighter's taker delay, or
bridge queues. Completed-bar turnover is only a causal liquidity proxy. An
execution-grade test requires prospectively recorded books, exact PumpSwap
events/reserves, target-size quotes, per-venue capital and margin ledgers, and
prepositioned inventory; Unit transfers should not be treated as an atomic leg.
