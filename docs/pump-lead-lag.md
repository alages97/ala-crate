# PUMP lead/lag capture

`pump-record` is a credential-free research recorder for sub-second PUMP price
discovery. It is separate from `live-md`: the production publisher deliberately
coalesces BBOs, whereas this recorder preserves every received upstream payload
and timestamps it before JSON decoding with one shared process-local monotonic
clock.

The recorder opens only public mainnet feeds and cannot place orders.

## Captured instruments

- Binance USD-M `PUMPUSDT` perpetual: 20-level depth snapshots every 100 ms.
- Hyperliquid `@188` UPUMP spot: BBO, 20-level book snapshots, and trades.
- Hyperliquid `PUMP` perpetual: BBO, 20-level book snapshots, and trades.
- Lighter PUMP perpetual market 45: ticker, incremental order book, and trades.
- Native PUMP/USDC PumpSwap pool
  `2uF4Xh61rDwxnG9woyxsVQP7zuA6kLFpb3NvnRQeoiSd`: PUMP and USDC vault
  changes, pool-state changes, and transaction logs.

Bybit is not included in the current run because its `PUMPUSDT` linear
instrument reports `Closed`. The recorder includes Binance as an external
price-discovery reference even though the initial execution routes are
Hyperliquid, Lighter, and PumpSwap.

## Build and capture

```bash
cargo build --locked --release --bin pump-record

./target/release/pump-record \
  --output data/pump-lead-lag/capture.ndjson \
  --duration-seconds 3600
```

The duration begins only after all five required sources have produced usable
data. Startup fails after the configurable warm-up timeout if any source remains
missing. Records are flushed every second by default.

While recording, data is written to `capture.ndjson.part`. Clean completion
syncs the file, atomically renames it to `capture.ndjson`, and writes
`capture.manifest.json`. Cancellation, warm-up failure, or another early stop
keeps `capture.ndjson.part` and writes
`capture.ndjson.part.manifest.json`; `completed_requested_duration` is false and
`termination_reason` says why it stopped (`termination_detail` carries the
underlying error, when there is one). The recorder never gives an
incomplete artifact the final filename.

## Ordering and clocks

Every row contains:

- `receive_monotonic_ns`: the primary analysis clock, relative to one process
  origin and captured immediately after the WebSocket read;
- `receive_unix_ns`: diagnostic wall time;
- `decode_done_monotonic_ns`: local decoding completion time;
- `local_sequence`: writer ordering for equal or near-equal receive times;
- `connection_generation`: reconnect boundary;
- `delivery_semantics`: present as `initial_snapshot_or_backfill` on the first
  Hyperliquid trade batch for each instrument/connection generation and on
  Lighter's `subscribed/trade` response;
- the unmodified upstream JSON payload.

No market-data update is intentionally coalesced or deduplicated. Analysis must
sort by `(receive_monotonic_ns, local_sequence)` and treat reconnects, source
sequence gaps, and stale books explicitly. Records tagged
`initial_snapshot_or_backfill` contain history delivered at subscription time;
their individual trades are not causally live at the local receive timestamp.

The manifest records `started_monotonic_ns`, optional `ready_monotonic_ns`,
optional `deadline_monotonic_ns`, and `ended_monotonic_ns` in the same process
clock domain. `ready_monotonic_ns` is absent if all sources never became ready;
the deadline is exactly the requested-duration boundary armed from readiness.

## Analyze a completed capture

```bash
cargo build --locked --release --bin pump-lead-lag

./target/release/pump-lead-lag \
  --input data/pump-lead-lag/capture.ndjson \
  --output-directory backtest-output/pump-lead-lag \
  --max-quote-age-ms 1000 \
  --min-leader-move-bps 1
```

The analyzer uses strict receive-time as-of joins at 25, 50, 100, 250, 500,
1,000, and 2,000 ms. It writes `summary.json`, `lead_lag.csv`, and
`common_shocks.csv`, including full/first-half/second-half results, pairing
coverage, quote-age and convergence percentiles, overlap load, and a
per-direction non-overlapping screen. `common_shocks.csv` is stricter: the
first qualifying venue in a pair starts an episode and suppresses both
directions until that horizon ends. A positive
`mean_paired_spread_convergence_bps` is the gross midpoint return of buying the
putative lagger and shorting the leader in the direction of the leader shock;
it is not a fill or net-PnL estimate.

For a first screen, predeclare the one-basis-point threshold and focus on the
500 and 1,000 ms rows. Do not name a leader unless its direction is consistent
in both halves and under tighter quote-age sensitivity. Reverse directions use
different shock sets, and even the non-overlapping rows are descriptive rather
than a matched-shock confidence test.

## PumpSwap notes

The native pool is PUMP/USDC, with six decimals on both sides. The recorder
subscribes to both token vaults at `processed` commitment and also records logs
that contain exact PumpSwap buy/sell events. Swap events are preferred for
transaction-level post-swap reserves; separate vault notifications catch
liquidity changes and provide a cross-check.

Public Solana RPC delivery latency is not validator-colocated execution latency.
Processed updates can be rolled back, two vault notifications from one
transaction arrive separately, and provider jitter can resemble economic lag.
Retain slots and signatures when interpreting an apparent onchain follower.

## Interpretation limits

- Receipt-time leadership measures the complete venue-to-public-endpoint path,
  not matching-engine leadership in isolation.
- Binance quotes USDT; the other captured instruments use USDC economics. The
  first experiment assumes the stablecoin conversion is constant over short
  horizons and must not interpret a persistent USDT/USDC basis as PUMP alpha.
- A reserve ratio or midpoint is not an executable price. Monetization analysis
  must reconstruct target-size VWAPs, include PumpSwap fees and impact, and model
  two-leg arrival/fill latency.
- One hour is a connectivity and hypothesis screen, not enough evidence to arm
  live trading. Leadership should be stable across later sessions and regimes.
