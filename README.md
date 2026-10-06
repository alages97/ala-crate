# ala-crate

This repository contains the original `wide-quoter` Rust package and the independent
[`pump-copy`](pump-copy/README.md) research crate. They have separate Cargo manifests
and lockfiles; this is not a Cargo workspace. Run each crate's build and tests
explicitly. The root package's live-trading capabilities do not apply to `pump-copy`.

| Project | Purpose | Documentation |
| --- | --- | --- |
| Root `wide-quoter` package | Hyperliquid/Lighter market making, market-data capture, backtesting, and PUMP/Kuru research tools | Sections below and [`docs/`](docs/) |
| `pump-copy/` | Pump.fun/PumpSwap wallet research, large-buyer signals, raw recording, transaction accounting and paper strategy evaluation | [`pump-copy/README.md`](pump-copy/README.md) |

For the copy-trader work through **October 6, 2026**, start with the
[current status](pump-copy/README.md#current-status--october-6-2026),
[research findings](pump-copy/README.md#research-history-and-findings), and
[source/data locations](pump-copy/README.md#source-and-local-evidence-locations).
The latest bounded recording has finished. Transaction accounting has been
validated on a sample, but executable pricing for the latest raw recordings
remains unresolved. No funded copy trading is implemented or enabled.

Generated builds, recordings, local credentials and research output are excluded
by `.gitignore`; source, lockfiles, configurations, schemas and small test fixtures
remain eligible for version control. Research datasets and some audit helper
scripts also live outside this repository; their locations are recorded in the
`pump-copy` README. Initializing Git here does not back up those external files.

## Wide-quoter

`wide-quoter` is a live-mainnet Rust market-making skeleton for Hyperliquid and
Lighter. It separates venue-neutral desired-order management from venue signing,
wire formats, nonces, and private execution streams. The included strategy quotes
a fixed quantity at a deliberately wide spread around a weighted multi-venue BBO
midpoint.

> **Live funds are at risk.** There is no paper or testnet execution mode. The
> sample configurations are real-mainnet configurations, and the current risk
> controls are not sufficient for unattended production trading. Read
> [Known limitations](#known-limitations) before supplying credentials.

## Architecture

```text
separate market-data process
  Binance / Bybit / Hyperliquid / Lighter WebSockets
                         |
                         | normalized BBOs (TCP NDJSON)
                         v
MarketDataClient -> WideQuoter -> OrderManager<G> -> venue Gateway -> mainnet
                                         ^               |
                                         |_______________|
                                     private orders/fills

                  structured logs + Prometheus metrics
```

- `WideQuoter` is synchronous and venue-neutral. It uses exact decimals, requires
  a fresh target-venue BBO plus the configured minimum number of fresh sources,
  calculates a weighted mid, rounds quotes outward to the execution tick, and
  suppresses quotes that exceed the per-order notional cap. A monotonic local
  interval limits order-increasing replace/re-entry actions without delaying
  cancellations. If a material reprice is due before that interval elapses, the
  old quote is canceled immediately and the strategy stays flat until re-entry
  is allowed.
- `OrderManager<G>` is a serialized desired-state actor. It owns one logical
  order per `(strategy, instrument, side)`, orders cancel-before-replace, rejects
  stale revisions, bounds active fill deduplication, and keeps the private stream
  authoritative for terminal state. It can also latch a halt on the first owned
  fill. Indeterminate submissions are never blindly resubmitted.
- `OrderGateway` is the generic execution boundary. `HyperliquidGateway` and
  `LighterGateway` contain all venue-specific validation, signing, identifiers,
  API calls, and private-stream normalization.
- One `quoter` process executes on one venue. Run the two supplied configs as two
  processes to quote both venues; they can consume the same market-data publisher.

The example universe is the native `BTC` perpetual on both execution venues,
with `BTCUSDT` reference feeds on Binance and Bybit. Internally it is named
`BTC-USD-PERP`; venue collateral and settlement conventions remain venue-specific.

## Safety and arming

A trading run has two deliberate gates:

1. The parsed config must contain `live_trading = true`.
2. The command must include `--acknowledge-live-trading`.

Validation does not read credentials, connect to a venue, or submit orders:

```bash
cargo run --locked --bin quoter -- \
  --config config/hyperliquid-btc.toml \
  --validate-only

cargo run --locked --bin quoter -- \
  --config config/lighter-btc.toml \
  --validate-only
```

Before a first funded run, use dedicated low-balance accounts/subaccounts,
dedicated API credentials, venue-side maker-only permissions where available,
the smallest valid quantity, and an independent venue-side cancel procedure.
Confirm tick/lot sizes, minimum notional, fees, leverage, margin mode, and the
market-data publisher's clock and sequence behavior immediately before launch.

`mock-md` marks every frame as `environment = "synthetic"`. An armed live quoter
rejects the first such frame and enters cancel-and-confirm shutdown before quote
evaluation or order submission. This is a guard against accidental use, not a
reason to connect development tooling to a live process.

## Build and verify

Rust 1.87 or newer is required. Keep the lockfile in live deployments.
All commands below assume `ala-crate` is the current working directory.

```bash
cargo test --locked --all-targets
cargo build --locked --release
```

The Hyperliquid adapter is pure Rust. The Lighter adapter additionally needs a
platform C toolchain, Go with CGO enabled, and the official Lighter signer shared
library. The pinned signer tree declares Go 1.23.0 with the Go 1.23.1 toolchain;
the helper supports macOS and Linux on x86_64 or arm64.

## Credentials

Secrets are read from environment variable names selected by the config. The
process does not load `.env` files automatically; `.env.example` is only an
inventory. Inject values through your shell, service manager, or secret manager,
and never commit them. The supplied binary caps legacy dependency logs below
TRACE because raw WebSocket frame traces can contain Lighter authentication
tokens; do not remove that cap or log private-stream payloads in an embedding
application.

Hyperliquid:

- `HYPERLIQUID_AGENT_PRIVATE_KEY`: a dedicated approved API/agent wallet key.
- `HYPERLIQUID_ACCOUNT_ADDRESS`: the actual trading address whose private orders
  and fills must be subscribed to, not necessarily the agent address.
- A vault can be enabled by adding `vault_address_env` to the gateway config and
  exporting the named variable. In that mode, `HYPERLIQUID_ACCOUNT_ADDRESS` and
  the vault variable must contain the same vault/subaccount address; a master
  address is not valid for that private subscription, and the gateway rejects a
  mismatch.

Lighter:

- `LIGHTER_SIGNER_LIBRARY_PATH`: absolute path to the compiled signer library.
- `LIGHTER_ACCOUNT_INDEX`: trading account index.
- `LIGHTER_API_KEY_INDEX`: API key index in `0..=254`.
- `LIGHTER_API_PRIVATE_KEY`: dedicated API private key; prefer a maker-only key
  where the account tier supports one.

### Build the pinned Lighter signer

The adapter deliberately does not reimplement Lighter's Poseidon/Schnorr signing.
The build script clones the official [`lighter-go`](https://github.com/elliottech/lighter-go)
repository, checks out commit
[`8854554703385766c73410b43e93075447f36a4c`](https://github.com/elliottech/lighter-go/commit/8854554703385766c73410b43e93075447f36a4c),
and compiles `sharedlib/main.go` as a native shared library:

```bash
./scripts/build-lighter-signer.sh
```

The platform-specific artifact is written under `vendor/lighter-signer/` and its
SHA-256 is printed. Record that hash in deployment metadata, then export its
absolute path as `LIGHTER_SIGNER_LIBRARY_PATH`. `vendor/` is gitignored. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) before redistributing the
artifact.

## Configuration

Start from one of the reviewed examples:

- [`config/hyperliquid-btc.toml`](config/hyperliquid-btc.toml)
- [`config/lighter-btc.toml`](config/lighter-btc.toml)

The top-level config, the `[gateway]` table and its venue-specific settings,
`[runtime]`, `[telemetry]`, `[market_data]`, `[quoter]`,
`[quoter.instrument]`, and each
`[[quoter.reference_sources]]` object reject unknown keys. A misspelled or
unsupported field is therefore a startup parse error instead of being silently
ignored; run `--validate-only` after every config change.

Important strategy controls are `half_spread_bps`, `quote_quantity`,
`max_order_notional`, `reprice_threshold_bps`, `min_quote_interval_ms`,
`max_source_age_ms`, and `min_fresh_sources`. `max_order_notional` applies to
each order; it is not an account, position, or aggregate exposure limit. Decimal
configuration values are quoted strings to preserve exactness.

`min_quote_interval_ms` is required and must be positive. It uses the process's
monotonic clock to limit every `QuoteChange::Replace`, including re-entry after a
stale-data cancellation; risk-reducing `QuoteChange::Cancel` remains immediate.
The Hyperliquid sample uses 1,000 ms. The Lighter sample and config validator use
at least 10,000 ms as a deliberately conservative allowance under Lighter's
[current rate limits](https://apidocs.lighter.xyz/docs/rate-limits): Standard
accounts have a 60-request rolling-minute `sendTx` limit and ordinary transaction
types have a 40-request-per-minute limit. One two-sided cancel-before-replace can
consume four transactions, so a 10-second steady-state interval budgets roughly
24 of those transactions per minute and retains capacity for emergency cancels.
Limits can change; verify them before deployment. This is a local strategy guard,
not an account-wide rate coordinator.

Market-data admission is independently bounded by `max_publisher_lag_ms`,
`max_exchange_lag_ms`, and `max_future_skew_ms`. A frame outside those wall-clock
limits is rejected before it can advance sequence state or refresh a strategy
source. The sample limits are 500 ms publisher lag, 2,500 ms exchange lag, and
1,000 ms future-clock tolerance; tune them to the deployment region and keep the
publisher and trading hosts synchronized. These checks complement the quoter's
monotonic `max_source_age_ms` cache limit rather than replacing it.

`runtime.shutdown_timeout_ms` must be strictly greater than twice the selected
gateway's `request_timeout_ms`. The two quote-side cancels may be serialized
(Lighter serializes them for nonce safety), so config validation reserves two
full request windows plus positive margin. The samples use 15,000 ms for a
5,000 ms request timeout. This is the OMS's logical cancel-confirm deadline, not
a hard process-exit bound; see [Execution failures and shutdown behavior](#execution-failures-and-shutdown-behavior)
before configuring a service-manager stop timeout.

The Lighter gateway discovers the configured symbol and its current market ID,
precision, and minimums from mainnet at startup. The Hyperliquid gateway uses
live metadata for symbol and precision validation. Hyperliquid is hard-wired to
mainnet. The Lighter adapter accepts only its official
`mainnet.zklighter.elliot.ai` API host (with an optional `https://` prefix) and
rejects testnet or arbitrary HTTPS hosts before reading credentials.

Run only one active execution process for a Lighter trading account and API-key
nonce lane. Do not overlap supervisors or rely on the local quote interval to
coordinate another writer. A restart resets the in-memory interval state; wait
for cancel-and-confirm shutdown, then independently verify that no bot orders
remain and that the position is understood before starting the replacement
process.

## Market data

`live-md` is the separate production publisher. It connects without credentials
to the public Binance, Bybit, Hyperliquid, and Lighter mainnet BBO streams,
normalizes exact decimals and genuine venue timestamps, and fans out one
versioned JSON object per line on TCP. Its endpoint is a trusted pricing boundary
and binds to loopback by default.

Build, validate, and run only live market data:

```bash
cargo build --locked --release --bin live-md

./target/release/live-md \
  --config config/live-market-data.toml \
  --validate-only

./target/release/live-md \
  --config config/live-market-data.toml
```

The default metrics endpoint is `http://127.0.0.1:9900/metrics`. Start the
publisher before either quoter and require all four `market_data_feed_up` gauges
plus `market_data_publisher_ready` to be `1` before arming execution.

In a second terminal, print the validated live stream:

```bash
cargo run --locked --bin md-tail
```

Use `--json` for protocol-v1 NDJSON, or `--address HOST:PORT` for a non-default
publisher. A validated timed capture can be written without terminal output:

```bash
cargo run --locked --bin md-tail -- \
  --output data/market-data/capture.ndjson \
  --quiet \
  --duration-seconds 3600
```

The output uses create-new semantics and is flushed and synchronized on a clean
exit. `md-tail` is read-only: it connects only to the local publisher and cannot
place orders. Human-readable output includes signed publisher and exchange ages;
a negative age means the remote clock is ahead of the consumer.

Each market uses a checked publisher-owned sequence and one process-lifetime
stream ID. Raw bursts are coalesced in bounded latest-value slots before sequence
allocation. Slow TCP clients are disconnected before queued frames exceed the
configured age budget; no client can backpressure venue WebSocket readers.
Production frames explicitly carry `environment = "live"`. Sequence and timestamp
state in each quoter survives TCP reconnects, and stale or implausibly future
frames are discarded before quote evaluation.

The full publisher operations guide is in
[`docs/live-market-data.md`](docs/live-market-data.md), and the wire contract is
in [`docs/market-data-protocol.md`](docs/market-data-protocol.md). `mock-md`
remains available for isolated IPC development:

```bash
cargo run --locked --bin mock-md -- --listen 127.0.0.1:9101
```

### PUMP lead/lag research capture

The raw PUMP recorder keeps sub-second public updates that the production BBO
publisher intentionally coalesces. It records Binance PUMP perp, Hyperliquid
UPUMP spot and PUMP perp, Lighter PUMP perp, and native PumpSwap PUMP/USDC under
one process-local monotonic clock:

```bash
cargo build --locked --release --bin pump-record --bin pump-lead-lag

./target/release/pump-record \
  --output data/pump-lead-lag/capture.ndjson \
  --duration-seconds 3600

./target/release/pump-lead-lag \
  --input data/pump-lead-lag/capture.ndjson \
  --output-directory backtest-output/pump-lead-lag \
  --max-quote-age-ms 1000 \
  --min-leader-move-bps 1
```

The timed period starts only after every required source is live. Completed
runs use the requested `.ndjson` path; interrupted runs retain `.ndjson.part`
with an explicit `.ndjson.part.manifest.json` rather than looking complete. See
[`docs/pump-lead-lag.md`](docs/pump-lead-lag.md) for the raw schema, PumpSwap
account/log sources, causal analyzer outputs, completion behavior, and the limits
on interpreting public endpoint receipt time as executable latency.

The live runtime refuses these explicitly synthetic frames before calculating a
quote. The environment field is not authentication, so protect the publisher
endpoint from processes that could falsely label injected data as `live`.

### Kuru/Monad arbitrage research capture

`kuru-record` records seven live-mainnet sources under one local monotonic clock:
Kuru's native MON/USDC book, Kuru Flow quotes, direct Uniswap v4 MON/USDC quotes,
Binance and Bybit MON perpetuals, Binance USDC/USDT, and Monad block heads. It is
read-only, needs no private keys, and cannot trade. Its default timed capture is
six hours and begins only after every source is ready:

```bash
cargo build --locked --release --bin kuru-record

./target/release/kuru-record \
  --output data/kuru-monad/capture.ndjson
```

Completed runs produce the requested NDJSON plus a manifest. Interrupted or
failed runs retain an explicit `.ndjson.part` artifact and partial manifest.
See [`docs/kuru-monad-capture.md`](docs/kuru-monad-capture.md) for source and unit
definitions, timing fields, completion semantics, and the distinction between
same-chain atomic Kuru/Uniswap research and non-atomic CEX leader/lag analysis.

`kuru-shadow` is the next-stage read-only prover. It composes both Kuru/Uniswap
round trips from calls pinned to one canonical Monad block hash, at several
exact sizes, and records gross edge plus RPC/proof latency. It auto-detects the
legacy market versus Kuru V2, but deliberately labels its results as composed
quotes rather than an atomic transaction simulation:

```bash
cargo build --locked --release --bin kuru-shadow

./target/release/kuru-shadow \
  --config config/kuru-monad-shadow.toml \
  --once \
  --output data/kuru-monad/kuru-shadow-smoke.ndjson
```

See [`docs/kuru-monad-shadow.md`](docs/kuru-monad-shadow.md) for the output
contract, telemetry, V2 behavior, RPC load, and what remains before an edge can
be considered executable. The dated
[`Monad arbitrage findings`](docs/monad-arb-findings-2026-09-28.md) preserve the
multi-block Kuru/Uniswap, Kuru/Crystal, and Kuru/perpetual results and their
timing limitations.

The Kuru/Crystal tail can also be replayed through the actual atomic venue
entrypoints on a Monad-aware Foundry fork. The test suite checks every captured
block hash, uses Kuru fill-or-kill and Crystal complete-fill orders, handles
native MON/WMON conversion, and reverts unprofitable routes atomically. The
workspace-local Foundry binary is ignored by version control; install Foundry
1.8.3 or place the verified binary at the path below before reproducing it:

```bash
MONAD_RPC_URL=https://rpc.monad.xyz \
  .tools/foundry-v1.8.3/forge test \
  --network monad \
  --match-contract KuruCrystalForkReplayTest \
  -vvvv
```

The suite reports both an optimistic first-in-next-block replay from captured
post-N state and a conservative replay after importing the real N+1 post-state.
It never broadcasts to mainnet.

## Backtesting

`backtest` is a separate offline research binary with no gateway or credential
path. It supports pluggable multi-asset strategies, exact-decimal linear-perp
accounting, point-in-time universe attributes, aggregate funding, fixed fees and
slippage, next-bar-open execution, and deterministic CSV/JSON reports.

Validate the bundled cross-sectional momentum configuration:

```bash
cargo run --locked --bin backtest -- \
  --config config/backtest-momentum.toml \
  --validate-only
```

The strategy ranks historically eligible Hyperliquid perps by short/long
momentum, EMA trend, and RSI, then holds a separately budgeted long-strong and
short-weak book. Actual runs require a point-in-time historical dataset; current
OI, market cap, or the current surviving symbol list must never be backfilled
into the past. The input contract, causal event order, output files, and known
simulation limits are documented in
[`docs/backtesting.md`](docs/backtesting.md).

The PUMP basis screen fetches public mainnet history for native Solana PUMP,
Hyperliquid `@188`/UPUMP spot, Hyperliquid PUMP perpetuals, and Lighter market
45. UPUMP is sized one-for-one with PUMP, while its observed exchange price is
retained so tracking deviations are not hidden:

```bash
# Recent one-minute screen.
cargo run --release --locked --bin pump-arb -- \
  --config config/pump-arb.toml

# Coarser 180-day screen.
cargo run --release --locked --bin pump-arb -- \
  --config config/pump-arb-4h.toml
```

These commands use public data only and cannot place orders. They write the
normalized input, a source manifest, summary, equity curve, and trade ledger
under `backtest-output/`.

## Run live

After independent review, credential injection, and a clean telemetry scrape:

```bash
./target/release/quoter \
  --config config/hyperliquid-btc.toml \
  --acknowledge-live-trading

./target/release/quoter \
  --config config/lighter-btc.toml \
  --acknowledge-live-trading
```

Use separate supervisors and log streams. The example metrics endpoints are
`127.0.0.1:9898` for Hyperliquid and `127.0.0.1:9899` for Lighter:

```bash
curl --fail http://127.0.0.1:9898/metrics
curl --fail http://127.0.0.1:9899/metrics
```

Latency stages, labels, interpretation, and starter PromQL are documented in
[`docs/telemetry.md`](docs/telemetry.md).

## Execution failures and shutdown behavior

The private execution stream is authoritative, and a gap is fail-stop rather
than resumable. On any observed private-stream disconnect, closed event channel,
or lag in the OMS execution-event receiver, the OMS itself immediately marks
execution unavailable, revokes every preexisting desired quote, and starts
canceling tracked orders. This revocation happens inside the OMS before the
strategy reacts, closing the race with a fast reconnect or terminal event.

The supplied live runtime treats the resulting connectivity event as fatal. It
withdraws both strategy sides, exits its quote loop with an error, and always
runs cancel-and-confirm shutdown. A later transport reconnect cannot resurrect
the revoked desired state, and the runtime never automatically resumes quoting
after an execution-event gap: neither venue adapter performs a REST open-order
reconciliation that could prove the account state safe for resumption.

Execution readiness remains level-triggered for race-free startup. The generic
OMS subscribes to transitions and samples current gateway readiness; a desired
revision explicitly issued while the OMS is initially disconnected can wait for
the first ready state. The supplied runtime does not normally use that path,
because each gateway establishes initial private-stream readiness before the
live strategy loop begins. This initial-start behavior is distinct from recovery
after a gap.

Both venue adapters send a private-stream keepalive every 30 seconds and treat
90 seconds without an incoming frame as socket inactivity. Independently, each
tracked order has a 30-second private lifecycle deadline while awaiting private
acceptance, a terminal cancel update, or authoritative fill reconciliation. A
lifecycle stall takes execution readiness down and emits the same disconnect
used by the fail-stop path; a responsive socket is not sufficient if an order's
required private transition never arrives.

Lighter private authentication defaults to a seven-hour token. The adapter
deliberately rotates the private session after six hours so the token cannot
expire in place. Before announcing each connection ready, it fetches the
server's next nonce and advances the local serialized nonce lane to the maximum
of the local and server values; reconnect cannot move the local nonce backward.
Under the supplied runtime's no-resume policy, the observed rotation disconnect
still ends that quoting session and triggers cancel-and-confirm rather than
continuing transparently. Treat six hours as the maximum continuous Lighter run;
do not let a supervisor restart it until shutdown has completed and the account
has been independently verified to have no leftover bot orders and any position
is understood.

A pending-new order remains tracked until private resolution. Once its public
submit has a successful ingress acknowledgement, the OMS can cancel it when the
desired order changes or is withdrawn even if private `Accepted` has not arrived.
If a submit outcome is `Indeterminate`, the OMS keeps its client ID and, when the
desired order changes or is withdrawn, makes one conservative cancel attempt for
that reconciliation; it does not blindly resubmit or loop cancellation on the
same desired revision.

The supplied live runtime sets `OrderManagerConfig::halt_on_any_fill = true`.
When the serialized OMS applies the first owned fill, partial or final, it
latches `halted_after_fill`, clears desired state for every slot, and begins
canceling every remaining owned order before it publishes the `Fill` event to
the strategy. Later `set_desired` calls fail with `HaltedAfterFill`, including
commands already queued by a concurrent market-data decision. This actor-level
ordering closes the fill/cancel/requote race. The runtime then records the fill
fail-stop, exits its quote loop, and completes cancel-and-confirm; it does not
hedge or adjust the opposite quote.

`halt_on_any_fill` defaults to `false` for generic `OrderManager` users. An
inventory-aware strategy may leave it disabled, but then owns the position,
quote-revision, and risk response to every fill.

A private venue rejection, cancel rejection, cancellation for which the OMS has
no active cancel request, or gateway submit/cancel failure puts the affected
side into a blocked error state. The supplied runtime additionally enables
`fail_closed_on_execution_fault`: the serialized OMS permanently latches the
session fault before publishing it, withdraws all desired sides, begins
risk-reducing cancellation, rejects every later strategy revision, and ignores
a transport `Reconnected` event. This closes races in which the strategy is
still awaiting a command or has not yet consumed the fatal event.

`fail_closed_on_execution_fault` defaults to `false` for generic
`OrderManager` users that implement their own reconciliation and resumption
policy. The supplied live binary has no such recovery path and always enables
it.

Operators must stop or otherwise revoke the bot before manually canceling its
orders; an out-of-band cancellation while it is active is intentionally treated
as an unsafe state transition.

On `Ctrl-C` or a fail-stop condition, the market-data task stops, desired orders
are cleared, and the OMS requests cancellation of every order it owns. At
`runtime.shutdown_timeout_ms` the OMS marks shutdown timed out, but it lets any
already-dispatched gateway futures settle before returning the timeout. This
avoids abandoning a Lighter request after reserving or submitting a nonce.
Lighter operations serialize on one nonce lane, and a failed `sendTx` can be
followed by a `nextNonce` recovery request. With two quote slots, already-started
operations, their risk-reducing cancels, and the final owned-order `cancel_all`
fallback can therefore consume several request-timeout windows after the OMS
deadline—not merely one.

Do not configure an external supervisor to send `SIGKILL` at the OMS deadline.
For the supplied two-slot Lighter runtime, a conservative minimum stop grace is
`shutdown_timeout_ms + 12 * request_timeout_ms`, plus operational margin (75
seconds plus margin for the supplied 15-second/5-second settings). This budget
covers serialized `sendTx`/nonce-recovery paths and two fallback cancels; a hung
native signer call is not time-bounded, so validate the actual deployment and
retain an independent venue-side cancel procedure. After in-flight work settles,
if terminal state is still unconfirmed, the binary attempts the gateway's
owned-order `cancel_all` fallback and exits with an error. Treat a non-zero exit
as an operator incident and verify open orders directly at the venue.

Process termination, host failure, or `SIGKILL` cannot run this shutdown path.
Use venue-side dead-man controls where available and always keep an independent
cancel tool ready.

## Known limitations

This is a foundation, not a production-complete trading system:

- No inventory model, position limit, account-level exposure limit, PnL/drawdown
  guard, liquidation-distance check, or automated kill switch exists. The
  supplied runtime therefore fail-stops on the first partial or final fill and
  leaves the resulting inventory for the operator to manage.
- `max_order_notional` limits each quote only, and both sides always use the same
  fixed quantity. Because positions are neither loaded nor reconciled, exposure
  can still accumulate across restarts or separate runs.
- OMS and gateway state are in memory. After restart, orders from the previous
  process are not adopted or reconciled; `cancel_all` means orders tracked by the
  current process, not every open order on the account.
- The quote interval guard is process-local and not persisted. A randomized
  nonzero uint48 client-order-ID seed reduces accidental restart collisions, but
  neither feature coordinates overlapping writers or makes an unclean restart
  safe. Use one active process per trading account/API-key lane and reconcile a
  clean shutdown before restarting.
- Fill deduplication retains at most 65,536 active `(client_order_id, trade_id)`
  identities, evicting the oldest entry if that bound is reached and clearing an
  order's identities when it becomes terminal. It is not persisted across a
  restart.
- There is no HA leader election, durable journal, replay, operator control API,
  or cross-process risk service.
- Public venue schemas and connection policies can change. The live publisher
  has typed parser fixtures and reconnect supervision, but should be canaried and
  alerted before its output is trusted for funded execution.
- Neither venue adapter implements REST open-order reconciliation after a
  private-stream event gap. Connection transport may reconnect, but the live
  quoting session fails stop and cannot automatically resume; there is also no
  durable restart reconciliation workflow.
- The 90-second socket watchdog measures any incoming frame, not independent
  liveness of each private subscription. When no order lifecycle transition is
  pending, traffic on one channel can mask silence on another; periodic REST
  reconciliation is not implemented.
- Lighter's six-hour private-auth rotation is a deliberate reconnect and thus a
  fail-stop event for the supplied live runtime, not a seamless long-running
  reauthentication cycle. Automated restart without first verifying a clean
  account is unsafe.
- Prometheus metrics and structured logs are present; distributed traces, alert
  routing, dashboards, and long-term storage are not bundled.
- Mainnet adapters have not been end-to-end validated with funded credentials in
  this repository. Unit tests and config validation cannot establish live venue
  correctness.

## License

Project code is MIT licensed; see [`LICENSE`](LICENSE). The optional official
Lighter signer artifact and Rust dependencies retain their upstream licenses; see
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

## Pump.fun / PumpSwap copy-trading research

The independent [`pump-copy`](pump-copy/README.md) crate adds wallet discovery and
rankings, large-buyer inflow signals, a hold-until-target paper strategy, historical
replay, parameter sweeps and training/holdout evaluation. Later work added independent
analysis of every alert, confirmed/finalized feed comparisons, bounded all-wallet
recording, balance receipts, and executed-transfer reconciliation. It has its own
Cargo manifest and lockfile. Funded transaction execution is not implemented.

The current research policy collects all wallets, then narrows the analysis.
Each signal is evaluated independently without shared position slots. Positions
hold until their net target is reached, with no stop loss or holding timeout.
As of October 6, 290 full transactions yielded 241 verified swaps and one
preliminary wallet candidate. This does not establish profitable copying:
unresolved event/reserve layouts still block target backtests on the latest
recordings. See the crate README for the complete results and limitations.

Run `cargo run --locked --manifest-path pump-copy/Cargo.toml -- --config pump-copy/config/default.toml validate`
from this directory, or see its README for collection and backtest commands.
