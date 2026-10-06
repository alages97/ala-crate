# Pump copy trader: research and paper engine

This independent Rust crate lives inside `ala-crate/pump-copy`. It implements durable live log capture, activity discovery, transaction-history acquisition, wallet balance audits, trader rankings, large-buyer flow signals, paper copy trading, replay, parameter sweeps, and training/holdout evaluation. It has its own lockfile and does not change the existing market-making binaries.

**Current stage: functional research and paper trading. Transaction construction, signing, submission and funded execution are not implemented.** There are no private-key inputs or live-order switches.

The exit rule is **sell the whole position when estimated net proceeds reach the configured multiple of all-in entry cost; otherwise keep holding**. There is no stop loss, maximum holding period, automatic write-off, or forced sale at the end of a dataset. A zero-recovery column is an accounting scenario; it does not close positions or replenish cash.

## Current status — October 6, 2026

The software can record all-wallet activity and independently verify supported
historical swaps from full transaction evidence. **Profitable copying has not been
demonstrated, and target backtesting of the October 5–6 recordings remains blocked
by unresolved financial event/reserve layouts.** Existing replay commands still
work on supported historical/synthetic inputs; raw identity-only records are not
valid prices or volumes.

| Area | Implemented / observed | Remaining boundary |
| --- | --- | --- |
| Collection | All Pump.fun/PumpSwap program logs, separate confirmed/finalized SQLite stores, bounded unattended launcher, original payloads and receipt timestamps | Logs are not a complete transaction/account-state archive; gaps and conflicts remain explicit |
| Discovery | Wallet/market activity census, known-schema volume/whale signals, candidate screening | Identity-only events contain no validated amounts; activity is not profitability or whale volume |
| Accounting | Balance receipts and Rust executed-transfer reconciliation, including pools with SOL on either side | Unsupported instructions/flows remain excluded; complete wallet/ATA history is not certified |
| Latest validation | 290 full transactions, 241 verified swaps, 115 validated pool/mint mappings | Sparse transaction evidence supplies no continuous executable quote history |
| Wallet audit | Four activity-selected wallets; one positive observed-window candidate | Retrospective, small sample; no wallet approved for funded copying |
| Strategy evaluation | Independent signal analysis, latency/cost diagnostics, replay/sweeps/holdout machinery | Latest capture has no eligible 1.5x/2x backtest; historical simulations are not actual fills |
| Execution | Research and paper only | No transaction construction, signing, submission or confirmation system |

The agreed research workflow is to **capture all wallets, then filter during
analysis**, and evaluate **every signal independently** without shared cash,
exposure or position-slot limits. The older portfolio simulator remains available
as a separate diagnostic. Keep unhit targets open and missing outcomes unknown.
Collector processes perform no model calls or scheduled analysis; no recurring
review is active. No paid RPC service or funded wallet has been used.

The latest recording finished on October 6 at 12:28 Singapore time. The latest
financial audit covers a bounded enrichment of that recording, not its entire
financial history. See [research findings](#research-history-and-findings) and
[evidence locations](#source-and-local-evidence-locations).

## Run locally

Rust 1.87 or newer. From the existing repository:

```sh
cd /Users/alages/Documents/Codex/2026-09-14/rust/ala-crate/pump-copy
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked -- validate
```

Or, from `ala-crate`, use `cargo run --locked --manifest-path pump-copy/Cargo.toml -- --config pump-copy/config/default.toml validate`.

Create an offline example and replay it:

```sh
cargo run --locked -- demo --output runs/demo
cargo run --locked -- --config runs/demo/demo.toml backtest \
  --input runs/demo/synthetic.jsonl --output runs/demo/backtest
```

The demo is explicitly synthetic: fictional wallets and tokens, a winner that graduates, and a position that remains open after crashing. It demonstrates behavior, not expected returns.

## Collect a historical universe

Supply an HTTP RPC URL through the environment. Public mainnet worked for the single-block smoke test; broad historical scans need a provider with sufficient archive access, throughput, and a known cost budget. Do not place API keys in source/configuration files or CLI arguments.

```sh
# Public endpoint for a small smoke test only:
export SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
cargo run --locked -- collect \
  --start-slot 451893882 --end-slot 451893882 --archive runs/archive
```

For your provider, set `SOLANA_RPC_URL` outside this tool, or use `--rpc-env NAME` to select another existing environment variable. No secret value is printed by RPC transport errors.

`collect` enumerates all produced slots with `getBlocks`, requests finalized blocks with full transaction metadata, and records:

- `raw/SLOT.json.gz`: gzip-compressed RPC block, retained for decoder changes and audits. Earlier uncompressed `.json` files remain readable.
- `blocks/SLOT.json`: normalized SOL-quoted Pump trades, ordered by transaction and event index.
- `pools.json`: immutable pool identity metadata. Current liquidity is never imported into historical observations.
- `coverage.json`: produced slots in completed capture ranges.

An HTTP 413 response can use a smaller ordered account inventory, then retrieve every successful transaction touching either target program. Lookup-table keys, transaction signatures, slot and ordering are checked. Original inventories and fetched payloads are cached; the assembled raw block explicitly marks this capture mode. Failed and unrelated transactions retain account/balance metadata, not full instructions/logs, in this mode.

Rerunning the same range reuses captured files. An unavailable produced block or decoding error stops capture instead of silently dropping it. Skipped Solana slots are distinguished from missing produced blocks by `getBlocks`. Coverage is committed in ranges of at most 32 slots. Checks reject missing recorded slots, a raw archive without a completed manifest, and files beyond its committed range. Retry the interrupted capture range before using it for research. Use one writer per archive directory.

Full blocks consume substantial network and disk space. This collector is a correctness-oriented bootstrap, not a scalable replacement for a transaction indexer. Loading/replaying an archive currently uses memory proportional to its size. For weeks of history, the next data adapter should deliver program-filtered transactions and retain the same normalized contract.

All observed wallets enter the research universe; collection does not start from a current winners list. An unbiased dataset must include unsuccessful coins and the trading history before the evaluation period.

## Persist a live discovery feed

Use an optimized build for collectors. HTTP requests are paced by `PUMP_COPY_RPC_INTERVAL_MS` (default 1200 ms); HTTP 429 responses honor numeric `Retry-After` values, bounded to 1–60 seconds. Limits are per process, so account for concurrent collectors.

```sh
cargo build --release --locked
export SOLANA_WS_URL=wss://api.mainnet-beta.solana.com
./target/release/pump-copy record-stream \
  --db runs/observations.sqlite --status runs/capture-status.json \
  --seconds 7200 --max-bytes 8000000000
```

The recorder subscribes to finalized logs for both programs. SQLite WAL storage retains compressed original notifications, receipt times, failed transactions, decoded events and decoding failures. Signatures are deduplicated across subscriptions. Session/reconnect records expose potential gaps. A separate SQLite capture lock prevents simultaneous recorders on the same database path. Status counters update transactionally, avoiding rescanning a growing database for each heartbeat. Ctrl-C stops cleanly even under a busy feed.

`record-stream --commitment confirmed` enables earlier research observations. The default remains `finalized`; each commitment must use a separate database. Repeated signatures with a changed slot or payload are retained in `stream_conflicts` rather than silently overwritten. The offline exporter excludes these conflicts. Confirmed observations still require reconciliation against a finalized shadow capture; the generic discovery snapshot alone does not perform that reconciliation or implement trading rollback.

`examples/latency_probe.rs` compares processed, confirmed and finalized notifications for both programs on one WebSocket. It uses a monotonic local receipt clock and matches signature plus slot, without relying on chain clock timestamps. It stores compact receipt metadata and three sample payloads, with a duration and byte cap. Startup and terminal observations are excluded from the matched timing cohort; missing later observations are reported, not classified as forks.

Duration and approximate database-size caps bound a run. Size is checked every 15 seconds, so the cap can be exceeded by traffic received during that interval; WAL and sidecar files add overhead. The logs are **discovery observations, not complete chronological replay data**: notification receipt order is not transaction order within a block, and reconnects or truncated logs can omit events.

In another terminal, resolve canonical pool identities and refresh activity/flow reports:

```sh
./target/release/pump-copy discover \
  --db runs/observations.sqlite --output runs/discovery \
  --follow-seconds 7200 --interval-seconds 120
./target/release/pump-copy stream-status --db runs/observations.sqlite
```

Omit `--follow-seconds` for one snapshot. Each snapshot freezes its input at a message-row boundary and shares the block decoder's event normalization. Pool lookup is bounded by `--max-pools`; unresolved pools are counted, not assumed canonical. Outputs include:

- `activity-candidates.json`: observed buy/sell activity and cash flow, prioritized for research. These are not certified profitable wallets.
- `flow-alerts.json`: historical threshold crossings on local receipt-time windows; distinct net-buying addresses count once.
- `current-flow.json`: the largest recent net-inflow windows, including those below the alert threshold. Windows state their observation time; stale feeds clear this output.
- `discovery.json`: snapshot totals, excluded events, rejection examples, freshness, candidates and signals. Its funded copy list remains empty.

Compare the feed with independently captured complete blocks:

```sh
./target/release/pump-copy verify-stream \
  --db runs/observations.sqlite --archive runs/verification-blocks \
  --output runs/stream-comparison.json
```

This compares multisets of supported swap identities and amounts, allowing genuine repeated events while detecting missing or extra ones. Matching a sampled block does not establish uninterrupted capture.

## Acquire and audit candidate histories

```sh
./target/release/pump-copy wallet-history \
  --wallet ADDRESS --db runs/history.sqlite \
  --since-ms START_UNIX_MS --until-ms END_UNIX_MS --max-transactions 1000
./target/release/pump-copy audit-wallet \
  --wallet ADDRESS --db runs/history.sqlite --output runs/audit.json
```

Standard RPC enumerates address signatures and retrieves full transactions, caching results for reuse. Its scope is direct address references; ATA-only transfers can be absent. Set an existing Helius RPC URL in the environment and add `--helius` to use `getTransactionsForAddress`, including associated token-account activity and failed transactions. Requests use explicit time bounds and transaction-version support. The transaction cap bounds returned payloads and signature pagination has a 100-page cap. An interrupted run remains incomplete. Reruns reuse payloads, but restart enumeration; increase the cap to extend the acquired range.

`import-history --archive PATH --db PATH` seeds the transaction cache from captured program transactions. This saves duplicate payload downloads but does not replace address-history enumeration.

The audit measures actual fee-payer expenses, failed fees, native SOL plus WSOL changes, supported swap amounts, unexplained extra outflows/inflows, and per-mint token balance mismatches. Wrapping SOL does not become income. It audits all cached history for that wallet and lists acquisition windows/completeness; aggregate reconciliation does not assume signature order is chain order. Cash flow is not investment P&L. Initial/open inventory, transfer origins, external venues and incomplete histories still require resolution. **Audits do not yet feed adjustments back into the swap-only ranking engine or certify funded eligibility.**

## Rank traders and inspect copying results

```sh
cargo run --locked -- rank --input runs/archive --output runs/ranking
cargo run --locked -- backtest --input runs/archive --output runs/backtest
```

`wallets.json` reports every observed trader and why it does or does not qualify. `copy-list.json` contains eligible candidates at the **end** of that sample. It is an output for inspection, never an input retroactively applied to the same backtest.

Wallet accounting uses average acquisition cost, allocates cost on partial sales, includes the network fee of each successful swap, and counts fully closed position cycles. Sell quantities beyond recorded inventory are flagged as unknown cost basis; affected wallets cannot qualify. Open inventory is charged at full cost and valued at zero for selection, so a wallet cannot qualify solely by selling winners while hiding large unresolved losers.

Selection also requires a minimum number of closed positions and distinct mints, sufficient observation time, positive profit and return after the open-position haircut, recent activity, and limited concentration in one profitable mint. Eligibility is checked before incorporating the current source trade. It changes as observations arrive; there is no hindsight-ranked static allowlist.

`copy-performance.json` reports **our simulated** entries, exits, realized P&L, open cost and zero-recovery P&L by source wallet. A `null` source wallet groups flow-only entries. This is the distinction between a trader making money and our delayed copying making money. Shared cash, exposure limits and competing signals affect these results; they are portfolio attributions, not isolated simulations of each leader.

Scores describe observed supported swaps, not a complete wallet audit. Transfers, trading on other venues, failed-transaction fees, separate tips, source-wallet account rent, airdrops, rewards and funding flows are not reconciled. A history window starting mid-position can exclude otherwise useful wallets. A zero-recovery score is conservative about observed inventory, not proof of net profitability across every wallet expense.

## Large-buyer flow signals

The rolling window groups trades by token mint, preserving continuity across graduation. It tracks purchase/sale value, net inflow, distinct buyers with large cumulative purchases, and a buy/sell imbalance. A buyer must still be a net buyer to count toward the large-buyer threshold. Repeated trades by one address count once. Alerts have a cooldown.

“Whale” means **large observed buying in this window**, not proven net worth. Different addresses are not necessarily different entities. This version does not detect common funders, bundles, insiders or wash trading.

The default is a 60-second window, at least three addresses buying at least 5 SOL each, at least 15 SOL net inflow, and a buy/sell ratio of at least 2:1. These are configurable starting values, not calibrated thresholds.

`entry_mode` can be `copy`, `flow`, `either` or `both`. Signals are emitted for inspection even when the configured entry mode does not trade them.

## Follow the chain in paper mode

```sh
cargo run --locked -- watch \
  --archive runs/watch --output runs/paper --seconds 300
```

New archives start at the latest finalized slot; `--start-slot` can request an earlier starting point. Existing archives replay history and continue from their last block, rebuilding the same deterministic simulated portfolio. New signals and simulated fills are JSON lines on stdout. The command also saves reports and exits after the requested duration or Ctrl-C. In-flight block processing can extend that duration.

This is a finalized-block observer with a delayed replay model. It is not a low-latency transaction execution feed. `latency_ms` is the assumed total source-trade-to-copy inclusion delay on the chain timeline; it must include the delay of the eventual feed and submission path. Values shorter than real finalization delay are hypothetical sensitivity scenarios. The `watch` output is simulated, not an executable quote at wall-clock arrival time.

## Backtesting and iteration

```sh
cargo run --locked -- sweep --input runs/archive --output runs/sweep \
  --targets-bps 15000,20000,30000,50000 --latencies-ms 1000,5000,15000
```

Targets are basis points of the all-in entry cost: `20000` means 2x. Latency is a measured execution constraint to stress-test, not a parameter that can simply be chosen to improve returns. To warm up rankings on old observations but trade only later data, set `strategy.trade_start_ms` or pass `--trade-start-ms` to `backtest`/`sweep`.

For a single chronological training/holdout fold:

```sh
# Substitute UNIX millisecond boundaries inside your collected history.
cargo run --locked -- walk-forward --input runs/archive --output runs/fold \
  --train-end-ms TRAIN_END_MS --test-end-ms TEST_END_MS \
  --targets-bps 15000,20000,30000,50000
```

This chooses the target using only training-period cash after valuing unresolved positions at zero. It keeps the configured latency fixed, then evaluates the selected target on the later holdout. Rankings warm up on prior observations; the holdout portfolio starts flat. Training positions remain open in their own report, never silently sold or carried into the independent holdout. No training entries means no target can be selected. Repeat with later boundaries for additional folds, and keep a final untouched period for your eventual strategy choice.

Every report includes:

- Cash, realized P&L, estimated liquidation equity and zero-recovery equity.
- Open positions with cost and mark availability, pending orders and rejected/expired entries.
- Drawdown measured from estimated liquidation equity.
- Signal/fill history, source-wallet copy results and the effective configuration.
- Explicit execution/accounting assumptions.

No same-block fills are allowed. A later block must contain a fresh trade for that mint, and the fill uses its end-of-block reserves. Real zero-proceeds dust sales are retained in source accounting with their fees. Quotes model size-dependent constant-product impact, observed fee rates, configurable adverse slippage, network fees and entry rent. The target is rechecked after exit latency; if it is no longer reachable, the position stays open. Completed curves wait for a PumpSwap observation before they can be sold. Stale, missing or unliquidatable marks have zero estimated value.

## Incomplete-log execution diagnostics

`observed-sweep` accepts a frozen alert/coverage JSON file and gzip JSONL quote snapshots. It is a separate research path from the coverage-checked block backtester:

```sh
cargo run --release --locked -- observed-sweep \
  --input runs/execution-input.json --quotes runs/execution-quotes.jsonl.gz \
  --output runs/target-delay-comparison
```

Input JSON contains `end_ms`, `coverage` intervals with `chain_start_ms`/`chain_end_ms`, and `alerts` with `mint`, `observed_at_ms`, `venue`, and `whale_buy_slots`. Each quote has `mint`, `received_ms`, `chain_ms`, `slot`, `signature`, `event_index`, `venue`, and the normalized `reserve` object. Intervals describe recording sessions; they do not prove complete coverage within a session. Quotes must be within the frozen cutoff and their receipt cannot precede their chain timestamp.

The grid compares 1.5x/2x/3x net targets, an additional 1/3/10-second submission delay, and a 1%/2% adverse haircut on each leg. It retains the measured feed lag. A modeled fill uses the first observed mint block in the next whole second after submission, within three seconds, without bridging a recorded gap. Unknown intra-block order uses the least favorable observed quote. Admission also limits buys to 1% of real quote reserves and sells to 1% of real token reserves under the default configuration.

Fill results are recognized only when the witness block arrives. Cash and capacity remain reserved until then; pending sell fees cannot be spent by competing orders. This receipt is a conservative acknowledgement proxy, not a measured confirmation of our own transaction. Failed sell attempts charge the assumed network fee, increase position cost, and require another observation after a five-second execution-time cooldown. Each attempt must still meet the net target at execution. No holding timeout or forced sale is introduced.

Unobserved entry outcomes lock their full intended cost as unknown inventory. Unobserved sells are assumed failed with a network fee. These are explicit adverse scenarios, not claims that those transactions occurred. End marks require a quote no more than 30 seconds old; missing marks are counted and valued at zero. `comparison.json` and `details.json` retain settings, uncertainty counts, cash reconciliation and every execution witness. Zero-recovery valuation does not liquidate holdings. All output is labeled `incomplete_log_execution_diagnostic` and cannot establish funded readiness.

`examples/verify_executions.rs` can check a bounded plan of quote witnesses against complete RPC blocks. It verifies signature/mint/reserve matches and recalculates the last observed swap quote in the block. It does not certify hypothetical inclusion, intervening liquidity changes, actual submission costs or unsampled fills. Cached blocks are reused.

## Independent analysis of every signal

Use `analyze-signals` to evaluate every alert without a shared bankroll, position slots or exposure cap:

```sh
cargo run --release --locked -- analyze-signals \
  --input runs/execution-input.json --quotes runs/execution-quotes.jsonl.gz \
  --output runs/all-signals
```

Each alert is replayed independently, including repeats on the same mint. `first_for_mint` permits a separate distinct-coin analysis. The original 18 timing/cost combinations are retained, with additional one-second-feed cases that accelerate entry only, exit observation and confirmation only, or both. Another diagnostic removes the extra slippage haircut, network fee and entry rent while keeping DEX fees and impact. These produce 33 scenarios per alert. No signal is skipped because another position remains open.

Faster-feed cases keep the original alert conditions and move their availability to one second after the latest chain event already known when that alert originally fired. They never move a signal ahead of its required information. This is a timing sensitivity study of fixed alerts, not a regeneration of the whale detector, a measured provider comparison or a claim about achievable execution. Matched comparisons must use the same signals with usable entries in each case; unknown fills must not become losses or be silently dropped from denominators.

`all-signals.json` includes every independent result, signal timestamps, cohort, entry/exit witnesses, and observed post-entry peak and horizon values. Path maxima are hindsight diagnostics and never trigger retroactive fills. Quotes after a modeled exit remain in the path diagnostic. Missing horizon marks remain null. A separate 100-SOL internal fee reserve per signal prevents accounting constraints; it is not an investment bankroll or a denominator for strategy returns. Unresolved positions at the capture boundary are censored observations, with zero recovery retained only as an explicit adverse valuation scenario.

## Transaction identity and graduation

The `compare-entry-rules` command accepts a JSON object with `end_ms`, `coverage`, `signals`, `rule_status` and `provenance`, plus the same gzip quote format as `analyze-signals`. Each signal contains the alert fields, `rule`, unique `signal_id` and optional `source_wallet`. It compares only net 1.5x and 2x targets, 1%/2% per-leg haircuts and a one-second additional submission delay, with every entry independent. Disabled audited-copy rules remain visible with zero eligible signals. Exploratory copying of nominees must be labeled separately from qualified copying.

`examples/screen_wallets.rs` nominates wallets using only a supplied earlier cutoff. Its relaxed 20-minute history screen is explicitly for choosing audits; it does not replace the default one-day history requirement. Ranking outputs label this `nomination_eligible` and keep `qualified_for_copy` false. Subsequent source history, initial inventory, external transfers, failed fees and balance mismatches must still be resolved.

The recorder stores one raw notification per transaction signature and decoded events under `(signature, event_index)`. A signature identifies a transaction, not a position. Pump.fun trade events provide a token mint directly. PumpSwap events provide a pool; verified pool metadata or a captured creation event resolves that pool to its base and quote mints. Only canonical SOL pools enter the supported dataset.

Trader inventory is grouped by `(wallet, mint)`, with average acquisition cost and partial-sale accounting. Market flow and our simulated position are grouped by mint, preserving continuity across graduation. Different wallet addresses are not assumed to share an owner. A matching buy and sell quantity is supporting evidence for a round trip, not proof that transfers or earlier inventory are absent.

Complete blocks provide `(slot, tx_index, event_index)` order. Stream receipt time records when information became available; it cannot establish transaction order within a slot. Raw logs alone do not contain complete transfer, rent, tip or fee-payer accounting, so observed swap cash flow remains separate from audited wallet P&L.

## Default position sizing

These are the **legacy portfolio simulator** defaults, not constraints on the
current independent-signal research workflow. `analyze-signals` and
`compare-entry-rules` do not share position slots or a bankroll across signals.

The default simulated bankroll is 10 SOL, each entry uses 0.1 SOL plus costs, and at most 2 SOL of acquisition cost or ten positions can remain exposed. An order must also fit within 1% of observed effective quote reserves. Unresolved losses keep using both cash and position capacity. There is one position or pending entry per mint.

All values in the normalized data are raw integer token amounts or lamports. Token decimals are not needed for reserve calculations. Statistical SOL display values are converted only when printing summaries.

## Protocol coverage and verification

The strict event-based pricing/replay path supports known-layout Pump.fun
bonding-curve SOL pairs and canonical graduated PumpSwap SOL pairs. Canonical
pools are checked against the Pump program's pool-authority PDA and index zero.
Non-SOL pairs and noncanonical pools are excluded from that path. Position
identity is the mint, so graduation does not split inventory into unrelated
assets. The newer transfer reconciler can verify non-SOL-token/SOL flows with SOL
on either side of a pool, but does not certify canonical-pool eligibility or
provide reserves for the pricing engine.

The decoder uses pinned official IDL event fields, checks emitting program identity, supports Anchor CPI events and complete log events, excludes failed transactions, and handles v0, v1 and legacy transactions through RPC JSON. It rejects truncated known events, unexpected trailing fields, missing metadata and event streams it cannot safely order. Older append-only event layouts are accepted only after their known core. Very old pre-creator-fee histories are not supported.

Fee allocation fields for buybacks, cashback and holder rewards are not counted again as additional trading fees. PumpSwap effective quote reserves include the event's virtual quote reserve adjustment.

As of October 6, validation passed on Rust 1.87 with **74 Rust unit/integration
tests**, all-target Clippy with warnings denied, and a release build. Separate
Python checks passed for the all-wallet launcher (3) and the external cohort audit
(5); the earlier fresh-pilot reporting checks (5) and end-to-end fixture also
passed. These are recorded validation results, not a claim that every historical
report was regenerated after each change.

The eight new transfer-reconciliation regressions cover both venues/sides,
reverse SOL pools, token amount/owner mutations, failed or truncated execution,
missing CPI stack information, unknown funding, explicit unmapped instructions,
independence from event amounts, and a native-sale fee/tip accounting oracle.
There were 271 raw-capture/full-RPC log comparisons with zero mismatches in the
latest audit; the other 19 fetched transactions were non-Pump wallet activity.

Earlier checks exercised CLI demo/replay/sweep, observed-log and independent-signal
diagnostics, and a real mainnet smoke capture at slot `451893882`. That block
produced 37 supported trades: 8 Pump.fun and 29 PumpSwap. No wallet qualified under
the defaults. The fixture includes real version-1 transactions and is retained
under `tests/fixtures` for offline regressions. These older supported layouts do
not establish that current unknown event layouts are safe to decode.

## Remaining work before funded copying

1. Resolve current event/reserve schemas or validate an independent market-state/quote path. The transfer fallback proves individual historical amounts, not executable prices over time. Do not promote identity-only records into financial data.
2. Expand bounded wallet audits, then freeze candidates and strategy rules before a future observation window. The current positive candidate is descriptive, not an out-of-sample selection. Preserve all-wallet capture while adding full transaction and market evidence where needed.
3. Upgrade the observer to a low-latency, full-transaction feed with bounded queues, complete reconnect/backfill and commitment/reorg handling. Compare public and paid service coverage, receipt timing, quote availability and cost on the same workload before deciding whether paid RPC is worthwhile. Faster notifications alone do not establish a trading edge.
4. Extend the implemented balance audit into complete cost-basis accounting and integrate its exclusions/adjustments into causal rankings. Resolve transfers, separate tips, external venues, creators, common funders and suspicious address clusters. The new fallback is not yet automatically integrated into the legacy wallet audit/ranker.
5. Implement exact venue quotes from fresh accounts and fee configuration, token-program/extension handling, transaction construction, signing, simulation, submission and confirmation. Include idempotent restart/reconciliation, stale-quote protection, actual slippage limits and a kill switch.
6. Compare model fills with observed execution. The present model sums fee rates and approximates venue rounding, sees liquidity changes only at trade events, assumes each hypothetical trade does not alter the later historical path, and does not simulate MEV or unsuccessful submitted transactions. A limit at the target must not be treated as a guaranteed fill. Evaluate 1.5x/2x targets independently, with no stop loss, holding timeout or forced end-of-data exit.

## Data-provider pilot

As checked on September 30, 2026, Helius lists Developer at $49/month with 10 million credits. Its current history reference meters full address history at 10 credits per 100 returned transactions, rounded up with a 10-credit minimum. One million full transactions in full pages would therefore use approximately 100,000 history credits; market-path backfills and other requests add usage. Current streaming billing is 2 credits per 0.1 MB of **uncompressed** data. The pricing FAQ and credit reference contain some inconsistent legacy details; verify current dashboard terms before subscribing.

A one-minute public-feed measurement captured about 91 MB of unique uncompressed notifications, projecting to roughly 109,000 streaming credits/hour at that rate. Duplicate notifications were excluded, so this is a lower-bound projection for the broad two-program feed, not measured Helius usage or a monthly guarantee. These are dated planning estimates, not current purchase terms. The later collection decision is to retain all-wallet raw capture and narrow during analysis; any selective enrichment is additional evidence, not a replacement for that universe. No paid service was purchased, and the latest full-transaction audit used the public endpoint with requests spaced at least five seconds apart.

Success means persistent source-wallet performance and positive delayed-copy performance on untouched data after fees, impact, slippage and unresolved holdings. A profitable-looking activity list or working decoder is not evidence of an edge. The current observations are too short for that decision.

## References

- [Official Pump program documentation](https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/docs/PUMP_PROGRAM_README.md)
- [Official PumpSwap documentation](https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/docs/PUMP_SWAP_README.md)
- [Canonical pool creator derivation](https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/docs/PUMP_SWAP_CREATOR_FEE_README.md)
- [Holder rewards: existing creator-fee fields remain authoritative](https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/docs/HOLDER_REWARDS_README.md)
- [Official Pump SDK](https://www.npmjs.com/package/@pump-fun/pump-sdk), inspected version 2.0.0
- [Official PumpSwap SDK](https://www.npmjs.com/package/@pump-fun/pump-swap-sdk), inspected version 1.20.0
- [Solana getBlock](https://solana.com/docs/rpc/http/getblock), [RPC JSON structures](https://solana.com/docs/rpc/json-structures), [Anchor CPI events](https://www.anchor-lang.com/docs/features/events)

Schema provenance: official `pump-fun/pump-public-docs` commit `cb188ce08b5069196eef1f3e4a0c43b70099793b`. The files under `schema/` are the relevant event/type definitions extracted from that revision. Refresh them deliberately and rerun fixture/accounting checks when protocol layouts change.

- [Helius pricing](https://www.helius.dev/pricing), [history API](https://www.helius.dev/docs/rpc/gettransactionsforaddress), [credit reference](https://www.helius.dev/docs/billing/credits)


## Targeted unattended research capture

`cargo run --release --example targeted_capture -- --output DATA_DIR --status STATUS_JSON --wallets WALLETS_JSON --pools POOLS_JSON` records raw address-filtered observations for a saved shortlist. `WALLETS_JSON` is an array of public addresses and `POOLS_JSON` uses the existing pool-identity schema. Set `SOLANA_WS_URL` to the WebSocket endpoint.

The default bounds are 24 hours, a 12-hour new-market admission window, 64 watched markets, 8 GB of local capture files and 40 GB of received WebSocket text. A watched wallet's observed buy starts subscriptions for its known mint or unresolved pool. Both confirmed and finalized observations are retained in separate SQLite databases. Subscription acknowledgements, admission limits and reconnect gaps are recorded in `subscriptions.jsonl`. The market-data cap limits observation coverage; it does not simulate a trading-position limit. New wallet messages are still retained when market follow-up admission is capped.

This mode does not rank wallets, calculate live strategy outcomes, invoke a model, schedule Codex reviews, sign orders or submit transactions. A stopped collection leaves open hypothetical positions unresolved. Address-filtered capture is not complete market discovery or proof of full token-account history.

`repair_pools` and `audit_cached` are bounded enrichment/offline-audit examples. Pool metadata recovered after a capture is explicitly retrospective and must never substitute current reserves for historical quotes. Address-filtered subscriptions follow the [Solana logsSubscribe API](https://solana.com/docs/rpc/websocket/logssubscribe).


When live events contain unknown trailing fields, the strict trade decoder continues to reject them. Targeted capture can separately parse only wallet/market identifiers for routing subscriptions, with `_capture_identity_only` metadata and the unknown tail length. Those records are stored in `capture_identity_events`, never promoted to priced trades. The capture status explicitly exposes strict decode failures and identity-only routing. `recover_capture_identities` can recover routing hints from saved raw notifications; it cannot reconstruct market follow-up that was never recorded. `--seed-markets` starts a new bounded capture segment from saved routing hints without overwriting the original capture.

For an offline activity census of a stopped all-program recording, run `cargo run --release --example inspect_raw_capture -- CAPTURE_DB POOLS_JSON WATCH_WALLETS_JSON OUTPUT_DIR`. It scans successful raw notifications, excludes conflicting signatures and the all-zero placeholder, and exports wallet/market identities and counts. The watchlist only annotates the output; it does not filter the scan. These are activity counts, not volume or profit rankings. Unknown financial layouts remain rejected; the identity export contains no amounts or reserves and cannot establish target-hit rates. The input must be closed and checkpointed because the example opens an immutable SQLite snapshot.

## Bounded all-wallet recording

Build once with `cargo build --release --locked`, then run:

```sh
python3 scripts/record_all.py --hours 3 --output /path/to/new/raw-capture --status-dir /path/to/new/capture-status
```

Use `--until 23:00 --timezone Asia/Singapore` instead of `--hours` to stop at today's specified time. Past times are rejected. `--dry-run` prints the plan without starting collectors or creating files. Both directories must be new; omitting `--status-dir` keeps status beside the data. The launcher snapshots the executable/configuration, runs both confirmation levels, and stops at the absolute deadline, 32 GB aggregate capture limit, or 8 GB free-space reserve. Bounds are configurable. It uses the public Solana endpoint unless `SOLANA_WS_URL` is set and never writes endpoint credentials to the plan. It performs no analysis, model calls, scheduling or trading.

The all-program Rust recorder now counts **consecutive unsuccessful sessions**, resetting after at least 60 seconds subscribed with recent notifications. It retains total disconnects for diagnostics. Retries use 10–60 second exponential backoff, respecting a longer numeric HTTP `Retry-After`; the capture deadline also interrupts connection attempts, sends and backoff. Ten consecutive failures still stop the recorder. A subscribed global feed without notifications for 90 seconds reconnects and records a coverage gap. The address-filtered `targeted_capture` example has a separate retry implementation; the new launcher uses `record-stream` for all-wallet capture.

Messages are timestamped on local WebSocket delivery before JSON parsing. Database commits batch up to 64 messages or 250 ms; shutdown flushes the pending batch. An abrupt process kill can lose that uncommitted batch. These timestamps do not measure NIC arrival or on-chain execution. Status includes per-session disconnect categories, HTTP/RPC codes, last notification time and the maximum database flush duration. Provider URLs, response bodies and close-reason text are excluded from diagnostics. Storage bounds include SQLite WAL/SHM files.

## Balance evidence when event decoding is unsupported

```sh
target/release/pump-copy balance-receipt --input saved-getTransaction.json --wallet PUBLIC_ADDRESS --output receipt.json
```

This offline command accepts a full `getTransaction` JSON result or its JSON-RPC response wrapper. It reads native/token pre/post balances, loaded account addresses and the actual fee payer without interpreting Pump event amounts. Raw token deltas are decimal strings. It rejects missing owners, duplicate balance indices and inconsistent account arrays; ownership changes or incomplete token-account snapshots are flagged. Separate native+WSOL and native+owned-token-account-lamport measures make wrapping and rent deposits visible without double-counting WSOL backing.

`audit-wallet` and `audit_cached` now preserve this evidence when an event schema or pool scan cannot be decoded. Unsupported swap cashflow is `null`, its decode error is retained, and incomplete accounting is not reported as a fee-only loss. Balance changes can still include transfers, funding, rent and multiple swaps; they do not establish trade-level prices, cost basis or P&L. Unknown event layouts remain blocked from strategy pricing, and no funded execution is enabled.

## Verified transfers when the event schema is incomplete

```sh
target/release/pump-copy reconcile-transaction --input saved-getTransaction.json --output reconciled.json
cargo run --release --example reconcile_transactions -- /path/to/transaction-directory /path/to/reconciled-batch.json
```

The Rust transfer reconciler derives historical swap amounts independently of Pump event financial fields. It checks compiled instruction roles against token ownership, checks SPL transfers against full token balance changes, and reconstructs native lamport movements for supported Pump.fun trades. A native swap is accepted only when remaining debits belong to its known Pump endpoints and other residual credits belong to accounts passed to that invocation. Unknown funding sources, caught inner failures, truncated execution logs, inconsistent CPI stacks, unsupported token extensions and ambiguous flows are rejected. Published Pump V2 instruction roles are supported; unpublished V3 instruction identities remain visibly unaccounted for.

PumpSwap pools with SOL on either side are normalized to buying/selling the non-SOL coin. Network fees are recorded with the actual payer and must be charged once per transaction. Explicit transfers outside a native swap remain separate, and the fallback does not claim a complete LP/protocol fee breakdown. Instruction source hashes and account roles are pinned in `schema/transfer-reconciliation.json`.

The result supplies observed token amounts and cash flows, **not reserves, wallet-profit certification or an executable backtest archive**. The strict event decoder is unchanged. A successful sample does not authorize accepting unknown event bytes in other transactions: every fallback result needs that transaction's complete execution and balance evidence. Unknown inventory, associated-account coverage, non-swap flows and price-history coverage must still pass their own checks before strategy evaluation.

The fallback is exposed through `reconcile-transaction` and the batch example.
It does not silently replace `audit-wallet`'s event-based swap totals or the
ranking engine. The October 6 cohort report uses a separate Python audit over
these explicit reconciliation outputs; its scripts and cached evidence are
listed below. Rejected/unmapped records and incomplete transaction accounting
must remain visible even when other swaps in that transaction are verified.

## Research history and findings

### September 30: bootstrap, strategy diagnostics and prospective pilot

- Built the independent Rust research/paper crate, strict decoder, complete-block
  collector, trader rankings, flow signals, target-only simulator, parameter
  sweeps, and chronological training/holdout evaluation. Captured 600 produced
  blocks containing 23,731 supported trades; this short archive did not yield a
  qualified copy-wallet list.
- Added durable live logs, transaction-history caching and wallet balance audits.
  The larger discovery snapshot contained 2,183,006 supported trades from 144,996
  wallet identifiers and 13,144 mints. Initial inventory and unexplained transfers
  made positive observed cash flow an unreliable shortcut to trader profitability.
- Replaced capacity-limited research comparisons with independent analysis of all
  **59 alerts across 49 mints**, using **33 scenarios per alert (1,947 evaluations)**.
  No signal was skipped for occupied positions. Timing sensitivities separate
  faster entry from faster exit observation; lower-cost scenarios separate costs
  from the chosen signals. All reuse inspected data and are exploratory.
- The recorded finalized-feed alert lag had a 9.734-second median, using chain
  event timestamps of one-second resolution. A separate five-minute same-socket
  probe measured median processed-to-confirmed receipt difference of 123 ms and
  confirmed-to-finalized difference of 8,438 ms. These measure receipt differences,
  not source inclusion to our own fill, and do not prove latency caused the
  strategy's underperformance.
- Tested a frozen prospective baseline flow rule with 1.5x primary and 2x secondary
  net targets. Fourteen alerts were observed; two were excluded for unverified
  dependencies. Of 12 evaluated signals, 10 had known modeled entries over nine
  coins, one entry was unknown and one buy failed. The primary rule had two exits
  and eight positions still open. Its conservative zero-recovery valuation of
  known entries was -0.698723239 SOL; this is not realized loss or forced liquidation.
  Too few coins, missing evidence and nonpositive conservative results left the
  pilot **inconclusive**. Three earlier wallet audits also had incomplete histories;
  no strategy or wallet was promoted. The scheduled review was deleted.
- Saved real raw RPC messages and transaction-link examples. Signatures link
  notifications to full transactions; mint identity links venue changes through
  graduation. Within-slot chain order requires block transaction order, not local
  receipt order.

### October 5: protocol drift and all-wallet recording

Live trade events contained eight trailing bytes absent from the pinned official
IDL. Added a separate identity-only extraction path for capture routing and
activity census while keeping the strict financial decoder unchanged. Recovered
148 identities from 272 early targeted messages and seeded 42 markets; missing
earlier market follow-up could not be reconstructed from those messages alone.
Targeted capture remained an available research tool, but subsequent recording
followed the all-wallet preference.

The October 5 all-wallet recording observed about 2.469 million trade identities,
237,137 wallets and 15,734 markets. Its old cumulative reconnect limit stopped
confirmed collection around 21:50 and finalized collection around 22:53 Singapore
time, short of the intended 23:00 deadline. Confirmed receipts arrived after
their finalized matches in 8.01% of matched successful observations. These
findings motivated recorder reliability work; identity counts alone could not
produce profits, volume rankings or target-hit rates.

### October 6: recorder repairs and completed three-hour capture

Repaired the all-program recorder and added regression checks for:

- Receipt timestamps before JSON parsing/persistence and SQLite batches of up to
  64 messages or 250 ms, including graceful shutdown flush.
- Bounded connection/subscription waits, heartbeat/stall detection, reconnect
  backoff, and deadline interruption during connection, sends and retries.
- A limit on **consecutive** failed sessions, reset after a healthy session,
  instead of a cumulative disconnect limit.
- Conflict retention, sanitized diagnostic codes and explicit coverage gaps.
- A launcher with new-directory checks, pinned executable/config/script hashes,
  absolute deadline, aggregate storage/free-space bounds and no model calls.

The replacement run completed **09:28–12:28 Singapore time**, with zero disconnects
on either feed. Final counts were **10,739,688 confirmed** and **10,684,145 finalized**
stored messages. The raw databases totaled **23,672,410,112 bytes** after shutdown.
Zero disconnects is not proof of complete ledger coverage; duplicate conflicts
and unsupported financial decoding remain separately recorded.

The detailed activity census used an **interim cutoff around 12:07**, not the
whole completed three-hour run: 2,810,587 trade identities, 257,104 wallets,
14,970 markets and at least 10,129 known mints. There were 82,059 wallet identifiers
in both the October 5 and October 6 censuses. All 2.81 million trade identities
still had unknown trailing bytes; none became a strictly accepted priced trade.

A uniform sample of 50,000 notifications per feed yielded 30,322 matched successful
observations. Confirmed receipt preceded finalized by a median **8,323 ms** and
p95 **8,954 ms**; no sampled confirmed receipt arrived after its finalized match.
This differs from October 5's full-feed comparison in date, sample and market
conditions, so it is not a controlled estimate of the code change's effect or
the value of a paid provider.

### October 6: full-transaction accounting and wallet audit

Fetched 120 transactions chosen without financial outcomes across venues, sides
and routes. Of 150 observed trade identities, **129 swaps were verified**,
**17 known-instruction cases were explicitly rejected**, and **four identities
remained unmapped**. The verified set included 59 swaps in pools with reverse
SOL orientation. Sampled `BuyExactQuoteInV3` / `SellV3` instruction discriminators
were absent from the fetched published IDL and were not guessed.

Separately selected four wallets by repeated activity before computing their
financial outcomes, then fetched 170 full transactions and verified 112 swaps.
The cohort still uses activity from the evaluation window, so its results are
**descriptive, not out of sample**. Address-history windows were completed;
complete passive associated-token-account history was not certified.

Inventory was reconciled by token account, native balance continuity was checked,
failed transaction fees were retained, and a same-slot ordering ambiguity was
resolved using ordered block signatures. The conservative cash measure below
deducts network fees and all other native outflows, and excludes other inflows.
Other outflows can include funding/transfers, so a negative value is not by
itself a proven trading loss. Open holdings are unvalued, not written off.

| Wallet | Verified swaps | Closed / open markets | Conservative observed-window cash outcome (SOL) |
| --- | ---: | ---: | ---: |
| `3fRRrQx4Ai4rbDPnStgqgzAg2bDKcrdMftBYYHs326zx` | 30 | 15 / 0 | -0.155143260 |
| `CHzNxiDnfpjC6iwfCurswe1itXH4jzbcqik9YFzG3kre` | 27 | 9 / 0 | -4.104636709 |
| `J4YZv17nyZn7caU4tAqXmGJkqpc44A322aULdaXc7qe9` | 24 | 8 / 0 | +0.535833744 |
| `5bKGAcspbrTJ5K1JQSb2J9DwUr79iWD6zoaJWbqUaNhi` | 31 | 4 / 1 | Unresolved; one market remains open |

`CHz...` had 39 failed transactions and 5.733581324 SOL of other native outflows;
their purpose is not classified. `J4YZ...` had 0.576465386 SOL of verified swap
cash flow, less 0.008240024 SOL of network fees and 0.032391618 SOL of other outflows.
Independent native-plus-owned-account asset deltas matched the cash result for
all three flat wallets. `J4YZ...` is a **candidate for future observation only**,
not a certified profitable whole wallet or proof of profitable delayed copying.

Combined enrichment exported **241 verified swaps from 290 full transactions**
and **115 pool/mint mappings**. Canonical-pool status is not implied by these
mappings. None of the raw-only events was promoted to a financial quote, and
**no 1.5x/2x target backtest was performed on this enriched sample**: it lacks a
continuous validated price path and a causal candidate-selection window.

## Source and local evidence locations

The working source is `/Users/alages/Documents/Codex/2026-09-14/rust/ala-crate/pump-copy`.
Git is initialized at its parent `ala-crate`, covering both independent crates.

| Source | Role |
| --- | --- |
| `src/stream.rs`, `scripts/record_all.py` | Durable program-log recording and bounded dual-feed launcher |
| `src/rpc.rs`, `src/history.rs` | Block/transaction acquisition, caching and legacy wallet audit |
| `src/decoder.rs`, `schema/` | Strict event decoding, identity-only capture support and pinned instruction/schema evidence |
| `src/balances.rs`, `src/reconcile.rs` | Schema-independent balance receipts and executed-transfer verification |
| `src/discovery.rs`, `src/wallets.rs` | Activity/flow discovery and supported-swap wallet screening |
| `src/engine.rs`, `src/research.rs`, `src/observed/` | Portfolio replay, sweeps, holdout and independent-signal diagnostics |
| `examples/`, `tests/`, `tests/fixtures/` | Bounded research utilities, regression checks and small public transaction fixtures |

Raw research data and run-specific Python helpers remain **outside the Git
repository** under `/Users/alages/Documents/Codex/2026-09-30/use-x20/`.
The paths below are machine-local evidence, not files supplied by a Git clone:

| Location relative to that research directory | Contents |
| --- | --- |
| `work/research-archive/`, `work/live-observations.sqlite`, `work/wallet-history.sqlite` | September 30 block archive, live log recording and address-history cache |
| `work/all-wallets-2026-10-05/`, `work/all-wallets-2026-10-06/` | Original all-wallet confirmed/finalized SQLite recordings |
| `work/financial-repair-2026-10-06/` | Sample/cohort full transactions, pinned downloaded IDLs, acquisition scripts, `audit_cohort.py`, its five regression checks, and report/export helper |
| `work/run-state.json`, `outputs/research-decision.json` | Research status and decision records; historical sections retain their original date/scope |
| `outputs/rpc-message-example.json`, `outputs/transaction-link-example.json` | Actual RPC notification and transaction-link explanation |
| `outputs/all-signal-analysis/`, `outputs/entry-rule-comparison/`, `outputs/latency-pilot/` | Independent alert scenarios, entry-rule comparisons and receipt timing evidence |
| `outputs/fresh-pilot/` | Frozen plan, prospective capture and inconclusive completion review |
| `outputs/all-wallets-2026-10-05/analysis/`, `outputs/all-wallets-2026-10-06/analysis/` | Identity censuses and delivery diagnostics; October 6 census is interim |
| `outputs/collector-fixes-2026-10-06/` | Recorder validation and frozen build artifacts |
| `outputs/financial-repair-2026-10-06/` | Latest findings, cohort audit, selection/acquisition plans, reconciliation exports, validation and evidence hashes |

Direct links to the latest evidence:

- [Financial findings](/Users/alages/Documents/Codex/2026-09-30/use-x20/outputs/financial-repair-2026-10-06/findings.json)
- [Four-wallet audit](/Users/alages/Documents/Codex/2026-09-30/use-x20/outputs/financial-repair-2026-10-06/cohort-audit.json)
- [Validation record](/Users/alages/Documents/Codex/2026-09-30/use-x20/outputs/financial-repair-2026-10-06/validation.json)
- [Completed recording status](/Users/alages/Documents/Codex/2026-09-30/use-x20/outputs/all-wallets-2026-10-06/run.json)

The versioned `outputs/financial-repair-2026-10-06/pump-copy-source.zip` and
`kit/bin/pump-copy` are the source/build snapshots from that audit. They predate
this README update and Git initialization. Older recording kits and source ZIPs
are retained as historical artifacts; use the repository working tree for new
development. Hash manifests describe the frozen artifacts they accompanied,
not subsequent edits to the working tree.

To reproduce an offline swap check, build this crate and pass a saved full
transaction to `reconcile-transaction`. To reproduce the complete cohort report,
also retain the external acquisition plans, transaction caches, block-order
evidence and audit helper. A source checkout alone does not recreate the dataset.
