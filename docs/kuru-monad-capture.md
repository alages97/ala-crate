# Kuru/Monad research capture

`kuru-record` is a read-only, live-mainnet recorder for MON price discovery and
arbitrage research. It keeps raw public Kuru, Monad, Uniswap v4, Binance, and
Bybit payloads in one append-only NDJSON stream and timestamps them with one
process-local monotonic clock.

The recorder has no order gateway, private key, wallet signing, transaction
broadcast, or trading path. Kuru Flow authentication uses a fixed read-only
address only to request public quotes; the short-lived bearer token is kept in
memory and is never written to the capture.

## Build and run

From the `ala-crate` directory:

```bash
cargo build --locked --release --bin kuru-record

./target/release/kuru-record \
  --output data/kuru-monad/capture.ndjson
```

The default timed capture is six hours. Importantly, that timer begins only
after all seven required sources have produced a valid data record, not when the
process starts. The default warm-up deadline is 120 seconds.

The main controls are:

```text
--duration-seconds <SECONDS>       default 21600 (six hours; maximum 24 hours)
--warmup-timeout-seconds <SECONDS> default 120
--flush-interval-ms <MILLISECONDS> default 1000
--channel-capacity <RECORDS>       default 65536
--quote-interval-ms <MILLISECONDS> default 1200; minimum 1100
--quote-notional-usdc <USDC>       default 100
```

`--quote-interval-ms` is applied independently to each HTTP quote source. It is
at least 1,100 ms because the Kuru Flow token currently advertises a
one-request-per-second allowance. Each quote source alternates a fixed-USDC buy
with a sale of the exact MON output from that buy; the two directions are
therefore not simultaneous.

Use `Ctrl-C` for an orderly stop. The recorder flushes and synchronizes the
partial artifact before exiting.

## The seven sources

The raw upstream JSON is retained under `payload`; the recorder does not
normalize price or quantity fields in place.

| `source` | Live-mainnet input | Recorded `stream` values | Purpose |
| --- | --- | --- | --- |
| `kuru_mon_usdc_book` | Kuru `frontendOrderbook` WebSocket for market `0x065C9d28E428A0db40191a54d33d5b7c71a9C394` | `order_book_snapshot`, `order_book_update` | Native MON/USDC CLOB plus integrated-AMM book and events |
| `kuru_flow_mon_usdc_quote` | Kuru Flow HTTP quote API | `usdc_to_mon`, `mon_to_usdc` | Aggregate executable-route indication for the configured size |
| `uniswap_v4_mon_usdc_quote` | Monad JSON-RPC `eth_call` to the official Uniswap v4 V4Quoter | `usdc_to_mon`, `mon_to_usdc` | Direct quote for the native MON/USDC 5 bp v4 pool |
| `binance_mon_perp` | Binance USD-M combined WebSocket for `MONUSDT` | `depth20`, `agg_trade` | External perpetual order book and trades |
| `bybit_mon_perp` | Bybit linear public WebSocket for `MONUSDT` | `orderbook`, `trade` | Second external perpetual order book and trades |
| `binance_usdc_usdt_spot` | Binance spot `USDCUSDT@bookTicker` | `book_ticker` | USDC/USDT conversion needed to compare on-chain and CEX prices |
| `monad_blocks` | Monad mainnet WebSocket `eth_subscribe(newHeads)`, chain ID 143 | `new_heads` | Chain-head timing, block number, and base-fee context |

Trade messages from Binance and Bybit are recorded, but readiness is based on
their first valid order-book message. The capture similarly waits for a valid
Kuru book, successful Flow and Uniswap quotes, a USDC/USDT book ticker, and a
Monad block head before starting the six-hour timer.

Kuru Flow is an aggregate router, not necessarily an independent venue. Its
route may include Kuru itself or another Monad liquidity source. Preserve its
returned route/build data and inspect the eventual calldata before classifying
any Flow-versus-Kuru difference as arbitrage.

## Files and completion semantics

For a requested output `data/kuru-monad/capture.ndjson`, recording starts in:

```text
data/kuru-monad/capture.ndjson.part
```

On successful completion of the requested timed duration, the recorder
atomically renames that file to the requested `.ndjson` path and writes:

```text
data/kuru-monad/capture.manifest.json
```

If the process is stopped, warm-up expires, or a supervisor fails, the partial
file is deliberately not given the completed filename. It remains:

```text
data/kuru-monad/capture.ndjson.part
data/kuru-monad/capture.ndjson.part.manifest.json
```

The manifest records `termination_reason`, `termination_detail`, readiness and
deadline timestamps, requested and captured durations, completion status, and
per-source data/status counts. A partial capture can still be inspected, but an
analysis must not silently treat it as a complete six-hour sample. The recorder
refuses to start if any of the requested, partial, or corresponding manifest
paths already exist.

## NDJSON record contract

Each line is one schema-versioned object:

```json
{
  "schema_version": 1,
  "capture_id": "kuru-monad-...",
  "local_sequence": 1,
  "record_type": "data",
  "source": "kuru_mon_usdc_book",
  "stream": "order_book_snapshot",
  "connection_generation": 1,
  "receive_unix_ns": 0,
  "receive_monotonic_ns": 0,
  "decode_done_monotonic_ns": 0,
  "writer_dequeue_monotonic_ns": 0,
  "decode_latency_ns": 0,
  "writer_queue_latency_ns": 0,
  "payload": {}
}
```

`record_type = "data"` denotes a market, quote, or block observation.
`record_type = "status"` records connection, subscription, authentication,
disconnect, retry, and recorder-health lifecycle events. A
`recorder_health:source_stale` record is emitted if a required source produces
no usable data for 30 seconds, followed by `source_recovered` when it resumes.
`connection_generation` increments on
each source reconnect. `local_sequence` is assigned by the single file writer
in channel-receive order; it is deterministic capture order, not proof of
exchange or block-level causality.

### Timestamps and latency

- `receive_unix_ns` is diagnostic system wall time at WebSocket frame receipt,
  or at completed HTTP response-body decoding for quote records.
- `receive_monotonic_ns` is the corresponding elapsed time from one shared
  process-local `Instant` origin. Use this field for within-capture ordering and
  lead/lag calculations.
- `decode_done_monotonic_ns` is stamped after the upstream JSON has been
  decoded and the record prepared.
- `writer_dequeue_monotonic_ns` is stamped when the single writer removes the
  record from its bounded channel.
- `decode_latency_ns` is
  `decode_done_monotonic_ns - receive_monotonic_ns`.
- `writer_queue_latency_ns` is
  `writer_dequeue_monotonic_ns - decode_done_monotonic_ns`.
- Flow quote payloads also contain `request_started_unix_ns`,
  `request_started_monotonic_ns`, `response_body_received_unix_ns`,
  `response_body_received_monotonic_ns`, `response_decoded_monotonic_ns`,
  `request_duration_ns`, and `response_decode_latency_ns`.
- Uniswap quote payloads contain the request-start and response-decoded clocks
  plus `request_and_decode_duration_ns`; the quote itself includes its explicit
  Monad block tag and full JSON-RPC response.

Monotonic timestamps are comparable only within one capture process. Unix time
is useful for joining external logs, but clock adjustment can make it unsuitable
for causal latency measurement. Decode and writer-queue latency measure local
pipeline health. Flow request duration stops when the response body has arrived;
Uniswap request-and-decode duration additionally includes local ABI decoding.
Both include network and public endpoint/server/RPC time. None measures
matching-engine, validator, or colocated execution latency.

## Units and instrument identity

- Kuru frontend-book prices are raw integers scaled by `10^18`. Book sizes use
  this market's `size_precision` of `10^10`. Divide the raw `b`/`a` price and
  size entries by those values for USDC per MON and MON respectively. The
  market's on-chain order-entry `price_precision` is `10^8`; do not apply that
  precision to the raw frontend WebSocket prices.
- Native MON and WMON use 18 decimal places. Monad USDC at
  `0x754704Bc059F8C67012fEd69BC8A327a5aafb603` uses 6 decimal places. Flow and
  Uniswap quote amounts are decimal strings in raw token units, so divide MON
  or WMON by `10^18` and USDC by `10^6`.
- The fixed default buy input is 100 USDC, encoded as `100000000` raw units.
  The following `mon_to_usdc` request uses the exact output of that source's
  preceding `usdc_to_mon` quote.
- Binance and Bybit `MONUSDT` payloads retain venue-native decimal strings:
  prices are USDT per MON and sizes are MON. Their exchange timestamps retain
  the venues' documented units inside the raw payload.
- Binance `USDCUSDT` prices are USDT per USDC; bid/ask quantities are USDC.
- Monad block header numbers, timestamps, fees, and related numeric values are
  retained in JSON-RPC hex encoding.

Native MON, WMON, spot MON, and a MON perpetual are economically related but
not identical instruments. Analysis must model wrapping, settlement, funding,
basis, collateral, and transfer/rebalancing constraints rather than treating
their displayed numbers as frictionless substitutes.

## Why Uniswap v4 is the direct atomic competitor

The Kuru MON/USDC market and the canonical Uniswap v4 native MON/USDC pool live
on the same Monad chain. A purpose-built transaction can, in principle, execute
both swap legs in one transaction and revert the whole transaction unless the
post-fee, post-gas result exceeds a minimum profit. That makes Uniswap v4 the
direct venue for testing an atomic Kuru-versus-DEX route.

Binance and Bybit are still valuable price-discovery and hedge references, but
a Monad transaction and a CEX order cannot commit atomically. Monetizing a CEX
lead/lag therefore requires pre-positioned inventory, two independent fills,
and explicit exposure to network delay, rejection, partial fill, basis, funding,
and rebalancing risk.

Kuru Flow is useful as an aggregate benchmark but is not automatically the
other leg of an arb: it can select Kuru or Uniswap liquidity itself. The raw
Uniswap v4 quoter call isolates a named on-chain pool and is therefore the
cleaner direct comparison. The current recorder only observes quotes and books;
it does not build, simulate, or submit a two-venue atomic transaction.

## Analysis caveats

- A visible spread is not PnL. Apply Kuru fees, Uniswap LP fees, gas and priority
  fees, Flow/router costs, CEX fees, expected slippage, and inventory-rebalancing
  costs before declaring an opportunity.
- Kuru book updates and Uniswap/Flow quotes are observations at different
  instants. Alternating buy/sell polls are also separated by the configured
  interval. Require a conservative freshness window and do not combine stale
  legs as though they were simultaneous.
- Each Uniswap `eth_call` is pinned to the block number from the latest Monad
  head received by this process. It is still only a simulation, not a reserved
  fill or guarantee of inclusion. A block number alone is not a reorg-proof
  state commitment; retain the matching captured head hash during analysis.
- Public WebSocket, REST, and RPC receipt order includes geography, provider
  routing, batching, and serialization. It cannot by itself establish which
  venue a colocated strategy would observe first.
- Monad's transaction ordering and competing searchers matter. A newer chain
  does not imply that a displayed edge is accessible or durable.
- USDC and USDT are not assumed equal. Convert CEX prices through the captured
  `USDCUSDT` bid/ask in the direction actually traded.
- CEX perpetual prices include basis and funding. Comparing them with native
  spot can identify price discovery or a hedge candidate, but it is not an
  atomic spot arbitrage.
- Order-book depth, quote output, and router response data should be evaluated
  at one common trade size. Top-of-book gaps alone overstate executable edge.
- Reconnect periods and `connection_generation` boundaries require explicit
  handling. Do not forward-fill across a disconnect without a strict freshness
  cap.

Treat the first pass as a feasibility screen. A positive screen should be
followed by block-pinned quote replay, contract-level route simulation, revert
and gas modeling, and only then a deliberately bounded execution design.
