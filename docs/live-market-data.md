# Live market-data publisher

`live-md` is a credential-free process that connects to the four official
mainnet public BBO feeds configured for the BTC strategy and publishes protocol
v1 NDJSON on a bounded TCP fanout socket.

## Build and run

```bash
cargo build --locked --release --bin live-md

./target/release/live-md \
  --config config/live-market-data.toml \
  --validate-only

./target/release/live-md \
  --config config/live-market-data.toml
```

No API keys, account identifiers, signer library, or live-trading acknowledgement
are used by this process. The default TCP endpoint is `127.0.0.1:9101`; metrics
are available at `http://127.0.0.1:9900/metrics`.

To inspect normalized frames without running an execution process:

```bash
cargo run --locked --bin md-tail
```

The tail client validates framing, schema, timestamps, and sequencing, and
reconnects automatically. Pass `--json` to print protocol-v1 NDJSON instead of
compact human-readable BBO lines. It is read-only and never connects to an
exchange. New consumers wait for the next genuine update because the publisher
does not replay prior frames.

To retain the validated stream for offline analysis, use a fresh output name:

```bash
./target/release/md-tail \
  --address 127.0.0.1:9101 \
  --output data/market-data/capture.ndjson \
  --quiet \
  --duration-seconds 3600
```

The capture file is opened with create-new semantics, then flushed and
synchronized on a clean timed or signal-driven exit. `--quiet` requires
`--output`. The file contains the same protocol-v1 frames that passed the
client's schema, sequence, and age checks; it has no completion trailer, so
retain the process exit status alongside long research captures.

The supplied Hyperliquid and Lighter quoter configs both consume the default TCP
endpoint. Start `live-md` first, verify all four `market_data_feed_up` series and
`market_data_publisher_ready`, then start either trading process separately.

## Upstream feeds

The endpoints are compiled in and cannot be redirected to testnet through the
config file:

- Binance USD-M: `wss://fstream.binance.com/public/ws/<symbol>@bookTicker`
- Bybit linear: `wss://stream.bybit.com/v5/public/linear`, topic
  `orderbook.1.<symbol>`
- Hyperliquid: `wss://api.hyperliquid.xyz/ws`, subscription type `bbo`
- Lighter: `wss://mainnet.zklighter.elliot.ai/stream?readonly=true`, channel
  `ticker/<market_index>`

The sample maps Binance and Bybit to `BTCUSDT`, and Hyperliquid and Lighter to
their native `BTC` perpetual. Before every Lighter WebSocket connection, the
publisher queries the official public metadata endpoint and refuses to subscribe
unless configured market index `1` is an active `BTC` perpetual.

The implementation follows the venue documentation for
[Binance book ticker streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#individual-symbol-book-ticker-streams),
[Bybit level-one order books](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook),
[Hyperliquid BBO subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions),
and [Lighter ticker channels](https://apidocs.lighter.xyz/docs/websocket-reference).

## Ordering, latency, and backpressure

One random `stream_id` is retained for the publisher process lifetime. Each
market has its own checked, strictly increasing sequence that survives upstream
and downstream reconnects. A publisher restart creates a new stream ID. Genuine
exchange timestamps are never clamped or fabricated; stale, future, or regressing
updates are rejected before sequence allocation.

Raw venue updates are kept in one latest-value slot per feed and published at
most once per `runtime.publish_interval_ms`. Coalescing happens before sequence
allocation. All connected consumers therefore receive the same encoded frame
and sequence, while intermediate raw updates can be intentionally coalesced.

One slow TCP consumer cannot block venue readers or other consumers. Each client
has a write deadline and maximum frame-queue age. A lagging client is disconnected
and must reconnect; skipped globally sequenced frames then appear as a sequence
gap in that consumer. New clients wait for the next genuine venue update rather
than receiving a replay with a fabricated timestamp.

The WebSocket supervisors implement venue keepalives, BBO and socket inactivity
deadlines, per-operation handshake/write deadlines, capped exponential reconnect
backoff, and a planned Binance rotation before its maximum connection age. A
feed is not marked up until one of its BBOs passes normalization and enters the
shared fanout. A stream that supplies only rejected stale, future, or regressing
BBOs is forced to reconnect after the configured BBO deadline.

Stop execution processes and confirm their orders are canceled before stopping
market data. Protocol v1 has no shutdown control message, so an execution process
that loses this TCP feed withdraws quotes through its normal freshness guard.
