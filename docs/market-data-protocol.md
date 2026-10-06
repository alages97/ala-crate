# Market-data IPC protocol v1

The quoter connects as a TCP client to the separate market-data publisher. The
publisher sends one UTF-8 JSON object per line. A line must be no larger than
65,536 bytes, excluding its trailing newline.

The first supported message is a best-bid-and-offer update:

```json
{"type":"bbo","schema_version":1,"environment":"live","venue":"binance","symbol":"BTCUSDT","bid_price":"99999.9","bid_qty":"0.12500","ask_price":"100000.0","ask_qty":"0.25000","exchange_timestamp_ms":1789372800000,"publisher_timestamp_ms":1789372800001,"sequence":812345,"stream_id":"binance-btcusdt-4f71"}
```

Prices and quantities are JSON strings, not numbers. This is intentional: it
keeps every value exact from publisher to tick/lot validation.

Fields:

- `schema_version`: must be `1`.
- `environment`: `live` for exchange-sourced data or `synthetic` for development
  publishers. The live quoter fails closed if it receives `synthetic` data.
- `venue`: `binance`, `bybit`, `hyperliquid`, or `lighter`.
- `symbol`: the source venue's native symbol configured by the strategy.
- `bid_price`, `bid_qty`, `ask_price`, `ask_qty`: positive decimal strings.
  Locked books are accepted; crossed books are rejected.
- `exchange_timestamp_ms`: source exchange event time in Unix milliseconds.
- `publisher_timestamp_ms`: time the market-data process published this line,
  in Unix milliseconds.
- `sequence`: strictly increasing for each
  `(venue, symbol, stream_id)` logical stream.
- `stream_id`: stable for one publisher/session stream and changed whenever a
  publisher restarts its sequence.

The consumer applies three wall-clock admission limits from its `[market_data]`
configuration:

- `max_publisher_lag_ms`: maximum accepted age of `publisher_timestamp_ms`.
- `max_exchange_lag_ms`: maximum accepted age of `exchange_timestamp_ms`.
- `max_future_skew_ms`: maximum accepted amount either remote timestamp may be
  ahead of the consumer clock. Zero is a strict no-future-skew policy.

An age or future skew exactly equal to its limit is accepted; a value greater
than the limit is rejected. The supplied configs use 500 ms publisher lag,
2,500 ms exchange lag, and 1,000 ms future tolerance. These are deployment
defaults, not universal latency SLOs. Both publisher and consumer hosts should
be clock-synchronized.

Timestamp admission happens before sequence observation and before delivery to
the strategy. A rejected frame therefore cannot refresh the quoter's source
cache or advance the client's stored sequence. If a later accepted frame skips
over it, the missing sequence is subsequently visible as a forward gap.

Rejected timestamp frames increment `market_data_invalid_frames_total` with
`reason="stale_publisher_timestamp"`, `reason="stale_exchange_timestamp"`, or
`reason="future_timestamp"`. The structured warning identifies which timestamp
was in the future; the counter intentionally keeps only the common future reason
to limit label cardinality.

The client retains ordering state across TCP reconnects. A duplicate or
regressing sequence within one stream is discarded. A forward gap is delivered
but reported by the `market_data_sequence_gaps_total` counter so the strategy can
continue to use top-of-book data while operators investigate.

Rotating `stream_id` establishes a new sequence baseline, but it does not reset
market time ordering. `publisher_timestamp_ms` and `exchange_timestamp_ms` must
each be nondecreasing across all accepted frames for the same `(venue, symbol)`,
including across stream rotations. Equal timestamps are accepted; a rollback is
rejected before any ordering state or strategy cache is updated. The exact
`market_data_invalid_frames_total` reasons are
`non_monotonic_publisher_timestamp` and
`non_monotonic_exchange_timestamp`.

Ordering memory is bounded for one client process to 1,024 distinct
`(venue, symbol)` market keys and 4,096 distinct
`(venue, symbol, stream_id)` stream keys. Existing keys continue to work at the
limit, but a frame introducing a new key is rejected rather than growing memory.
The exact invalid-frame reasons are `market_capacity_exceeded` and
`stream_capacity_exceeded`. There is no eviction; restart planning must account
for the cumulative number of stream IDs a long-running publisher creates.

The TCP server does not receive subscriptions in v1. The publisher decides
which configured feeds are sent on the socket. Strategy-side source filters
discard unrelated messages. Authentication and private execution events must
never share this channel.

`environment` is an operator guard, not authentication. The publisher endpoint
is a trusted live-trading boundary: bind it to loopback or protect the network
path so an untrusted process cannot inject prices while claiming `live`.
Timestamp limits likewise reject obviously stale or skewed data but do not prove
the publisher or prices are authentic.

The consumer's `[market_data]` config object is strict: unknown keys are startup
parse errors. The same fail-fast rule applies to `[quoter]`,
`[quoter.instrument]`, and every `[[quoter.reference_sources]]` entry.
