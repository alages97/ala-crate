# Telemetry

Each process exposes Prometheus text format at `/metrics`. Execution processes
use `telemetry.metrics_listen_address`; the market-data publisher uses its
top-level `metrics_listen_address`. The supplied configs bind only to loopback:

- Hyperliquid: `http://127.0.0.1:9898/metrics`
- Lighter: `http://127.0.0.1:9899/metrics`
- Live market data: `http://127.0.0.1:9900/metrics`

Keep the endpoint private. The embedded listener does not provide authentication
or TLS. Logs are structured by `tracing`; verbosity comes from
`telemetry.log_filter` in the TOML config. The shipped binary permanently caps
legacy dependency `log` records at `debug`: tungstenite's `trace` output contains
complete WebSocket frames, and a Lighter subscription frame carries its bearer
token. Native `wide_quoter` tracing remains configurable, but dependency TRACE
payload logging is intentionally unavailable. Embedders that construct the
gateway outside this binary must enforce the same restriction and must never log
raw private WebSocket frames.

## Latency path

The measurements intentionally expose overlapping stages rather than claiming a
single additive end-to-end number:

```text
exchange timestamp
  -> MD publisher -> TCP consumer/parse -> fair value -> quote decision
  -> strategy command -> OMS queue -> gateway sign/HTTP acknowledgment
  -> private WS parse -> accept/reject/fill/cancel-terminal -> OMS
```

Durations measured inside one process use monotonic `Instant` clocks. Metrics
derived from exchange or publisher Unix timestamps require NTP-synchronized
hosts and accurate upstream timestamps; clock skew can dominate them. All
histograms record seconds. The exporter emits classic Prometheus histogram
buckets at 10, 25, 50, 100, 250, and 500 microseconds; 1, 2.5, 5, 10, 25, 50,
100, 250, and 500 milliseconds; and 1, 2.5, 5, 10, 30, and 90 seconds, plus
`+Inf`. The `_bucket` queries below therefore aggregate correctly across
processes and scrape targets.

Before recording wall-clock lag or observing sequence, the consumer rejects a
frame older than `market_data.max_publisher_lag_ms` or
`market_data.max_exchange_lag_ms`, or one whose remote timestamp is ahead by
more than `market_data.max_future_skew_ms`. Rejected frames never enter the
consumer queue or quoter cache.

## Live publisher metrics

These series are emitted by `live-md` before data reaches an execution process:

| Metric | Type | Meaning | Labels |
| --- | --- | --- | --- |
| `market_data_feed_ws_connect_seconds` | histogram | Public WebSocket handshake | `venue` |
| `market_data_feed_ws_write_seconds` | histogram | Public-feed subscription, heartbeat, or pong write | `venue`, `operation` |
| `market_data_feed_metadata_seconds` | histogram | Lighter public market-identity validation | `venue` |
| `market_data_feed_ws_parse_seconds` | histogram | Venue message decode and BBO validation | `venue` |
| `market_data_feed_receive_to_publish_seconds` | histogram | Local socket receipt through coalescing and publication | `venue` |
| `market_data_feed_exchange_to_publisher_lag_seconds` | histogram | Nonnegative venue event-time to publisher-clock lag | `venue` |
| `market_data_feed_origin_to_publisher_lag_seconds` | histogram | Optional matching-engine/transaction time to publisher clock | `venue` |
| `market_data_feed_exchange_clock_ahead_seconds` | histogram | Venue event clock ahead of the publisher but within its admission tolerance | `venue` |
| `market_data_feed_origin_clock_ahead_seconds` | histogram | Optional origin clock ahead of the publisher | `venue` |
| `market_data_publisher_normalize_seconds` | histogram | Timestamp admission, sequence allocation, and wire serialization | `venue` |
| `market_data_publisher_client_queue_seconds` | histogram | Encoded fanout frame waiting for one TCP writer | — |
| `market_data_publisher_client_write_seconds` | histogram | One complete NDJSON frame write | — |
| `market_data_feed_up` | gauge | A current connection generation has produced an accepted BBO | `venue` |
| `market_data_publisher_ready` | gauge | Every enabled feed is up | — |
| `market_data_feed_last_received_timestamp_seconds` | gauge | Local wall time of the most recent valid raw BBO | `venue` |
| `market_data_feed_last_published_timestamp_seconds` | gauge | Publisher timestamp of the most recent fanout BBO | `venue` |
| `market_data_publisher_clients` | gauge | Current connected TCP consumers | — |
| `market_data_publisher_mailbox_entries` | gauge | Occupied per-feed latest-value slots, bounded by four | — |
| `market_data_feed_updates_total` | counter | Valid raw upstream BBO messages | `venue` |
| `market_data_feed_reconnects_total` | counter | Public-feed reconnect cycles by bounded cause | `venue`, `reason` |
| `market_data_feed_duplicate_updates_total` | counter | Exact upstream cursors intentionally suppressed | `venue` |
| `market_data_publisher_coalesced_updates_total` | counter | Valid raw BBOs replaced by a newer BBO before the next publication tick | `venue` |
| `market_data_publisher_rejected_updates_total` | counter | Stale, future, regressing, clock-rollback, or stale-generation BBOs rejected before sequencing | `venue`, `reason` |
| `market_data_publisher_frames_total` | counter | Globally sequenced live BBO frames | `venue` |
| `market_data_publisher_bytes_total` | counter | Encoded NDJSON bytes including delimiters | `venue` |
| `market_data_publisher_rejected_clients_total` | counter | TCP clients rejected at capacity or socket setup | `reason` |
| `market_data_publisher_client_disconnects_total` | counter | Slow, lagged, timed-out, or failed TCP writers | `reason` |

The `*_clock_ahead_seconds` histograms separate small host-clock differences
from ordinary nonnegative lag. Values beyond `runtime.max_future_skew_ms` are
rejected and counted instead. Alert on publisher readiness, reconnects, rejected
updates, client disconnects, and `time() -
market_data_feed_last_published_timestamp_seconds`, not only average latency.

## Core metrics

| Metric | Type | Meaning | Labels |
| --- | --- | --- | --- |
| `market_data_parse_latency_seconds` | histogram | JSON decode, validation, and sequence check for one frame | `source` |
| `market_data_publisher_to_consumer_lag_seconds` | histogram | Consumer wall clock minus publisher timestamp | `source`, `venue`, `symbol` |
| `market_data_exchange_to_consumer_lag_seconds` | histogram | Consumer wall clock minus exchange event timestamp | `source`, `venue`, `symbol` |
| `market_data_consumer_queue_latency_seconds` | histogram | Parsed-frame ingress through dequeue by the strategy loop | `source`, `venue`, `symbol` |
| `quoter_fair_value_latency_seconds` | histogram | Fresh-source scan and weighted-mid calculation | `venue`, `symbol` |
| `quoter_quote_decision_latency_seconds` | histogram | Full quote evaluation, including fair value, guards, rounding, and reprice policy | `venue`, `symbol` |
| `strategy_to_oms_latency_seconds` | histogram | Start of the triggering evaluation through acceptance of both desired-side updates by the OMS | `venue`, `symbol` |
| `oms_queue_latency_seconds` | histogram | Time an OMS command waits in its actor queue | `venue` |
| `oms_submit_latency_seconds` | histogram | OMS gateway-submit future through its acknowledgment/error | `venue` |
| `oms_cancel_latency_seconds` | histogram | OMS gateway-cancel future through its acknowledgment/error | `venue` |
| `oms_submit_to_private_accept_latency_seconds` | histogram | OMS submit dispatch through the first matching private `Accepted` event | `venue` |
| `oms_submit_to_private_reject_latency_seconds` | histogram | OMS submit dispatch through a matching private `Rejected` event when no private accept was previously observed | `venue` |
| `oms_submit_to_first_fill_latency_seconds` | histogram | OMS submit dispatch through the first matching private fill, whether or not `Accepted` arrived first | `venue` |
| `oms_cancel_to_terminal_latency_seconds` | histogram | OMS cancel dispatch through a matching terminal private event | `venue`, `outcome` |
| `gateway_exchange_event_lag_seconds` | histogram | Local receipt/normalization time minus venue event timestamp | `venue` |
| `oms_execution_event_latency_seconds` | histogram | Normalized event timestamp through OMS consumption; venue-originated for order/fill transitions and locally generated for connectivity transitions | `venue`, `kind` |

`strategy_to_oms_latency_seconds` ends when desired state is accepted by the
actor; it does **not** include venue acknowledgment. Likewise, a successful
Lighter HTTP `sendTx` response confirms transaction ingress, while its private
stream remains authoritative for order state. The submit-to-private histograms
measure that second phase. `oms_cancel_to_terminal_latency_seconds` is emitted
only after a cancel was dispatched; its current `outcome` values are `canceled`,
`cancel_rejected`, `filled`, and `rejected`, covering terminal races as well as a
normal cancellation.

## Venue gateway timing

| Metric | Venue | Meaning | Extra labels |
| --- | --- | --- | --- |
| `gateway_connect_seconds` | Hyperliquid | Exchange client and metadata connection | — |
| `gateway_sign_and_submit_seconds` | Hyperliquid | SDK sign plus submit round trip | — |
| `gateway_cancel_roundtrip_seconds` | Hyperliquid | SDK cancel round trip | — |
| `gateway_market_discovery_seconds` | Lighter | Live symbol/precision/minimum discovery | — |
| `gateway_signer_initialization_seconds` | Lighter | Shared-library client creation and key validation | — |
| `gateway_nonce_queue_seconds` | Lighter | Wait to enter the serialized nonce lane | — |
| `gateway_sign_seconds` | Lighter | Native signer invocation | — |
| `gateway_http_roundtrip_seconds` | Lighter | `sendTx` HTTP round trip | `operation=submit|cancel` |
| `gateway_ws_connect_seconds` | Both | Private WebSocket connection handshake | — |
| `gateway_ws_parse_seconds` | Both | Private WebSocket JSON decode | — |

These scopes overlap with OMS latency. For example,
`gateway_sign_and_submit_seconds` is contained within
`oms_submit_latency_seconds`; do not add them together.

## Health, integrity, and outcome metrics

| Metric | Type | Meaning | Labels |
| --- | --- | --- | --- |
| `gateway_private_stream_up` | gauge | `1` only when the private execution stream is ready, otherwise `0` | `venue` |
| `strategy_execution_ready` | gauge | Level-triggered startup/execution gate: `1` is ready; a post-start transition to `0` is fatal to the supplied live runtime, not a resumable pause | `venue` |
| `market_data_reconnects_total` | counter | TCP reconnect attempts after EOF/read/connect failure | `source`, `reason` |
| `market_data_invalid_frames_total` | counter | Rejected framing, schema, BBO, timestamp, ordering, or tracker-capacity frames | `source`, `reason` |
| `market_data_sequence_gaps_total` | counter | Number of missing sequence values, not number of gap events | `source`, `venue`, `symbol` |
| `market_data_consumer_queue_drops_total` | counter | Valid BBOs discarded because the bounded consumer queue was full | `source`, `venue`, `symbol` |
| `quoter_stale_source_total` | counter | Evaluation encountered a missing or expired configured source | `target_venue`, `source_venue`, `source_symbol`, `reason` |
| `quoter_no_quote_total` | counter | A guard suppressed/withdrew quotes | `venue`, `symbol`, `reason` |
| `quoter_replace_rate_limited_total` | counter | An otherwise-actionable replace or post-cancel re-entry was suppressed by the local monotonic quote interval | `venue`, `symbol`, `reason=min_quote_interval` |
| `oms_execution_events_total` | counter | Normalized private events consumed | `venue`, `kind` |
| `oms_ignored_execution_events_total` | counter | Wrong-venue, missing-ID, unknown-ID, or fail-closed reconnect events ignored | `venue`, `reason` |
| `oms_duplicate_fills_total` | counter | Repeated active-order `(client_order_id, trade_id)` fills discarded by the bounded in-memory deduper | `venue` |
| `oms_execution_event_lag_total` | counter | Number of private events skipped because the OMS execution receiver lagged; this immediately revokes desired quotes and initiates fail-stop cancellation, and despite its name is not a time duration | `venue` |
| `oms_actor_failures_total` | counter | OMS actor task panics/join failures surfaced to the runtime as fatal connectivity loss | `venue` |
| `oms_execution_fault_halts_total` | counter | Actor-level permanent live-session halt after a private disconnect, gateway failure, private rejection, cancel rejection, or unexpected cancel | `venue`, `reason` |
| `oms_fill_halts_total` | counter | Actor-level halt latched on the first owned partial or final fill; this is authoritative even if the outer runtime is awaiting another OMS command | `venue`, `side` |
| `oms_unexpected_cancellations_total` | counter | Private cancellations received without an active OMS cancel request; the affected side is blocked and the supplied runtime fail-stops | `venue` |
| `strategy_fill_fail_stops_total` | counter | Owned partial or final fills published after the OMS has latched its configured fill halt, then consumed by the supplied runtime; use `oms_fill_halts_total` as the authoritative halt count | `venue`, `side` |
| `gateway_orders_submitted_total` | counter | Submit requests acknowledged by a gateway | `venue` |
| `gateway_submit_errors_total` | counter | Submit failures categorized by adapter | `venue`, `kind` |
| `gateway_events_total` | counter | Normalized private events emitted by Hyperliquid | `venue` |
| `gateway_cancel_requests_total` | counter | Cancel requests acknowledged for ingress by Hyperliquid or Lighter; terminal state still comes from the private stream | `venue` |
| `gateway_private_stream_reconnects_total` | counter | Private-stream reconnect cycles; a reconnect does not resume the failed quoting session | `venue` |
| `gateway_private_lifecycle_timeouts_total` | counter | A tracked order waited 30 seconds without its required private acceptance, cancel-terminal update, or authoritative fill reconciliation | `venue`, `kind` |
| `gateway_ws_invalid_messages_total` | counter | Private messages that could not be normalized | `venue`, `channel` |
| `gateway_ws_ignored_messages_total` | counter | Valid private messages ignored because they belong to another market or cannot belong to an order tracked by this process | `venue`, `reason` |
| `gateway_orphan_fills_buffered` | gauge | Hyperliquid fills awaiting an order-ID mapping from a racing acknowledgement/update | `venue` |
| `gateway_orphan_fills_rejected_total` | counter | Hyperliquid orphan-fill capacity breaches rejected by fail-closing the private stream instead of evicting a possibly owned fill | `venue` |

Counters are adapter-specific where noted; absence of one venue's series is not
zero activity for another metric with different semantics.

On the first rate-limited material reprice, an existing desired quote is
withdrawn immediately instead of being left exposed until replacement is
allowed. Later evaluations remain flat until the interval elapses; post-cancel
re-entry behaves the same way. Cancellation itself bypasses the guard. Because
a suppressed replacement never reaches the OMS as a submit, it contributes no
downstream submit-latency sample (the immediate withdrawal can still contribute
cancel latency).

### Fail-stop execution policy

A private-stream disconnect, closed OMS execution-event channel, or nonzero
increment to `oms_execution_event_lag_total` is a quoting-session failure. The
supplied live runtime enables the actor's permanent execution-fault latch. The
OMS marks execution unavailable, revokes all desired quotes, and begins
canceling tracked orders before notifying the strategy; every later desired
revision is rejected and a `Reconnected` transition is ignored for execution.
The process then exits through cancel-and-confirm with an error. Treat a later
`gateway_private_stream_up = 1` as transport recovery for observation and
terminal events, not permission to resume quoting; neither adapter reconciles
open orders over REST after a gap. The latch increments
`oms_execution_fault_halts_total` once with a `reason` of `submit_failure`,
`cancel_failure`, `private_rejection`, `unexpected_cancel`,
`cancel_rejection`, or `private_disconnect`.

The supplied live runtime configures `halt_on_any_fill = true`. On the first
owned partial or final fill, the serialized OMS latches `halted_after_fill`,
atomically clears every desired slot at the actor level, begins canceling
remaining owned orders, and rejects every later `set_desired` command with
`HaltedAfterFill`. Only then does it publish the `Fill` event. This prevents a
queued market-data decision from restoring a quote between fill handling and
cancellation. The actor increments `oms_fill_halts_total{venue,side}` at latch
time. When the supplied strategy consumes the later event, it also increments
`strategy_fill_fail_stops_total{venue,side}`; that outer counter can be absent if
a concurrently awaited OMS command returns the already-latched error first.
Either path exits and completes cancel-and-confirm.

`halt_on_any_fill` defaults to `false` for generic OMS users. An inventory-aware
consumer may leave it disabled and implement its own position and post-fill
quote policy; the supplied binary does neither.

A private rejection, cancel rejection, or cancellation without an active OMS
cancel request leaves a nonempty `StateChanged.last_error`; an unexpected
cancellation also increments `oms_unexpected_cancellations_total{venue}`. The
supplied runtime treats that state as fatal and cancels both sides. Operators
must stop or revoke the bot before manually canceling one of its orders, rather
than creating an out-of-band cancellation during an active session.

Both private adapters send 30-second keepalives and disconnect after 90 seconds
without an incoming socket frame. A separate 30-second per-order lifecycle
deadline covers private acceptance, cancel-terminal delivery, and authoritative
fill reconciliation. Its expiry increments
`gateway_private_lifecycle_timeouts_total{venue,kind}`, takes readiness down, and
emits a disconnect even if the socket itself had other traffic. Current `kind`
values are `private acceptance`, `cancel terminal update`, and
`authoritative fill reconciliation`.

Lighter rotates its default seven-hour private auth token by reconnecting after
six hours. On every ready connection it max-merges the server's next nonce into
its local serialized nonce lane, so reconnect cannot regress a locally advanced
nonce. That deliberate reconnect is still a fail-stop event for the supplied
runtime and does not authorize transparent quote resumption. Six hours is
therefore the maximum continuous Lighter session: restart only after
cancel-and-confirm completes and an operator independently verifies that the
account has no leftover bot orders and any position is understood.

The level-triggered readiness flag also closes a separate startup race. A client
of the generic OMS may explicitly issue a desired revision while initially
unready and let it wait for first readiness. The supplied live runtime starts
only after gateway startup readiness, and never uses a post-gap reconnect to
recreate revoked quotes.

Ordering-related `market_data_invalid_frames_total` reasons are
`non_monotonic_sequence`, `non_monotonic_publisher_timestamp`,
`non_monotonic_exchange_timestamp`, `stream_capacity_exceeded`, and
`market_capacity_exceeded`. Time-admission reasons remain
`stale_publisher_timestamp`, `stale_exchange_timestamp`, and `future_timestamp`.
The tracker caps are 4,096 stream keys and 1,024 market keys; frames introducing
new keys after those caps fail closed.

OMS fill deduplication keeps at most 65,536 active
`(client_order_id, trade_id)` identities, evicts the oldest identity at capacity,
and clears all identities for an order at terminal state. The set is in memory
only and provides no restart-to-restart duplicate suppression.

## Scrape example

```yaml
scrape_configs:
  - job_name: wide-quoter
    static_configs:
      - targets:
          - 127.0.0.1:9898
          - 127.0.0.1:9899
```

Useful starting queries:

```promql
# p99 OMS submit acknowledgment latency by venue
histogram_quantile(
  0.99,
  sum by (le, venue) (rate(oms_submit_latency_seconds_bucket[5m]))
)

# p99 private accept latency after submit dispatch
histogram_quantile(
  0.99,
  sum by (le, venue) (
    rate(oms_submit_to_private_accept_latency_seconds_bucket[5m])
  )
)

# p99 cancel-to-terminal latency, retaining the terminal outcome
histogram_quantile(
  0.99,
  sum by (le, venue, outcome) (
    rate(oms_cancel_to_terminal_latency_seconds_bucket[5m])
  )
)

# p99 exchange-to-consumer market-data lag by source venue
histogram_quantile(
  0.99,
  sum by (le, venue) (rate(market_data_exchange_to_consumer_lag_seconds_bucket[5m]))
)

# any sequence values missed over five minutes
sum by (source, venue, symbol) (
  increase(market_data_sequence_gaps_total[5m])
)

# private stream not ready (fatal after the live session starts)
gateway_private_stream_up != 1

# BBOs dropped because the strategy consumer could not keep up
sum by (source, venue, symbol) (
  increase(market_data_consumer_queue_drops_total[5m])
)

# rejected market-data frames by cause
sum by (source, reason) (
  increase(market_data_invalid_frames_total[5m])
)

# actor-authoritative fills that stopped a non-inventory-aware quoting session
sum by (venue, side) (
  increase(oms_fill_halts_total[5m])
)

# private order transitions that violated OMS expectations
sum by (venue) (
  increase(oms_unexpected_cancellations_total[5m])
)

# tracked order lifecycle deadlines exceeded
sum by (venue, kind) (
  increase(gateway_private_lifecycle_timeouts_total[5m])
)

# internal task or orphan-fill integrity failures
sum by (venue) (
  increase({__name__=~"oms_actor_failures_total|gateway_orphan_fills_rejected_total"}[5m])
)

# actor-latched execution faults by cause
sum by (venue, reason) (
  increase(oms_execution_fault_halts_total[5m])
)
```

At minimum, page immediately on private stream down, strategy execution not
ready, any OMS execution-fault or fill halt, OMS actor failure or execution-event
lag, unexpected cancellation, orphan-fill capacity rejection, or lifecycle
timeout; these are fail-stop session errors. Also alert on invalid frames,
sequence gaps, consumer queue drops, indeterminate gateway errors in logs,
shutdown failure, and sustained high p99 market-data/submit latency. Choose
thresholds from measured baselines; the repository does not assume a network
region or latency SLO.

## Interpretation cautions

- A missing time series usually means that path has not executed yet, not that
  the value is zero.
- Stale frames and frames beyond the allowed future skew are rejected before
  lag histograms, sequence state, or strategy caching. A timestamp that is ahead
  but still within `max_future_skew_ms` is accepted, but its corresponding lag
  histogram sample is omitted because negative lag is not recorded.
- Timestamp rejection uses the consumer's wall clock. Monitor clock
  synchronization independently; a local clock step can reject otherwise valid
  data. The `future_timestamp` metric reason covers either publisher or exchange
  time, while the structured warning names the offending field.
- Sequence is strict within `(venue, symbol, stream_id)`, while publisher and
  exchange timestamps are independently nondecreasing per `(venue, symbol)`
  across stream rotations. Ordering and capacity failures are rejected before
  the strategy cache; the bounded tracker is not an unbounded stream-ID archive.
- OMS fill deduplication is bounded and session-local. A process restart has no
  durable fill-ID memory, and reaching the bound evicts the oldest retained
  identity.
- `min_quote_interval_ms` and `quoter_replace_rate_limited_total` describe one
  process only. They do not coordinate an account, API key, or Lighter nonce lane
  across processes, and the monotonic state resets at restart. Use one active
  writer per account/key lane and restart only after cancel-and-confirm plus
  independent open-order and position verification.
- The OMS shutdown deadline is not a hard process deadline. It records timeout
  but allows all already-dispatched gateway futures to settle before fallback,
  avoiding abandonment of a Lighter nonce submission. Lighter serializes its
  nonce lane, and each failed transaction can use both a `sendTx` and sequential
  `nextNonce` request. For the supplied two-slot runtime, give an external
  supervisor at least `shutdown_timeout_ms + 12 * request_timeout_ms`, plus
  operational margin, before any hard kill. The native signer itself has no
  enforced call timeout; validate the real deployment and keep an independent
  venue-side cancel path.
- The 90-second private socket watchdog is reset by any incoming frame; it does
  not independently prove that every subscribed orders/fills channel is live.
  The 30-second lifecycle deadline covers expected order transitions, but an
  otherwise quiet resting-order channel has no periodic REST reconciliation.
- Labels are deliberately low-cardinality configuration values. Do not add order
  IDs, transaction hashes, or trade IDs as metric labels.
- Prometheus histograms do not correlate one market-data update with one venue
  acknowledgment. Use structured logs for incident reconstruction; durable
  tracing and an order journal remain future work.
