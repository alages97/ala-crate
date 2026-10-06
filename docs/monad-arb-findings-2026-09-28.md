# Monad MON arbitrage findings — 2026-09-28

This note records the read-only mainnet experiments run on 2026-09-28. It
separates same-chain, same-block quote proofs from non-atomic spot/perpetual
basis screens. None of the results below is realized PnL or authorization to
trade.

## Kuru versus Uniswap v4

Two five-minute captures sampled 595 canonical Monad blocks across 1,973 block
heights. At four sizes and in both directions this produced 4,760 gross route
observations. No observation reached the configured 5 basis-point threshold.
The best observation was +3.724 bps at $100 before gas; all $1,000 and $5,000
round trips were negative.

The current Kuru MON/USDC deployment is a legacy market. Its estimator returns
before the final fill-or-kill checks, so none of these records proves complete
input consumption. A positive screen would still require an atomic executor
simulation with approvals, wrapping, gas, state contention, and a minimum-profit
revert guard.

Artifacts:

- `data/kuru-monad/kuru-shadow-multiblock-20260928T1200Z.ndjson`
- `data/kuru-monad/kuru-shadow-crossvenue-20260928T1212Z.ndjson`

Reproduce either summary with:

```bash
./target/release/kuru-shadow-analyze \
  --input data/kuru-monad/kuru-shadow-multiblock-20260928T1200Z.ndjson
```

## Other Monad DEXs

Initial block-hash-pinned spot checks found no positive same-chain route:

| Route | $100 result at reference block | Additional evidence |
|---|---:|---|
| Kuru -> Crystal | -1.343 bps | 12 more samples: -28.101 to -6.835 bps |
| Crystal -> Kuru | -24.266 bps | 12 more samples: -17.834 to -5.805 bps |
| Pancake v3 -> Crystal | -1.553 bps | Deteriorated faster with size |
| LFJ POE -> Crystal | -14.554 bps | Negative in both directions |

The earlier apparent +16.7 bps LFJ/Crystal result came from quotes taken at
different blocks. It disappeared under one canonical EIP-1898 block hash. This
is why asynchronous venue snapshots must not be labeled arbitrage.

Crystal is still the best next adapter: it is a composable on-chain CLOB and its
quote returns consumed input and output. Its documentation warns that the quote
uses aggregate price-level shortcuts, so a positive screen must be confirmed by
fork-simulating the actual market order. See the
[Monad deployment registry](https://github.com/monad-crypto/protocols/blob/8146495fd5d6cfb593350ad9059feb78d711e84c/mainnet/crystal.jsonc),
[quote interface](https://github.com/CrystalExch/Crystal-Contracts/blob/e1550adf2fb97838524b30df28ffee1e22decc52/contracts/core/Crystal.sol#L712-L731),
and [implementation caveat](https://github.com/CrystalExch/Crystal-Contracts/blob/e1550adf2fb97838524b30df28ffee1e22decc52/contracts/core/CrystalMarket.sol#L755-L771).

The Crystal adapter is now part of `kuru-shadow`. Its first persisted one-block
smoke was canonical and error-free, and every Crystal leg returned consumed
input equal to requested input. It was negative in both directions: at $100,
Kuru -> Crystal was -7.122 bps and Crystal -> Kuru was -12.895 bps; the larger
sizes deteriorated further.

A subsequent five-minute run changed the research priority. It sampled 290
canonical blocks across 985 block heights with zero proof errors. The median
$100 edge remained negative (-14.379 bps Kuru -> Crystal and -12.988 bps in the
reverse direction), but 16 route-size observations across 13 sampled blocks
crossed +5 bps. The maximum was +40.125 bps for a $100 Kuru -> Crystal route.
Only three $500 observations crossed the threshold; no $1,000 or $5,000 route
was positive. The longest observed threshold streak was two sampled
observations, and no opportunity spanned consecutive chain heights in the
sampled data.

The historical-fork replay closes the contract-execution gap. Against each
captured post-block state, all 16 screens executed atomically through the real
Kuru fill-or-kill entrypoint, WMON wrap/unwrap, approvals, and Crystal's
complete-fill market order. Every final USDC balance matched the original quote
exactly. This proves that the legacy Kuru estimator and Crystal quote described
an executable route at that state; it does not prove that a transaction could
arrive before the state changed.

That timing distinction is decisive. Replaying the same route after importing
the real next block's complete post-state left only 5 of 16 profitable. Eleven
calls reverted at the executor's atomic `NotProfitable` guard. Median quoted
edge moved from +11.985 bps at the signal state to -10.248 bps one block later,
and the strongest $100 screen moved from +40.1253 bps to -48.0987 bps. Because
post-N+1 state is after every N+1 transaction, it is a conservative latency
case rather than a reconstruction of a particular insertion position within
that block.

The five routes still profitable after the full N+1 state transition were:

| Original screen | Size | Direction | N edge | N+1 atomic edge |
|---:|---:|---|---:|---:|
| 3 | $100 | Kuru -> Crystal | +17.0315 bps | +8.1777 bps |
| 5 | $100 | Crystal -> Kuru | +11.7489 bps | +15.1818 bps |
| 8 | $100 | Crystal -> Kuru | +19.3652 bps | +23.7642 bps |
| 9 | $500 | Crystal -> Kuru | +10.9189 bps | +15.2766 bps |
| 11 | $100 | Kuru -> Crystal | +12.0029 bps | +6.7002 bps |

For the strongest optimistic replay, a genuine locally signed fork transaction
used a 691,388 gas limit and 689,202 receipt gas, returning $100.401253 from
$100. At the historical next-block base fee plus a 2 gwei tip, Monad's
gas-limit charging model costs about 0.07052 MON, or $0.00204 at the realized
Kuru price. Estimated net profit is therefore about $0.39921, or 39.92 bps.
Gas is not the binding constraint in this sample; one-block state decay is.

Artifacts:

- `data/kuru-monad/kuru-crystal-shadow-smoke-20260928.ndjson`
- `data/kuru-monad/kuru-crystal-shadow-multiblock-20260928T1234Z.ndjson`
- `contracts/KuruCrystalAtomicExecutor.sol`
- `test/KuruCrystalForkReplay.t.sol`

Run another create-new one-block screen with:

```bash
./target/release/kuru-shadow \
  --config config/kuru-crystal-shadow.toml \
  --once \
  --output data/kuru-monad/kuru-crystal-shadow-next.ndjson

./target/release/kuru-shadow-analyze \
  --input data/kuru-monad/kuru-crystal-shadow-multiblock-20260928T1234Z.ndjson

MONAD_RPC_URL=https://rpc.monad.xyz \
  .tools/foundry-v1.8.3/forge test \
  --network monad \
  --match-contract KuruCrystalForkReplayTest \
  -vvvv
```

## Kuru versus Hyperliquid and Lighter

The paired five-minute capture contains 299 Kuru proofs and 2,458 normalized
MON perpetual BBO frames. The analyzer selects only a BBO whose publisher
timestamp is no later than proof completion and no more than 500 ms old.

Depth-qualified long-Kuru/short-perpetual screens were:

| Venue | Size | Median | p95 | Maximum | At least 5 bps and enough top size |
|---|---:|---:|---:|---:|---:|
| Hyperliquid | $100 | 10.998 bps | 16.446 bps | 20.571 bps | 170 |
| Hyperliquid | $500 | 10.599 bps | 15.059 bps | 17.430 bps | 40 |
| Lighter | $100 | 20.934 bps | 30.906 bps | 45.662 bps | 201 |
| Lighter | $500 | 20.912 bps | 31.518 bps | 45.615 bps | 162 |

The inverse Kuru-sell/long-perpetual direction was always negative. Displayed
top-of-book capacity fell sharply above $500. Every strict observation count is
zero because the legacy Kuru quote cannot prove complete input consumption.

This is a gross, non-atomic entry basis—not an arbitrage return. The Kuru state
was already 1,339 ms old at median decision time and 2,200 ms old at p95. The
selected BBO publisher ages were much lower (130 ms median on Hyperliquid and
110 ms on Lighter), so part of the measured gap can disappear before an on-chain
order lands. Publisher timestamp also precedes downstream consumer receipt.

The five-minute sample did not establish convergence or venue leadership.
Hyperliquid basis narrowed about half the time over 5–60 seconds. Lighter showed
mild apparent narrowing, but the 60-second result has only about four independent
intervals. A lead/lag correlation reverses when Kuru is timestamped at its block
instead of proof completion, which diagnoses measurement delay rather than an
economic leader.

The economics remain account-specific. Hyperliquid's base perpetual taker fee
is currently 4.5 bps per side. Lighter Standard is currently zero-fee but applies
300 ms taker latency; Premium starts at 2.8 bps taker with 140 ms latency. Exit
fees, Kuru spread and unwind, gas, funding, partial fills, and leg risk are not in
the table. See the current [Hyperliquid fee schedule](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees)
and [Lighter account tiers](https://apidocs.lighter.xyz/docs/account-types).

Artifacts:

- `data/kuru-monad/kuru-shadow-crossvenue-20260928T1212Z.ndjson`
- `data/kuru-monad/mon-hedges-crossvenue-20260928T1212Z.ndjson`

Reproduce the causal, depth-qualified report with:

```bash
./target/release/kuru-cross-venue-analyze \
  --kuru-input data/kuru-monad/kuru-shadow-crossvenue-20260928T1212Z.ndjson \
  --bbo-input data/kuru-monad/mon-hedges-crossvenue-20260928T1212Z.ndjson
```

## Recommended next experiment

1. Capture 24–72 hours, preferably seven days, across volatile and quiet
   regimes. Timestamp Monad head receipt and every CEX socket receipt with the
   same local monotonic clock.
2. Quote one small Kuru size on every head; quote larger ladders less often.
   Record executable depth, trades, funding, mark/index prices, gas, and every
   decision/gateway/acknowledgement timestamp.
3. Produce fee- and latency-profile-specific markouts at 250 ms, 500 ms, 1 s,
   2 s, 5 s, 15 s, 30 s, 60 s, and 5 minutes. Use non-overlapping samples or
   clustered inference rather than treating every overlapping horizon as an
   independent trade.
4. Measure transaction propagation and reconstruct realistic insertion points
   within N+1. The completed post-state replay establishes contract
   executability but shows that a full block of latency destroys most screens.
5. Keep Pancake/Hanji comparisons same-block and read-only. Promote them to
   fork simulation only after a screen survives full-input, gas, and persistence
   gates.

The current evidence supports continued measurement of $100–$500 Kuru/Lighter
basis and a shadow-only, no-broadcast Kuru/Crystal decision loop with realistic
arrival simulation. It does not support live execution yet.
