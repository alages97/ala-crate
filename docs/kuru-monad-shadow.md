# Kuru same-block shadow prover

`kuru-shadow` compares both exact-input round trips between Kuru MON/USDC and a
configured Monad counterparty. Uniswap v4 and Crystal are supported. It is a
read-only mainnet tool: it has no private key, never signs, and never
broadcasts a transaction.

For each new Monad head it pins every `eth_call` to the same canonical block
hash using EIP-1898, then evaluates these paths for every configured size:

1. USDC -> MON on Uniswap v4 -> USDC on Kuru.
2. USDC -> MON on Kuru -> USDC on Uniswap v4.

With `counterparty = "crystal"`, the corresponding routes are:

1. USDC -> WMON on Crystal -> 1:1 unwrap -> native MON -> USDC on Kuru.
2. USDC -> native MON on Kuru -> 1:1 wrap -> WMON -> USDC on Crystal.

The second wave uses the exact raw MON output from the first wave. The record is
written only after the block number is fetched again and its hash is compared
with the quoted hash. It also records the Kuru proxy implementation address and
checks it against the startup identity.
If the Kuru proxy implementation changes, the run writes an error and a
`kuru_implementation_changed` shutdown record, then stops. Restart it into a
fresh output file so interface detection and startup identity are rerun.

## Build and run

From `ala-crate`:

```bash
cargo build --locked --release --bin kuru-shadow

# Recommended first check: one block, unique output file.
./target/release/kuru-shadow \
  --config config/kuru-monad-shadow.toml \
  --once \
  --output data/kuru-monad/kuru-shadow-smoke.ndjson

# Timed capture (six hours by default). Pick another unused output name.
./target/release/kuru-shadow \
  --config config/kuru-monad-shadow.toml \
  --output data/kuru-monad/kuru-shadow-6h.ndjson

# Crystal one-block screen, also read-only.
./target/release/kuru-shadow \
  --config config/kuru-crystal-shadow.toml \
  --once \
  --output data/kuru-monad/kuru-crystal-shadow-smoke.ndjson
```

The output is deliberately opened with create-new semantics. A typo cannot
silently overwrite an earlier capture. `Ctrl-C` flushes and synchronizes the
records before the process exits. The default four sizes produce sixteen quote
calls per probed block, so a dedicated Monad RPC is preferable for an extended
run; set `rpc_http_url` to that endpoint in a private config.

Prometheus telemetry is exposed at `http://127.0.0.1:9904/metrics` by default.
Important measurements include per-venue RPC latency, head-to-probe age,
whole-block proof duration, observed/coalesced heads, proof failures, canonical
hash mismatches, candidate screens, and latest gross edge by route and size.

## What the proof means

Each `block_proof` record has:

- `proof_kind = "same_block_composed_quotes"`;
- `atomic_simulated = false`;
- an EIP-1898 block reference on every leg;
- exact integer token amounts and calldata;
- gross output, PnL, and edge in basis points;
- `full_input_consumption_proven`, `gas_adjusted`, and candidate flags;
- local RPC and end-to-end proof latency.

A candidate is only a screen for deeper simulation. It is not executable PnL.
The quoted calls do not model a combined executor, approvals, venue settlement,
priority fees, executor overhead, or state contention between the two legs. A
production decision must come from one atomic executor transaction simulated
against the same state, with a minimum-profit revert guard.

## Crystal counterparty

The Crystal sample uses the singleton and WMON/USDC market published in
Monad's [official protocol registry](https://github.com/monad-crypto/protocols/blob/main/mainnet/crystal.jsonc).
Startup calls `getMarket` at the EIP-1898 startup block and refuses to run
unless the returned quote asset is Monad USDC and the base asset is WMON. It
also records the singleton and market bytecode hashes.

Every Crystal leg calls
`getQuote(market, isBuy, true, true, amountIn, 0)`. Amounts remain `U256`
integers throughout. Complete-fill mode is not trusted by itself: the leg is
marked complete only when returned `amountIn` equals the requested input;
over-consumption is rejected and partial consumption is recorded with its
residual.

Crystal's own
[quote implementation](https://github.com/CrystalExch/Crystal-Contracts/blob/main/contracts/core/CrystalMarket.sol)
warns that `getQuote` uses price-level shortcuts and is not directly equivalent
to the actual trade result. Treat it as a screen, not an execution guarantee.
Crystal uses WMON while Kuru uses native MON. Wrapping and unwrapping are 1:1
in raw 18-decimal units, but this prover only labels that conversion; it does
not execute the wrap/unwrap calls, price their gas, model approvals, or simulate
the combined atomic transaction.

Crystal captures use record schema v2, where each notional has a generic
`routes` array. Existing Uniswap captures and configs retain schema v1, and
`kuru-shadow-analyze` reads both formats.

## Kuru legacy versus V2

The tool detects the configured market at startup and persists its EIP-1967
implementation address and proxy/implementation bytecode hashes. The current
mainnet MON/USDC address in the sample config is a legacy market. As of
2026-09-28, Kuru's [official deployment page](https://docs.kuru.io/contracts/Contract-addresses)
still lists that address, while the new
[V2 SDK](https://github.com/Kuru-Labs/ts-sdk/tree/0727c1391916b0bfec310951957940e2d1f2ce6a)
does not publish a mainnet V2 market or AccountCore address. The SDK examples
currently use Monad testnet chain ID 10143 and its testnet relay. Address changes
must therefore come from an official deployment source, not a guessed upgrade.

Legacy Kuru quotes use the contract's zero-sender estimator path, the same basic
mechanism used by Kuru's legacy SDK. That path returns before its final
fill-or-kill and minimum-output checks. It therefore supplies a price/output
indication but does **not** prove that all input was consumed. Legacy routes are
marked `full_input_consumption_proven = false`; the optional gas estimate is
also labeled as a zero-sender lower bound, not executor gas.

Kuru V2 introduces `estimateSwap(bool,uint128)`, returning both
`amountInUsed` and `amountOut`. When an actual V2 market is configured, the
prover records residual input and marks complete consumption only when
`amountInUsed == amountIn`. This is a real improvement for screening partial
fills. V2 does not itself provide a Kuru-to-Uniswap-v4 atomic router, and its
quote does not estimate the gas of a real authorized account's `swap` call.
The SDK's [request builders](https://github.com/Kuru-Labs/ts-sdk/blob/0727c1391916b0bfec310951957940e2d1f2ce6a/src/spot/requests.ts#L160-L217)
also expose user-ID and builder-fee quote overloads. Those will matter when the
executor has an actual funded Kuru account and a known fee tier.

`kuru_version = "auto"` is intended for a rollout: it first probes a V2-unique
interface, otherwise decodes and validates the legacy MON/USDC market. Do not
reuse a completed output file across an upgrade. Start a new capture so every
record has one unambiguous startup contract identity.

## Output lifecycle and caveats

The first line is a `startup` record containing the chain, block, Kuru runtime
type, market parameters, implementation/code hashes, sizes, and explicit
caveats. Later lines are `block_proof` or structured error records. A graceful
exit appends a `shutdown` record with counters and its termination reason; a
missing shutdown line means the process did not finish cleanly. Each output file
is self-identifying through a UUID run ID.

On a public RPC, errors or coalesced blocks are expected under rate limiting.
Never reinterpret a missing proof as a zero edge. Likewise, a positive legacy
screen is not actionable until full-input consumption and atomic executor gas
are proven independently.
