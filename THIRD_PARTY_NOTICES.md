# Third-party notices

This project uses third-party components under their own licenses. This file is
informational and does not change those terms.

## Lighter official signer

The optional shared library loaded by `LighterGateway` is compiled by
`scripts/build-lighter-signer.sh` from the official
[`elliottech/lighter-go`](https://github.com/elliottech/lighter-go) repository:

- Pinned commit: `8854554703385766c73410b43e93075447f36a4c`
- Compiled entry point: `sharedlib/main.go`
- Upstream license: Apache License 2.0
- Local license copy: [`LICENSES/Apache-2.0.txt`](LICENSES/Apache-2.0.txt)

The repository does not check in the upstream source or compiled signer. The
locally generated artifact under `vendor/lighter-signer/` remains an Apache-2.0
component. If you distribute it, include the Apache-2.0 license and preserve all
applicable upstream copyright, patent, trademark, and attribution notices. The
pinned upstream tree contains no separate `NOTICE` file.

No local modifications are applied to the upstream source by the build script;
it selects a commit and compiles the official shared-library target.

The shared library also incorporates the Go modules resolved by that pinned
repository's `go.mod` and `go.sum`. Those packages retain their upstream
licenses. Generate and review a license report for the exact signer build before
redistribution; the Apache-2.0 license above covers `lighter-go` itself, not an
automatic relicensing of its dependencies.

## Hyperliquid Rust SDK

The Cargo dependency `hyperliquid_rust_sdk` version `0.6.0` is licensed under
the MIT License:

> Copyright (c) 2024 Hyperliquid Labs Pte. Ltd.

The dependency's full notice is retained in
[`LICENSES/hyperliquid-rust-sdk-MIT.txt`](LICENSES/hyperliquid-rust-sdk-MIT.txt).

## Other Rust dependencies

`Cargo.lock` records the complete resolved Rust dependency graph. Each package
retains its upstream license. Before distributing a binary, generate and review
an exhaustive software bill of materials/license report for that exact locked
build (for example with `cargo about` or `cargo deny`) and ship every notice its
licenses require.
