# Stateless tracked-buy Turbine client

`solana-turbine-client` is a receive-only Solana Gossip/Turbine client for early, top-level Pump.fun buy detection. It joins Gossip, advertises a TVU endpoint, reconstructs entries entirely in bounded RAM, and sends a compact event only when a Pump.fun `buy` targets a mint in the runtime watchlist.

## Hot path

```text
TVU UDP
  -> canonical legacy or Merkle shred decode
  -> 8-slot ShredId dedupe
  -> bounded (slot, FEC set) RAM accumulator
  -> Merkle recovery, with legacy Reed-Solomon fallback
  -> consecutive data-shred block / Entry decode
  -> VersionedTransaction top-level instructions
  -> Pump.fun program + BUY discriminator
  -> resolve classic buy accounts from static message keys
  -> in-memory mint watchlist
  -> nonblocking Unix datagram send
```

There is no per-shred stdout output. Startup and exceptional diagnostics go to stderr through the logger. A missing, full, or disconnected bot socket never blocks the receive loop; that event is dropped.

The client deliberately does **not** instantiate Blockstore, RocksDB, AccountsDB, BankForks, ReplayStage, BankingStage, an SVM, RPC, Redis, persistence, or a network relay. `GossipService` is started with `bank_forks = None`. The advertised TPU UDP socket is only drained so this fork does not classify the contact record as a spy.

## Runtime watchlist control

The client owns `/tmp/turbine-control.sock` by default. Send one UTF-8 command per Unix datagram:

```text
ADD <mint>
REMOVE <mint>
REPLACE <mint> <mint> ...
REPLACE_WATCHLIST <mint>,<mint>,...
```

`REPLACE` with no mints clears the watchlist. Commands are case-insensitive; Pubkeys are validated before a write. Each update takes one short write lock. The hot path takes a read lock only after a transaction has already matched the Pump.fun program and buy discriminator. A stale filesystem entry is removed only when it is actually a Unix socket; the client refuses to overwrite a regular file.

Use `--control-socket PATH` to select another location.

## Bot event socket and wire format

The colocated bot owns and binds `/tmp/pumpfun.sock`, preserving the old hook's destination and buy tag (`0`). The client uses an unbound, nonblocking Unix datagram socket and `send_to()` for each tracked event. Use `--event-socket PATH` to override the destination.

The old hook mixed two serializers and supplied an all-zero placeholder signature. This client emits a deterministic, versioned little-endian payload containing the real first signature from the decoded `VersionedTransaction`:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic ASCII `PFB1` |
| 4 | 1 | legacy-compatible buy tag `0` |
| 5 | 8 | observation Unix timestamp, nanoseconds |
| 13 | 8 | slot |
| 21 | 64 | transaction signature |
| 85 | 32 | mint |
| 117 | 32 | bonding curve |
| 149 | 32 | associated bonding curve |
| 181 | 32 | buyer/user |
| 213 | 8 | token amount from buy instruction data |
| 221 | 8 | max SOL cost from buy instruction data |
| 229 | 32 | recent blockhash |
| 261 | 2 | instruction-data length |
| 263 | variable | complete Pump.fun instruction data |

One event is one datagram. There is no framing across datagrams and no acknowledgement in the latency-sensitive path.

## Bounds

- shred and transaction-signature dedupe: 8 slots
- FEC accumulator: 8 slots and at most 256 FEC sets total
- individual FEC set: at most 128 shreds
- observed shreds: at most 8192 per slot
- decoded transaction signatures: at most 65,536 per slot
- entry assembly: 8 slots and at most 4096 data shreds per slot
- runtime watchlist: at most 65,536 mints

When a bound is exceeded, old state is evicted or the oversized incomplete slot assembly is dropped. No state is persisted.

## Build and test

This historical Jito workspace retains build-time path dependencies even though the process does not run validator state. Initialize the required submodules:

```bash
git checkout Turbine-Client
git submodule update --init --depth=1 anchor jito-programs jito-protos/protos
```

On Ubuntu/Debian, install the standard Jito build dependencies and `libudev-dev`, then run:

```bash
cargo fmt --all -- --check
cargo check -p solana-gossip --bin solana-turbine-client
cargo test -p solana-gossip --bin solana-turbine-client
cargo build --release -p solana-gossip --bin solana-turbine-client
```

## Run

```bash
./target/release/solana-turbine-client \
  --identity /path/to/identity.json \
  --entrypoint HOST:PORT \
  --event-socket /tmp/pumpfun.sock \
  --control-socket /tmp/turbine-control.sock
```

Defaults are `0.0.0.0`, Gossip UDP/TCP `8001`, TVU UDP `8002`, TPU discard UDP `8003`, `/tmp/pumpfun.sock`, and `/tmp/turbine-control.sock`. Public IP and shred version are inferred from the entrypoint unless explicitly supplied.

## Known limitations

- Turbine is stake-weighted. A zero-stake identity can join Gossip correctly yet receive sparse or no Turbine delivery; this client does not add repair or a relay to compensate.
- Detection covers direct, top-level calls to the classic Pump.fun `buy` discriminator and account layout only. A Pump invocation that exists solely as a CPI is not present in the transaction's top-level compiled instructions and requires execution-derived data, which this client intentionally does not produce.
- A v0 instruction whose Pump program or required buy accounts are supplied only through address lookup tables is skipped. `VersionedTransaction` carries lookup descriptors but not the loaded addresses, and resolving them would require an external account/ALT source forbidden by this design.
- The client can decode a completed data block only after every data shred in that block is present or recoverable. Joining in the middle of a slot can therefore miss that slot's earlier blocks.
