# Stateless Turbine Client

`solana-turbine-client` is a receive-only Solana Gossip/Turbine client. It joins Gossip, advertises a TVU endpoint, receives Turbine UDP datagrams, parses canonical shreds, deduplicates them in a bounded in-memory window, and emits shred metadata to stdout.

It does **not** start or maintain validator state.

## Runtime architecture

```text
identity keypair
      |
      v
Gossip / CRDS --------------------+
  |                               |
  | advertise Gossip + TVU + TPU  |
  | refresh ContactInfo wallclock |
  v                               |
Turbine peers                     |
  |                               |
  v                               |
TVU UDP socket <------------------+
  |
  v
Shred::new_from_serialized_shred
  |
  v
8-slot in-memory ShredId dedupe
  |
  v
TSV stdout
```

A TPU UDP socket is advertised and drained so the ContactInfo record is not classified as a spy by this Jito/Solana codebase. No BankingStage or transaction execution is attached to it.

## Deliberately absent

The binary does not instantiate:

- Blockstore / RocksDB ledger persistence
- AccountsDB
- BankForks
- ReplayStage
- BankingStage
- PoH
- voting or VoteService
- snapshots or snapshot download
- RPC serving
- transaction status/history storage
- ledger cleanup

`GossipService` is started with `bank_forks = None`.

## Build

This fork is an older Jito monorepo and its Cargo workspace still has build-time path dependencies on Jito/Anchor components even though the Turbine client does not run them. Initialize only the required submodules:

```bash
git checkout Turbine-Client
git submodule update --init --depth=1 anchor jito-programs jito-protos/protos
```

On Ubuntu/Debian, the workspace build currently needs the normal Jito build dependencies plus `libudev-dev`:

```bash
source .github/scripts/install-all-deps.sh Linux
sudo apt-get install -y libudev-dev
```

Build the client:

```bash
cargo build --release -p solana-gossip --bin solana-turbine-client
```

The branch CI also runs:

```bash
cargo check -p solana-gossip --bin solana-turbine-client
```

## Run

At minimum provide an identity keypair and a reachable Gossip entrypoint:

```bash
./target/release/solana-turbine-client \
  --identity /path/to/identity.json \
  --entrypoint HOST:PORT
```

Defaults:

- bind address: `0.0.0.0`
- Gossip: UDP/TCP `8001`
- TVU: UDP `8002`
- TPU discard socket: UDP `8003`

The public IP and cluster shred version are queried from the entrypoint when they are not supplied explicitly. They can be overridden with `--public-address` and `--shred-version`.

For Turbine delivery, the advertised public TVU address must actually reach the process. NAT/firewall rules therefore need to forward the selected TVU UDP port. The Gossip UDP/TCP port must also be reachable for normal cluster participation.

## Output

stdout is tab-separated:

```text
wallclock_ms    source    slot    index    type    fec_set_index
```

Each line corresponds to a newly observed, successfully parsed shred. Duplicate `ShredId`s are suppressed within an eight-slot in-memory window.

Startup/network diagnostics are written to stderr so stdout can be piped to another process.

## Current boundary

This first client proves the network/control-plane boundary: Gossip participation plus direct Turbine shred receipt with no ledger or bank state.

It currently parses and exposes shred metadata. It does not yet reconstruct missing data shreds from coding shreds, reconstruct entries, decode transactions, retransmit Turbine shreds, or perform repair.

The runtime is stateless, but the **build graph is not yet fully lightweight** because the historical `solana-gossip` crate has normal dependencies on validator-era runtime/client/ledger crates. Removing those build-time dependencies is a separate crate/feature-gating refactor and is not required for the process to run without validator storage.
