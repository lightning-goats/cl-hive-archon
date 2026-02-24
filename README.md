# cl-hive-archon

**Phase 6B identity and governance plugin for the cl-hive Lightning fleet stack.**

cl-hive-archon provides every node in a hive fleet with a cryptographic identity (DID) and a proof-of-stake governance system for distributed decision-making. It is the credential and voting substrate that cl-hive's expansion elections, ban proposals, and fleet-wide polls run on.

## How It Fits

```
cl-hive (Coordination)        <-- fleet gossip, intents, topology
    |
    |-- delegates signing -->  cl-hive-archon (Identity + Governance)
    |-- delegates transport -> cl-hive-comms  (Nostr/REST transport)
    |
cl-revenue-ops (Execution)    <-- fees, rebalancing, local policy
    |
Core Lightning
```

When archon is active, cl-hive's `RemoteArchonIdentity` adapter routes all `signmessage` calls through archon's `hive-archon-sign-message` RPC, with a circuit breaker that falls back to local CLN HSM signing on failure. This lets the fleet use a unified identity layer without losing resilience.

When archon is absent, cl-hive operates normally using local HSM signing.

## Features

- **DID Identity** — Each node gets a `did:cid:` identifier (CIDv1 base32lower) derived from its pubkey. Used as a stable identity across key rotations and DID reprovisioning.
- **Nostr & CLN Bindings** — Cryptographically attest links between a DID and Nostr pubkeys or CLN node pubkeys. Attestations are signed via CLN HSM.
- **Proof-of-Stake Governance** — Nodes upgrade to governance tier by proving channel balance >= bond threshold (default 50,000 sats). Prevents sybil voting.
- **Poll-Based Elections** — Create polls with 2-10 options and a deadline. One vote per voter per poll, enforced at the database level. Votes are canonically signed. Spoiled ballots supported via `choice=spoil`.
- **Optional Gateway Sync** — Polls and votes can optionally sync to an Archon gatekeeper/keymaster via the standard `did:cid` API (`/api/v1/did/generate`, `/api/v1/polls`). Disabled by default (dark launch). Failed syncs queue to an outbox with exponential backoff retry.

## Requirements

- Core Lightning v25.02+
- `pyln-client >= 25.0`
- **cl-hive-comms** must be loaded first (warning logged if absent)
- No external crypto libraries — all signing via CLN HSM

## Install

### Docker (with cl-hive)

Set `HIVE_ARCHON_ENABLED=true` in your docker environment. The entrypoint loads archon automatically before cl-hive.

### Manual

```bash
# Clone next to cl-hive
git clone https://github.com/lightning-goats/cl-hive-archon.git

# Add to lightningd config (load before cl-hive)
echo "plugin=/path/to/cl-hive-archon/cl-hive-archon.py" >> ~/.lightning/config
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `hive-archon-db-path` | `~/.lightning/cl_hive_archon.db` | SQLite database path |
| `hive-archon-gateway` | `https://archon.technology` | Archon gatekeeper/keymaster base URL for optional remote sync |
| `hive-archon-network-enabled` | `false` | Enable gateway HTTP calls (safe to leave off) |
| `hive-archon-governance-min-bond` | `50000` | Minimum sats in channels to unlock governance tier |
| `hive-archon-gateway-auth-token` | `""` | Bearer token for Archon gateway API authentication |

## Quick Start

```bash
# Create your node's DID identity
lightning-cli hive-archon-provision

# Check identity and governance status
lightning-cli hive-archon-status

# Bind your Nostr pubkey to your DID
lightning-cli hive-archon-bind-nostr nostr_pubkey=<64-char-hex>

# Upgrade to governance tier (requires channel balance >= bond)
lightning-cli hive-archon-upgrade target_tier=governance bond_sats=50000

# Create a fleet poll
lightning-cli hive-poll-create poll_type=expansion title="Open channel to ACINQ?" \
  options_json='["yes","no","abstain"]' deadline=1771700000

# Vote
lightning-cli hive-vote poll_id=<id> choice=yes reason="Good connectivity"

# Spoiled ballot (abstain without choosing an option)
lightning-cli hive-vote poll_id=<id> choice=spoil reason="No opinion"

# View your votes
lightning-cli hive-my-votes
```

## RPC Methods

| Method | Tier | Description |
|--------|------|-------------|
| `hive-archon-provision` | any | Create or re-provision a DID identity |
| `hive-archon-status` | any | Identity info, binding counts, governance tier |
| `hive-archon-bind-nostr` | any | Bind a Nostr pubkey with signed attestation |
| `hive-archon-bind-cln` | any | Bind a CLN node pubkey with signed attestation |
| `hive-archon-upgrade` | any | Upgrade to governance tier with proof-of-stake bond |
| `hive-archon-sign-message` | any | Sign a message via CLN HSM (used by cl-hive) |
| `hive-poll-create` | governance | Create a poll with options and deadline |
| `hive-poll-status` | any | Poll details, vote tally, voter list |
| `hive-vote` | governance | Cast a signed vote on an active poll |
| `hive-my-votes` | any | List this node's recent votes |
| `hive-archon-prune` | any | Delete completed polls older than retention window |
| `hive-archon-process-outbox` | any | Retry failed gateway sync operations |

## Security

- **All signing via CLN HSM** — no crypto libraries imported, no private keys in memory
- **Voter identity pinned to node pubkey** — immutable across DID reprovisioning, prevents sybil voting
- **DB file permissions 0o600** — owner read/write only
- **SSRF protection** — gateway calls resolve DNS first, block private/loopback IP ranges; DNS resolution failures are fail-closed
- **Fail-closed** — network errors queue to outbox, never corrupt local state
- **Gateway auth** — optional Bearer token authentication for Archon gateway API calls
- **Input validation** — strict bounds on all fields (pubkey format, option count, metadata size)
- **SQLite WAL mode** with thread-local connections for concurrent access

## Database

5 tables: `archon_identity`, `archon_bindings`, `archon_polls`, `archon_votes`, `archon_outbox`. Bounded at 5,000 polls and 50,000 votes with automatic pruning.

## Development

```bash
python3 -m pytest tests/ -v    # 47 tests
```

## License

MIT
