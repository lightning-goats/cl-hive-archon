# cl-hive-archon

Phase 6B plugin for Archon identity and governance workflows.

## Status
- Implemented as a standalone plugin (no `cl-hive` integration required yet).
- Safe dark-launch defaults: Archon network calls are disabled unless explicitly enabled.

## RPC Methods
- `hive-archon-provision`
- `hive-archon-bind-nostr`
- `hive-archon-bind-cln`
- `hive-archon-status`
- `hive-archon-upgrade`
- `hive-poll-create`
- `hive-poll-status`
- `hive-vote`
- `hive-my-votes`

## Config Options
- `hive-archon-db-path` (default: `~/.lightning/cl_hive_archon.db`)
- `hive-archon-gateway` (default: `https://archon.technology`)
- `hive-archon-network-enabled` (default: `false`)
- `hive-archon-governance-min-bond` (default: `50000`)

## Install
1. Place `cl-hive-archon.py` in your CLN plugin path.
2. Add to `lightningd` config:

```ini
plugin=/path/to/cl-hive-archon.py
hive-archon-network-enabled=false
```

3. Restart `lightningd`.

## Quick Start
```bash
lightning-cli hive-archon-provision
lightning-cli hive-archon-status
lightning-cli hive-archon-upgrade target_tier=governance bond_sats=50000
```

## Development
```bash
python3 -m pytest tests -q
```
