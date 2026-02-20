#!/usr/bin/env python3
"""cl-hive-archon: Phase 6B Archon identity/governance plugin."""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict

# Ensure this script's real directory is on sys.path so that `from modules.X`
# works even when CLN loads the plugin via a symlink in the plugins directory.
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

from pyln.client import Plugin

from modules.archon_service import ArchonService, ArchonStore

plugin = Plugin()
service: ArchonService | None = None


plugin.add_option(
    name="hive-archon-db-path",
    default="~/.lightning/cl_hive_archon.db",
    description="SQLite path for cl-hive-archon state",
)

plugin.add_option(
    name="hive-archon-gateway",
    default="https://archon.technology",
    description="Archon gateway base URL",
)

plugin.add_option(
    name="hive-archon-network-enabled",
    default="false",
    description="Enable Archon gateway HTTP calls (default false for dark launch)",
)

plugin.add_option(
    name="hive-archon-governance-min-bond",
    default="50000",
    description="Minimum bond (sats) required to upgrade to governance tier",
)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _parse_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _logger(message: str, level: str = "info") -> None:
    plugin.log(message, level=level)


def _require_service() -> ArchonService:
    if service is None:
        raise RuntimeError("service not initialized")
    return service


@plugin.init()
def init(options: Dict[str, Any], configuration: Dict[str, Any], plugin: Plugin, **kwargs: Any) -> None:
    del kwargs

    db_path_opt = str(options.get("hive-archon-db-path") or "~/.lightning/cl_hive_archon.db")
    db_path = os.path.expanduser(db_path_opt)
    if not os.path.isabs(db_path):
        lightning_dir = str(configuration.get("lightning-dir") or os.path.expanduser("~/.lightning"))
        db_path = os.path.join(lightning_dir, db_path)

    gateway_url = str(options.get("hive-archon-gateway") or "").strip()
    network_enabled = _parse_bool(options.get("hive-archon-network-enabled"))
    min_bond = max(1, _parse_int(options.get("hive-archon-governance-min-bond"), 50_000))

    store = ArchonStore(db_path=db_path, logger=_logger)

    global service
    service = ArchonService(
        store=store,
        rpc=plugin.rpc,
        logger=_logger,
        gateway_url=gateway_url,
        network_enabled=network_enabled,
        min_governance_bond_sats=min_bond,
    )

    # Warn if cl-hive-comms is not detected (unsupported configuration)
    comms_detected = False
    try:
        try:
            plugins_resp = plugin.rpc.plugin("list")
        except Exception:
            plugins_resp = plugin.rpc.listplugins()
        for entry in plugins_resp.get("plugins", []):
            raw_name = entry.get("name") or entry.get("path") or entry.get("plugin") or ""
            if "cl-hive-comms" in os.path.basename(str(raw_name)).lower():
                comms_detected = bool(entry.get("active", False))
                break
    except Exception:
        pass
    if not comms_detected:
        plugin.log(
            "WARNING: cl-hive-comms not detected. cl-hive-archon without cl-hive-comms "
            "is not a supported Phase 6 configuration.",
            level="warn",
        )

    plugin.log(
        "cl-hive-archon initialized "
        f"(db_path={db_path}, network_enabled={network_enabled}, gateway={gateway_url})"
    )


@plugin.method("hive-archon-provision")
def hive_archon_provision(plugin: Plugin, force: str = "false", label: str = "") -> Dict[str, Any]:
    del plugin
    return _require_service().provision(force=_parse_bool(force), label=label)


@plugin.method("hive-archon-bind-nostr")
def hive_archon_bind_nostr(plugin: Plugin, nostr_pubkey: str, did: str = "") -> Dict[str, Any]:
    del plugin
    return _require_service().bind_nostr(nostr_pubkey=nostr_pubkey, did=did)


@plugin.method("hive-archon-bind-cln")
def hive_archon_bind_cln(plugin: Plugin, cln_pubkey: str = "", did: str = "") -> Dict[str, Any]:
    del plugin
    return _require_service().bind_cln(cln_pubkey=cln_pubkey, did=did)


@plugin.method("hive-archon-status")
def hive_archon_status(plugin: Plugin) -> Dict[str, Any]:
    del plugin
    return _require_service().status()


@plugin.method("hive-archon-upgrade")
def hive_archon_upgrade(
    plugin: Plugin,
    target_tier: str = "governance",
    bond_sats: int = 0,
) -> Dict[str, Any]:
    del plugin
    return _require_service().upgrade(target_tier=target_tier, bond_sats=_parse_int(bond_sats, 0))


@plugin.method("hive-poll-create")
def hive_poll_create(
    plugin: Plugin,
    poll_type: str,
    title: str,
    options_json: str,
    deadline: int,
    metadata_json: str = "{}",
) -> Dict[str, Any]:
    del plugin

    try:
        options = json.loads(options_json)
    except (json.JSONDecodeError, TypeError):
        return {"error": "invalid options_json"}

    try:
        metadata = json.loads(metadata_json) if metadata_json else {}
    except (json.JSONDecodeError, TypeError):
        return {"error": "invalid metadata_json"}

    if not isinstance(metadata, dict):
        return {"error": "metadata_json must decode to an object"}

    return _require_service().poll_create(
        poll_type=poll_type,
        title=title,
        options=options,
        deadline=_parse_int(deadline, 0),
        metadata=metadata,
    )


@plugin.method("hive-poll-status")
def hive_poll_status(plugin: Plugin, poll_id: str) -> Dict[str, Any]:
    del plugin
    return _require_service().poll_status(poll_id=poll_id)


@plugin.method("hive-vote")
def hive_vote(plugin: Plugin, poll_id: str, choice: str, reason: str = "") -> Dict[str, Any]:
    del plugin
    return _require_service().vote(poll_id=poll_id, choice=choice, reason=reason)


@plugin.method("hive-my-votes")
def hive_my_votes(plugin: Plugin, limit: int = 50) -> Dict[str, Any]:
    del plugin
    return _require_service().my_votes(limit=_parse_int(limit, 50))


@plugin.method("hive-archon-sign-message")
def hive_archon_sign_message(plugin: Plugin, message: str) -> Dict[str, Any]:
    del plugin
    return _require_service().sign_message(message=message)


@plugin.method("hive-archon-prune")
def hive_archon_prune(plugin: Plugin, retention_days: int = 90) -> Dict[str, Any]:
    del plugin
    return _require_service().prune(retention_days=_parse_int(retention_days, 90))


@plugin.method("hive-archon-process-outbox")
def hive_archon_process_outbox(plugin: Plugin, max_entries: int = 10) -> Dict[str, Any]:
    del plugin
    return _require_service().process_outbox(max_entries=_parse_int(max_entries, 10))


if __name__ == "__main__":
    plugin.run()
