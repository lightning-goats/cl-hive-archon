"""Core service and persistence layer for cl-hive-archon."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
import urllib.request
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse


def _is_hex(value: str, expected_len: int) -> bool:
    if not isinstance(value, str) or len(value) != expected_len:
        return False
    try:
        int(value, 16)
        return True
    except ValueError:
        return False


def _is_valid_nostr_pubkey(value: str) -> bool:
    return _is_hex(value, 64)


def _is_valid_cln_pubkey(value: str) -> bool:
    if not isinstance(value, str):
        return False
    if len(value) != 66 or value[:2] not in ("02", "03"):
        return False
    return _is_hex(value, 66)


def _is_valid_did(value: str) -> bool:
    if not isinstance(value, str):
        return False
    did = value.strip()
    if len(did) < 12 or len(did) > 128:
        return False
    if not did.startswith("did:cid:"):
        return False
    suffix = did[8:]
    if not suffix:
        return False
    return all(ch.isalnum() or ch in "-._:" for ch in suffix)


class ArchonStore:
    """SQLite persistence for archon identity, bindings, polls, and votes."""

    def __init__(self, db_path: str, logger: Optional[Callable[[str, str], None]] = None):
        self.db_path = os.path.expanduser(db_path)
        self._logger = logger
        self._local = threading.local()

    def _log(self, message: str, level: str = "info") -> None:
        if self._logger:
            self._logger(message, level)

    def _get_connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            conn = sqlite3.connect(
                self.db_path,
                isolation_level=None,
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            self._local.conn = conn
        return conn

    def initialize(self) -> None:
        conn = self._get_connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS archon_identity (
                singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
                did TEXT NOT NULL,
                governance_tier TEXT NOT NULL DEFAULT 'basic',
                status TEXT NOT NULL DEFAULT 'active',
                source TEXT NOT NULL DEFAULT 'local-fallback',
                gateway_url TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS archon_bindings (
                binding_id TEXT PRIMARY KEY,
                did TEXT NOT NULL,
                binding_type TEXT NOT NULL,
                subject TEXT NOT NULL,
                attestation_json TEXT NOT NULL,
                signature TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(binding_type, subject)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archon_bindings_did
            ON archon_bindings(did, binding_type)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS archon_polls (
                poll_id TEXT PRIMARY KEY,
                remote_poll_id TEXT,
                poll_type TEXT NOT NULL,
                title TEXT NOT NULL,
                options_json TEXT NOT NULL,
                metadata_json TEXT,
                created_by TEXT NOT NULL,
                deadline INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archon_polls_status_deadline
            ON archon_polls(status, deadline)
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS archon_votes (
                vote_id TEXT PRIMARY KEY,
                poll_id TEXT NOT NULL,
                voter_id TEXT NOT NULL,
                choice TEXT NOT NULL,
                reason TEXT,
                voted_at INTEGER NOT NULL,
                signature TEXT,
                FOREIGN KEY(poll_id) REFERENCES archon_polls(poll_id),
                UNIQUE(poll_id, voter_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archon_votes_voter
            ON archon_votes(voter_id, voted_at DESC)
            """
        )

        conn.execute("PRAGMA optimize;")

    def get_identity(self) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM archon_identity WHERE singleton_id = 1"
        ).fetchone()
        return dict(row) if row else None

    def upsert_identity(
        self,
        did: str,
        governance_tier: str,
        status: str,
        source: str,
        gateway_url: str,
        now_ts: int,
    ) -> None:
        conn = self._get_connection()
        existing = self.get_identity()
        created_at = existing["created_at"] if existing else now_ts
        conn.execute(
            """
            INSERT OR REPLACE INTO archon_identity (
                singleton_id, did, governance_tier, status,
                source, gateway_url, created_at, updated_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (did, governance_tier, status, source, gateway_url, created_at, now_ts),
        )

    def update_governance_tier(self, governance_tier: str, now_ts: int) -> None:
        conn = self._get_connection()
        conn.execute(
            "UPDATE archon_identity SET governance_tier = ?, updated_at = ? WHERE singleton_id = 1",
            (governance_tier, now_ts),
        )

    def upsert_binding(
        self,
        binding_id: str,
        did: str,
        binding_type: str,
        subject: str,
        attestation_json: str,
        signature: str,
        now_ts: int,
    ) -> None:
        conn = self._get_connection()
        conn.execute(
            """
            INSERT INTO archon_bindings (
                binding_id, did, binding_type, subject,
                attestation_json, signature, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(binding_type, subject) DO UPDATE SET
                binding_id = excluded.binding_id,
                did = excluded.did,
                attestation_json = excluded.attestation_json,
                signature = excluded.signature,
                updated_at = excluded.updated_at
            """,
            (
                binding_id,
                did,
                binding_type,
                subject,
                attestation_json,
                signature,
                now_ts,
                now_ts,
            ),
        )

    def delete_bindings_for_did(self, did: str) -> int:
        """Remove all bindings associated with a DID. Returns count deleted."""
        conn = self._get_connection()
        cursor = conn.execute(
            "DELETE FROM archon_bindings WHERE did = ?", (did,)
        )
        return cursor.rowcount

    def list_bindings(self) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM archon_bindings ORDER BY updated_at DESC"
        ).fetchall()
        return [dict(row) for row in rows]

    def create_poll(
        self,
        poll_id: str,
        remote_poll_id: str,
        poll_type: str,
        title: str,
        options_json: str,
        metadata_json: str,
        created_by: str,
        deadline: int,
        now_ts: int,
    ) -> None:
        conn = self._get_connection()
        conn.execute(
            """
            INSERT INTO archon_polls (
                poll_id, remote_poll_id, poll_type, title, options_json,
                metadata_json, created_by, deadline, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                poll_id,
                remote_poll_id,
                poll_type,
                title,
                options_json,
                metadata_json,
                created_by,
                deadline,
                now_ts,
                now_ts,
            ),
        )

    def get_poll(self, poll_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM archon_polls WHERE poll_id = ?",
            (poll_id,),
        ).fetchone()
        return dict(row) if row else None

    def set_poll_status(self, poll_id: str, status: str, now_ts: int) -> None:
        conn = self._get_connection()
        conn.execute(
            "UPDATE archon_polls SET status = ?, updated_at = ? WHERE poll_id = ?",
            (status, now_ts, poll_id),
        )

    def complete_expired_polls(self, now_ts: int) -> int:
        """Transition expired active polls to completed and return count."""
        conn = self._get_connection()
        cursor = conn.execute(
            """
            UPDATE archon_polls
            SET status = 'completed', updated_at = ?
            WHERE status = 'active' AND deadline <= ?
            """,
            (now_ts, now_ts),
        )
        return cursor.rowcount

    def count_polls_by_status(self, status: str) -> int:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM archon_polls WHERE status = ?",
            (status,),
        ).fetchone()
        return int(row["cnt"] or 0)

    def count_total_polls(self) -> int:
        conn = self._get_connection()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM archon_polls").fetchone()
        return int(row["cnt"] or 0)

    def list_votes_for_poll(self, poll_id: str) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM archon_votes WHERE poll_id = ? ORDER BY voted_at ASC",
            (poll_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def add_vote(
        self,
        vote_id: str,
        poll_id: str,
        voter_id: str,
        choice: str,
        reason: str,
        voted_at: int,
        signature: str,
    ) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO archon_votes (
                    vote_id, poll_id, voter_id, choice, reason, voted_at, signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (vote_id, poll_id, voter_id, choice, reason, voted_at, signature),
            )
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            return False

    def count_total_votes(self) -> int:
        conn = self._get_connection()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM archon_votes").fetchone()
        return int(row["cnt"] or 0)

    def list_votes_for_voter(self, voter_id: str, limit: int) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        rows = conn.execute(
            """
            SELECT v.*, p.title, p.poll_type, p.status, p.deadline
            FROM archon_votes v
            JOIN archon_polls p ON p.poll_id = v.poll_id
            WHERE v.voter_id = ?
            ORDER BY v.voted_at DESC
            LIMIT ?
            """,
            (voter_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def prune_completed_polls(self, before_ts: int) -> int:
        """Delete completed polls (and their votes) older than before_ts.

        Returns the number of polls deleted.
        """
        conn = self._get_connection()
        # Delete votes first (foreign key constraint)
        conn.execute(
            """
            DELETE FROM archon_votes WHERE poll_id IN (
                SELECT poll_id FROM archon_polls
                WHERE status = 'completed' AND deadline < ?
            )
            """,
            (before_ts,),
        )
        cursor = conn.execute(
            "DELETE FROM archon_polls WHERE status = 'completed' AND deadline < ?",
            (before_ts,),
        )
        return cursor.rowcount


class ArchonGatewayClient:
    """Small HTTP client for optional Archon gateway integration."""

    def __init__(self, base_url: str, timeout_seconds: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _request(self, method: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}

    def provision_identity(self, node_pubkey: str, label: str) -> Optional[str]:
        payload = {"node_pubkey": node_pubkey, "label": label}
        data = self._request("POST", "/v1/hive/provision", payload)
        did = data.get("did")
        if isinstance(did, str) and did.startswith("did:cid:"):
            return did
        return None

    def create_poll(
        self,
        poll_type: str,
        title: str,
        options: List[str],
        deadline: int,
        metadata: Dict[str, Any],
        creator: str,
    ) -> Optional[str]:
        payload = {
            "poll_type": poll_type,
            "title": title,
            "options": options,
            "deadline": deadline,
            "metadata": metadata,
            "creator": creator,
        }
        data = self._request("POST", "/v1/hive/polls", payload)
        poll_id = data.get("poll_id")
        if isinstance(poll_id, str) and poll_id:
            return poll_id
        return None

    def submit_vote(self, poll_id: str, voter_id: str, choice: str, reason: str) -> bool:
        from urllib.parse import quote as _url_quote
        payload = {
            "poll_id": poll_id,
            "voter_id": voter_id,
            "choice": choice,
            "reason": reason,
        }
        safe_poll_id = _url_quote(poll_id, safe="")
        data = self._request("POST", f"/v1/hive/polls/{safe_poll_id}/votes", payload)
        return bool(data.get("ok", False))


class ArchonService:
    """Phase 6B service API used by cl-hive-archon RPC methods."""

    VALID_GOVERNANCE_TIERS = {"basic", "governance"}

    MAX_LABEL_LEN = 120
    MAX_POLL_TYPE_LEN = 32
    MAX_POLL_TITLE_LEN = 200
    MAX_METADATA_JSON_LEN = 8_192
    MAX_REASON_LEN = 500
    MAX_TOTAL_POLLS = 5_000
    MAX_TOTAL_VOTES = 50_000

    def __init__(
        self,
        store: ArchonStore,
        rpc: Any = None,
        logger: Optional[Callable[[str, str], None]] = None,
        gateway_url: str = "",
        network_enabled: bool = False,
        min_governance_bond_sats: int = 50_000,
        time_fn: Callable[[], float] = time.time,
    ):
        self.store = store
        self.rpc = rpc
        self._logger = logger
        self.gateway_url = gateway_url.strip()
        self.network_enabled = bool(network_enabled)
        self.min_governance_bond_sats = max(1, int(min_governance_bond_sats))
        self._time_fn = time_fn

        if self.network_enabled and not self._is_valid_gateway_url(self.gateway_url):
            self._log("archon: invalid gateway URL; disabling network integration", "warn")
            self.network_enabled = False
            self.gateway_url = ""

        self._gateway_client = (
            ArchonGatewayClient(self.gateway_url) if self.network_enabled and self.gateway_url else None
        )
        self.store.initialize()

    def _log(self, message: str, level: str = "info") -> None:
        if self._logger:
            self._logger(message, level)

    def _now(self) -> int:
        return int(self._time_fn())

    def _is_valid_gateway_url(self, url: str) -> bool:
        if not isinstance(url, str) or not url.strip():
            return False
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http"):
            return False
        if not parsed.netloc:
            return False
        if not parsed.hostname:
            return False
        return True

    def _our_node_pubkey(self) -> str:
        if not self.rpc:
            return ""
        try:
            info = self.rpc.getinfo()
            if isinstance(info, dict):
                pubkey = str(info.get("id", ""))
                if _is_valid_cln_pubkey(pubkey):
                    return pubkey
        except Exception as exc:
            self._log(f"archon: getinfo failed: {exc}", "warn")
        return ""

    def _sign_message(self, payload: str, required: bool = False) -> str:
        """Sign a message via CLN HSM.

        Args:
            payload: The message to sign.
            required: If True, raise on failure instead of returning "".
        """
        if not self.rpc:
            if required:
                raise RuntimeError("RPC not available for signing")
            return ""
        try:
            result = self.rpc.signmessage(payload)
            if isinstance(result, dict):
                sig = str(result.get("zbase", "") or "")
                if sig:
                    return sig
        except Exception as exc:
            self._log(f"archon: signmessage failed: {exc}", "warn")
            if required:
                raise
        if required:
            raise RuntimeError("signmessage returned empty signature")
        return ""

    def _resolve_did(self, did: str = "") -> str:
        if did:
            return did.strip()
        identity = self.store.get_identity()
        return str(identity.get("did", "")) if identity else ""

    def _generate_local_did(self, node_pubkey: str, label: str) -> str:
        material = f"{node_pubkey}:{label}:{self._now()}:{uuid.uuid4()}"
        digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
        return f"did:cid:{digest[:48]}"

    def _build_attestation(self, binding_type: str, did: str, subject: str) -> Dict[str, Any]:
        payload = {
            "binding_type": binding_type,
            "did": did,
            "subject": subject,
            "node_pubkey": self._our_node_pubkey(),
            "timestamp": self._now(),
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        signature = self._sign_message(canonical, required=True)
        return {
            "payload": payload,
            "signature": signature,
            "canonical": canonical,
        }

    def _require_governance(self) -> Optional[Dict[str, Any]]:
        identity = self.store.get_identity()
        if not identity:
            return {
                "error": "identity not provisioned",
                "hint": "run hive-archon-provision first",
            }
        if identity.get("governance_tier") != "governance":
            return {
                "error": "governance tier required",
                "hint": "run hive-archon-upgrade target_tier=governance bond_sats=50000",
            }
        return None

    def provision(self, force: bool = False, label: str = "") -> Dict[str, Any]:
        if not isinstance(label, str):
            return {"error": "label must be a string"}
        label = label.strip()
        if len(label) > self.MAX_LABEL_LEN:
            return {"error": f"label too long (max {self.MAX_LABEL_LEN} chars)"}

        identity = self.store.get_identity()
        if identity and not force:
            return {
                "ok": True,
                "already_provisioned": True,
                "did": identity["did"],
                "governance_tier": identity["governance_tier"],
                "source": identity["source"],
                "gateway_url": identity.get("gateway_url") or "",
            }

        node_pubkey = self._our_node_pubkey()
        source = "local-fallback"
        did = ""

        if self.network_enabled and self._gateway_client:
            try:
                did = self._gateway_client.provision_identity(node_pubkey=node_pubkey, label=label)
                if did:
                    source = "archon-gateway"
            except Exception as exc:
                self._log(f"archon: gateway provisioning failed, using local fallback: {exc}", "warn")

        if not did:
            did = self._generate_local_did(node_pubkey=node_pubkey, label=label)

        governance_tier = "basic"
        if identity:
            governance_tier = str(identity.get("governance_tier") or "basic")
            # Clean up bindings referencing the old DID when force-reprovisioning
            old_did = str(identity.get("did") or "")
            if old_did and old_did != did:
                removed = self.store.delete_bindings_for_did(old_did)
                if removed:
                    self._log(f"archon: removed {removed} orphaned binding(s) for old DID", "info")

        now_ts = self._now()
        self.store.upsert_identity(
            did=did,
            governance_tier=governance_tier,
            status="active",
            source=source,
            gateway_url=self.gateway_url if source == "archon-gateway" else "",
            now_ts=now_ts,
        )

        return {
            "ok": True,
            "did": did,
            "source": source,
            "governance_tier": governance_tier,
            "gateway_url": self.gateway_url if source == "archon-gateway" else "",
        }

    def bind_nostr(self, nostr_pubkey: str, did: str = "") -> Dict[str, Any]:
        if not _is_valid_nostr_pubkey(nostr_pubkey):
            return {"error": "invalid nostr_pubkey (expected 64 hex chars)"}

        if did and not _is_valid_did(did):
            return {"error": "invalid did format"}

        identity = self.store.get_identity()
        if not identity:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}

        resolved_did = self._resolve_did(did)
        if not resolved_did:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}
        if not _is_valid_did(resolved_did):
            return {"error": "invalid did format"}

        owned_did = str(identity.get("did") or "")
        if resolved_did != owned_did:
            return {"error": "cannot bind to a DID not owned by this node"}

        try:
            attestation = self._build_attestation("nostr", resolved_did, nostr_pubkey)
        except RuntimeError as exc:
            return {"error": f"signing failed: {exc}"}
        binding_id = hashlib.sha256(
            f"{resolved_did}:nostr:{nostr_pubkey}".encode("utf-8")
        ).hexdigest()[:32]
        now_ts = self._now()
        self.store.upsert_binding(
            binding_id=binding_id,
            did=resolved_did,
            binding_type="nostr",
            subject=nostr_pubkey,
            attestation_json=json.dumps(attestation, sort_keys=True, separators=(",", ":")),
            signature=attestation["signature"],
            now_ts=now_ts,
        )
        return {
            "ok": True,
            "binding_id": binding_id,
            "did": resolved_did,
            "binding_type": "nostr",
            "subject": nostr_pubkey,
        }

    def bind_cln(self, cln_pubkey: str = "", did: str = "") -> Dict[str, Any]:
        subject = cln_pubkey.strip() if isinstance(cln_pubkey, str) else cln_pubkey
        subject = subject or self._our_node_pubkey()
        if not _is_valid_cln_pubkey(subject):
            return {"error": "invalid cln_pubkey (expected 66-char compressed secp256k1 pubkey)"}

        if did and not _is_valid_did(did):
            return {"error": "invalid did format"}

        identity = self.store.get_identity()
        if not identity:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}

        resolved_did = self._resolve_did(did)
        if not resolved_did:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}
        if not _is_valid_did(resolved_did):
            return {"error": "invalid did format"}

        owned_did = str(identity.get("did") or "")
        if resolved_did != owned_did:
            return {"error": "cannot bind to a DID not owned by this node"}

        try:
            attestation = self._build_attestation("cln", resolved_did, subject)
        except RuntimeError as exc:
            return {"error": f"signing failed: {exc}"}
        binding_id = hashlib.sha256(
            f"{resolved_did}:cln:{subject}".encode("utf-8")
        ).hexdigest()[:32]
        now_ts = self._now()
        self.store.upsert_binding(
            binding_id=binding_id,
            did=resolved_did,
            binding_type="cln",
            subject=subject,
            attestation_json=json.dumps(attestation, sort_keys=True, separators=(",", ":")),
            signature=attestation["signature"],
            now_ts=now_ts,
        )
        return {
            "ok": True,
            "binding_id": binding_id,
            "did": resolved_did,
            "binding_type": "cln",
            "subject": subject,
        }

    def status(self) -> Dict[str, Any]:
        identity = self.store.get_identity()
        bindings = self.store.list_bindings()

        binding_summary: Dict[str, int] = {"nostr": 0, "cln": 0}
        for row in bindings:
            binding_type = str(row.get("binding_type") or "")
            if binding_type in binding_summary:
                binding_summary[binding_type] += 1

        return {
            "ok": True,
            "identity": identity,
            "bindings": binding_summary,
            "active_polls": self.store.count_polls_by_status("active"),
            "completed_polls": self.store.count_polls_by_status("completed"),
            "total_polls": self.store.count_total_polls(),
            "total_votes": self.store.count_total_votes(),
            "network_enabled": self.network_enabled,
            "gateway_url": self.gateway_url,
            "min_governance_bond_sats": self.min_governance_bond_sats,
        }

    def upgrade(self, target_tier: str = "governance", bond_sats: int = 0) -> Dict[str, Any]:
        if target_tier not in self.VALID_GOVERNANCE_TIERS:
            return {
                "error": "invalid target_tier",
                "valid_tiers": sorted(self.VALID_GOVERNANCE_TIERS),
            }

        identity = self.store.get_identity()
        if not identity:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}

        if target_tier == "governance" and int(bond_sats) < self.min_governance_bond_sats:
            return {
                "error": "insufficient bond for governance tier",
                "required_bond_sats": self.min_governance_bond_sats,
            }

        self.store.update_governance_tier(target_tier, self._now())
        identity = self.store.get_identity() or {}
        return {
            "ok": True,
            "did": identity.get("did", ""),
            "governance_tier": identity.get("governance_tier", target_tier),
        }

    def _normalize_poll_options(self, options: List[Any]) -> Optional[List[str]]:
        if not isinstance(options, list):
            return None
        cleaned: List[str] = []
        for item in options:
            if not isinstance(item, str):
                return None
            value = item.strip()
            if not value or len(value) > 64:
                return None
            if value in cleaned:
                return None
            cleaned.append(value)
        if len(cleaned) < 2 or len(cleaned) > 10:
            return None
        return cleaned

    def _refresh_poll_state(self, poll: Dict[str, Any]) -> Dict[str, Any]:
        if poll.get("status") == "active" and int(poll.get("deadline") or 0) <= self._now():
            self.store.set_poll_status(str(poll["poll_id"]), "completed", self._now())
            updated = self.store.get_poll(str(poll["poll_id"]))
            return updated or poll
        return poll

    def poll_create(
        self,
        poll_type: str,
        title: str,
        options: List[Any],
        deadline: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        tier_error = self._require_governance()
        if tier_error:
            return tier_error

        if self.store.count_total_polls() >= self.MAX_TOTAL_POLLS:
            return {"error": "poll capacity reached"}

        if not isinstance(poll_type, str):
            return {"error": "invalid poll_type"}
        poll_type = poll_type.strip()
        if not poll_type or len(poll_type) > self.MAX_POLL_TYPE_LEN:
            return {"error": "invalid poll_type"}
        if not all(ch.isalnum() or ch in "-_" for ch in poll_type):
            return {"error": "invalid poll_type (alphanumeric, hyphens, underscores only)"}

        if not isinstance(title, str):
            return {"error": "invalid title"}
        title = title.strip()
        if not title or len(title) > self.MAX_POLL_TITLE_LEN:
            return {"error": "invalid title"}

        if not isinstance(deadline, int) or deadline <= self._now():
            return {"error": "invalid deadline (must be a future unix timestamp)"}

        cleaned_options = self._normalize_poll_options(options)
        if cleaned_options is None:
            return {"error": "invalid options (expected 2-10 unique non-empty strings)"}

        metadata = metadata or {}
        if not isinstance(metadata, dict):
            return {"error": "metadata must be an object"}
        try:
            metadata_json = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            return {"error": "metadata must be JSON-serializable"}
        if len(metadata_json) > self.MAX_METADATA_JSON_LEN:
            return {"error": f"metadata too large (max {self.MAX_METADATA_JSON_LEN} bytes)"}

        identity = self.store.get_identity() or {}
        created_by = str(identity.get("did") or self._our_node_pubkey() or "local-node")

        poll_id = str(uuid.uuid4())
        remote_poll_id = ""

        if self.network_enabled and self._gateway_client:
            try:
                remote = self._gateway_client.create_poll(
                    poll_type=poll_type,
                    title=title,
                    options=cleaned_options,
                    deadline=deadline,
                    metadata=metadata,
                    creator=created_by,
                )
                if remote:
                    remote_poll_id = remote
            except Exception as exc:
                self._log(f"archon: remote poll creation failed; keeping local poll only: {exc}", "warn")

        now_ts = self._now()
        self.store.create_poll(
            poll_id=poll_id,
            remote_poll_id=remote_poll_id,
            poll_type=poll_type,
            title=title,
            options_json=json.dumps(cleaned_options, sort_keys=True, separators=(",", ":")),
            metadata_json=metadata_json,
            created_by=created_by,
            deadline=deadline,
            now_ts=now_ts,
        )

        return {
            "ok": True,
            "poll_id": poll_id,
            "remote_poll_id": remote_poll_id,
            "status": "active",
            "deadline": deadline,
        }

    def poll_status(self, poll_id: str) -> Dict[str, Any]:
        if not isinstance(poll_id, str) or not poll_id:
            return {"error": "poll_id is required"}

        poll = self.store.get_poll(poll_id)
        if not poll:
            return {"error": "poll not found"}

        poll = self._refresh_poll_state(poll)

        try:
            options = json.loads(poll.get("options_json") or "[]")
            if not isinstance(options, list):
                options = []
        except (json.JSONDecodeError, TypeError):
            options = []

        votes = self.store.list_votes_for_poll(poll_id)
        tally: Dict[str, int] = {opt: 0 for opt in options if isinstance(opt, str)}
        for vote in votes:
            choice = vote.get("choice")
            if isinstance(choice, str):
                tally[choice] = tally.get(choice, 0) + 1

        return {
            "ok": True,
            "poll": {
                "poll_id": poll.get("poll_id"),
                "remote_poll_id": poll.get("remote_poll_id") or "",
                "poll_type": poll.get("poll_type"),
                "title": poll.get("title"),
                "created_by": poll.get("created_by"),
                "deadline": poll.get("deadline"),
                "status": poll.get("status"),
            },
            "tally": tally,
            "vote_count": len(votes),
            "voters": [vote.get("voter_id") for vote in votes],
        }

    def _voter_id(self) -> str:
        identity = self.store.get_identity() or {}
        did = str(identity.get("did") or "")
        if did:
            return did
        node_pubkey = self._our_node_pubkey()
        return node_pubkey if node_pubkey else "local-node"

    def vote(self, poll_id: str, choice: str, reason: str = "") -> Dict[str, Any]:
        tier_error = self._require_governance()
        if tier_error:
            return tier_error

        if self.store.count_total_votes() >= self.MAX_TOTAL_VOTES:
            return {"error": "vote capacity reached"}

        if not isinstance(poll_id, str) or not poll_id:
            return {"error": "poll_id is required"}

        if not isinstance(choice, str):
            return {"error": "choice is required"}
        choice = choice.strip()
        if not choice:
            return {"error": "choice is required"}

        if not isinstance(reason, str):
            return {"error": "reason must be a string"}
        reason = reason.strip()
        if len(reason) > self.MAX_REASON_LEN:
            return {"error": f"reason too long (max {self.MAX_REASON_LEN} chars)"}

        poll = self.store.get_poll(poll_id)
        if not poll:
            return {"error": "poll not found"}

        poll = self._refresh_poll_state(poll)
        if poll.get("status") != "active":
            return {"error": "poll is not active", "status": poll.get("status")}

        try:
            options = json.loads(poll.get("options_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            options = []
        if choice not in options:
            return {"error": "invalid choice", "valid_choices": options}

        voter_id = self._voter_id()
        voted_at = self._now()
        canonical = json.dumps(
            {
                "poll_id": poll_id,
                "voter_id": voter_id,
                "choice": choice,
                "reason": reason,
                "voted_at": voted_at,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        try:
            signature = self._sign_message(canonical, required=True)
        except RuntimeError as exc:
            return {"error": f"vote signing failed: {exc}"}

        vote_id = hashlib.sha256(
            f"{poll_id}:{voter_id}:{choice}:{voted_at}".encode("utf-8")
        ).hexdigest()[:32]

        inserted = self.store.add_vote(
            vote_id=vote_id,
            poll_id=poll_id,
            voter_id=voter_id,
            choice=choice,
            reason=reason,
            voted_at=voted_at,
            signature=signature,
        )
        if not inserted:
            return {"error": "vote already exists for this voter and poll"}

        remote_vote_sent = False
        remote_poll_id = str(poll.get("remote_poll_id") or "")
        if self.network_enabled and self._gateway_client and remote_poll_id:
            try:
                remote_vote_sent = self._gateway_client.submit_vote(
                    poll_id=remote_poll_id,
                    voter_id=voter_id,
                    choice=choice,
                    reason=reason,
                )
            except Exception as exc:
                self._log(f"archon: remote vote submit failed (local vote preserved): {exc}", "warn")

        return {
            "ok": True,
            "vote_id": vote_id,
            "poll_id": poll_id,
            "voter_id": voter_id,
            "choice": choice,
            "remote_vote_sent": remote_vote_sent,
        }

    def my_votes(self, limit: int = 50) -> Dict[str, Any]:
        if not isinstance(limit, int) or limit <= 0:
            return {"error": "limit must be positive"}
        if limit > 500:
            limit = 500

        voter_id = self._voter_id()
        votes = self.store.list_votes_for_voter(voter_id=voter_id, limit=limit)
        return {
            "ok": True,
            "voter_id": voter_id,
            "count": len(votes),
            "votes": votes,
        }

    def prune(self, retention_days: int = 90) -> Dict[str, Any]:
        if not isinstance(retention_days, int) or retention_days < 1:
            return {"error": "retention_days must be a positive integer"}
        now_ts = self._now()
        completed = self.store.complete_expired_polls(now_ts=now_ts)
        cutoff = now_ts - (retention_days * 86400)
        removed = self.store.prune_completed_polls(before_ts=cutoff)
        return {
            "ok": True,
            "polls_completed": completed,
            "polls_removed": removed,
            "retention_days": retention_days,
        }
