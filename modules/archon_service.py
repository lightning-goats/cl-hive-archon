"""Core service and persistence layer for cl-hive-archon."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional


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


class ArchonStore:
    """SQLite persistence for archon identity, bindings, polls, and votes."""

    def __init__(self, db_path: str, logger: Optional[Callable[[str, str], None]] = None):
        self.db_path = os.path.expanduser(db_path)
        self._logger = logger
        self._conn: Optional[sqlite3.Connection] = None

    def _log(self, message: str, level: str = "info") -> None:
        if self._logger:
            self._logger(message, level)

    def _get_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
        return self._conn

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

        conn.commit()

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
        conn.commit()

    def update_governance_tier(self, governance_tier: str, now_ts: int) -> None:
        conn = self._get_connection()
        conn.execute(
            "UPDATE archon_identity SET governance_tier = ?, updated_at = ? WHERE singleton_id = 1",
            (governance_tier, now_ts),
        )
        conn.commit()

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
        conn.commit()

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
        conn.commit()

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
        conn.commit()

    def count_polls_by_status(self, status: str) -> int:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM archon_polls WHERE status = ?",
            (status,),
        ).fetchone()
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
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            return False

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
        payload = {
            "poll_id": poll_id,
            "voter_id": voter_id,
            "choice": choice,
            "reason": reason,
        }
        data = self._request("POST", f"/v1/hive/polls/{poll_id}/votes", payload)
        return bool(data.get("ok", False))


class ArchonService:
    """Phase 6B service API used by cl-hive-archon RPC methods."""

    VALID_GOVERNANCE_TIERS = {"basic", "governance"}

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
        self.network_enabled = network_enabled
        self.min_governance_bond_sats = max(1, int(min_governance_bond_sats))
        self._time_fn = time_fn
        self._gateway_client = (
            ArchonGatewayClient(self.gateway_url) if self.gateway_url else None
        )
        self.store.initialize()

    def _log(self, message: str, level: str = "info") -> None:
        if self._logger:
            self._logger(message, level)

    def _now(self) -> int:
        return int(self._time_fn())

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

    def _sign_message(self, payload: str) -> str:
        if not self.rpc:
            return ""
        try:
            result = self.rpc.signmessage(payload)
            if isinstance(result, dict):
                return str(result.get("zbase", "") or "")
        except Exception as exc:
            self._log(f"archon: signmessage failed: {exc}", "warn")
        return ""

    def _resolve_did(self, did: str = "") -> str:
        if did:
            return did
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
        signature = self._sign_message(canonical)
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
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
                self._log(f"archon: gateway provisioning failed, using local fallback: {exc}", "warn")

        if not did:
            did = self._generate_local_did(node_pubkey=node_pubkey, label=label)

        governance_tier = "basic"
        if identity:
            governance_tier = str(identity.get("governance_tier") or "basic")

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

        resolved_did = self._resolve_did(did)
        if not resolved_did:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}

        attestation = self._build_attestation("nostr", resolved_did, nostr_pubkey)
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
        subject = cln_pubkey or self._our_node_pubkey()
        if not _is_valid_cln_pubkey(subject):
            return {"error": "invalid cln_pubkey (expected 66-char compressed secp256k1 pubkey)"}

        resolved_did = self._resolve_did(did)
        if not resolved_did:
            return {"error": "identity not provisioned", "hint": "run hive-archon-provision"}

        attestation = self._build_attestation("cln", resolved_did, subject)
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
        identity = self.store.get_identity()
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
            self.store.set_poll_status(poll["poll_id"], "completed", self._now())
            updated = self.store.get_poll(poll["poll_id"])
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

        if not isinstance(poll_type, str) or not poll_type.strip() or len(poll_type) > 32:
            return {"error": "invalid poll_type"}

        if not isinstance(title, str) or not title.strip() or len(title) > 200:
            return {"error": "invalid title"}

        if not isinstance(deadline, int) or deadline <= self._now():
            return {"error": "invalid deadline (must be a future unix timestamp)"}

        cleaned_options = self._normalize_poll_options(options)
        if cleaned_options is None:
            return {"error": "invalid options (expected 2-10 unique non-empty strings)"}

        metadata = metadata or {}
        if not isinstance(metadata, dict):
            return {"error": "metadata must be an object"}

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
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
                self._log(f"archon: remote poll creation failed; keeping local poll only: {exc}", "warn")

        now_ts = self._now()
        self.store.create_poll(
            poll_id=poll_id,
            remote_poll_id=remote_poll_id,
            poll_type=poll_type,
            title=title,
            options_json=json.dumps(cleaned_options, sort_keys=True, separators=(",", ":")),
            metadata_json=json.dumps(metadata, sort_keys=True, separators=(",", ":")),
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

        if not isinstance(poll_id, str) or not poll_id:
            return {"error": "poll_id is required"}

        if not isinstance(choice, str) or not choice.strip():
            return {"error": "choice is required"}

        if not isinstance(reason, str):
            return {"error": "reason must be a string"}

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
        signature = self._sign_message(canonical)

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
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as exc:
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
