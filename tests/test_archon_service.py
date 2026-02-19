"""Unit tests for cl-hive-archon core service."""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.archon_service import ArchonService, ArchonStore


class _MockRpc:
    """Minimal RPC mock that provides signmessage and getinfo."""

    def __init__(self, pubkey: str = "02" + "ab" * 32):
        self._pubkey = pubkey

    def getinfo(self):
        return {"id": self._pubkey}

    def signmessage(self, message: str):
        return {"zbase": "mock_sig_" + message[:16]}


def _make_service(tmp_path, **kwargs):
    db_path = str(tmp_path / "archon.db")
    store = ArchonStore(db_path=db_path)
    kwargs.setdefault("rpc", _MockRpc())
    return ArchonService(store=store, network_enabled=False, **kwargs)


def test_provision_local_identity(tmp_path):
    service = _make_service(tmp_path)

    result = service.provision()
    assert result["ok"] is True
    assert result["did"].startswith("did:cid:")
    assert result["source"] == "local-fallback"

    again = service.provision()
    assert again["ok"] is True
    assert again["already_provisioned"] is True
    assert again["did"] == result["did"]


def test_bind_nostr_requires_valid_pubkey(tmp_path):
    service = _make_service(tmp_path)
    service.provision()

    bad = service.bind_nostr("not-hex")
    assert "error" in bad

    good_key = "ab" * 32
    ok = service.bind_nostr(good_key)
    assert ok["ok"] is True
    assert ok["binding_type"] == "nostr"


def test_bind_cln_with_explicit_pubkey(tmp_path):
    service = _make_service(tmp_path)
    service.provision()

    pubkey = "02" + "cd" * 32
    result = service.bind_cln(cln_pubkey=pubkey)
    assert result["ok"] is True
    assert result["binding_type"] == "cln"
    assert result["subject"] == pubkey


def test_bind_rejects_invalid_explicit_did(tmp_path):
    service = _make_service(tmp_path)
    service.provision()

    res = service.bind_nostr("ab" * 32, did="not-a-did")
    assert "error" in res


def test_upgrade_requires_bond_for_governance(tmp_path):
    service = _make_service(tmp_path)
    service.provision()

    denied = service.upgrade(target_tier="governance", bond_sats=10)
    assert "error" in denied
    assert denied["required_bond_sats"] >= 50_000

    allowed = service.upgrade(target_tier="governance", bond_sats=100_000)
    assert allowed["ok"] is True
    assert allowed["governance_tier"] == "governance"


def test_poll_create_vote_and_status(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    create = service.poll_create(
        poll_type="config",
        title="Adjust fee floor",
        options=["yes", "no"],
        deadline=int(time.time()) + 3600,
        metadata={"change": "fee_floor"},
    )
    assert create["ok"] is True

    poll_id = create["poll_id"]
    vote = service.vote(poll_id=poll_id, choice="yes", reason="needed")
    assert vote["ok"] is True

    dup = service.vote(poll_id=poll_id, choice="yes")
    assert "error" in dup

    status = service.poll_status(poll_id)
    assert status["ok"] is True
    assert status["tally"]["yes"] == 1
    assert status["tally"]["no"] == 0


def test_poll_rejects_oversized_metadata(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    big = {"data": "x" * 9000}
    result = service.poll_create(
        poll_type="config",
        title="Large metadata",
        options=["yes", "no"],
        deadline=int(time.time()) + 3600,
        metadata=big,
    )
    assert "error" in result
    assert "metadata too large" in result["error"]


def test_vote_rejects_too_long_reason(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    create = service.poll_create(
        poll_type="promotion",
        title="Promote peer",
        options=["promote", "hold"],
        deadline=int(time.time()) + 3600,
        metadata={},
    )
    assert create["ok"] is True

    reason = "r" * 600
    vote = service.vote(create["poll_id"], "promote", reason)
    assert "error" in vote
    assert "reason too long" in vote["error"]


def test_expired_poll_auto_completes(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    create = service.poll_create(
        poll_type="ban",
        title="Ban peer A",
        options=["ban", "no-ban"],
        deadline=int(time.time()) + 1,
        metadata={},
    )
    assert create["ok"] is True

    time.sleep(1.1)
    status = service.poll_status(create["poll_id"])
    assert status["ok"] is True
    assert status["poll"]["status"] == "completed"

    late_vote = service.vote(create["poll_id"], "ban", "late")
    assert "error" in late_vote
    assert late_vote.get("status") == "completed"


def test_my_votes_returns_recent_votes(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    first = service.poll_create(
        poll_type="ban",
        title="Ban peer A",
        options=["ban", "no-ban"],
        deadline=int(time.time()) + 3600,
        metadata={},
    )
    second = service.poll_create(
        poll_type="promotion",
        title="Promote peer B",
        options=["promote", "hold"],
        deadline=int(time.time()) + 3600,
        metadata={},
    )

    service.vote(first["poll_id"], "ban", "evidence")
    service.vote(second["poll_id"], "promote", "good performer")

    votes = service.my_votes(limit=10)
    assert votes["ok"] is True
    assert votes["count"] == 2
    assert len(votes["votes"]) == 2


def test_bind_rejects_foreign_did(tmp_path):
    service = _make_service(tmp_path)
    service.provision()

    foreign_did = "did:cid:" + "ff" * 24
    res = service.bind_nostr("ab" * 32, did=foreign_did)
    assert "error" in res
    assert "not owned" in res["error"]

    res2 = service.bind_cln("02" + "cd" * 32, did=foreign_did)
    assert "error" in res2
    assert "not owned" in res2["error"]


def test_bind_rejects_explicit_did_without_identity(tmp_path):
    service = _make_service(tmp_path)
    unowned_did = "did:cid:" + "aa" * 24

    res = service.bind_nostr("ab" * 32, did=unowned_did)
    assert "error" in res
    assert "identity not provisioned" in res["error"]

    res2 = service.bind_cln("02" + "cd" * 32, did=unowned_did)
    assert "error" in res2
    assert "identity not provisioned" in res2["error"]


def test_prune_completed_polls(tmp_path):
    # Use a controllable time function so we can "age" polls
    current_time = [time.time()]
    service = _make_service(tmp_path, time_fn=lambda: current_time[0])
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    # Create a poll that expires in 1 second
    create = service.poll_create(
        poll_type="config",
        title="Old poll",
        options=["yes", "no"],
        deadline=int(current_time[0]) + 1,
        metadata={},
    )
    assert create["ok"] is True

    # Advance time past deadline and retention without touching poll status.
    current_time[0] += (100 * 86400) + 10
    result = service.prune(retention_days=90)
    assert result["ok"] is True
    assert result["polls_completed"] >= 1
    assert result["polls_removed"] >= 1

    # Poll should be gone
    status = service.poll_status(create["poll_id"])
    assert "error" in status


def test_gateway_validation_allows_local_http(tmp_path):
    store = ArchonStore(db_path=str(tmp_path / "archon.db"))
    service = ArchonService(
        store=store,
        rpc=_MockRpc(),
        gateway_url="http://localhost:4224",
        network_enabled=True,
    )
    assert service.network_enabled is True
    assert service.gateway_url == "http://localhost:4224"


def test_force_reprovision_cleans_bindings(tmp_path):
    service = _make_service(tmp_path)
    result = service.provision(label="first")
    old_did = result["did"]

    service.bind_nostr("ab" * 32)
    bindings = service.store.list_bindings()
    assert len(bindings) == 1
    assert bindings[0]["did"] == old_did

    # Force re-provision generates new DID, should clean old bindings
    result2 = service.provision(force=True, label="second")
    assert result2["did"] != old_did

    bindings_after = service.store.list_bindings()
    assert len(bindings_after) == 0


def test_status_returns_full_state(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.bind_nostr("ab" * 32)
    status = service.status()
    assert status["ok"] is True
    assert status["identity"] is not None
    assert status["identity"]["did"].startswith("did:cid:")
    assert "bindings" in status
    assert status["bindings"]["nostr"] >= 1
    assert status["network_enabled"] is False


def test_upgrade_rejects_invalid_tier(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    result = service.upgrade(target_tier="admin")
    assert "error" in result
    assert "invalid target_tier" in result["error"]


def test_vote_rejects_invalid_choice(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)
    create = service.poll_create(
        poll_type="config", title="Test", options=["yes", "no"],
        deadline=int(time.time()) + 3600,
    )
    assert create["ok"] is True
    result = service.vote(create["poll_id"], "maybe")
    assert "error" in result


def test_poll_create_rejects_invalid_poll_type(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)
    result = service.poll_create(
        poll_type="config; DROP TABLE",
        title="Bad",
        options=["yes", "no"],
        deadline=int(time.time()) + 3600,
    )
    assert "error" in result


def test_gateway_rejects_private_ips(tmp_path):
    """SSRF protection: private IPs should be rejected."""
    service = _make_service(tmp_path)
    assert service._is_valid_gateway_url("http://169.254.169.254/latest/meta-data/") is False
    assert service._is_valid_gateway_url("http://10.0.0.1/api") is False
    assert service._is_valid_gateway_url("http://192.168.1.1/api") is False
    assert service._is_valid_gateway_url("http://172.16.0.1/api") is False
    # HTTP only allowed for localhost
    assert service._is_valid_gateway_url("http://example.com/api") is False
    assert service._is_valid_gateway_url("https://example.com/api") is True
    assert service._is_valid_gateway_url("http://localhost/api") is True


def test_store_close(tmp_path):
    """ArchonStore.close() releases the thread-local connection."""
    db_path = str(tmp_path / "archon.db")
    store = ArchonStore(db_path=db_path)
    store.initialize()
    conn = store._get_connection()
    assert conn is not None
    store.close()
    assert getattr(store._local, "conn", None) is None


def test_prune_rejects_invalid_retention(tmp_path):
    service = _make_service(tmp_path)
    assert "error" in service.prune(retention_days=0)
    assert "error" in service.prune(retention_days=-5)


def test_upgrade_bond_sats_type_coercion(tmp_path):
    service = _make_service(tmp_path)
    service.provision()
    result = service.upgrade(target_tier="governance", bond_sats="not_a_number")
    assert "error" in result
    assert "bond" in result["error"].lower() or "insufficient" in result["error"].lower()
