"""Unit tests for cl-hive-archon core service."""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.archon_service import ArchonService, ArchonStore


def _make_service(tmp_path, **kwargs):
    db_path = str(tmp_path / "archon.db")
    store = ArchonStore(db_path=db_path)
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
