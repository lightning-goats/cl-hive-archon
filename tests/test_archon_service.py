"""Unit tests for cl-hive-archon core service."""

import json
import os
import sys
import time
from unittest.mock import patch, MagicMock
from urllib.error import URLError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.archon_service import ArchonGatewayClient, ArchonService, ArchonStore


class _MockRpc:
    """Minimal RPC mock that provides signmessage, getinfo, and listfunds."""

    def __init__(self, pubkey: str = "02" + "ab" * 32, local_balance_msat: int = 500_000_000_000):
        self._pubkey = pubkey
        self._local_balance_msat = local_balance_msat

    def getinfo(self):
        return {"id": self._pubkey}

    def signmessage(self, message: str):
        return {"zbase": "mock_sig_" + message[:16]}

    def listfunds(self):
        return {
            "channels": [
                {"our_amount_msat": self._local_balance_msat},
            ],
            "outputs": [],
        }


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
    current_time = [time.time()]
    service = _make_service(tmp_path, time_fn=lambda: current_time[0])
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    create = service.poll_create(
        poll_type="ban",
        title="Ban peer A",
        options=["ban", "no-ban"],
        deadline=int(current_time[0]) + 10,
        metadata={},
    )
    assert create["ok"] is True

    # Advance time past deadline
    current_time[0] += 11
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

    foreign_did = "did:cid:b" + "abcdefgh" * 6
    res = service.bind_nostr("ab" * 32, did=foreign_did)
    assert "error" in res
    assert "not owned" in res["error"]

    res2 = service.bind_cln("02" + "cd" * 32, did=foreign_did)
    assert "error" in res2
    assert "not owned" in res2["error"]


def test_bind_rejects_explicit_did_without_identity(tmp_path):
    service = _make_service(tmp_path)
    unowned_did = "did:cid:b" + "ijklmnop" * 6

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


# ---------------------------------------------------------------------------
# Audit action-item tests
# ---------------------------------------------------------------------------


def test_bond_verification_fails_insufficient_balance(tmp_path):
    """Bond verification should reject upgrade when channel balance is too low."""
    low_balance_rpc = _MockRpc(local_balance_msat=10_000_000)  # 10,000 sats
    service = _make_service(tmp_path, rpc=low_balance_rpc)
    service.provision()

    result = service.upgrade(target_tier="governance", bond_sats=100_000)
    assert "error" in result
    assert "bond verification failed" in result["error"]
    assert result.get("local_balance_sats", 0) < 100_000


def test_voter_id_pinned_to_node_pubkey(tmp_path):
    """Voter ID must be the node pubkey, not the DID, to prevent sybil voting."""
    expected_pubkey = "02" + "ab" * 32
    service = _make_service(tmp_path)
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    create = service.poll_create(
        poll_type="config",
        title="Test voter pinning",
        options=["yes", "no"],
        deadline=int(time.time()) + 3600,
        metadata={},
    )
    assert create["ok"] is True

    poll_id = create["poll_id"]
    vote_result = service.vote(poll_id=poll_id, choice="yes", reason="test")
    assert vote_result["ok"] is True
    assert vote_result["voter_id"] == expected_pubkey

    # Also verify via my_votes() that the voter_id is the pubkey
    my = service.my_votes(limit=10)
    assert my["ok"] is True
    assert my["voter_id"] == expected_pubkey
    assert my["count"] == 1
    assert my["votes"][0]["voter_id"] == expected_pubkey

    # Confirm voter_id is NOT the DID
    identity = service.store.get_identity()
    assert identity is not None
    did = identity.get("did", "")
    assert did.startswith("did:cid:")
    assert vote_result["voter_id"] != did


def test_outbox_queuing_and_processing(tmp_path):
    """Outbox should process pending entries and return correct structure."""
    db_path = str(tmp_path / "archon_outbox.db")
    store = ArchonStore(db_path=db_path)
    service = ArchonService(
        store=store,
        rpc=_MockRpc(),
        network_enabled=True,
        gateway_url="http://localhost:9999",
    )

    result = service.provision()
    assert result["ok"] is True
    # Provision falls back to local when the gateway at :9999 is unreachable,
    # and queues a retry entry in the outbox.
    assert result["source"] == "local-fallback"

    # process_outbox should return the expected structure
    outbox_result = service.process_outbox()
    assert "processed" in outbox_result
    assert "succeeded" in outbox_result
    assert "failed" in outbox_result
    # The queued provision retry should be attempted (and fail against :9999)
    assert outbox_result["processed"] >= 1
    assert outbox_result["failed"] >= 1


def test_outbox_store_methods(tmp_path):
    """Outbox store methods: add, list, mark_success, mark_failed, prune."""
    db_path = str(tmp_path / "archon_store.db")
    store = ArchonStore(db_path=db_path)
    store.initialize()

    now = int(time.time())

    # add_outbox_entry
    store.add_outbox_entry(
        entry_id="entry-001",
        operation="provision",
        payload_json='{"node_pubkey":"02aabb"}',
        now_ts=now,
        max_retries=3,
    )
    store.add_outbox_entry(
        entry_id="entry-002",
        operation="create_poll",
        payload_json='{"poll_type":"config"}',
        now_ts=now,
        max_retries=5,
    )

    # list_outbox_pending - both entries should be pending
    pending = store.list_outbox_pending(now_ts=now + 1, limit=50)
    assert len(pending) == 2
    entry_ids = {e["entry_id"] for e in pending}
    assert "entry-001" in entry_ids
    assert "entry-002" in entry_ids

    # mark_outbox_success
    store.mark_outbox_success("entry-001")
    pending_after = store.list_outbox_pending(now_ts=now + 1, limit=50)
    assert len(pending_after) == 1
    assert pending_after[0]["entry_id"] == "entry-002"

    # mark_outbox_failed - retry_count increments, next_retry_at set
    store.mark_outbox_failed("entry-002", error="connection refused", next_retry_at=now + 120)
    # Should not appear in pending if now < next_retry_at
    pending_immediate = store.list_outbox_pending(now_ts=now + 1, limit=50)
    assert len(pending_immediate) == 0
    # But should appear once next_retry_at is reached
    pending_later = store.list_outbox_pending(now_ts=now + 121, limit=50)
    assert len(pending_later) == 1
    assert pending_later[0]["retry_count"] == 1
    assert pending_later[0]["last_error"] == "connection refused"

    # Exhaust retries (max_retries=5, already at 1)
    for i in range(4):
        store.mark_outbox_failed("entry-002", error=f"fail {i+2}", next_retry_at=now + 200 + i)
    exhausted = store.list_outbox_pending(now_ts=now + 9999, limit=50)
    assert len(exhausted) == 0  # entry-002 should be exhausted

    # prune_outbox - removes completed/exhausted entries older than cutoff
    pruned = store.prune_outbox(before_ts=now + 10000)
    assert pruned == 2  # entry-001 (completed) + entry-002 (exhausted)


# ---------------------------------------------------------------------------
# sign_message tests
# ---------------------------------------------------------------------------


def test_sign_message_success(tmp_path):
    """sign_message should return ok and a signature for valid input."""
    service = _make_service(tmp_path)
    result = service.sign_message("hello world")
    assert result["ok"] is True
    assert result["signature"].startswith("mock_sig_")


def test_sign_message_rejects_oversized(tmp_path):
    """sign_message should reject messages exceeding MAX_SIGN_MESSAGE_LEN."""
    service = _make_service(tmp_path)
    big = "x" * (service.MAX_SIGN_MESSAGE_LEN + 1)
    result = service.sign_message(big)
    assert "error" in result
    assert "characters" in result["error"]


def test_sign_message_handles_rpc_failure(tmp_path):
    """sign_message should return a truncated error when RPC fails."""
    class _FailRpc(_MockRpc):
        def signmessage(self, message):
            raise RuntimeError("HSM connection lost " + "x" * 300)

    service = _make_service(tmp_path, rpc=_FailRpc())
    result = service.sign_message("test")
    assert "error" in result
    assert len(result["error"]) <= 200


# ---------------------------------------------------------------------------
# Capacity limit tests
# ---------------------------------------------------------------------------


def test_poll_capacity_limit(tmp_path):
    """poll_create should reject when MAX_TOTAL_POLLS is reached."""
    current_time = [time.time()]
    service = _make_service(tmp_path, time_fn=lambda: current_time[0])
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    # Patch MAX_TOTAL_POLLS to a small number for testing
    original = service.MAX_TOTAL_POLLS
    service.MAX_TOTAL_POLLS = 3
    try:
        for i in range(3):
            current_time[0] += 1
            result = service.poll_create(
                poll_type="config", title=f"Poll {i}",
                options=["yes", "no"],
                deadline=int(current_time[0]) + 36000,
            )
            assert result["ok"] is True

        current_time[0] += 1
        over = service.poll_create(
            poll_type="config", title="Over limit",
            options=["yes", "no"],
            deadline=int(current_time[0]) + 36000,
        )
        assert "error" in over
        assert "capacity" in over["error"]
    finally:
        service.MAX_TOTAL_POLLS = original


def test_vote_capacity_limit(tmp_path):
    """vote should reject when MAX_TOTAL_VOTES is reached."""
    current_time = [time.time()]
    service = _make_service(tmp_path, time_fn=lambda: current_time[0])
    service.provision()
    service.upgrade(target_tier="governance", bond_sats=100_000)

    # Patch MAX_TOTAL_VOTES to a small number for testing
    original = service.MAX_TOTAL_VOTES
    service.MAX_TOTAL_VOTES = 1
    try:
        current_time[0] += 1
        p1 = service.poll_create(
            poll_type="config", title="Poll A",
            options=["yes", "no"],
            deadline=int(current_time[0]) + 36000,
        )
        assert p1["ok"] is True

        current_time[0] += 1
        v1 = service.vote(p1["poll_id"], "yes")
        assert v1["ok"] is True

        current_time[0] += 1
        p2 = service.poll_create(
            poll_type="config", title="Poll B",
            options=["yes", "no"],
            deadline=int(current_time[0]) + 36000,
        )
        assert p2["ok"] is True

        current_time[0] += 1
        v2 = service.vote(p2["poll_id"], "no")
        assert "error" in v2
        assert "capacity" in v2["error"]
    finally:
        service.MAX_TOTAL_VOTES = original


# ---------------------------------------------------------------------------
# my_votes edge cases
# ---------------------------------------------------------------------------


def test_my_votes_rejects_bad_limits(tmp_path):
    """my_votes should reject zero and negative limits."""
    service = _make_service(tmp_path)
    assert "error" in service.my_votes(limit=0)
    assert "error" in service.my_votes(limit=-1)


# ---------------------------------------------------------------------------
# ArchonGatewayClient tests
# ---------------------------------------------------------------------------


def _mock_urlopen_response(data: dict, status: int = 200):
    """Create a mock context manager for urllib.request.urlopen."""
    body = json.dumps(data).encode("utf-8")
    mock_response = MagicMock()
    mock_response.read.return_value = body
    mock_response.status = status
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)
    return mock_response


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_provision_success(mock_urlopen, mock_dns):
    """provision_identity should POST to /api/v1/did and return a DID."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"did": "did:cid:btest123"})

    client = ArchonGatewayClient("https://archon.example.com")
    result = client.provision_identity("02" + "ab" * 32, "my-label")
    assert result == "did:cid:btest123"

    # Verify correct API path and archon Operation format
    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert "/api/v1/did" in req.full_url
    body = json.loads(req.data)
    assert body["type"] == "create"
    assert "created" in body
    assert body["registration"]["version"] == 1
    assert body["registration"]["type"] == "agent"
    assert body["data"]["node_pubkey"] == "02" + "ab" * 32


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_provision_bad_response(mock_urlopen, mock_dns):
    """provision_identity should return None when response has no valid DID."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"error": "bad"})

    client = ArchonGatewayClient("https://archon.example.com")
    result = client.provision_identity("02" + "ab" * 32, "label")
    assert result is None


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_create_poll_success(mock_urlopen, mock_dns):
    """create_poll should POST to /api/v1/polls with PollConfig v2 format."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"did": "did:cid:bpoll123"})

    client = ArchonGatewayClient("https://archon.example.com")
    result = client.create_poll("config", "Test Poll", ["yes", "no"], 1771700000, {"desc": "test"}, "creator-1")
    assert result == "did:cid:bpoll123"

    # Verify correct API path and PollConfig format
    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert "/api/v1/polls" in req.full_url
    body = json.loads(req.data)
    assert body["poll"]["version"] == 2
    assert body["poll"]["name"] == "Test Poll"
    assert body["poll"]["options"] == ["yes", "no"]
    assert "T" in body["poll"]["deadline"]  # ISO 8601 format


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_submit_vote_success(mock_urlopen, mock_dns):
    """submit_vote should POST to /api/v1/polls/:id/vote with integer vote."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"did": "did:cid:bballot1"})

    client = ArchonGatewayClient("https://archon.example.com")
    result = client.submit_vote("did:cid:bpoll1", vote_index=1, voter_id="voter-1")
    assert result is True

    # Verify correct API path and vote format
    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert "/api/v1/polls/" in req.full_url
    assert req.full_url.endswith("/vote")
    body = json.loads(req.data)
    assert body["vote"] == 1
    assert "choice" not in body  # Must be integer vote, not string choice


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_submit_vote_failure(mock_urlopen, mock_dns):
    """submit_vote should return False when gateway returns no DID."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"error": "not authorized"})

    client = ArchonGatewayClient("https://archon.example.com")
    result = client.submit_vote("did:cid:bpoll1", vote_index=1, voter_id="voter-1")
    assert result is False


@patch("modules.archon_service.socket.getaddrinfo")
def test_gateway_client_blocks_private_ip(mock_dns):
    """Gateway client should reject requests to private IPs."""
    mock_dns.return_value = [(2, 1, 6, '', ('10.0.0.1', 0))]

    client = ArchonGatewayClient("https://internal.example.com")
    # _request returns {} on blocked IP, so provision returns None
    result = client.provision_identity("02" + "ab" * 32, "label")
    assert result is None


@patch("modules.archon_service.socket.getaddrinfo")
def test_gateway_client_blocks_loopback_dns(mock_dns):
    """Gateway client should reject DNS names that resolve to loopback."""
    mock_dns.return_value = [(2, 1, 6, '', ('127.0.0.1', 0))]

    client = ArchonGatewayClient("https://evil.example.com")
    result = client.provision_identity("02" + "ab" * 32, "label")
    assert result is None


@patch("modules.archon_service.socket.getaddrinfo")
def test_gateway_client_rejects_dns_failure(mock_dns):
    """Gateway client should reject requests when DNS resolution fails."""
    mock_dns.side_effect = OSError("DNS failure")

    client = ArchonGatewayClient("https://unreachable.example.com")
    result = client.provision_identity("02" + "ab" * 32, "label")
    assert result is None


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_sends_auth_header(mock_urlopen, mock_dns):
    """Gateway client should send Authorization header when auth_token is set."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"did": "did:cid:btest"})

    client = ArchonGatewayClient("https://archon.example.com", auth_token="secret-token-123")
    client.provision_identity("02" + "ab" * 32, "label")

    # Verify the request was made with the auth header
    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert req.get_header("Authorization") == "Bearer secret-token-123"


@patch("modules.archon_service.socket.getaddrinfo")
@patch("modules.archon_service.urllib.request.urlopen")
def test_gateway_client_no_auth_header_when_empty(mock_urlopen, mock_dns):
    """Gateway client should not send Authorization header when auth_token is empty."""
    mock_dns.return_value = [(2, 1, 6, '', ('93.184.216.34', 0))]
    mock_urlopen.return_value = _mock_urlopen_response({"did": "did:cid:btest"})

    client = ArchonGatewayClient("https://archon.example.com", auth_token="")
    client.provision_identity("02" + "ab" * 32, "label")

    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    assert req.get_header("Authorization") is None
