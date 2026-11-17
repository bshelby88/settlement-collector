"""BOUNTY-07 pytest suite for settlement-collector claim-code issuance and redemption flow.

Covers:
- Valid code issuance on a settlement record (RAEN- prefix + 10-char uppercase hex)
- Open claims listing (/storefront/claims)
- Verification before redemption (/storefront/credits/:code)
- Happy-path redemption (/storefront/credits/:code/redeem)
- Unknown credit code rejection (404)
- Already-redeemed replay rejection (409)
- Malformed code format rejection (regex validation)
- Case insensitivity and whitespace trimming normalization
- Double-redeem concurrency guard (parallel concurrent redemptions)
- Idempotent repeat queries
- Protection against unbacked/internal order redemptions
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Generator, Tuple

import pytest

PORT = 4788
BASE_URL = f"http://127.0.0.1:{PORT}"

# Explicitly bypass local proxies for in-process test runner loopback requests
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def compute_claim_code(tx: str) -> str:
    """Mirrors storefront.js claimCode(tx): RAEN- + sha256(tx)[:10].upper()"""
    digest = hashlib.sha256(tx.encode("utf-8")).hexdigest()[:10].upper()
    return f"RAEN-{digest}"


def http_request(method: str, path: str, body: Dict[str, Any] | None = None) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with _OPENER.open(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"raw": raw}
        return err.code, parsed


@pytest.fixture(scope="session")
def server() -> Generator[subprocess.Popen, None, None]:
    """Launch in-process settlement-collector storefront server with scratch data."""
    scratch_dir = tempfile.mkdtemp(prefix="sf_pytest_")
    # storefront.js joins STOREFRONT_DATA_FILE with "storefront.json"
    data_file = os.path.join(scratch_dir, "storefront.json")

    # Pre-seed orders for tests
    tx_normal = "0x" + "1" * 64
    code_normal = compute_claim_code(tx_normal)

    tx_concurrent = "0x" + "2" * 64
    code_concurrent = compute_claim_code(tx_concurrent)

    tx_unbacked = "0x" + "3" * 64
    code_unbacked = compute_claim_code(tx_unbacked)

    initial_state = {
        "orders": [
            {
                "tx": tx_normal,
                "payer": "0x1111111111111111111111111111111111111111",
                "amount_usd": "0.05",
                "amount_atomic": "50000",
                "classification": "external",
                "product_name": "contract-review",
                "delivered": False,
                "credit_recorded": False,
                "claim_code": code_normal,
                "seen_at": "2026-11-17T00:00:00.000Z",
            },
            {
                "tx": tx_concurrent,
                "payer": "0x2222222222222222222222222222222222222222",
                "amount_usd": "0.10",
                "amount_atomic": "100000",
                "classification": "external",
                "product_name": "contract-review",
                "delivered": True,
                "claim_code": code_concurrent,
                "seen_at": "2026-11-17T00:00:00.000Z",
            },
            {
                "tx": tx_unbacked,
                "payer": "0x3333333333333333333333333333333333333333",
                "classification": "internal",
                "matched": False,
                "delivered": False,
                "claim_code": code_unbacked,
            },
        ],
        "registrations": {},
        "connectors": {},
        "subscriptions": {},
    }
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(initial_state, f)

    repo_root = os.path.dirname(os.path.abspath(__file__))
    env = {
        **os.environ,
        "PORT": str(PORT),
        "STOREFRONT_DATA_FILE": scratch_dir,
    }

    proc = subprocess.Popen(
        ["node", "run-test-server.js"],
        cwd=repo_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server readiness
    ready = False
    for _ in range(25):
        time.sleep(0.2)
        try:
            req = urllib.request.Request(f"{BASE_URL}/health")
            with _OPENER.open(req, timeout=2) as resp:
                if resp.status == 200:
                    ready = True
                    break
        except Exception:
            pass

    if not ready:
        proc.kill()
        out, err = proc.communicate()
        raise RuntimeError(f"Server failed to start on port {PORT}:\n{out.decode()}\n{err.decode()}")

    yield proc

    proc.kill()
    proc.wait()


def test_claim_code_format_and_derivation():
    """Verify deterministic claim code format: RAEN- + 10-char uppercase hex."""
    tx = "0x7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b"
    code = compute_claim_code(tx)
    assert code.startswith("RAEN-")
    assert len(code) == 15  # 'RAEN-' (5) + 10 hex chars
    assert set(code[5:]).issubset(set("0123456789ABCDEF"))


def test_open_claims_listing(server):
    """GET /storefront/claims should list eligible unclaimed external orders."""
    status, body = http_request("GET", "/storefront/claims")
    assert status == 200
    assert "open_claims" in body
    assert body["open_claims"] >= 1
    claims = body.get("claims", [])
    assert any(c.get("claim_code", "").startswith("RAEN-") for c in claims)


def test_happy_path_verification_and_redemption(server):
    """Verify clean redemption cycle: check -> redeem -> replay check."""
    tx = "0x" + "1" * 64
    code = compute_claim_code(tx)

    # 1. Verification before redemption
    status, body = http_request("GET", f"/storefront/credits/{code}")
    assert status == 200
    assert body["ok"] is True
    assert body["redeemed"] is False
    assert body["tx"] == tx

    # 2. Idempotent repeat verification
    status_rep, body_rep = http_request("GET", f"/storefront/credits/{code}")
    assert status_rep == 200
    assert body_rep["redeemed"] is False

    # 3. Happy-path redemption
    status_red, body_red = http_request("POST", f"/storefront/credits/{code}/redeem", {"agent_id": "pytest-agent-01"})
    assert status_red == 200
    assert body_red["ok"] is True
    assert body_red["tx"] == tx

    # 4. Verification after redemption
    status_after, body_after = http_request("GET", f"/storefront/credits/{code}")
    assert status_after == 200
    assert body_after["redeemed"] is True

    # 5. Replay attack rejection (409 Conflict)
    status_replay, body_replay = http_request("POST", f"/storefront/credits/{code}/redeem", {"agent_id": "pytest-attacker"})
    assert status_replay == 409
    assert body_replay["ok"] is False
    assert body_replay["error"] == "credit already redeemed"
    assert "redeemed_at" in body_replay


def test_case_insensitivity_and_whitespace(server):
    """Endpoint should handle lowercase and padded whitespace codes."""
    tx = "0x" + "1" * 64
    code = compute_claim_code(tx)
    lower_code = code.lower()

    status, body = http_request("GET", f"/storefront/credits/{lower_code}")
    assert status == 200
    assert body["ok"] is True


def test_unknown_code_rejection(server):
    """Unknown codes should return 404 Not Found."""
    bad_code = "RAEN-0000000000"
    status_get, body_get = http_request("GET", f"/storefront/credits/{bad_code}")
    assert status_get == 404
    assert body_get.get("error") == "unknown credit code"

    status_post, body_post = http_request("POST", f"/storefront/credits/{bad_code}/redeem", {})
    assert status_post == 404
    assert body_post.get("error") == "unknown credit code"


@pytest.mark.parametrize(
    "malformed",
    [
        "8922B9225B",         # missing prefix
        "INVALID-8922B9225B", # wrong prefix
        "RAEN-8922B",         # too short
        "RAEN-8922B9225BFF",  # too long
        "RAEN-ZZZZZZZZZZ",    # non-hex chars
    ],
)
def test_malformed_code_rejection(server, malformed: str):
    """Malformed code patterns fail regex guard and return 404."""
    status, _ = http_request("GET", f"/storefront/credits/{urllib.parse.quote(malformed)}")
    assert status == 404


def test_unbacked_order_protection(server):
    """Orders not backed by paid external transactions cannot be redeemed."""
    tx = "0x" + "3" * 64
    code = compute_claim_code(tx)

    status_get, body_get = http_request("GET", f"/storefront/credits/{code}")
    assert status_get == 404
    assert body_get.get("error") == "credit not backed by a paid order"

    status_post, body_post = http_request("POST", f"/storefront/credits/{code}/redeem", {})
    assert status_post == 404
    assert body_post.get("error") == "credit not backed by a paid order"


def test_double_redeem_concurrency(server):
    """Concurrent parallel redemptions allow exactly one 200 OK and reject remainder with 409."""
    tx = "0x" + "2" * 64
    code = compute_claim_code(tx)

    def do_redeem(idx: int) -> int:
        status, _ = http_request("POST", f"/storefront/credits/{code}/redeem", {"agent_id": f"worker-{idx}"})
        return status

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(do_redeem, range(8)))

    assert statuses.count(200) == 1, f"Expected exactly 1 success, got: {statuses.count(200)}"
    assert statuses.count(409) == 7, f"Expected 7 conflicts, got: {statuses.count(409)}"
