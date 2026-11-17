# settlement-collector

RAEN fleet settlement telemetry collector — per-service paid-route events, funnel stats, on-chain settlement ledger, and storefront prepaid claim-code authority.

## Storefront Claim-Code Flow (Credit Authority)

When an external on-chain USDC payment is confirmed on Base mainnet without prior email binding or manual order resolution, the settlement collector issues a self-serve claim code:

- **Format:** `RAEN-[0-9A-F]{10}` (`RAEN-` prefix followed by the first 10 hex characters of `sha256(tx)` in uppercase).
- **One-time & Non-expiring:** Credit is burned upon single redemption and rejects replay attempts with `409 Conflict`.
- **Prepaid Header:** Redeemed credits allow prepaid execution via `X-RAEN-CREDIT: <claim_code>` across participating RAEN fleet services.

### Key Endpoints

- `GET /storefront/claims` — Lists open external settled orders awaiting email binding or claim resolution.
- `GET /storefront/credits/:code` — Validates claim code existence, product association, and redemption status.
- `POST /storefront/credits/:code/redeem` — One-time atomic redemption marking the credit burned for fulfillment.

---

## Running the Test Suite (BOUNTY-07)

The repository provides both native Node.js and Python `pytest` test runners. Neither runner requires live blockchain spend or network access.

### 1. Native Node.js Test Suite

```bash
npm test
# or directly:
node test-claim-code.sim.js
```

### 2. Python pytest Test Suite

```bash
python -m pip install pytest
python -m pytest -v test_claim_code.py
```

---

## Test Coverage (BOUNTY-07 Acceptance Matrix)

| Requirement | Test Group / Function | Status |
|---|---|---|
| **Deterministic Code Issuance** | `Test Group 1` / `test_claim_code_format_and_derivation` | ✅ Asserts `RAEN-[0-9A-F]{10}` format and pure determinism |
| **Open Claims Listing** | `Test Group 2` / `test_open_claims_listing` | ✅ Verifies `GET /storefront/claims` lists open orders with valid claim codes |
| **Credit Verification** | `Test Group 3` / `test_happy_path_verification_and_redemption` | ✅ Asserts `GET /storefront/credits/:code` returns unredeemed state |
| **Happy-Path Redemption** | `Test Group 6` / `test_happy_path_verification_and_redemption` | ✅ Verifies `POST /storefront/credits/:code/redeem` returns `200 OK` and burns credit |
| **Unknown Code Handling** | `Test Group 8` / `test_unknown_code_rejection` | ✅ Asserts non-existent codes return `404 Not Found` on both GET and POST |
| **Replay Attack Guard** | `Test Group 7` / `test_happy_path_verification_and_redemption` | ✅ Asserts subsequent redemption attempts reject with `409 Conflict` |
| **Malformed Code Guard** | `Test Group 9` / `test_malformed_code_rejection` | ✅ Tests missing prefix, wrong prefix, short, long, and non-hex inputs return `404` |
| **Case & Whitespace Normalization** | `Test Group 5` / `test_case_insensitivity_and_whitespace` | ✅ Asserts lowercase and whitespace-padded inputs normalize cleanly |
| **Unbacked Order Guard** | `Test Group 10` / `test_unbacked_order_protection` | ✅ Rejects unbacked/internal order redemptions with `404` |
| **Double-Redeem Concurrency** | `Test Group 11` / `test_double_redeem_concurrency` | ✅ Executes concurrent parallel redemptions; exactly 1 succeeds and remainder return `409` |
| **Idempotent Verification** | `Test Group 4` / `test_happy_path_verification_and_redemption` | ✅ Asserts repeated GET queries do not alter redemption state |
