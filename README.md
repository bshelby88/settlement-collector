# settlement-collector

RAEN fleet settlement telemetry collector and autonomous USDC storefront (`index.js`, `storefront.js`, `clearing.js`).

## Test Suites (Offline Simulation)

All simulation suites run offline with mocked JSON-RPC (`eth_blockNumber` / `eth_getLogs`) and mocked Resend email delivery:

```bash
# Claim-code issuance, verification, one-time redemption & concurrency suite
node test-claim-code.sim.js

# Contract Watch subscription & reminder cron lifecycle suite
node test-subscriptions.sim.js

# Manual-pay recovery flow suite
node test-recovery.sim.js

# Machine webhook registration suite
node test_machine_register.sim.js
```

### Claim-Code Test Coverage (`test-claim-code.sim.js`)

- **Code generation & format:** deterministic `RAEN-<10 uppercase hex>` code derivation (`claimCode(tx)`).
- **Issuance rules (`GET /storefront/claims`):** issues claim codes for external unmatched transfers `>= $0.01` and matched SKU transfers missing email; excludes `< $0.01` scatter dust and internal unmatched transfers.
- **Verification (`GET /storefront/credits/:code`):** idempotent repeat reads, case-insensitive / whitespace-trimmed code normalization, and `404` rejection for unknown, malformed, or internal unmatched codes.
- **One-time redemption (`POST /storefront/credits/:code/redeem`):** happy-path redemption with atomic persistence to `storefront.json`, `409` conflict rejection on replay, and concurrent double-redeem race guard (`Promise.all` guarantees strictly 1 winner and `N - 1` `409` conflicts).
- **Self-serve email registration (`POST /storefront/register`):** triggers credit confirmation email, sets `credit_recorded: true`, and removes resolved orders from `/storefront/claims`.
