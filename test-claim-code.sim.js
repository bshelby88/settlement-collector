// Test suite for settlement-collector claim-code issuance and redemption flow (BOUNTY-07)
// Covers:
// 1. Valid code issuance (RAEN- prefix + 10-char uppercase hex digest)
// 2. Open claims listing (/storefront/claims)
// 3. Credit verification (/storefront/credits/:code)
// 4. Happy-path redemption (/storefront/credits/:code/redeem)
// 5. Unknown credit code rejection (404)
// 6. Already-redeemed replay rejection (409)
// 7. Malformed code rejection (invalid prefix, wrong length, non-hex chars)
// 8. Case insensitivity and whitespace trimming normalization
// 9. Unbacked / unpaid order protection (404 not backed by a paid order)
// 10. Concurrency: race condition double-redeem protection (Promise.all)
// 11. Idempotent repeat verification queries

const http = require("http");
const os = require("os");
const path = require("path");
const fs = require("fs");
const express = require("express");

// Set up clean temp data file
const testDataFile = path.join(os.tmpdir(), `sf_claim_test_${Date.now()}.json`);
process.env.STOREFRONT_DATA_FILE = testDataFile;

const { wireStorefront, claimCode } = require("./storefront");

const app = express();
app.use(express.json());

const store = wireStorefront(app, { isInternalPayer: () => false });

let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (condition) {
    passed++;
    console.log(`  ✓ PASS: ${message}`);
  } else {
    failed++;
    console.error(`  ✗ FAIL: ${message}`);
  }
}

async function run() {
  console.log("==================================================================");
  console.log("  Running settlement-collector Claim-Code Test Suite (BOUNTY-07)");
  console.log("==================================================================\n");

  // Spin up ephemeral HTTP server on free port
  const server = http.createServer(app);
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}`;

  const request = async (method, pathUrl, body = null) => {
    const opts = {
      method,
      headers: { "Content-Type": "application/json" },
    };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(`${base}${pathUrl}`, opts);
    let data = null;
    try { data = await res.json(); } catch (_) {}
    return { status: res.status, body: data };
  };

  try {
    // -------------------------------------------------------------------------
    // 1. Code Issuance Specification (RAEN- prefix + 10-char uppercase hex)
    // -------------------------------------------------------------------------
    console.log("Test Group 1: Claim Code Format & Deterministic Derivation");
    const testTx1 = "0x7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b";
    const code1 = claimCode(testTx1);

    assert(typeof code1 === "string", "claimCode returns a string");
    assert(code1.startsWith("RAEN-"), "claim code has 'RAEN-' prefix");
    assert(/^RAEN-[0-9A-F]{10}$/.test(code1), `code matches /^RAEN-[0-9A-F]{10}$/ pattern (got: ${code1})`);

    const code1Repeat = claimCode(testTx1);
    assert(code1 === code1Repeat, "claim code generation is pure and deterministic");

    // -------------------------------------------------------------------------
    // 2. Open Claims Discovery (/storefront/claims)
    // -------------------------------------------------------------------------
    console.log("\nTest Group 2: Open Claims Listing Endpoint (/storefront/claims)");
    const order1 = {
      tx: testTx1,
      payer: "0x1111111111111111111111111111111111111111",
      amount_usd: "0.05",
      amount_atomic: "50000",
      classification: "external",
      product_name: "legal-brief",
      delivered: false,
      credit_recorded: false,
      claim_code: code1,
      seen_at: new Date().toISOString(),
    };
    store.addOrder(order1);

    const claimsRes = await request("GET", "/storefront/claims");
    assert(claimsRes.status === 200, "GET /storefront/claims returns HTTP 200");
    assert(claimsRes.body && claimsRes.body.open_claims > 0, "open_claims count is > 0");
    const foundClaim = (claimsRes.body.claims || []).find(c => c.tx === testTx1);
    assert(Boolean(foundClaim), "unclaimed order is listed in open claims");
    assert(foundClaim && foundClaim.claim_code === code1, "open claim entry contains correct claim_code");

    // -------------------------------------------------------------------------
    // 3. Credit Verification Before Redemption
    // -------------------------------------------------------------------------
    console.log("\nTest Group 3: Credit Verification (/storefront/credits/:code)");
    const verifyBefore = await request("GET", `/storefront/credits/${code1}`);
    assert(verifyBefore.status === 200, "GET /storefront/credits/:code returns HTTP 200");
    assert(verifyBefore.body && verifyBefore.body.ok === true, "verification response has ok: true");
    assert(verifyBefore.body && verifyBefore.body.redeemed === false, "credit is initially not redeemed");
    assert(verifyBefore.body && verifyBefore.body.tx === testTx1, "verification response reports matching tx");

    // -------------------------------------------------------------------------
    // 4. Idempotent Repeat Verification Queries
    // -------------------------------------------------------------------------
    console.log("\nTest Group 4: Idempotent Verification Queries");
    const verifyRepeat = await request("GET", `/storefront/credits/${code1}`);
    assert(verifyRepeat.status === 200 && verifyRepeat.body.redeemed === false, "repeated GET queries remain idempotent and do not mutate state");

    // -------------------------------------------------------------------------
    // 5. Normalization: Case-Insensitivity & Whitespace Trimming
    // -------------------------------------------------------------------------
    console.log("\nTest Group 5: Code Normalization (Case & Whitespace)");
    const lowerCode = code1.toLowerCase();
    const paddedCode = `  ${code1}  `;
    const verifyLower = await request("GET", `/storefront/credits/${lowerCode}`);
    assert(verifyLower.status === 200, "verification handles lowercase claim codes");
    const verifyPadded = await request("GET", `/storefront/credits/${encodeURIComponent(paddedCode)}`);
    assert(verifyPadded.status === 200, "verification trims whitespace");

    // -------------------------------------------------------------------------
    // 6. Happy-Path Redemption (/storefront/credits/:code/redeem)
    // -------------------------------------------------------------------------
    console.log("\nTest Group 6: Happy-Path Redemption");
    const redeemRes = await request("POST", `/storefront/credits/${code1}/redeem`, { agent_id: "agent-alpha-01" });
    assert(redeemRes.status === 200, "POST /storefront/credits/:code/redeem returns HTTP 200 on valid code");
    assert(redeemRes.body && redeemRes.body.ok === true, "redemption response indicates ok: true");
    assert(redeemRes.body && redeemRes.body.tx === testTx1, "redemption response returns matching tx");

    const verifyAfter = await request("GET", `/storefront/credits/${code1}`);
    assert(verifyAfter.status === 200 && verifyAfter.body.redeemed === true, "order is now marked as redeemed in verification endpoint");
    assert(order1.credit_redeemed === true, "order in-memory state updated with credit_redeemed = true");
    assert(Boolean(order1.credit_redeemed_at), "order recorded credit_redeemed_at ISO timestamp");

    // -------------------------------------------------------------------------
    // 7. Already-Redeemed Code (Replay Attack Rejection)
    // -------------------------------------------------------------------------
    console.log("\nTest Group 7: Replay Attack Rejection (409 Conflict)");
    const replayRes = await request("POST", `/storefront/credits/${code1}/redeem`, { agent_id: "agent-attacker" });
    assert(replayRes.status === 409, "subsequent redemption attempt rejected with HTTP 409 Conflict");
    assert(replayRes.body && replayRes.body.ok === false, "replay error payload has ok: false");
    assert(replayRes.body && replayRes.body.error === "credit already redeemed", "replay error message is 'credit already redeemed'");
    assert(Boolean(replayRes.body && replayRes.body.redeemed_at), "replay response includes original redeemed_at timestamp");

    // -------------------------------------------------------------------------
    // 8. Unknown Code Rejection (404 Not Found)
    // -------------------------------------------------------------------------
    console.log("\nTest Group 8: Unknown Code Handling (404 Not Found)");
    const unknownCode = "RAEN-0000000000";
    const unknownGet = await request("GET", `/storefront/credits/${unknownCode}`);
    assert(unknownGet.status === 404, "GET non-existent code returns HTTP 404");
    assert(unknownGet.body && unknownGet.body.error === "unknown credit code", "GET returns 'unknown credit code' error message");

    const unknownPost = await request("POST", `/storefront/credits/${unknownCode}/redeem`, { agent_id: "agent-02" });
    assert(unknownPost.status === 404, "POST non-existent code returns HTTP 404");
    assert(unknownPost.body && unknownPost.body.error === "unknown credit code", "POST returns 'unknown credit code' error message");

    // -------------------------------------------------------------------------
    // 9. Malformed Code Rejection
    // -------------------------------------------------------------------------
    console.log("\nTest Group 9: Malformed Code Rejection");
    const malformedCases = [
      "8922B9225B",              // missing RAEN- prefix
      "INVALID-8922B9225B",      // wrong prefix
      "RAEN-8922B",              // too short (5 chars)
      "RAEN-8922B9225BFF",       // too long (12 chars)
      "RAEN-ZZZZZZZZZZ",         // non-hex characters
      "RAEN-!@#$%^&*()",         // special symbols
    ];

    for (const bad of malformedCases) {
      const badRes = await request("GET", `/storefront/credits/${encodeURIComponent(bad)}`);
      assert(badRes.status === 404, `malformed code '${bad}' rejected with HTTP 404`);
    }

    // -------------------------------------------------------------------------
    // 10. Protection for Unbacked Orders (Not Delivered, Not Matched, Not External)
    // -------------------------------------------------------------------------
    console.log("\nTest Group 10: Unbacked Order Guard");
    const unbackedTx = "0x9999999999999999999999999999999999999999999999999999999999999999";
    const unbackedCode = claimCode(unbackedTx);
    const unbackedOrder = {
      tx: unbackedTx,
      payer: "0x2222222222222222222222222222222222222222",
      classification: "internal", // internal traffic, not paid
      matched: false,
      delivered: false,
      claim_code: unbackedCode,
    };
    store.addOrder(unbackedOrder);

    const unbackedGet = await request("GET", `/storefront/credits/${unbackedCode}`);
    assert(unbackedGet.status === 404, "unbacked order returns HTTP 404");
    assert(unbackedGet.body && unbackedGet.body.error === "credit not backed by a paid order", "returns 'credit not backed by a paid order'");

    const unbackedRedeem = await request("POST", `/storefront/credits/${unbackedCode}/redeem`, {});
    assert(unbackedRedeem.status === 404, "redeeming unbacked order returns HTTP 404");

    // -------------------------------------------------------------------------
    // 11. Concurrency: Double-Redeem Race Condition Guard
    // -------------------------------------------------------------------------
    console.log("\nTest Group 11: Concurrency / Race Condition Double-Redeem Guard");
    const concurrentTx = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const concurrentCode = claimCode(concurrentTx);
    store.addOrder({
      tx: concurrentTx,
      payer: "0x3333333333333333333333333333333333333333",
      amount_usd: "0.10",
      amount_atomic: "100000",
      classification: "external",
      product_name: "contract-analysis",
      delivered: true,
      claim_code: concurrentCode,
    });

    // Fire 10 parallel simultaneous redemption requests
    const concurrentAttempts = 10;
    const promises = [];
    for (let i = 0; i < concurrentAttempts; i++) {
      promises.push(request("POST", `/storefront/credits/${concurrentCode}/redeem`, { agent_id: `agent-race-${i}` }));
    }
    const results = await Promise.all(promises);

    const successCount = results.filter(r => r.status === 200).length;
    const conflictCount = results.filter(r => r.status === 409).length;

    assert(successCount === 1, `exactly ONE concurrent request succeeds with 200 OK (got: ${successCount})`);
    assert(conflictCount === concurrentAttempts - 1, `remaining ${concurrentAttempts - 1} concurrent requests rejected with 409 Conflict (got: ${conflictCount})`);

    // Clean up temp test data file
    try { fs.unlinkSync(testDataFile); } catch (_) {}
    server.close();

    console.log("\n==================================================================");
    console.log(`  Test Summary: ${passed} passed, ${failed} failed`);
    console.log("==================================================================");

    process.exit(failed === 0 ? 0 : 1);
  } catch (err) {
    console.error("UNEXPECTED TEST EXCEPTION:", err);
    try { fs.unlinkSync(testDataFile); } catch (_) {}
    server.close();
    process.exit(1);
  }
}

run();
