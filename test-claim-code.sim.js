// Simulation & unit test suite for the settlement-collector claim-code flow (BOUNTY-07).
// Mocks Base RPC + Resend; drives the real wireStorefront Express app end-to-end:
//   - Claim-code generation & format invariant (RAEN-<10-hex SHA-256 prefix>)
//   - Valid issuance on unmatched external transfers (>= $0.01 floor) & un-emailed SKU orders
//   - Dust below floor (< $0.01) excluded from /storefront/claims; internal unmatched rejected
//   - GET /storefront/credits/:code verification (valid, idempotent repeat reads, normalization)
//   - Unknown / malformed / unpaid codes rejected with 404
//   - POST /storefront/credits/:code/redeem one-time redemption + disk persistence
//   - Replay redemption rejected with HTTP 409
//   - Concurrent double-redeem race (Promise.all) allows strictly 1 winner (200) and N-1 409s
// Run: node test-claim-code.sim.js

const fs = require("fs");
const os = require("os");
const path = require("path");

const TEST_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "raen-claim-test-"));
process.env.STOREFRONT_DATA_FILE = TEST_DIR;
process.env.REPORT_CRON_SECRET = "testsecret";
process.env.RESEND_API_KEY = "test-key";

const express = require("express");
const { wireStorefront, claimCode, makeStore } = require("./storefront.js");

const CANONICAL = "9b8a27e03ceba490953cda8ab1291c0e13baf72f";
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const EXT_PAYER = "0x" + "b".repeat(40);
const INT_PAYER = "0x" + "9".repeat(40);
const H0 = 2_000_000;

let head = H0;
const pending = [];
const served = new Set();

function addTransfer(tx, payer, amountAtomic, block) {
  pending.push({
    blockNumber: "0x" + block.toString(16),
    transactionHash: tx,
    topics: [
      TRANSFER_TOPIC,
      "0x" + payer.replace(/^0x/, "").padStart(64, "0"),
      "0x" + CANONICAL.padStart(64, "0"),
    ],
    data: "0x" + BigInt(amountAtomic).toString(16).padStart(64, "0"),
  });
}

let resendCalls = [];
const realFetch = global.fetch;
global.fetch = async (url, opts = {}) => {
  const u = String(url);
  if (u.startsWith("http://127.0.0.1")) return realFetch(url, opts);
  if (u.includes("api.resend.com")) {
    const body = JSON.parse(opts.body);
    resendCalls.push({ to: body.to, subject: body.subject, html: body.html });
    return {
      ok: true,
      status: 200,
      json: async () => ({ id: "mock-email-" + resendCalls.length }),
      text: async () => "{}",
    };
  }
  const rpc = JSON.parse(opts.body);
  const reply = (result) =>
    new Response(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result }), { status: 200 });
  if (rpc.method === "eth_blockNumber") return reply("0x" + head.toString(16));
  if (rpc.method === "eth_getLogs") {
    const from = parseInt(rpc.params[0].fromBlock, 16);
    const to = parseInt(rpc.params[0].toBlock, 16);
    const out = pending.filter(
      (l) =>
        !served.has(l.transactionHash) &&
        parseInt(l.blockNumber, 16) >= from &&
        parseInt(l.blockNumber, 16) <= to
    );
    out.forEach((l) => served.add(l.transactionHash));
    return reply(out);
  }
  return { ok: false, status: 404, text: async () => "unmocked " + u };
};

const app = express();
app.use(express.json({ limit: "256kb" }));
const store = wireStorefront(app, {
  isInternalPayer: (addr) => addr.toLowerCase() === INT_PAYER.toLowerCase(),
});
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}`;

async function reqJson(p, method = "GET", body) {
  const r = await fetch(base + p, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  return { status: r.status, json: await r.json().catch(() => null) };
}

let failures = 0;
function check(name, cond, detail) {
  if (cond) console.log("PASS", name);
  else {
    failures++;
    console.log("FAIL", name, detail !== undefined ? JSON.stringify(detail) : "");
  }
}

(async () => {
  const TX_UNMATCHED_EXT = "0x" + "1".repeat(64); // $7.50 external non-catalog transfer -> claim issued
  const TX_SKU_NO_EMAIL = "0x" + "2".repeat(64);  // $49.00 Dispute Package without email -> claim issued
  const TX_CONCURRENT = "0x" + "3".repeat(64);    // $19.00 Contract Review without email -> concurrency test
  const TX_DUST = "0x" + "4".repeat(64);          // $0.007 external scatter dust -> below $0.01 claim floor
  const TX_INTERNAL_UNPAID = "0x" + "5".repeat(64); // internal unmatched tx -> cannot be credited/redeemed

  // 1. Unit invariant on claimCode()
  const codeSample = claimCode(TX_UNMATCHED_EXT);
  check(
    "claimCode produces RAEN-<10 uppercase hex> format",
    /^RAEN-[0-9A-F]{10}$/.test(codeSample) && claimCode(TX_UNMATCHED_EXT) === codeSample,
    codeSample
  );

  // Stage synthetic on-chain settlements and run watcher scanOnce
  addTransfer(TX_UNMATCHED_EXT, EXT_PAYER, 7_500_000, H0 - 5);
  addTransfer(TX_SKU_NO_EMAIL, EXT_PAYER, 49_000_000, H0 - 4);
  addTransfer(TX_CONCURRENT, EXT_PAYER, 19_000_000, H0 - 3);
  addTransfer(TX_DUST, EXT_PAYER, 7_000, H0 - 2);
  addTransfer(TX_INTERNAL_UNPAID, INT_PAYER, 7_500_000, H0 - 1);

  const scanRes = await reqJson("/storefront/scan?secret=testsecret");
  check("scanOnce processed all 5 transfers", scanRes.status === 200 && scanRes.json.scanned === 5, scanRes);

  // 2. Open claims list excludes dust & internal unmatched transfers
  const claimsRes = await reqJson("/storefront/claims");
  const openTxs = (claimsRes.json && claimsRes.json.claims || []).map((c) => c.tx);
  check("open_claims includes external unmatched >= $0.01", openTxs.includes(TX_UNMATCHED_EXT), openTxs);
  check("open_claims includes matched SKU missing email", openTxs.includes(TX_SKU_NO_EMAIL), openTxs);
  check("open_claims excludes sub-$0.01 scatter dust", !openTxs.includes(TX_DUST), openTxs);
  check("open_claims excludes internal unmatched traffic", !openTxs.includes(TX_INTERNAL_UNPAID), openTxs);

  // 3. Idempotent verification on GET /storefront/credits/:code (exact & case-insensitive whitespace)
  const v1 = await reqJson(`/storefront/credits/${codeSample}`);
  const v2 = await reqJson(`/storefront/credits/${codeSample.toLowerCase()}%20`);
  check(
    "GET verify returns 200 unredeemed credit",
    v1.status === 200 && v1.json.ok === true && v1.json.redeemed === false && v1.json.amount_usd === 7.5,
    v1
  );
  check(
    "repeat idempotent GET verify stays unredeemed and normalizes case",
    v2.status === 200 && v2.json.ok === true && v2.json.redeemed === false && v2.json.tx === TX_UNMATCHED_EXT,
    v2
  );

  // 4. Reject unknown, malformed, dust/internal-unpaid credit codes
  const unknownCode = await reqJson("/storefront/credits/RAEN-0000000000");
  check("unknown credit code returns 404", unknownCode.status === 404 && unknownCode.json.ok === false, unknownCode);

  for (const bad of ["BADCODE", "RAEN-123", "RAEN-ZZZZZZZZZZ", "raen-1234567890ab"]) {
    const rGet = await reqJson(`/storefront/credits/${bad}`);
    const rPost = await reqJson(`/storefront/credits/${bad}/redeem`, "POST", { agent_id: "test-agent" });
    check(
      `malformed code '${bad}' rejected with 404 on GET & POST`,
      rGet.status === 404 && rPost.status === 404,
      { rGet, rPost }
    );
  }

  const internalCode = claimCode(TX_INTERNAL_UNPAID);
  const internalRes = await reqJson(`/storefront/credits/${internalCode}/redeem`, "POST", { agent_id: "test-agent" });
  check("internal unmatched order rejected as unpaid credit (404)", internalRes.status === 404, internalRes);

  // 5. Happy-path one-time redemption & replay 409 check + persistence verification
  const skuCode = claimCode(TX_SKU_NO_EMAIL);
  const redeem1 = await reqJson(`/storefront/credits/${skuCode}/redeem`, "POST", { agent_id: "contract-eye" });
  check(
    "first POST /redeem succeeds (200) with product metadata",
    redeem1.status === 200 && redeem1.json.ok === true && redeem1.json.product === "dispute-package",
    redeem1
  );

  const afterRedeem = await reqJson(`/storefront/credits/${skuCode}`);
  check("GET after redeem reports redeemed=true", afterRedeem.status === 200 && afterRedeem.json.redeemed === true, afterRedeem);

  const replay = await reqJson(`/storefront/credits/${skuCode}/redeem`, "POST", { agent_id: "attacker" });
  check(
    "replay POST /redeem rejected with 409 credit already redeemed",
    replay.status === 409 && replay.json.ok === false && Boolean(replay.json.redeemed_at),
    replay
  );

  // Reload store from disk after debounced save() (250ms) to prove state durability
  await new Promise((r) => setTimeout(r, 400));
  const reloadedStore = makeStore(() => {});
  const persistedOrder = reloadedStore.findOrder(TX_SKU_NO_EMAIL);
  check(
    "redeemed state persisted to storefront.json",
    Boolean(persistedOrder && persistedOrder.credit_redeemed === true && persistedOrder.credit_redeemed_at),
    persistedOrder
  );

  // 6. Concurrent double-redeem race check (5 parallel POSTs -> strictly 1 succeeds, 4 get 409)
  const raceCode = claimCode(TX_CONCURRENT);
  const raceResponses = await Promise.all(
    Array.from({ length: 5 }, (_, idx) =>
      reqJson(`/storefront/credits/${raceCode}/redeem`, "POST", { agent_id: `worker-${idx}` })
    )
  );
  const winners = raceResponses.filter((r) => r.status === 200 && r.json && r.json.ok === true);
  const conflicts = raceResponses.filter((r) => r.status === 409 && r.json && r.json.ok === false);
  check(
    "concurrent double-redeem race grants strictly 1 win (200) and 4 conflicts (409)",
    winners.length === 1 && conflicts.length === 4,
    raceResponses.map((r) => r.status)
  );

  // 7. Claim resolution via /storefront/register sends email & removes from /storefront/claims
  const regRes = await reqJson("/storefront/register", "POST", {
    tx_or_intent: TX_UNMATCHED_EXT,
    email: "claimant@example.dev",
  });
  check(
    "registering email on open external claim triggers credit confirmation",
    regRes.status === 200 && regRes.json.ok === true && regRes.json.credit_recorded === true,
    regRes
  );
  const claimsAfter = await reqJson("/storefront/claims");
  const stillOpen = (claimsAfter.json.claims || []).some((c) => c.tx === TX_UNMATCHED_EXT);
  check("resolved claim removed from open /storefront/claims list", !stillOpen, claimsAfter.json);

  // Code still remains valid on /storefront/credits/:code (prepaid credits do not expire by registration)
  const redeemExt = await reqJson(`/storefront/credits/${codeSample}/redeem`, "POST", { agent_id: "briefsnap" });
  check("registered external claim code still redeems once (200)", redeemExt.status === 200 && redeemExt.json.ok, redeemExt);

  console.log(failures === 0 ? "\nALL CLAIM-CODE CHECKS PASSED" : `\n${failures} CHECK(S) FAILED`);
  server.close();
  fs.rmSync(TEST_DIR, { recursive: true, force: true });
  process.exit(failures === 0 ? 0 : 1);
})().catch((e) => {
  console.error("test crashed:", e);
  server.close();
  process.exit(1);
});
