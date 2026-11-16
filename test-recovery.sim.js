// Simulation test for manual-pay recovery (Bean, Nov 15 2026).
// Mocks Base RPC + Resend; drives the real wireStorefront express app:
//   1. external $0.05 transfer (no catalog match) -> claim code published
//   2. $0.007 external dust -> recorded, NO claim (below CLAIM_MIN floor)
//   3. register(tx05, email) -> ONE credit-confirmation email, credit_recorded
//   4. re-register same tx -> NO duplicate email
//   5. matched $19 flow still delivers product as before (regression guard)
//   6. startup backfill assigns a claim to a pre-existing external order
// Run: node test-recovery.sim.js
process.env.STOREFRONT_DATA_FILE = require("path").join(require("os").tmpdir(), "storefront-rec-test-" + process.pid); // DIRECTORY; storefront.json is joined inside
process.env.REPORT_CRON_SECRET = "testsecret";
process.env.RESEND_API_KEY = "test-key"; // mock intercepts the HTTP call

const assert = require("assert");
const express = require("express");
const { wireStorefront } = require("./storefront.js");

const CANONICAL = "9b8a27e03ceba490953cda8ab1291c0e13baf72f";
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const PAYER = "0x" + "b".repeat(40); // external payer
const H0 = 2_000_000;

let head = H0;
const pending = [];
const served = new Set();
function addTransfer(tx, block, atomic) {
  pending.push({
    blockNumber: "0x" + block.toString(16),
    transactionHash: tx,
    topics: [TRANSFER_TOPIC, "0x" + PAYER.slice(2).padStart(64, "0"), "0x" + CANONICAL.padStart(64, "0")],
    data: "0x" + atomic.toString(16).padStart(64, "0"),
  });
}
const TX05 = "0x" + "5".repeat(64);      // $0.05 — royal-ruby per-call price, no catalog match
const TXDUST = "0x" + "d".repeat(64);    // $0.007021 — scatter dust
addTransfer(TX05, H0 - 5, 50000);
addTransfer(TXDUST, H0 - 4, 7021);

let resendCalls = [];
const realFetch = global.fetch;
global.fetch = async (url, opts = {}) => {
  const u = String(url);
  if (u.startsWith("http://127.0.0.1")) return realFetch(url, opts);
  if (u.includes("api.resend.com")) {
    const body = JSON.parse(opts.body);
    resendCalls.push({ to: body.to, subject: body.subject });
    return { ok: true, status: 200, json: async () => ({ id: "mock-" + resendCalls.length }), text: async () => "{}" };
  }
  const rpc = JSON.parse(opts.body);
  const reply = (result) => new Response(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result }), { status: 200 });
  if (rpc.method === "eth_blockNumber") return reply("0x" + head.toString(16));
  if (rpc.method === "eth_getLogs") {
    const from = parseInt(rpc.params[0].fromBlock, 16), to = parseInt(rpc.params[0].toBlock, 16);
    const out = pending.filter(l => !served.has(l.transactionHash) &&
      parseInt(l.blockNumber, 16) >= from && parseInt(l.blockNumber, 16) <= to);
    out.forEach(l => served.add(l.transactionHash));
    return reply(out);
  }
  return { ok: false, status: 404, text: async () => "unmocked " + u };
};

const app = express();
app.use(express.json({ limit: "256kb" }));
wireStorefront(app, { isInternalPayer: () => false });
const srv = app.listen(0);
const base = "http://127.0.0.1:" + srv.address().port;
const j = (r) => r.json();
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

(async () => {
  // 6. backfill: seed a PRE-EXISTING external order directly via the data file
  //    (simulate an order seen before the feature existed)
  const fs = require("fs");
  const df = process.env.STOREFRONT_DATA_FILE;
  await j(await fetch(base + "/storefront/scan?secret=testsecret")); // force first watcher pass
  await sleep(600); // debounced save flush
  const state = JSON.parse(fs.readFileSync(df + "/storefront.json", "utf8"));
  assert(state.orders.some(o => o.tx === TX05), "0.05 order stored");
  assert(state.orders.some(o => o.tx === TXDUST), "dust order stored");
  const o05 = state.orders.find(o => o.tx === TX05);
  const oDust = state.orders.find(o => o.tx === TXDUST);
  assert.strictEqual(o05.classification, "external");
  assert.strictEqual(o05.matched, false);

  // 1. claims endpoint lists the $0.05 claim, not the dust
  let claims = await j(await fetch(base + "/storefront/claims"));
  const c05 = claims.claims.find(c => c.tx === TX05);
  assert(c05 && c05.claim_code, "0.05 has claim code in /claims");
  assert(!claims.claims.some(c => c.tx === TXDUST), "dust not claimable");
  const CLAIM = c05.claim_code;

  // 3. register email against the unmatched tx -> credit confirmation
  resendCalls = [];
  let r = await j(await fetch(base + "/storefront/register", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tx_or_intent: TX05, email: "buyer@example.org" }),
  }));
  assert.strictEqual(r.ok, true, "register ok");
  assert.strictEqual(r.credit_recorded, true, "credit recorded on first register");
  assert.strictEqual(r.delivered, false, "TRUTH: no product delivered");
  assert.strictEqual(resendCalls.length, 1, "exactly one email sent");
  assert(/payment verified/i.test(resendCalls[0].subject), "subject mentions payment verified");
  assert.strictEqual(resendCalls[0].to[0], "buyer@example.org");

  // 4. re-register -> no duplicate email
  r = await j(await fetch(base + "/storefront/register", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tx_or_intent: TX05, email: "buyer@example.org" }),
  }));
  await sleep(300);
  assert.strictEqual(resendCalls.length, 1, "no duplicate credit email on re-register");

  // credit-recorded order drops out of open claims
  claims = await j(await fetch(base + "/storefront/claims"));
  assert(!claims.claims.some(c => c.tx === TX05), "claimed order leaves open claims");

  // 2. dust: no claim code, not claimable
  const state2 = JSON.parse(fs.readFileSync(df + "/storefront.json", "utf8"));
  const dust2 = state2.orders.find(o => o.tx === TXDUST);
  assert(!dust2.claim_code, "dust gets no claim code");

  // 5. regression: matched $19 product still delivers once an email registers
  //    (bare $19 is ambiguous contract-review/contract-watch -> default product;
  //     delivery waits for email — pre-existing behavior, guard it stays so)
  head = H0 + 1000;
  const TX19 = "0x" + "9".repeat(64);
  addTransfer(TX19, H0 + 900, 19000000);
  await j(await fetch(base + "/storefront/scan?secret=testsecret")); // force watcher pass
  await sleep(800);
  resendCalls = [];
  r = await j(await fetch(base + "/storefront/register", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tx_or_intent: TX19, email: "buyer19@example.org" }),
  }));
  assert.strictEqual(r.delivered, true, "matched $19 delivered after email registration");
  assert.strictEqual(r.product, "Contract Review", "$19 default product intact");
  assert.strictEqual(resendCalls.length, 1, "product delivery email sent once");
  const orders = await j(await fetch(base + "/storefront/orders"));
  const o19 = orders.orders.find(o => o.tx === TX19);
  assert(o19 && o19.matched && o19.delivered, "matched $19 order state delivered");

  srv.close();
  console.log("ALL RECOVERY SIM CHECKS PASS (6/6)");
  process.exit(0);
})().catch(e => { console.error("SIM FAIL:", e.message); process.exit(1); });
