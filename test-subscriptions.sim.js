// Simulation test for Contract Watch subscription flow (Bean, Nov 12 2026).
// Mocks Base RPC + Resend; drives the real wireStorefront express app end-to-end:
//   register(hint) -> scanOnce (simulated on-chain $19 payment) -> subscription created
//   second payment (renewal) -> expiry extends
//   T-7 / T-1 / expired cron passes -> reminders sent exactly once each
// Run: node test-subscriptions.sim.js
process.env.STOREFRONT_DATA_FILE = require("path").join(process.env.TEMP || "/tmp", "storefront-test-" + process.pid + ".json");
process.env.REPORT_CRON_SECRET = "testsecret";
process.env.RESEND_API_KEY = "test-key"; // mock intercepts the HTTP call

const express = require("express");
const { wireStorefront, CATALOG } = require("./storefront.js");

const CANONICAL = "9b8a27e03ceba490953cda8ab1291c0e13baf72f";
const USDC = "833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const PAYER = "0x" + "a".repeat(40);
const H0 = 1_000_000;

// pending simulated USDC transfers, served exactly once each
let head = H0;
const pending = [];
const served = new Set();
function addTransfer(tx, block) {
  pending.push({
    blockNumber: "0x" + block.toString(16),
    transactionHash: tx,
    topics: [TRANSFER_TOPIC, "0x" + PAYER.slice(2).padStart(64, "0"), "0x" + CANONICAL.padStart(64, "0")],
    data: "0x" + (19000000).toString(16).padStart(64, "0"), // $19.00 USDC
  });
}
addTransfer("0x" + "1".repeat(64), H0 - 2);

let resendCalls = [];
const realFetch = global.fetch;
global.fetch = async (url, opts = {}) => {
  const u = String(url);
  if (u.startsWith("http://127.0.0.1")) return realFetch(url, opts); // pass through to the app under test
  if (u.includes("api.resend.com")) {
    const body = JSON.parse(opts.body);
    resendCalls.push({ to: body.to, subject: body.subject });
    return { ok: true, status: 200, json: async () => ({ id: "mock-" + resendCalls.length }), text: async () => "{}" };
  }
  // JSON-RPC
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
const store = wireStorefront(app, { isInternalPayer: () => false });
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}`;
const j = async (path, method = "GET", body) => {
  const r = await fetch(base + path, { method, headers: { "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  return { status: r.status, json: await r.json().catch(() => null) };
};

let failures = 0;
function check(name, cond, detail) {
  if (cond) console.log("PASS", name);
  else { failures++; console.log("FAIL", name, detail !== undefined ? JSON.stringify(detail) : ""); }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

(async () => {
  const TX1 = "0x" + "1".repeat(64);
  const TX2 = "0x" + "2".repeat(64);

  // buyer registers intent with product hint before paying
  let r = await j("/storefront/register", "POST", { tx_or_intent: TX1, email: "buyer@test.dev", product: "contract-watch" });
  check("register T+hint accepted", r.status === 200 && r.json.ok, r);

  // simulated on-chain payment observed (background tick may race the manual scan — poll)
  await j("/storefront/scan?secret=testsecret");
  let o1 = null;
  for (let i = 0; i < 20 && !o1; i++) {
    await sleep(500);
    r = await j("/storefront/orders");
    o1 = r.json.orders.find(o => o.tx === TX1);
  }
  check("order observed on-chain and matched", Boolean(o1), r.json.total_orders);
  check("order product = contract-watch", o1 && o1.product === "contract-watch", o1);
  check("welcome email sent", resendCalls.some(c => c.to[0] === "buyer@test.dev" && /Contract Review Credit|Contract Watch/.test(c.subject)), resendCalls);

  r = await j("/storefront/subscriptions");
  const sub = r.json.subscriptions[0];
  check("subscription active", r.json.active === 1 && sub.status === "active", r.json);
  check("sub bound to payer+email", sub.payer === PAYER && sub.email === "buyer@test.dev", sub);
  check("30-day expiry", sub.days_remaining >= 29 && sub.days_remaining <= 30, sub.days_remaining);
  const exp1 = sub.expires_at;

  // renewal: same payer pays again next month
  head = H0 + 100;
  addTransfer(TX2, H0 + 50);
  await j("/storefront/register", "POST", { tx_or_intent: TX2, email: "buyer@test.dev", product: "contract-watch" });
  r = await j("/storefront/scan?secret=testsecret");
  check("renewal scan observed", r.json && r.json.scanned === 1, r);
  r = await j("/storefront/subscriptions");
  const sub2 = r.json.subscriptions[0];
  check("renewal extends expiry (+30d stacked)", sub2.renewals === 1 && sub2.days_remaining >= 59 && sub2.days_remaining <= 61 && sub2.expires_at > exp1, sub2);

  // T-7 reminder (mutate the real store, not endpoint copies)
  store.state.subscriptions[PAYER].expires_at = Date.now() + 7 * 86400000;
  resendCalls = [];
  r = await j("/storefront/subscription-cron?secret=testsecret");
  check("T-7 reminder sent once", r.json.reminded.length === 1 && r.json.reminded[0].kind === "t7", r.json);
  const before = resendCalls.length;
  r = await j("/storefront/subscription-cron?secret=testsecret");
  check("T-7 not re-sent", resendCalls.length === before, r.json);

  // T-1 reminder
  store.state.subscriptions[PAYER].expires_at = Date.now() + 1 * 86400000;
  r = await j("/storefront/subscription-cron?secret=testsecret");
  check("T-1 reminder sent", r.json.reminded.some(x => x.kind === "t1"), r.json);

  // expiry
  store.state.subscriptions[PAYER].expires_at = Date.now() - 1000;
  r = await j("/storefront/subscription-cron?secret=testsecret");
  check("expired sub marked expired, no reminder", r.json.expired === 1 && r.json.reminded.length === 0, r.json);
  r = await j("/storefront/subscriptions");
  check("subscriptions endpoint shows expired", r.json.active === 0 && r.json.subscriptions[0].status === "expired", r.json);

  // auth on the cron endpoint
  r = await j("/storefront/subscription-cron");
  check("cron requires secret", r.status === 401, r.status);

  console.log(failures === 0 ? "\nALL CHECKS PASSED" : `\n${failures} CHECK(S) FAILED`);
  server.close();
  process.exit(failures === 0 ? 0 : 1);
})().catch(e => { console.error("test crashed:", e); process.exit(1); });
