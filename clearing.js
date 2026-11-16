// RAEN CLEARING — the unified A2A crypto transaction interface (Bean, 2026-11-15).
// One call (POST /clearing/intent) gives any agent the full transaction path:
// manifest → live 402 quote → EIP-3009 settle instructions → fulfillment endpoint
// → claims URL. POST /clearing/record verifies the settle ON-CHAIN (Base RPC) before
// recording it — no mocked lifecycle, TRUTH law. GET /clearing/status/:tx returns the
// live lifecycle. GET /clearing documents the model for external agents/platforms.
"use strict";

const fs = require("fs");
const path = require("path");
const express = require("express");

const USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const RPC = "https://mainnet.base.org";
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const BASESCAN = "https://basescan.org/tx/";
const CLAIMS_URL = "https://settlement-collector.fly.dev/storefront/claims";
const COOKBOOK = "https://bshelby88.github.io/x402-cookbook/";

// The 13-SKU discovery layer: every live RAEN x402 manifest.
// amount_atomic = quoted USDC units; auto-re-harvested from each manifest every
// 5 min (see harvestPrices). /clearing/intent ALWAYS re-quotes live from the
// 402 challenge at call time — this table is discovery, not pricing truth.
const CATALOG = [
  { service: "royal-ruby",     host: "royal-ruby-x402.fly.dev",   route: "/api/law-lookup",       sku: "law-lookup",       amount: 50000,   name: "Law lookup" },
  { service: "dispute-forge",  host: "dispute-forge-x402.fly.dev",route: "/api/dispute-pack",     sku: "dispute-pack",     amount: 100000,  name: "Dispute pack" },
  { service: "nanobanana",     host: "nanobanana-x402.fly.dev",   route: "/api/generate-image",   sku: "generate-image",   amount: 10000,   name: "Image generation" },
  { service: "briefsnap",      host: "briefsnap-x402.fly.dev",    route: "/api/summarize",        sku: "summarize",        amount: 1000000, name: "Brief summary" },
  { service: "contract-eye",   host: "contract-eye-x402.fly.dev", route: "/api/analyze-contract", sku: "analyze-contract", amount: 50000,   name: "Contract analysis" },
  { service: "dispatch",       host: "dispatch-x402.fly.dev",     route: "/dispatch",             sku: "dispatch",         amount: 500000,  name: "Dispatch" },
  { service: "escrow",         host: "escrow-x402.fly.dev",       route: "/api/escrow/create",    sku: "escrow-create",    amount: 50000,   name: "Escrow (two-phase deals)" },
  { service: "sentry-forge",   host: "sentry-forge-x402.fly.dev", route: "/api/dispute-pack",     sku: "dispute-pack",     amount: 5000000, name: "Sentry dispute pack" },
  { service: "vault-pro",      host: "vault-pro-x402.fly.dev",    route: "/api/scaffold-project", sku: "scaffold-project", amount: 50000,   name: "Project scaffold" },
  { service: "x402-enterprise",host: "x402-enterprise.fly.dev",   route: "/api/dispute-pack",     sku: "dispute-pack",     amount: 750000,  name: "Enterprise dispute pack" },
  { service: "nft-alpha",      host: "nft-alpha-x402.fly.dev",    route: "/api/nft-signal",       sku: "nft-signal",       amount: 20000,   name: "NFT signal" },
  { service: "suprapack",      host: "suprapack-x402.fly.dev",    route: "/api/find-skill",       sku: "find-skill",       amount: 50000,   name: "Skill lookup" },
  { service: "tradingagents",  host: "tradingagents-x402.fly.dev",route: "/api/analyze-ticker",   sku: "analyze-ticker",   amount: 50000,   name: "Ticker analysis" },
];

// Price truth: manifests beat the static seed. Re-harvest every SKU's live price
// from its x402 manifest (5-min TTL, and on catalog request if stale) so the
// catalog never drifts from what the services actually charge.
const PRICE_TTL_MS = 5 * 60 * 1000;
let lastHarvest = 0;
function parseManifestPrice(accepts) {
  if (!accepts) return null;
  if (accepts.amount != null && Number.isFinite(Number(accepts.amount)) && Number(accepts.amount) > 0) return Number(accepts.amount);
  const p = typeof accepts.price === "string" ? parseFloat(accepts.price.replace(/[^0-9.]/g, "")) : NaN;
  return Number.isFinite(p) && p > 0 ? Math.round(p * 1e6) : null;
}
async function harvestPrices() {
  const results = await Promise.allSettled(CATALOG.map(async (c) => {
    const r = await fetch(`https://${c.host}/.well-known/x402.json`, { headers: { "User-Agent": "raen-clearing/1.0" }, signal: AbortSignal.timeout(15000) });
    if (!r.ok) throw new Error(`manifest HTTP ${r.status}`);
    const m = await r.json();
    const price = parseManifestPrice((((m || {}).endpoints || {})[c.route] || {}).accepts);
    if (price == null) throw new Error("no price in manifest");
    if (c.amount !== price) { console.error(`clearing price sync: ${c.service} ${dec6(c.amount)} -> ${dec6(price)} (manifest)`); c.amount = price; }
  }));
  lastHarvest = Date.now();
  return results;
}
function startPriceHarvest() {
  const run = () => harvestPrices().catch((e) => console.error("clearing price harvest failed:", e.message));
  run();
  const t = setInterval(run, PRICE_TTL_MS); if (t.unref) t.unref();
}
const byKey = {};
for (const c of CATALOG) { byKey[c.service] = c; byKey[c.sku] = c; }

const DATA_FILE = path.join("/data", "clearing.jsonl");
function persist(rec) {
  try { fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true }); fs.appendFileSync(DATA_FILE, JSON.stringify(rec) + "\n"); }
  catch (e) { console.error("clearing persist failed:", e.message); }
}

const rate = {}; // tiny in-memory limiter for record (10/min/IP)
function limited(ip) {
  const w = Math.floor(Date.now() / 60000);
  rate[ip + w] = (rate[ip + w] || 0) + 1;
  for (const k of Object.keys(rate)) if (!k.endsWith(w)) delete rate[k];
  return rate[ip + w] > 10;
}

function b64urlDecode(s) {
  const b = s.replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b + "=".repeat((4 - (b.length % 4)) % 4), "base64").toString("utf8");
}

function dec6(n) { return Math.round((n / 1e6) * 10000) / 10000; }

// Fetch the live 402 challenge from the service (quote = the 402 itself).
async function liveQuote(entry) {
  const url = `https://${entry.host}${entry.route}`;
  let code = 0, hdr = null, body = {};
  try {
    const r = await fetch(url, {
      method: "POST", headers: { "Content-Type": "application/json", "User-Agent": "raen-clearing/1.0" },
      body: JSON.stringify({}), signal: AbortSignal.timeout(15000),
    });
    code = r.status;
    hdr = r.headers.get("payment-required");
    body = await r.json().catch(() => ({}));
  } catch (e) {
    return { reachable: false, error: e.message };
  }
  let accepts = null, instructions = null;
  if (hdr) {
    try {
      const ch = JSON.parse(b64urlDecode(hdr));
      accepts = (ch.accepts && ch.accepts[0]) || null;
      instructions = ch.payment_instructions || null;
    } catch { /* header decode failed */ }
  }
  if (!accepts && body && body.accepts) { accepts = body.accepts[0] || null; instructions = body.payment_instructions || instructions; }
  return { reachable: code > 0 && code < 500, http_status: code, accepts, payment_instructions: instructions, resource: body.resource || null };
}

// On-chain verification: the settle tx must be a successful USDC Transfer (EIP-3009
// execution) of exactly the quoted amount, to the manifest payTo. No tx, no record.
async function verifyOnChain(txHash, entry) {
  const j = async (m, p) => {
    const r = await fetch(RPC, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: m, params: p }), signal: AbortSignal.timeout(20000) });
    return r.json();
  };
  const tx = await j("eth_getTransactionByHash", [txHash]);
  if (!tx || !tx.result) return { verified: false, reason: "tx not found on Base" };
  const rc = await j("eth_getTransactionReceipt", [txHash]);
  if (!rc || !rc.result) return { verified: false, reason: "no receipt yet (not mined)" };
  if (rc.result.status !== "0x1") return { verified: false, reason: "tx reverted" };
  const logs = (rc.result.logs || []).filter(l => (l.address || "").toLowerCase() === USDC && (l.topics || [])[0] === TRANSFER_TOPIC);
  if (!logs.length) return { verified: false, reason: "no USDC Transfer log in tx" };
  for (const l of logs) {
    const to = "0x" + l.topics[2].slice(26);
    if (to !== String(entry.payTo).toLowerCase()) continue;
    const value = BigInt(l.data);
    return {
      verified: true, token: USDC, to, value_atomic: value.toString(), value_usd: dec6(Number(value)),
      block: parseInt(rc.result.blockNumber, 16), block_hash: rc.result.blockHash, from: "0x" + l.topics[1].slice(26),
      gas_used: parseInt(rc.result.gasUsed, 16), explorer: BASESCAN + txHash,
    };
  }
  return { verified: false, reason: "USDC transfer present but recipient/amount does not match the quote" };
}

function wireClearing(app, events) {
  const evList = () => (typeof events === "function" ? events() : (events || []));
  const intents = new Map(); // clearing_id → intent record (memory; lifecycle steps appended + persisted)
  const byTx = new Map();    // tx hash → clearing_id
  try {
    if (fs.existsSync(DATA_FILE)) {
      for (const line of fs.readFileSync(DATA_FILE, "utf8").split("\n").filter(Boolean)) {
        try {
          const r = JSON.parse(line);
          if (r.type === "intent") { intents.set(r.clearing_id, r); if (r.settle && r.settle.tx) byTx.set(r.settle.tx, r.clearing_id); }
          else if (r.type === "step") {
            const it = intents.get(r.clearing_id);
            if (it) {
              it.lifecycle = it.lifecycle || [];
              it.lifecycle.push(r.step);
              if (r.step.tx) byTx.set(r.step.tx, r.clearing_id);
            }
          }
        } catch { /* skip bad line */ }
      }
    }
  } catch (e) { console.error("clearing restore failed:", e.message); }

  startPriceHarvest(); // keep catalog prices synced to live manifests (5-min TTL)

  // A. POST /clearing/intent {buyer, service, sku} → full transaction path envelope.
  app.post("/clearing/intent", async (req, res) => {
    const b = req.body || {};
    if (!b.buyer || !/^0x[0-9a-fA-F]{40}$/.test(String(b.buyer))) return res.status(400).json({ error: "buyer must be a 0x address" });
    const entry = byKey[String(b.service || "").toLowerCase()] || byKey[String(b.sku || "").toLowerCase()];
    if (!entry) return res.status(404).json({ error: "unknown service/sku", available: CATALOG.map(c => ({ service: c.service, sku: c.sku, usd: dec6(c.amount) })) });
    const q = await liveQuote(entry);
    const manifest_url = `https://${entry.host}/.well-known/x402.json`;
    const id = "clr_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 8);
    const envelope = {
      clearing_api_version: 2,
      clearing_id: id,
      buyer: String(b.buyer),
      service: entry.service,
      sku: entry.sku,
      model: "RAEN Clearing: discover → quote(402) → settle(EIP-3009) → retry(200) → record → status",
      step_1_discovery: { manifest_url, note: "GET this manifest to enumerate every SKU on this service." },
      step_2_quote: q.reachable && q.accepts ? {
        source: "live 402 challenge fetched at request time", http_status: q.http_status,
        scheme: q.accepts.scheme, network: q.accepts.network, asset: q.accepts.asset,
        amount_atomic: q.accepts.amount, amount_usd: dec6(Number(q.accepts.amount)),
        payTo: q.accepts.payTo, max_timeout_seconds: q.accepts.maxTimeoutSeconds || 300,
        extra: q.accepts.extra || null, payment_instructions: q.payment_instructions || null,
      } : { reachable: q.reachable, note: "live quote unavailable; catalog quote below", catalog_amount_atomic: entry.amount, catalog_amount_usd: dec6(entry.amount), error: q.error || null },
      step_3_settle: {
        rail: "EIP-3009 transferWithAuthorization, x402 'exact' scheme, USDC on Base (eip155:8453)",
        header_name: "x-payment", header_format: "base64 x402 v2 payload signed by the buyer EOA",
        facilitator: (q.accepts && q.accepts.extra && q.accepts.extra.facilitator) || "https://raen-facilitator.fly.dev/facilitator",
      },
      step_4_fulfillment: { method: "POST", url: `https://${entry.host}${entry.route}`, note: "REPEAT the original request with the x-payment header; the paid response returns with a payment-response header containing the settle tx hash." },
      step_5_record: { method: "POST", url: "https://settlement-collector.fly.dev/clearing/record", body: { clearing_id: id, tx: "<settle tx hash>", payer: "<buyer address>" }, note: "the collector verifies the settle on-chain before recording it" },
      step_6_status: { method: "GET", url: `https://settlement-collector.fly.dev/clearing/status/${id}` },
      step_7_machine_registration: {
        method: "POST",
        url: "https://settlement-collector.fly.dev/storefront/register",
        body: { tx_or_intent: "<settle tx hash or your intent id>", agent_id: "<your agent id (optional)>", callback_url: "https://<your-service>/webhook/raen" },
        note: "MACHINE BUYERS: register with callback_url (https) and the receipt + claims URL arrive as an HTTP POST to your webhook instead of email — onboarding steps (T+0 receipt, T+1 verified-tx follow-up, T+7 value ledger) are delivered as webhooks too. Omit callback_url to use the classic email path.",
      },
      claims_url: CLAIMS_URL, docs: COOKBOOK,
    };
    const rec = { type: "intent", clearing_id: id, ts: Date.now(), buyer: String(b.buyer).toLowerCase(), service: entry.service, sku: entry.sku, route: entry.route, quote: envelope.step_2_quote, lifecycle: [{ at: Date.now(), state: "intent" }, { at: Date.now(), state: "quoted", amount_usd: (envelope.step_2_quote.amount_usd || dec6(entry.amount)) }] };
    intents.set(id, rec); persist(rec);
    res.json(envelope);
  });

  // Catalog listing for agents. Re-harvests live manifest prices when stale so
  // catalog_price_usd tracks what the services actually charge (402 re-quote
  // still happens per-intent at /clearing/intent).
  app.get(["/clearing/catalog", "/clearing/catalog.json"], async (_req, res) => {
    if (Date.now() - lastHarvest > PRICE_TTL_MS) { try { await harvestPrices(); } catch { /* serve current table */ } }
    res.json({ version: 1, network: "eip155:8453", asset: USDC, prices_synced_at: lastHarvest ? new Date(lastHarvest).toISOString() : null, skus: CATALOG.map(c => ({ service: c.service, sku: c.sku, name: c.name, route: c.route, manifest_url: `https://${c.host}/.well-known/x402.json`, catalog_price_usd: dec6(c.amount), note: "synced from live x402 manifest; exact price re-quoted at POST /clearing/intent" })) });
  });

  // POST /clearing/record {clearing_id, tx, payer} → on-chain verify → record.
  app.post("/clearing/record", async (req, res) => {
    if (limited(req.ip)) return res.status(429).json({ error: "rate limited" });
    const b = req.body || {};
    const tx = String(b.tx || "").trim();
    if (!/^0x[0-9a-fA-F]{64}$/.test(tx)) return res.status(400).json({ error: "tx must be a 32-byte hash" });
    const it = b.clearing_id ? intents.get(String(b.clearing_id)) : null;
    const svc = it || byKey[String(b.service || b.sku || "").toLowerCase()];
    if (!svc) return res.status(404).json({ error: "unknown clearing_id/service — POST /clearing/intent first" });
    const entry = byKey[it ? it.service : svc.service];
    const v = await verifyOnChain(tx, { payTo: (it && it.quote && it.quote.payTo) || entry.payToHint || "0x9b8a27e03ceba490953cda8ab1291c0e13baf72f" });
    const step = {
      type: "step", clearing_id: (it && it.clearing_id) || String(b.clearing_id || "unattributed"), ts: Date.now(),
      state: v.verified ? "settled_on_chain" : "settle_rejected", tx,
      payer: String(b.payer || v.from || "").toLowerCase(), verification: v,
      fulfilled: Boolean(b.fulfilled), service: it ? it.service : entry.service, sku: it ? it.sku : entry.sku,
    };
    persist(step);
    if (it) {
      it.lifecycle.push({ at: Date.now(), state: step.state, tx, verified: v.verified });
      it.settle = { tx, payer: step.payer, ...v };
      if (v.verified) byTx.set(tx, it.clearing_id);
    }
    res.status(v.verified ? 201 : 422).json({ ok: v.verified, clearing_id: step.clearing_id, state: step.state, tx, verification: v, explorer: BASESCAN + tx });
  });

  // GET /clearing/status/:idOrTx → live clearing lifecycle.
  // GET /clearing/status (no key) → enumerate recent clearing intents so a
  // keyless probe returns useful truth instead of a bare 404 (RE-AUDIT Nov 16).
  app.get("/clearing/status", (_req, res) => {
    const rows = Array.from(intents.values()).sort((a, b) => b.ts - a.ts).slice(0, 25)
      .map((it) => ({ clearing_id: it.clearing_id, ts: it.ts, service: it.service, sku: it.sku, state: (it.lifecycle || []).some((s) => s && s.state === "settled_on_chain") ? "cleared" : "quoted" }));
    res.json({ version: 1, note: "GET /clearing/status/<clearing_id-or-tx> for the full lifecycle", count: rows.length, recent: rows });
  });

  app.get("/clearing/status/:key", (req, res) => {
    const k = String(req.params.key).trim();
    const id = k.startsWith("0x") && k.length === 66 ? byTx.get(k) : k;
    const it = intents.get(id);
    if (!it) {
      // Not a clearing intent — maybe it went straight through a service's own telemetry.
      const ev = evList().filter(e => e.tx === k);
      if (ev.length) return res.json({ found: true, source: "collector telemetry", tx: k, events: ev, explorer: BASESCAN + k });
      return res.status(404).json({ found: false, error: "unknown clearing_id or tx" });
    }
    const settled = (it.lifecycle || []).some(s => s.state === "settled_on_chain");
    const rejected = (it.lifecycle || []).some(s => s.state === "settle_rejected");
    const telemetry = evList().filter(e => it.settle && it.settle.tx && e.tx === it.settle.tx);
    res.json({
      found: true, clearing_id: it.clearing_id, ts: it.ts, buyer: it.buyer, service: it.service, sku: it.sku,
      state: settled ? "cleared" : rejected ? "settle_rejected" : "quoted",
      quote: it.quote, lifecycle: it.lifecycle || [],
      settle: it.settle || null, explorer: (it.settle && it.settle.tx) ? BASESCAN + it.settle.tx : null,
      collector_telemetry: telemetry.length ? telemetry : null,
      claims_url: CLAIMS_URL,
    });
  });

  // C. The platform page — the model documented for any external agent/platform.
  app.get("/clearing", (_req, res) => {
    const rows = CATALOG.map(c => `<tr><td>${c.service}</td><td><code>${c.sku}</code></td><td>$${dec6(c.amount).toFixed(2)}</td><td><a href="https://${c.host}/.well-known/x402.json">manifest</a></td></tr>`).join("");
    let demoTx = "";
    try { if (fs.existsSync(path.join("/data", "clearing-demo-tx.txt"))) demoTx = fs.readFileSync(path.join("/data", "clearing-demo-tx.txt"), "utf8").trim(); } catch { /* no demo yet */ }
    const demoHtml = demoTx
      ? `<p>Demo transaction: <a href="https://basescan.org/tx/${demoTx}">basescan.org/tx/${demoTx}</a> — a buyer-agent discovered via manifest, decoded the 402, settled USDC on-chain (EIP-3009), retried, received fulfillment, and the collector verified and recorded it. Status: <a href="/clearing/status/${demoTx}">/clearing/status/${demoTx}</a></p>`
      : `<p class="dim">Demo pending — run POST /clearing/intent, settle, then POST /clearing/record to light this up.</p>`;
    res.type("html").send(`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>RAEN Clearing — A2A crypto transactions, live</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#050607;--panel:#0d1013;--line:#1c2126;--ink:#e8ecef;--dim:#7d8a94;--green:#2fdc7e;--cyan:#5ae1e6;--mono:ui-monospace,Menlo,Consolas,monospace}
*{box-sizing:border-box}body{background:var(--bg);color:var(--ink);font-family:system-ui,sans-serif;max-width:960px;margin:0 auto;padding:2rem 1.25rem;line-height:1.55}
h1{font-size:1.7rem}h1 span{color:var(--green)}h2{font-size:1.1rem;margin-top:2rem;color:var(--cyan)}
code,pre{font-family:var(--mono);font-size:.85rem}pre{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:1rem;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:.85rem}td,th{border-bottom:1px solid var(--line);padding:.45rem .5rem;text-align:left}
a{color:var(--cyan)}.dim{color:var(--dim)}.pill{display:inline-block;background:var(--panel);border:1px solid var(--line);border-radius:99px;padding:.15rem .7rem;font-size:.75rem;margin:.15rem .15rem 0 0}
</style></head><body>
<h1>RAEN <span>Clearing</span> — agents transacting crypto with agents</h1>
<p>RAEN is the clearinghouse for agent-to-agent crypto commerce. Any agent or platform posts one call and receives the <em>entire</em> transaction path as a machine-readable envelope: discovery manifest → live quote (the 402 challenge) → EIP-3009 settle instructions → fulfillment endpoint → on-chain verification → live status.</p>
<p><span class="pill">Discovery: /.well-known/x402.json manifests</span><span class="pill">Quote: the 402 challenge</span><span class="pill">Payment: EIP-3009 USDC on Base</span><span class="pill">Clearing: on-chain verified records</span><span class="pill">Escrow: escrow-x402 SKU</span><span class="pill">Trust: public ledger + /live</span></p>
<h2>Open invitation</h2>
<p class="dim">Any agent can transact through RAEN Clearing right now. No account, no keys with us — you keep your wallet, we verify the chain.</p>
<h2>The API</h2>
<pre>POST /clearing/intent   {buyer, service, sku}  → full transaction path envelope
POST /clearing/record   {clearing_id, tx, payer} → on-chain verified clearing record
GET  /clearing/status/:txOrId                  → live lifecycle
GET  /clearing/catalog.json                    → all 13 SKUs</pre>
<h2>Proven tonight (live on-chain demo, internal self-settle)</h2>
${demoHtml}
<h2>The catalog (live)</h2>
<table><tr><th>service</th><th>sku</th><th>catalog price</th><th>manifest</th></tr>${rows}</table>
<p class="dim">Prices above are discovery defaults; every intent re-quotes live from the service's own 402 challenge. Failed delivery? Claim at <a href="${CLAIMS_URL}">${CLAIMS_URL}</a>. Cookbook: <a href="${COOKBOOK}">${COOKBOOK}</a></p>
<p class="dim">RAEN — The First Company That Can't Lie. Records only exist after on-chain verification. <a href="/ledger.html">Ledger</a> · <a href="/live">Live</a> · <a href="/status">Status</a></p>
</body></html>`);
  });

  return { intents, byTx };
}

module.exports = { wireClearing, CATALOG, verifyOnChain };
