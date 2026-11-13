// storefront.js — RAEN fully-autonomous USDC storefront (Bean, Nov 11 2026)
// Watches Base mainnet for real USDC transfers to the canonical payTo wallet,
// matches amounts to the product catalog, stores orders, and auto-delivers via
// the collector's working Resend path. No human in the transaction loop.
//
// Endpoints (wired in index.js):
//   GET  /storefront/catalog            — product catalog + payTo
//   GET  /storefront/orders             — matched orders
//   GET  /storefront/claims             — on-chain payments awaiting email/claim
//   POST /storefront/register           — {tx_or_intent, email} binds an email to a tx/payer
//   GET  /storefront/scan?secret=...    — cron-friendly manual watcher pass
//
// Data: /data/storefront.json (volume-mounted, same volume as events.jsonl).

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const CANONICAL = "0x9b8a27e03ceba490953cda8ab1291c0e13baf72f"; // RAEN treasury (payTo)
const USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"; // USDC on Base
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const RPCS = [
  "https://mainnet.base.org",
  "https://base-rpc.publicnode.com",
];
const MAX_CHUNK = 10000;         // working RPC pattern: getLogs chunks never exceed 10k blocks
const SCAN_INTERVAL_MS = Number(process.env.STOREFRONT_SCAN_MS || 60000);
const DATA_FILE = path.join(process.env.STOREFRONT_DATA_FILE || "/data", "storefront.json").replace(/\//g, require("path").sep);

const SKILLS_CONTENT_FILE = path.join(__dirname, "skills-content.json");
function loadSkillsContent() {
  try { return JSON.parse(fs.readFileSync(SKILLS_CONTENT_FILE, "utf8")); }
  catch (e) { console.error("skills-content load failed:", e.message); return {}; }
}
const SKILLS = loadSkillsContent();

// Catalog — exact prices in whole USDC. Delivery built per product.
const CATALOG = {
  "dispute-package": {
    name: "Dispute Package", price_usd: 49,
    blurb: "3 generated dispute letters + citation pack + filing guide",
  },
  "contract-review": {
    name: "Contract Review", price_usd: 19,
    blurb: "Contract-eye review credit + redemption instructions",
    default_product: true, // bare $19 transfers resolve here when no hint; Contract Watch buyers register with product=contract-watch
  },
  "contract-watch": {
    name: "Contract Watch (30-day)", price_usd: 19,
    blurb: "Weekly contract re-reviews for 30 days + emailed fresh reports — recurring",
  },
  "market-intel-pack": {
    name: "Market Intel Pack", price_usd: 29,
    blurb: "TradingAgents analysis credit + brief template",
  },
  // RICO PLAY free tier (Nov 13): 'first 10 dispute letters free' — auto-issued
  // after email registration, no payment. Price 0 = never matched by the
  // on-chain watcher (matchProduct skips zero-price products).
  "dispute-starter-free": {
    name: "First 10 Dispute Letters — Free", price_usd: 0,
    blurb: "Round-1 dispute letters for the 10 most-contested item types, free with email registration",
  },
};
// Skills ($29 each) share a price with market-intel-pack, so a bare $29
// transfer is ambiguous: it is only auto-resolved when the buyer registered
// with a product hint (the per-skill page sends one). Otherwise it becomes a
// claim the buyer resolves at /storefront/claims.
const SKILL_CATALOG = {
  "skill-steward-pattern": { name: "Skill: The Steward Pattern", price_usd: 29, skill_key: "steward-pattern" },
  "skill-charge-and-rebate": { name: "Skill: Charge-and-Rebate Funnel", price_usd: 29, skill_key: "charge-and-rebate" },
  "skill-x402-production": { name: "Skill: x402 Production Deployment", price_usd: 29, skill_key: "x402-production-deployment" },
  "skill-bundle": { name: "RAEN Skills Bundle (all 3)", price_usd: 149 },
};
for (const [id, p] of Object.entries(SKILL_CATALOG)) CATALOG[id] = p;
const TOLERANCE_ATOMIC = 10000; // $0.01 in 6-decimal USDC
const priceToAtomic = (usd) => BigInt(Math.round(usd * 1e6));

// ---------- data store ----------
function loadState() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const s = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
      return {
        cursor: Number(s.cursor) || 0,
        orders: Array.isArray(s.orders) ? s.orders : [],
        registrations: s.registrations || {},   // key: tx hash (lowercase) or payer addr -> email
        delivered: s.delivered || {},          // tx hash -> {email, sent_at, email_id}
        subscriptions: s.subscriptions || {},  // payer(lower) -> Contract Watch subscription
        subscribers: s.subscribers || {},      // email(lower) -> {source, at} (contact capture)
        followups: s.followups || {},          // email(lower) -> {due_at, sent_at} (Rico testimonial ask)
        connectors: s.connectors || {},        // code -> {name,email,audience,rate,created_at}
        connectorByEmail: s.connectorByEmail || {}, // email(lower) -> code
        commissions: s.commissions || [],      // {tx, connector, order_usd, commission_usd, at}
      };
    }
  } catch (e) { console.error("storefront restore failed:", e.message); }
  return { cursor: 0, orders: [], registrations: {}, delivered: {}, subscriptions: {}, subscribers: {}, followups: {}, connectors: {}, connectorByEmail: {}, commissions: [] };
}

function makeStore() {
  const state = loadState();
  let saveTimer = null;
  function save() {
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
      saveTimer = null;
      try {
        fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
        fs.writeFileSync(DATA_FILE, JSON.stringify(state));
      } catch (e) { /* volume absent — memory-only, fine */ }
    }, 250);
  }
  return {
    state,
    save,
    findOrder: (tx) => state.orders.find(o => o.tx === tx),
    addOrder: (o) => { state.orders.push(o); if (state.orders.length > 5000) state.orders = state.orders.slice(-5000); save(); },
    addRegistration: (key, email, productHint, ref) => { state.registrations[String(key).toLowerCase()] = { email, product_hint: productHint || null, ref: ref || null, at: Date.now() }; save(); },
    lookupEmail: (order) => {
      const keys = [order.tx, order.payer].map(k => String(k).toLowerCase());
      for (const k of keys) {
        const r = state.registrations[k];
        if (r) return typeof r === "string" ? r : r.email;
      }
      return null;
    },
    lookupHint: (order) => {
      const keys = [order.tx, order.payer].map(k => String(k).toLowerCase());
      for (const k of keys) {
        const r = state.registrations[k];
        if (r && typeof r === "object" && r.product_hint) return r.product_hint;
      }
      return null;
    },
    lookupRef: (order) => {
      const keys = [order.tx, order.payer].map(k => String(k).toLowerCase());
      for (const k of keys) {
        const r = state.registrations[k];
        if (r && typeof r === "object" && r.ref && state.connectors[r.ref]) return r.ref;
      }
      return null;
    },
    addSubscriber: (email, source) => { state.subscribers[email] = { source: source || null, at: Date.now() }; save(); },
    scheduleFollowup: (email, dueAtMs) => { state.followups[email] = { due_at: dueAtMs, sent_at: null }; save(); },
    markFollowupSent: (email) => { if (state.followups[email]) { state.followups[email].sent_at = Date.now(); save(); } },
    addConnector: (name, email, audience) => {
      const code = "rc_" + crypto.randomBytes(6).toString("hex");
      state.connectors[code] = { name, email, audience: audience || null, rate: 0.20, created_at: Date.now() };
      state.connectorByEmail[email] = code;
      save();
      return { code, ...state.connectors[code] }; // include code — callers build links from it
    },
    findConnectorByEmail: (email) => {
      const code = state.connectorByEmail[email];
      return code ? { code, ...state.connectors[code] } : null;
    },
    addCommission: (rec) => { state.commissions.push(rec); if (state.commissions.length > 5000) state.commissions = state.commissions.slice(-5000); save(); },
    markDelivered: (tx, rec) => { state.delivered[tx] = rec; save(); },
    findSubByPayer: (payer) => state.subscriptions[String(payer).toLowerCase()] || null,
    upsertSubscription: (sub) => { state.subscriptions[sub.payer] = sub; save(); return sub; },
  };
}

// ---------- RPC helpers (10k-block chunks, RPC failover) ----------
async function rpcCall(rpc, method, params) {
  const r = await fetch(rpc, {
    method: "POST",
    headers: { "Content-Type": "application/json", "User-Agent": "Mozilla/5.0" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    signal: AbortSignal.timeout(30000),
  });
  if (!r.ok) throw new Error(`rpc ${r.status}`);
  const j = await r.json();
  if (j.error) throw new Error(j.error.message || "rpc error");
  return j.result;
}

async function withFailover(fn) {
  let lastErr = null;
  for (const rpc of RPCS) {
    try { return await fn(rpc); } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

async function headNumber() {
  return Number(await withFailover(rpc => rpcCall(rpc, "eth_blockNumber", [])), 16);
}

// Match a USDC Transfer log to catalog products by exact amount (± $0.01).
// Returns ALL price-matching candidates; uniqueness is resolved by the caller.
function matchProduct(amountAtomic) {
  const a = BigInt(amountAtomic);
  if (a <= 0n) return []; // nothing to match; also keeps zero-price free-tier products unmatchable
  const hits = [];
  for (const [id, p] of Object.entries(CATALOG)) {
    if (!p.price_usd || p.price_usd <= 0) continue; // free-tier products are email-issued, never payment-matched
    const target = priceToAtomic(p.price_usd);
    const diff = a > target ? a - target : target - a;
    if (diff <= BigInt(TOLERANCE_ATOMIC)) hits.push({ id, ...p });
  }
  return hits;
}

const topicAddr = (addr) => "0x" + addr.toLowerCase().replace(/^0x/, "").padStart(64, "0");

// One watcher pass: scan (cursor, head] in ≤10k-block chunks for USDC transfers
// to the canonical payTo wallet, match to catalog, store orders.
async function scanOnce(store, isInternalPayer, deliverFn, log = console.log) {
  const head = await headNumber();
  let from = store.state.cursor > 0 ? store.state.cursor + 1 : Math.max(1, head - 100000);
  let scanned = 0, matched = 0;
  while (from <= head) {
    const to = Math.min(head, from + MAX_CHUNK - 1);
    const logs = await withFailover(rpc => rpcCall(rpc, "eth_getLogs", [{
      address: USDC_BASE,
      fromBlock: "0x" + from.toString(16),
      toBlock: "0x" + to.toString(16),
      topics: [TRANSFER_TOPIC, null, topicAddr(CANONICAL)],
    }])) || [];
    for (const lg of logs) {
      const tx = (lg.transactionHash || "").toLowerCase();
      if (!tx || store.findOrder(tx)) continue;
      const payer = "0x" + lg.topics[1].slice(-40);
      const amountAtomic = BigInt(lg.data).toString();
      const amountUsd = Number(amountAtomic) / 1e6;
      const candidates = matchProduct(amountAtomic);
      const product = candidates.length === 1 ? candidates[0] : null;
      const order = {
        tx, block: Number(BigInt(lg.blockNumber)),
        payer: payer.toLowerCase(),
        amount_atomic: amountAtomic, amount_usd: amountUsd,
        product: product ? product.id : null,
        product_name: product ? product.name : null,
        matched: candidates.length > 0,
        ambiguous: candidates.length > 1,
        candidates: candidates.map(c => c.id),
        classification: isInternalPayer(payer) ? "internal" : "external",
        seen_at: Date.now(),
        delivered: false,
      };
      store.addOrder(order);
      scanned++;
      if (product) {
        matched++;
        log(`storefront: matched ${product.name} ($${product.price_usd}) tx ${tx}`);
        try { await deliverFn(order, product); } catch (e) {
          console.error("storefront delivery failed:", e.message, "tx", tx);
        }
      } else if (candidates.length > 1) {
        log(`storefront: $${amountUsd} tx ${tx} ambiguous (${candidates.length} candidates) — trying hint resolution`);
        try { await deliverFn(order, null); } catch (e) {
          console.error("storefront hint delivery failed:", e.message, "tx", tx);
        }
      }
    }
    store.state.cursor = to;
    store.save();
    from = to + 1;
    if (head - to > 200000) break; // safety: never chase more than ~200k blocks per pass
  }
  return { head, scanned, matched, cursor: store.state.cursor };
}

// ---------- deliverables ----------
function claimCode(tx) {
  return "RAEN-" + crypto.createHash("sha256").update(tx).digest("hex").slice(0, 10).toUpperCase();
}

function esc(s) { return String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

function buildDeliverable(order, product) {
  const receipt = `Transaction: ${order.tx}\nBlock: ${order.block}\nPayer: ${order.payer}\nAmount: ${order.amount_usd} USDC\nNetwork: Base mainnet\nProduct: ${product.name} — $${product.price_usd}`;
  let title = "", body = "";
  if (product.id === "dispute-package") {
    title = "Your Dispute Package";
    body = `
<h2>1. Your three dispute letters</h2>
<p>Round-1 dispute letters, one per bureau. Replace the bracketed fields, sign, and mail
(certified mail, return receipt recommended). Send ONE letter per bureau — never all three in one envelope.</p>
${["Equifax", "Experian", "TransUnion"].map(b => `
<h3>Dispute letter — ${b}</h3>
<pre style="white-space:pre-wrap;font-family:Georgia,serif">[Your full legal name]
[Your mailing address]
[City, State ZIP]
[Date]

${b} Dispute Department
P.O. Box ${b === "Equifax" ? "740256, Atlanta, GA 30374" : b === "Experian" ? "4500, Allen, TX 75013" : "2000, Chester, PA 19016"}

Re: Formal dispute of inaccurate information — [Account name / item], [Account number]

To Whom It May Concern:

I dispute the following item reported on my consumer file: [item description].
This information is inaccurate and/or incomplete and cannot be verified by you
under the Fair Credit Reporting Act, 15 U.S.C. § 1681i.

I request that you conduct a reasonable reinvestigation and delete or correct the
item within thirty (30) days. Please provide me with the method of verification
and the name, address, and telephone number of the furnisher of the information.

Enclosed: copy of my driver's license and a utility bill confirming my identity
and address.

Sincerely,
[Signature]
[Printed name]
[Last 4 of SSN: XXX-XX-____]</pre>`).join("")}
<h2>2. Citation pack</h2>
<ul>
<li>FCRA § 1681i — duty to reinvestigate (30-day window).</li>
<li>FCRA § 1681s-2(b) — furnisher duties upon notice of dispute.</li>
<li>FCRA § 1681e(b) — maximum possible accuracy requirement.</li>
<li>CFPB complaint portal: consumerfinance.gov/complaint (escalation path).</li>
</ul>
<h2>3. Filing guide</h2>
<ol>
<li>Mail each letter separately, certified mail with return receipt. Keep every receipt.</li>
<li>Calendar the 30-day reinvestigation window from the delivery date.</li>
<li>No correction or deletion within 35 days → send a round-2 procedural-dispute letter and file a CFPB complaint referencing your round-1 certified-mail receipt.</li>
<li>If the item is verified but still wrong, a FCRA § 1681b/1681n statutory-damages letter to the furnisher is the next escalation.</li>
</ol>`;
  } else if (product.id === "contract-review") {
    title = "Your Contract Review Credit";
    body = `
<h2>1. Your Contract-eye review credit</h2>
<p>Code: <code style="font-size:1.1em">${claimCode(order.tx)}</code> (one-time, does not expire)</p>
<h2>2. How to redeem</h2>
<ol>
<li>Go to the RAEN contract-eye endpoint: <code>POST https://contract-eye-x402.fly.dev/analyze</code></li>
<li>Header: <code>X-RAEN-CREDIT: ${claimCode(order.tx)}</code> — this replaces the x402 payment header for prepaid purchases.</li>
<li>Body: <code>{"contract_text": "&lt;paste the full contract text&gt;"}</code></li>
<li>You receive the structured risk report: unfavorable clauses, missing protections, ambiguity flags, and suggested redlines.</li>
</ol>
<h2>3. Tips for the best review</h2>
<ul><li>Include every page and every amendment — partial contracts produce partial reviews.</li>
<li>One credit = one contract. Reply to this email if you need a second credit added to this purchase.</li></ul>`;
  } else if (product.id === "market-intel-pack") {
    title = "Your Market Intel Pack";
    body = `
<h2>1. Your TradingAgents analysis credit</h2>
<p>Code: <code style="font-size:1.1em">${claimCode(order.tx)}</code> (one-time, does not expire)</p>
<h2>2. How to redeem</h2>
<ol>
<li>Endpoint: <code>POST https://tradingagents-x402.fly.dev/analyze</code></li>
<li>Header: <code>X-RAEN-CREDIT: ${claimCode(order.tx)}</code> — this replaces the x402 payment header for prepaid purchases.</li>
<li>Body: <code>{"ticker": "AAPL", "depth": "standard"}</code></li>
<li>You receive the full analyst-team debate output: fundamentals, sentiment, news flow, and a risk-scored thesis.</li>
</ol>
<h2>3. Brief template (fill from your analysis output)</h2>
<pre style="white-space:pre-wrap;font-family:Georgia,serif">MARKET BRIEF — [TICKER] — [DATE]
Thesis (one line): ...
Bull case: 1) ... 2) ... 3) ...
Bear case: 1) ... 2) ...
Key catalysts + dates: ...
Valuation anchor: ...
Position sizing / risk: ...
Invalidation trigger: ...</pre>`;
  } else if (product.id.startsWith("skill-")) {
    const isBundle = product.id === "skill-bundle";
    const keys = isBundle ? ["steward-pattern", "charge-and-rebate", "x402-production-deployment"] : [product.skill_key];
    title = isBundle ? "RAEN Skills Bundle — All Three" : product.name;
    const sections = keys.map(k => {
      const s = SKILLS[k];
      if (!s) return `<p>Deliverable for ${esc(k)} — contact ops@royalruby.io.</p>`;
      return `<h2>${esc(s.title)}</h2><pre style="white-space:pre-wrap;font-family:ui-monospace,Consolas,monospace;font-size:.8rem">${esc(s.markdown)}</pre>`;
  }).join("\n");
    body = `
<h2>Payment receipt — verified on-chain before this email was composed</h2>
<p>Your one-time purchase is below. Personal/team use, no redistribution or resale.</p>
${sections}
<h2>Lifetime updates</h2>
<p>Reply to this email from the address you registered with any time — updated editions ship to the same address.</p>`;
  } else {
    title = product.name; body = `<p>${esc(product.blurb)}</p>`;
  }
  return { title, body, receipt };
}

// ---------- delivery via the collector's working Resend path ----------
async function deliverEmail(order, product, email, apiKey, baseUrl) {
  const d = buildDeliverable(order, product);
  const html = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${esc(d.title)} — RAEN</title><style>
body{font-family:Georgia,serif;background:#faf8f4;color:#222;margin:0;padding:1.5rem;line-height:1.5}
.wrap{max-width:720px;margin:0 auto;background:#fff;border:1px solid #ddd;padding:2rem;border-radius:8px}
h1{font-size:1.4rem}h1 span{color:#8a1538}
h2{color:#8a1538;font-size:1.05rem;font-family:system-ui,sans-serif}
.receipt{background:#f5efe9;border:1px solid #e5dcd2;border-radius:6px;padding:1rem;font-family:monospace;font-size:.8rem;white-space:pre-wrap}
pre{background:#f7f7f7;border:1px solid #eee;border-radius:6px;padding:1rem;font-size:.85rem}
code{background:#f0ece6;padding:2px 6px;border-radius:4px}
.foot{margin-top:1.5rem;font-size:.75rem;color:#999;font-family:system-ui,sans-serif}
</style></head><body><div class="wrap">
<h1><span>RAEN</span> — ${esc(d.title)}</h1>
<div class="receipt">RECEIPT (on-chain, Base mainnet)\n${esc(d.receipt)}</div>
${d.body}
<div class="foot">Delivered automatically by the RAEN autonomous storefront. Payment verified on-chain before this email was composed. RAEN — managed AI agent operations.</div>
</div></body></html>`;
  if (!apiKey) throw new Error("RESEND_API_KEY not configured");
  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from: "RAEN Storefront <ops@royalruby.io>",
      to: [email],
      subject: `${d.title} — delivered (tx ${order.tx.slice(0, 14)}…)`,
      html,
    }),
  });
  if (!r.ok) throw new Error(`resend ${r.status}: ${(await r.text()).slice(0, 200)}`);
  return r.json();
}

// ---------- Contract Watch subscriptions ($19/mo, payer-keyed) ----------
const SUB_PERIOD_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

// Called on every contract-watch order. Same payer paying again = renewal:
// extends from the later of (current expiry, now) by 30 days and re-arms
// reminders. First payment = new 30-day subscription.
function upsertContractWatch(store, order, email) {
  const payer = String(order.payer).toLowerCase();
  const now = Date.now();
  let sub = store.findSubByPayer(payer);
  if (sub && sub.last_tx === order.tx && sub.status === "active") {
    if (email && sub.email !== email) { sub.email = email; store.upsertSubscription(sub); }
    return sub; // same order re-delivered — do not double-extend
  }
  if (sub && (sub.status === "active" || sub.expires_at > now)) {
    sub.expires_at = Math.max(sub.expires_at, now) + SUB_PERIOD_MS;
    sub.renewals = (sub.renewals || 0) + 1;
    sub.reminders_sent = {};
    sub.status = "active";
  } else {
    sub = {
      id: "cw-" + payer.slice(2, 12),
      payer, product: "contract-watch",
      started_at: now, expires_at: now + SUB_PERIOD_MS,
      renewals: 0, reminders_sent: {}, status: "active",
    };
  }
  sub.last_tx = order.tx;
  sub.last_paid_at = now;
  sub.last_amount_usd = order.amount_usd;
  if (email) sub.email = email;
  store.upsertSubscription(sub);
  console.log(`[storefront] contract-watch ${sub.renewals ? "renewal #" + sub.renewals : "new"} for ${payer} → expires ${new Date(sub.expires_at).toISOString()}`);
  return sub;
}

const subDaysRemaining = (sub) => Math.ceil((sub.expires_at - Date.now()) / 86400000);

async function sendReminder(sub, days, apiKey) {
  if (!apiKey) throw new Error("RESEND_API_KEY not configured");
  const when = days <= 1 ? "tomorrow" : `in ${days} days`;
  const html = `<!doctype html><html><head><meta charset="utf-8"></head>
<body style="font-family:Georgia,serif;background:#faf8f4;color:#222;margin:0;padding:1.5rem;line-height:1.5">
<div style="max-width:640px;margin:0 auto;background:#fff;border:1px solid #ddd;padding:2rem;border-radius:8px">
<h1 style="font-size:1.3rem"><span style="color:#8a1538">RAEN</span> — Contract Watch renews ${when}</h1>
<p>Your Contract Watch subscription (weekly contract re-reviews, up to 3 contracts)
${days <= 1 ? "expires <b>tomorrow</b>" : `expires in <b>${days} days</b>`}.</p>
<p style="background:#f5efe9;border:1px solid #e5dcd2;border-radius:6px;padding:1rem;font-family:monospace;font-size:.85rem">
To renew: send <b>$19 USDC</b> on Base mainnet to<br>
<code>${CANONICAL}</code><br>
then register your tx at<br>
<code>https://settlement-collector.fly.dev/storefront/register</code><br>
with <code>product: "contract-watch"</code>.<br>
Your 30-day window extends automatically when payment is observed on-chain.</p>
<p style="font-size:.85rem;color:#666">No payment — no renewal. Your subscription simply lapses; you can resubscribe any time.
Questions: reply to this email (ops@royalruby.io).</p>
</div></body></html>`;
  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from: "RAEN Storefront <ops@royalruby.io>",
      to: [sub.email],
      subject: `Contract Watch renews ${when} — $19 USDC to continue`,
      html,
    }),
  });
  if (!r.ok) throw new Error(`resend ${r.status}: ${(await r.text()).slice(0, 200)}`);
  return r.json();
}

// One daily pass: expire lapsed subs, email renewal reminders at T-7 and T-1.
async function checkSubscriptions(store, apiKey, log = console.log) {
  const out = { checked: 0, reminded: [], expired: 0, skipped_no_email: 0 };
  for (const sub of Object.values(store.state.subscriptions)) {
    if (sub.status !== "active") continue;
    out.checked++;
    if (sub.expires_at <= Date.now()) {
      sub.status = "expired";
      out.expired++;
      log(`contract-watch ${sub.id} expired`);
      continue;
    }
    const days = subDaysRemaining(sub);
    if (days > 7) continue;
    for (const kind of ["t7", "t1"]) { // t7 first so a missed week still gets both notices
      const threshold = kind === "t7" ? 7 : 1;
      if (days <= threshold && !sub.reminders_sent[kind]) {
        if (!sub.email) { out.skipped_no_email++; continue; }
        try {
          await sendReminder(sub, days, apiKey);
          sub.reminders_sent[kind] = Date.now();
          out.reminded.push({ id: sub.id, kind, email: sub.email, days_remaining: days });
          log(`renewal reminder ${kind} sent to ${sub.email} (${days}d left)`);
        } catch (e) { log(`reminder ${kind} failed for ${sub.id}: ${e.message}`); }
      }
    }
  }
  store.save();
  return out;
}

// ---------- RICO PLAY (owner directive, Nov 13) ----------
// (1) Free tier: 'first 10 dispute letters free' auto-issued after email
//     registration (no payment) — the Joshua Rico model: do free work for a
//     connector, exchange = testimonial + 2 peer intros.
// (2) Post-delivery automated follow-up email asks for that testimonial +
//     the 2 intros (CONNECTOR-PLAYBOOK-2026-11-13).
// (3) /connector — public affiliate signup; auto-issues a connector code with
//     20% lifetime tracking wired into the same register/orders flow.

const FREE_LETTER_CAP_PER_DAY = Number(process.env.FREE_TIER_DAILY_CAP || 50); // abuse cap
const FREE_LETTERS = [
  { key: "collection-account", label: "Collection account (not mine / wrong amount)", desc: "a collection account listed on my file" },
  { key: "late-payment", label: "Inaccurate late payment", desc: "a late payment that was never late / was current" },
  { key: "charge-off", label: "Incorrect charge-off status or balance", desc: "a charge-off with an inaccurate status or balance" },
  { key: "hard-inquiry", label: "Unauthorized hard inquiry", desc: "a hard inquiry I did not authorize" },
  { key: "repossession", label: "Repossession reporting error", desc: "a repossession reported inaccurately" },
  { key: "medical-collection", label: "Medical collection (unverifiable / paid)", desc: "a medical collection that is paid or cannot be verified" },
  { key: "utility-telecom", label: "Utility / telecom collection", desc: "a utility or telecom collection that is inaccurate" },
  { key: "student-loan", label: "Student loan reporting error", desc: "a student loan reported inaccurately" },
  { key: "public-record", label: "Civil judgment / public record error", desc: "a public-record item that is inaccurate or outdated" },
  { key: "identity-mix", label: "Mixed / merged file (someone else's data)", desc: "another consumer's account information appearing on my file" },
];

function freeLetterHtml(letter) {
  return `<h3>Letter — ${esc(letter.label)}</h3>
<pre style="white-space:pre-wrap;font-family:Georgia,serif">[Your full legal name]
[Your mailing address]
[City, State ZIP]
[Date]

[Credit Bureau] Dispute Department
P.O. Box ${letter.bureauBox}

Re: Formal dispute of inaccurate information — ${esc(letter.desc)}, [Account number]

To Whom It May Concern:

I dispute the following item reported on my consumer file: [item description — ${esc(letter.desc)}].
This information is inaccurate and/or incomplete and cannot be verified by you
under the Fair Credit Reporting Act, 15 U.S.C. § 1681i.

I request that you conduct a reasonable reinvestigation and delete or correct the
item within thirty (30) days. Please provide me with the method of verification
and the name, address, and telephone number of the furnisher of the information.

Enclosed: copy of my driver's license and a utility bill confirming my identity
and address.

Sincerely,
[Signature]
[Printed name]
[Last 4 of SSN: XXX-XX-____]</pre>`;
}

const BUREAU_BOX = {
  Equifax: "740256, Atlanta, GA 30374",
  Experian: "4500, Allen, TX 75013",
  TransUnion: "2000, Chester, PA 19016",
};

function buildFreeDeliverable(email) {
  const letters = FREE_LETTERS.map(l => ({ ...l, bureauBox: BUREAU_BOX.Equifax }));
  const title = "Your First 10 Dispute Letters — Free";
  const body = `
<h2>How to use these</h2>
<p>Ten round-1 dispute letters, one per contested item type. For each item on your
report: pick the matching letter, fill the brackets, and mail it to each bureau that
reports the item (bureau P.O. Boxes below). Send ONE letter per bureau per item —
never all items in one envelope. Calendar the 30-day reinvestigation window from
the delivery date.</p>
${letters.map(freeLetterHtml).join("")}
<h2>Bureau dispute addresses</h2>
<ul>
<li>Equifax — P.O. Box ${BUREAU_BOX.Equifax}</li>
<li>Experian — P.O. Box ${BUREAU_BOX.Experian}</li>
<li>TransUnion — P.O. Box ${BUREAU_BOX.TransUnion}</li>
</ul>
<h2>Citations behind every letter</h2>
<ul>
<li>FCRA § 1681i — duty to reinvestigate (30-day window).</li>
<li>FCRA § 1681s-2(b) — furnisher duties upon notice of dispute.</li>
<li>FCRA § 1681e(b) — maximum possible accuracy requirement.</li>
<li>CFPB complaint portal: consumerfinance.gov/complaint (escalation path).</li>
</ul>
<p style="font-size:.85rem;color:#666">These letters are informational templates, not legal advice.
Need generated, case-specific letters with your citation pack? That's the
<a href="https://settlement-collector.fly.dev/storefront/catalog">Dispute Package ($49)</a>.</p>`;
  return { title, body };
}

async function sendHtmlEmail(apiKey, { to, subject, html, listUnsubscribe = true }) {
  if (!apiKey) throw new Error("RESEND_API_KEY not configured");
  const payload = {
    from: "RAEN Storefront <ops@royalruby.io>",
    to: [to],
    subject,
    html,
  };
  if (listUnsubscribe) {
    payload.headers = {
      "List-Unsubscribe": "<mailto:ops@royalruby.io?subject=unsubscribe>",
      "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    };
  }
  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(`resend ${r.status}: ${(await r.text()).slice(0, 200)}`);
  return r.json();
}

function storefrontShell(title, inner) {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${esc(title)} — RAEN</title><style>
body{font-family:Georgia,serif;background:#faf8f4;color:#222;margin:0;padding:1.5rem;line-height:1.5}
.wrap{max-width:720px;margin:0 auto;background:#fff;border:1px solid #ddd;padding:2rem;border-radius:8px}
h1{font-size:1.4rem}h1 span{color:#8a1538}h2{color:#8a1538;font-size:1.05rem;font-family:system-ui,sans-serif}
h3{font-size:.98rem;font-family:system-ui,sans-serif}
pre{background:#f7f7f7;border:1px solid #eee;border-radius:6px;padding:1rem;font-size:.85rem}
code{background:#f0ece6;padding:2px 6px;border-radius:4px}
.foot{margin-top:1.5rem;font-size:.75rem;color:#999;font-family:system-ui,sans-serif}
</style></head><body><div class="wrap">
<h1><span>RAEN</span> — ${esc(title)}</h1>
${inner}
<div class="foot">Delivered automatically by the RAEN autonomous storefront. Reply to this email to reach a human (ops@royalruby.io). <a href="mailto:ops@royalruby.io?subject=unsubscribe">Unsubscribe</a>.</div>
</div></body></html>`;
}

// Free-tier issuance: one package per email (ever), daily abuse cap.
async function deliverFreeStarter(store, email, ref, apiKey, log) {
  const day = new Date().toISOString().slice(0, 10);
  const freeToday = store.state.orders.filter(o => o.product === "dispute-starter-free" &&
    new Date(o.seen_at).toISOString().slice(0, 10) === day).length;
  if (freeToday >= FREE_LETTER_CAP_PER_DAY) throw new Error("free-tier daily cap reached; try tomorrow");
  const tx = "free-" + crypto.createHash("sha256").update("rico:" + email).digest("hex").slice(0, 12);
  if (store.findOrder(tx)) throw new Error("free package already issued for this email");
  const product = { id: "dispute-starter-free", ...CATALOG["dispute-starter-free"] };
  const order = {
    tx, block: null, payer: null,
    amount_atomic: "0", amount_usd: 0,
    product: product.id, product_name: product.name,
    matched: true, ambiguous: false, candidates: [product.id],
    classification: "free-tier", seen_at: Date.now(), delivered: false,
  };
  store.addOrder(order);
  store.addRegistration(tx, email, product.id, ref || null);
  const d = buildFreeDeliverable(email);
  const out = await sendHtmlEmail(apiKey, {
    to: email,
    subject: "Your 10 dispute letters (free) — RAEN",
    html: storefrontShell(d.title, d.body),
  });
  order.delivered = true;
  order.email = email;
  store.markDelivered(tx, { email, sent_at: Date.now(), email_id: out.id || null, free_tier: true });
  store.addSubscriber(email, "rico-free-tier");
  // Rico exchange: 3 days after delivery, ask for the testimonial + 2 peer intros.
  store.scheduleFollowup(email, Date.now() + 3 * 86400000);
  log(`free-tier 'first 10 dispute letters' issued to ${email}${ref ? " via connector " + ref : ""} (tx ${tx})`);
  return { tx, email };
}

// Daily pass: send the testimonial + 2-peer-intro ask 3 days after free delivery.
async function checkFollowups(store, apiKey, log = console.log) {
  const out = { checked: 0, sent: [], failed: 0 };
  for (const [email, f] of Object.entries(store.state.followups)) {
    if (f.sent_at || f.due_at > Date.now()) continue;
    out.checked++;
    const c = store.findConnectorByEmail(email);
    const link = c ? `https://settlement-collector.fly.dev/connector?ref=${c.code}` : "https://settlement-collector.fly.dev/connector";
    const html = storefrontShell("One small favor (and one for you)", `
<p>Three days ago we sent your 10 free dispute letters. Two things:</p>
<h2>1. The exchange</h2>
<p>The letters were free because we're building proof that AI-generated dispute
letters work at scale. If they helped, reply with a 2-3 sentence testimonial
(what you disputed, what happened). And if you know two peers — a credit-repair
coach, a SaaS vendor, a community leader with clients who need letters — an
introduction is worth more to us than payment right now.</p>
<h2>2. The ongoing offer</h2>
<p>If you serve clients who need dispute letters regularly: white-label letters
at $0.75/letter, your branding, no minimums, and <b>20% lifetime commission</b> on
everything your referrals spend. First 10 letters for each of your clients are free —
same as yours were.</p>
<p>Your connector link (share it — tracking is automatic):
<br><code>${link}</code></p>`);
    try {
      await sendHtmlEmail(apiKey, { to: email, subject: "Did the 10 letters land? (testimonial + intro exchange)", html });
      store.markFollowupSent(email);
      out.sent.push(email);
      log(`rico follow-up (testimonial + 2 intros) sent to ${email}`);
    } catch (e) {
      out.failed++;
      log(`rico follow-up failed for ${email}: ${e.message}`);
    }
  }
  return out;
}

// Public connector signup page (GET /connector) + JSON API.
function connectorPageHtml(msg) {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>RAEN Connector Program — 20% lifetime</title>
<style>
body{background:#0d1117;color:#e6edf3;font-family:system-ui,sans-serif;max-width:640px;margin:0 auto;padding:2.5rem 1.25rem 4rem;line-height:1.6}
h1{color:#58e6d9;font-size:1.7rem}code{background:#161b22;padding:.1rem .35rem;border-radius:4px}
input,textarea{width:100%;padding:.7rem;margin:.3rem 0 .8rem;background:#0a0f1e;border:1px solid #2b3760;border-radius:8px;color:#e6edf3;font-family:inherit}
button{padding:.8rem 1.4rem;background:#58e6d9;color:#04121a;border:0;border-radius:10px;font-weight:700;font-size:1rem;cursor:pointer}
.card{background:#10161d;border:1px solid #30363d;border-radius:12px;padding:1.2rem;margin:1rem 0}
.msg{background:#12261e;border:1px solid #2ea043;color:#7ee787;border-radius:8px;padding:.8rem 1rem;font-size:.9rem}
a{color:#58e6d9}.dim{color:#8b949e;font-size:.88rem}
</style></head><body>
<h1>RAEN Connector Program</h1>
<p class="dim">You already have the clients. We generate the letters. You keep the margin — plus <strong>20% lifetime</strong> on everything your referrals spend. White-label, $0.75/letter, no minimums, first 10 letters per client free.</p>
<div class="card">
${msg ? `<p class="msg">${esc(msg)}</p>` : ""}
<form id="cf">
<label>Name<br><input id="name" required maxlength="120"></label>
<label>Email<br><input id="email" type="email" required maxlength="254"></label>
<label>Who's your audience? (clients, community, list — one line)<br><textarea id="audience" rows="2" maxlength="300"></textarea></label>
<button type="submit">Get my connector code</button>
</form>
<p class="dim" id="out"></p>
</div>
<p class="dim">How it works: every buyer who registers with your code is attributed to you automatically; 20% of what they spend accrues to your ledger. Verify everything on the <a href="https://settlement-collector.fly.dev/proof">public ledger</a>. Questions: ops@royalruby.io.</p>
<script>
document.getElementById('cf').addEventListener('submit', async (e) => {
  e.preventDefault();
  const r = await fetch('/connector/signup', {method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({name:document.getElementById('name').value, email:document.getElementById('email').value, audience:document.getElementById('audience').value})});
  const j = await r.json();
  document.getElementById('out').innerHTML = j.ok
    ? 'Your code: <code>' + j.code + '</code> — share <code>' + j.link + '</code>. Also emailed to you.'
    : 'Error: ' + (j.error || 'try again');
});
</script>
</body></html>`;
}

// ---------- wiring ----------
function wireStorefront(app, { isInternalPayer }) {
  const store = makeStore();
  const log = (...a) => console.log("[storefront]", ...a);

  // Resolve product for an order: explicit match > registration hint > single candidate.
  function resolveProduct(order, productOverride) {
    if (productOverride) return productOverride;
    const hint = store.lookupHint(order);
    if (hint && CATALOG[hint]) return { id: hint, ...CATALOG[hint] };
    const cands = (order.candidates || []).filter(id => CATALOG[id]);
    if (cands.length === 1) return { id: cands[0], ...CATALOG[cands[0]] };
    if (cands.length > 1) {
      const def = cands.find(id => CATALOG[id] && CATALOG[id].default_product);
      if (def) return { id: def, ...CATALOG[def] }; // no hint → catalog default (contract-review)
    }
    return null;
  }

  async function deliver(order, productOverride) {
    const product = resolveProduct(order, productOverride);
    if (!product) { // unresolved (ambiguous) or unmatched — publish a claim code
      if (order.matched) {
        order.claim_code = claimCode(order.tx);
        store.save();
        log(`unresolved product for tx ${order.tx} — claim ${order.claim_code} published`);
      }
      return;
    }
    order.product = product.id;
    order.product_name = product.name;
    const email = store.lookupEmail(order);
    if (product.id === "contract-watch") upsertContractWatch(store, order, email);
    if (!email) { // on-chain only buyer — publish a claim code instead
      order.claim_code = claimCode(order.tx);
      store.save();
      log(`no email for tx ${order.tx} — claim ${order.claim_code} published`);
      return;
    }
    if (order.classification === "internal" && process.env.STOREFRONT_SKIP_INTERNAL_DELIVERY === "1") {
      order.delivered = false;
      store.save();
      return;
    }
    const out = await deliverEmail(order, product, email, process.env.RESEND_API_KEY);
    order.delivered = true;
    order.email = email;
    store.markDelivered(order.tx, { email, sent_at: Date.now(), email_id: out.id || null });
    store.save();
    log(`delivered ${product.name} to ${email} for tx ${order.tx}`);
    // RICO PLAY: 20% lifetime commission attribution to the connector whose
    // code was used at registration (order.tx or payer keyed).
    const ref = store.lookupRef(order);
    if (ref && product.price_usd > 0) {
      const commission = Math.round(product.price_usd * 0.20 * 10000) / 10000;
      store.addCommission({ tx: order.tx, connector: ref, order_usd: product.price_usd, commission_usd: commission, at: Date.now() });
      log(`commission ${commission} USDC (20%) attributed to connector ${ref} for tx ${order.tx}`);
    }
  }

  // background watcher
  let scanning = false;
  async function tick() {
    if (scanning) return;
    scanning = true;
    try { await scanOnce(store, isInternalPayer, deliver, log); }
    catch (e) { console.error("[storefront] scan failed:", e.message); }
    finally { scanning = false; }
  }
  setTimeout(tick, 5000);
  const iv = setInterval(tick, SCAN_INTERVAL_MS);
  if (iv.unref) iv.unref();

  app.get("/storefront/catalog", (_req, res) =>
    res.json({ pay_to: CANONICAL, network: "base", token: "USDC", usdc_contract: USDC_BASE, products: CATALOG }));

  app.get("/storefront/deliveries", (_req, res) => res.json({ deliveries: store.state.delivered }));

  app.get("/storefront/orders", (_req, res) => {
    const orders = store.state.orders.slice().reverse().slice(-200).map(o => ({
      ...o,
      email: o.email || (store.lookupEmail(o) || null),
    }));
    res.json({ total_orders: store.state.orders.length, matched: store.state.orders.filter(o => o.matched).length, orders });
  });

  app.get("/storefront/claims", (_req, res) => {
    const claims = store.state.orders
      .filter(o => o.matched && !o.delivered && !store.lookupEmail(o))
      .map(o => ({
        tx: o.tx, product: o.product_name, amount_usd: o.amount_usd, payer: o.payer,
        claim_code: o.claim_code, ambiguous: Boolean(o.ambiguous), candidates: o.candidates || [],
        seen_at: o.seen_at,
      }))
      .reverse();
    res.json({ open_claims: claims.length, claims, resolve_url: "https://settlement-collector.fly.dev/storefront/register" });
  });

  // POST /storefront/register {tx_or_intent, email, product?, ref?}
  app.post("/storefront/register", (req, res) => {
    const b = req.body || {};
    const email = String(b.email || "").trim();
    const key = String(b.tx_or_intent || "").trim();
    const hint = String(b.product || "").trim() && CATALOG[String(b.product || "").trim()] ? String(b.product).trim() : null;
    const ref = store.state.connectors[String(b.ref || "").trim()] ? String(b.ref).trim() : null;
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "valid email required" });
    if (!key) return res.status(400).json({ error: "tx_or_intent required" });
    store.addRegistration(key, email, hint, ref);
    // if this tx already arrived, deliver immediately
    const order = store.findOrder(key.toLowerCase());
    if (order && order.payer) { // bind email to the payer's subscription if it lacks one
      const sub = store.findSubByPayer(order.payer);
      if (sub && !sub.email) { sub.email = email; store.upsertSubscription(sub); }
    }
    if (order && order.matched && !order.delivered) {
      deliver(order, null)
        .then(() => res.json({ ok: true, delivered: Boolean(order.delivered), product: order.product_name || null, claim_code: order.claim_code || null }))
        .catch(e => res.status(502).json({ ok: true, delivered: false, error: e.message.slice(0, 160) }));
      return;
    }
    res.json({ ok: true, delivered: false, note: "email registered — delivery fires when the matching on-chain payment is observed" });
  });

  app.get("/storefront/scan", (req, res) => {
    if (process.env.REPORT_CRON_SECRET && req.query.secret !== process.env.REPORT_CRON_SECRET)
      return res.status(401).json({ error: "bad secret" });
    scanOnce(store, isInternalPayer, deliver, log)
      .then(r => res.json({ ok: true, ...r }))
      .catch(e => res.status(502).json({ error: e.message }));
  });

  // POST /storefront/deliver?secret=... {tx, product} — operator-forced delivery
  // for an order that already has a registered email (e.g. underpayments).
  // Uses the exact same delivery path as the automatic watcher.
  app.post("/storefront/deliver", (req, res) => {
    if (process.env.REPORT_CRON_SECRET && req.query.secret !== process.env.REPORT_CRON_SECRET)
      return res.status(401).json({ error: "bad secret" });
    const b = req.body || {};
    const order = store.findOrder(String(b.tx || "").toLowerCase());
    if (!order) return res.status(404).json({ error: "unknown tx" });
    if (!store.lookupEmail(order)) return res.status(400).json({ error: "no registered email for this tx — register first" });
    const product = (b.product && CATALOG[b.product]) ? { id: b.product, ...CATALOG[b.product] } : resolveProduct(order, null);
    if (!product) return res.status(400).json({ error: "no product resolvable; pass product" });
    deliver(order, product)
      .then(() => res.json({ ok: true, delivered: Boolean(order.delivered), tx: order.tx, product: product.id }))
      .catch(e => res.status(502).json({ error: e.message }));
  });

  // GET /storefront/subscriptions — Contract Watch subscription ledger
  app.get("/storefront/subscriptions", (_req, res) => {
    const now = Date.now();
    const subs = Object.values(store.state.subscriptions).map(s => ({
      id: s.id, product: s.product, payer: s.payer, email: s.email || null,
      status: s.expires_at <= now ? "expired" : s.status,
      started_at: s.started_at, expires_at: s.expires_at,
      days_remaining: Math.max(0, subDaysRemaining(s)),
      renewals: s.renewals || 0, last_tx: s.last_tx || null,
      reminders_sent: s.reminders_sent || {},
    })).sort((a, b) => a.expires_at - b.expires_at);
    res.json({ total: subs.length, active: subs.filter(s => s.status === "active").length, subscriptions: subs });
  });

  // GET /storefront/subscription-cron?secret=... — cron-friendly daily pass
  app.get("/storefront/subscription-cron", (req, res) => {
    if (process.env.REPORT_CRON_SECRET && req.query.secret !== process.env.REPORT_CRON_SECRET)
      return res.status(401).json({ error: "bad secret" });
    checkSubscriptions(store, process.env.RESEND_API_KEY, log)
      .then(r => res.json({ ok: true, ...r }))
      .catch(e => res.status(502).json({ error: e.message }));
  });

  // daily internal pass — belt-and-braces when no external cron fires the endpoint
  let cronBusy = false;
  const dailyIv = setInterval(() => {
    if (cronBusy) return;
    cronBusy = true;
    checkSubscriptions(store, process.env.RESEND_API_KEY, log)
      .catch(e => console.error("[storefront] subscription cron failed:", e.message))
      .finally(() => { cronBusy = false; });
  }, 24 * 60 * 60 * 1000);
  if (dailyIv.unref) dailyIv.unref();

  // ---- P2 contact capture (ERICA friction #7, Nov 13) ----
  // One-line email capture, e.g. the first-call-free page's "weekly proof ledger".
  app.post("/subscribe", (req, res) => {
    const b = req.body || {};
    const email = String(b.email || "").trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "valid email required" });
    store.addSubscriber(email, String(b.source || "").slice(0, 64) || null);
    log(`subscriber captured: ${email} (source: ${b.source || "n/a"})`);
    res.json({ ok: true, message: "you're on the list — the weekly proof ledger lands by email" });
  });

  // ---- RICO PLAY endpoints (Nov 13) ----
  // GET /connector?ref=CODE — public affiliate-signup page.
  app.get("/connector", (_req, res) => res.type("html").send(connectorPageHtml()));

  // POST /connector/signup {name, email, audience} — auto-issues a connector
  // code with 20% lifetime tracking. Idempotent per email.
  app.post("/connector/signup", async (req, res) => {
    const b = req.body || {};
    const name = String(b.name || "").trim().slice(0, 120);
    const email = String(b.email || "").trim().toLowerCase();
    const audience = String(b.audience || "").trim().slice(0, 300);
    if (!name) return res.status(400).json({ error: "name required" });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "valid email required" });
    let rec = store.findConnectorByEmail(email);
    if (!rec) {
      rec = store.addConnector(name, email, audience);
      log(`connector signed up: ${name} <${email}> -> ${rec.code} (audience: ${audience || "n/a"})`);
    }
    const link = `https://settlement-collector.fly.dev/storefront/register?ref=${rec.code}`;
    try { // confirmation email with the code + personal link (best-effort)
      await sendHtmlEmail(process.env.RESEND_API_KEY, {
        to: email,
        subject: "Your RAEN connector code — 20% lifetime",
        html: storefrontShell("You're a RAEN connector", `
<p>Code: <code style="font-size:1.1em">${rec.code}</code></p>
<p>Your personal link (every buyer who registers through it is attributed to you at <b>20% lifetime</b>):</p>
<p><code>${link}</code></p>
<p>The pitch you can use verbatim: "You have clients who need dispute letters.
We generate them at $0.75/letter, white-label, your branding, no minimums. You
mark up 2-3x. First 10 letters free so you can see the quality."</p>
<p>Track your referrals any time: <a href="https://settlement-collector.fly.dev/connector/stats">connector stats</a>.</p>`),
      });
    } catch (e) { log(`connector confirmation email failed for ${email}: ${e.message}`); }
    store.addSubscriber(email, "connector-signup");
    res.json({ ok: true, code: rec.code, rate: rec.rate, link });
  });

  // GET /connector/stats — public, honest connector ledger (TRUTH law: zeros stay zeros).
  app.get("/connector/stats", (_req, res) => {
    const byConnector = {};
    for (const rec of store.state.commissions) {
      const e = (byConnector[rec.connector] ||= { attributed_orders: 0, order_usd: 0, commission_usd: 0 });
      e.attributed_orders += 1;
      e.order_usd = Math.round((e.order_usd + rec.order_usd) * 10000) / 10000;
      e.commission_usd = Math.round((e.commission_usd + rec.commission_usd) * 10000) / 10000;
    }
    res.json({
      connectors: Object.keys(store.state.connectors).length,
      commission_rate: 0.20,
      total_commission_usd: Math.round(store.state.commissions.reduce((s, c) => s + c.commission_usd, 0) * 10000) / 10000,
      by_connector: byConnector,
      note: "Commissions accrue on attributed orders only — a signup with zero referrals shows zero. Public and honest by design.",
    });
  });

  // POST /storefront/free-tier {email, ref?} — 'first 10 dispute letters free',
  // auto-issued after email registration (no payment). One per email, daily cap.
  app.post("/storefront/free-tier", (req, res) => {
    const b = req.body || {};
    const email = String(b.email || "").trim().toLowerCase();
    const ref = store.state.connectors[String(b.ref || "").trim()] ? String(b.ref).trim() : null;
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "valid email required" });
    deliverFreeStarter(store, email, ref, process.env.RESEND_API_KEY, log)
      .then(r => res.json({ ok: true, ...r, note: "10 dispute letters sent — the Rico exchange (testimonial + 2 peer intros) arrives in 3 days" }))
      .catch(e => res.status(e.message.includes("already issued") ? 409 : 502).json({ ok: false, error: e.message.slice(0, 160) }));
  });

  // GET /storefront/followup-cron?secret=... — cron-friendly Rico follow-up pass
  app.get("/storefront/followup-cron", (req, res) => {
    if (process.env.REPORT_CRON_SECRET && req.query.secret !== process.env.REPORT_CRON_SECRET)
      return res.status(401).json({ error: "bad secret" });
    checkFollowups(store, process.env.RESEND_API_KEY, log)
      .then(r => res.json({ ok: true, ...r }))
      .catch(e => res.status(502).json({ error: e.message }));
  });

  // daily internal Rico follow-up pass (same belt-and-braces pattern as subscriptions)
  let fuBusy = false;
  const fuIv = setInterval(() => {
    if (fuBusy) return;
    fuBusy = true;
    checkFollowups(store, process.env.RESEND_API_KEY, log)
      .catch(e => console.error("[storefront] followup cron failed:", e.message))
      .finally(() => { fuBusy = false; });
  }, 24 * 60 * 60 * 1000);
  if (fuIv.unref) fuIv.unref();

  return store;
}

module.exports = { wireStorefront, CATALOG, CANONICAL, matchProduct, claimCode, buildDeliverable, scanOnce, makeStore, upsertContractWatch, checkSubscriptions };
