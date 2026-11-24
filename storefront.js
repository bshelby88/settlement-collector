// storefront.js — RAEN fully-autonomous USDC storefront (Bean, Nov 11 2026)
// Watches Base mainnet for real USDC transfers to the canonical payTo wallet,
// matches amounts to the product catalog, stores orders, and auto-delivers via
// the collector's working Resend path. No human in the transaction loop.
//
// LEGACY-PAYEE STRANDING WATCH (Staci, 2026-11-22): the retired treasury
// 0x7861…1393 is still SERVED by the facilitator (KEEP verdict — services and
// stale directories can still name it), so a buyer whose challenge said the
// legacy payee can pay real USDC that the canonical-only watcher would never
// see. scanOnce now ALSO scans transfers to the legacy payee and auto-creates
// an order + claim code for each one — a legacy-named settle is never
// invisible; its payer can self-serve at /storefront/claims.
//
// Endpoints (wired in index.js):
//   GET  /storefront/catalog            — product catalog + payTo + claims_help
//   GET  /storefront/orders             — matched orders (incl. legacy-payee orders)
//   GET  /storefront/claims             — on-chain payments awaiting email/claim
//   GET  /storefront/legacy-watch       — stranding-watch status + counts
//   POST /storefront/register           — {tx_or_intent, email} binds an email to a tx/payer
//   GET  /storefront/scan?secret=...    — cron-friendly manual watcher pass
//
// Manual-pay recovery (Bean, Nov 15): external transfers matching no catalog
// price (e.g. a manual EIP-3009 settle with no second X-PAYMENT call) get a
// claim code + a one-time payment-verified credit confirmation once an email
// is registered. TRUTH: delivered stays false until a real product ships.
//
// Data: /data/storefront.json (volume-mounted, same volume as events.jsonl).

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const CANONICAL = "0x9b8a27e03ceba490953cda8ab1291c0e13baf72f"; // RAEN treasury (payTo)
// LEGACY-PAYEE STRANDING WATCH (Staci, 2026-11-22): retired treasury, still
// served by the facilitator per the KEEP verdict. Full 40-hex from
// ops/CHARTER wallet records — never a shorthand (homoglyph law).
const LEGACY_PAYEE = "0x7861db4efc14a1ed5dd8c96c528a3796560f1393";
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
// Manual-pay recovery floor (Bean, Nov 15): external transfers >= $0.01 that
// match no catalog price get a self-serve claim code instead of silence.
// Below the floor (e.g. $0.007 scatter dust) is recorded but not claimable.
const CLAIM_MIN_ATOMIC = 10000n;

// ---------- data store ----------
function loadState() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const s = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
      return {
        cursor: Number(s.cursor) || 0,
        legacy_cursor: Number(s.legacy_cursor) || 0,
        orders: Array.isArray(s.orders) ? s.orders : [],
        registrations: s.registrations || {},   // key: tx hash (lowercase) or payer addr -> email
        delivered: s.delivered || {},          // tx hash -> {email, sent_at, email_id}
        subscriptions: s.subscriptions || {},  // payer(lower) -> Contract Watch subscription
        subscribers: s.subscribers || {},      // email(lower) -> {source, at} (contact capture)
        followups: s.followups || {},          // email(lower) -> {due_at, sent_at} (Rico testimonial ask)
        onboarding: s.onboarding || {},        // email(lower) -> PLAN 9 onboarding sequence state (Erica, Nov 15)
        connectors: s.connectors || {},        // code -> {name,email,audience,rate,created_at}
        connectorByEmail: s.connectorByEmail || {}, // email(lower) -> code
        commissions: s.commissions || [],      // {tx, connector, order_usd, commission_usd, at}
        delivery_evidence: s.delivery_evidence || {}, // tx(lower) -> claims delivery-binding evidence (Staci, 2026-11-24)
      };
    }
  } catch (e) { console.error("storefront restore failed:", e.message); }
  return { cursor: 0, legacy_cursor: 0, orders: [], registrations: {}, delivered: {}, subscriptions: {}, subscribers: {}, followups: {}, onboarding: {}, connectors: {}, connectorByEmail: {}, commissions: [], delivery_evidence: {} };
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
    addRegistration: (key, email, productHint, ref, extra) => { state.registrations[String(key).toLowerCase()] = { email: email || null, product_hint: productHint || null, ref: ref || null, at: Date.now(), ...(extra || {}) }; save(); },
    lookupRegistration: (order) => {
      const keys = [order.tx, order.payer].map(k => String(k).toLowerCase());
      for (const k of keys) {
        const r = state.registrations[k];
        if (r && typeof r === "object") return r;
      }
      return null;
    },
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

// ---------- CLAIMS DELIVERY-BINDING (Staci, 2026-11-24) — the P1 fix ----------
// A buyer who paid AND received product used to show matched:false,
// delivered:false, product:null at the claims desk (state "recorded") —
// indistinguishable from a stranded payment. Root cause: delivery flags came
// from receipt metadata/webhook acks, never from product proof.
//
// Delivery evidence model (TRUTH law — no delivery claim without verified
// evidence; the SERVICE's own key-gated telemetry is the only thing that flips
// delivered:false → true):
//   1. "settled" telemetry events (deployed settlement-telemetry middleware):
//      a paid request completed 2xx on the service's OWN route with the on-chain
//      tx decoded from the settlement receipt — the service that returned the
//      product attests the paid 200 for that exact tx. Facilitator routes
//      (/settle, /verify, /facilitator*) prove PAYMENT, not delivery — excluded.
//   2. "delivery" telemetry events (middleware addition staged in
//      RAEN/x402/settlement-telemetry.cjs): same attestation plus
//      product_digest = sha256 of the payload bytes actually sent.
//   3. POST /storefront/delivery-digest — buyer-presented sha256 of the payload
//      it actually received for a tx it paid. Attaches a digest to the evidence
//      record (and to a bound order); NEVER creates delivery state by itself.
// Binding: evidence matches the watcher order by tx → matched:true (the payment
// matched the paid route that delivered it), delivered:true, product = route id,
// product_digest recorded. Claims lookups then show honest, machine-readable
// delivery state with the payload proof.
const FACILITATOR_SERVICES = new Set(["raen-facilitator", "raen-facilitator-worker"]);
function isDeliveryRouteEvidence(service, route) {
  if (FACILITATOR_SERVICES.has(String(service || "").toLowerCase())) return false;
  const r = String(route || "");
  return !(r === "/settle" || r === "/verify" || r.startsWith("/facilitator") || r.startsWith("facilitator"));
}
function bindOrderFromEvidence(order, evd, log = console.log) {
  order.matched = true; // the payment matched the paid route that delivered it
  order.delivered = true;
  order.product = `route:${evd.service}:${evd.route || "(route)"}`;
  order.product_name = `Route delivery — ${evd.service} ${evd.route || ""} (service-attested paid 2xx, tx-verified)`;
  order.product_digest = evd.product_digest || order.product_digest || null;
  order.delivery_evidence = {
    source: "service-telemetry",
    service: evd.service || null,
    route: evd.route || null,
    tx: order.tx,
    product_digest: order.product_digest,
    digest_source: evd.digest_source || null,
    at: Date.now(),
  };
  return order;
}
function upsertDeliveryEvidence(store, { service, route, price_usd, payer, tx, product_digest, digest_source, service_attested }) {
  const key = String(tx || "").toLowerCase();
  if (!/^0x[0-9a-f]{64}$/.test(key)) return null;
  const st = store.state;
  if (!st.delivery_evidence) st.delivery_evidence = {};
  const cur = st.delivery_evidence[key] || {
    service: null, route: null, price_usd: null, payer: null,
    product_digest: null, digest_source: null, service_attested: false,
    first_at: Date.now(), events: [],
  };
  const evd = {
    ...cur,
    tx: key,
    ...(service_attested ? { service, route, price_usd, payer, service_attested: true } : {}),
    ...(product_digest ? { product_digest, digest_source: digest_source || cur.digest_source || "buyer-presented" } : {}),
  };
  evd.events = [...(cur.events || []), { service_attested: Boolean(service_attested), digest_source: digest_source || null, at: Date.now() }].slice(-10);
  st.delivery_evidence[key] = evd;
  const keys = Object.keys(st.delivery_evidence);
  if (keys.length > 5000) for (const k of keys.slice(0, keys.length - 5000)) delete st.delivery_evidence[k];
  const order = store.findOrder(key);
  if (order) {
    if (evd.service_attested && !order.delivered) {
      bindOrderFromEvidence(order, evd);
      store.markDelivered(order.tx, { kind: "route-delivery", service: evd.service, route: evd.route || null, digest: evd.product_digest || null, sent_at: Date.now(), email: null });
      console.log("[storefront] delivery-binding: tx", order.tx, "→ delivered via", evd.service, evd.route || "");
    } else if (evd.product_digest && order.delivered && !order.product_digest) {
      order.product_digest = evd.product_digest;
      if (order.delivery_evidence) order.delivery_evidence.product_digest = evd.product_digest;
      const rec = st.delivered[order.tx]; if (rec) rec.digest = evd.product_digest;
    }
  }
  store.save();
  return evd;
}


// One watcher pass: scan (cursor, head] in ≤10k-block chunks for USDC transfers
// to the canonical payTo wallet, match to catalog, store orders. The same pass
// also runs the LEGACY-PAYEE STRANDING WATCH (Staci, 2026-11-22): transfers to
// the retired-but-still-served payee get their own cursor and auto-create an
// order + claim code, so a buyer whose challenge named the legacy wallet is
// never stranded invisibly.
async function scanOnce(store, isInternalPayer, deliverFn, log = console.log) {
  const head = await headNumber();

  // Shared per-log pipeline for BOTH payees. payee:"canonical" | "legacy".
  // Legacy orders ALWAYS get a claim code at creation time — that is the point
  // of the watch (auto-create order + claim code, so legacy-named settles are
  // not invisible) — and follow the same match/deliver/claim rules as canonical.
  let scanned = 0, matched = 0;
  async function handleLog(lg, payee) {
    const tx = (lg.transactionHash || "").toLowerCase();
    if (!tx || store.findOrder(tx)) return;
    const payer = "0x" + lg.topics[1].slice(-40);
    const amountAtomic = BigInt(lg.data).toString();
    const amountUsd = Number(amountAtomic) / 1e6;
    const candidates = matchProduct(amountAtomic);
    const product = candidates.length === 1 ? candidates[0] : null;
    const order = {
      tx, block: Number(BigInt(lg.blockNumber)),
      payer: payer.toLowerCase(),
      payee, // canonical (default for pre-watch orders) | legacy
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
    if (payee === "legacy") {
      // Stranding watch: publish the claim code up-front. The claim code is
      // derivable from the tx hash anyway (RAEN-<sha256(tx)[:10]>), so this
      // adds no secret — it only makes the watch VISIBLE in /storefront/claims.
      order.claim_code = claimCode(order.tx);
    }
    // CLAIMS DELIVERY-BINDING (Staci, 2026-11-24): if the fleet service's own
    // key-gated telemetry already attested a paid 2xx for this exact tx
    // (settled/delivery event ingested via /events), bind it now — this is what
    // makes a delivered order machine-distinguishable from a stranded one.
    // A buyer-presented digest alone (service_attested:false) records the
    // payload proof but NEVER flips delivered (TRUTH law).
    const evd = (store.state.delivery_evidence || {})[tx];
    const routeDelivered = Boolean(evd && evd.service_attested);
    if (evd && evd.product_digest && !routeDelivered) order.product_digest = evd.product_digest;
    if (routeDelivered) bindOrderFromEvidence(order, evd, log);
    store.addOrder(order);
    scanned++;
    if (payee === "legacy") {
      log(`legacy-watch: $${amountUsd} tx ${tx} to retired payee ${LEGACY_PAYEE} — order + claim ${order.claim_code} auto-created`);
    }
    if (routeDelivered) {
      // delivered on the service's own paid route — no email/webhook pipeline
      store.markDelivered(order.tx, { kind: "route-delivery", service: evd.service, route: evd.route || null, digest: evd.product_digest || null, sent_at: Date.now(), email: null });
      log(`storefront: tx ${tx} DELIVERED via ${evd.service} ${evd.route || ""} (service-attested route delivery, $${amountUsd})`);
    } else if (product) {
      matched++;
      log(`storefront: matched ${product.name} ($${product.price_usd}) tx ${tx}${payee === "legacy" ? " (legacy payee)" : ""}`);
      try { await deliverFn(order, product); } catch (e) {
        console.error("storefront delivery failed:", e.message, "tx", tx);
      }
    } else if (candidates.length > 1) {
      log(`storefront: $${amountUsd} tx ${tx} ambiguous (${candidates.length} candidates) — trying hint resolution`);
      try { await deliverFn(order, null); } catch (e) {
        console.error("storefront hint delivery failed:", e.message, "tx", tx);
      }
    } else if (order.classification === "external" && BigInt(amountAtomic) >= CLAIM_MIN_ATOMIC) {
      // Manual-pay recovery: a real external payment that matches no catalog
      // price still gets a claim code so its payer can self-serve (register
      // an email, get a payment-verified credit confirmation). Never silence.
      log(`storefront: external $${amountUsd} tx ${tx}${payee === "legacy" ? " (legacy payee) " : ""}unmatched to catalog — publishing claim`);
      try { await deliverFn(order, null); } catch (e) {
        console.error("storefront claim publish failed:", e.message, "tx", tx);
      }
    }
  }

  async function scanPayee(payeeAddr, cursorField) {
    let from = store.state[cursorField] > 0 ? store.state[cursorField] + 1 : Math.max(1, head - 100000);
    while (from <= head) {
      const to = Math.min(head, from + MAX_CHUNK - 1);
      const logs = await withFailover(rpc => rpcCall(rpc, "eth_getLogs", [{
        address: USDC_BASE,
        fromBlock: "0x" + from.toString(16),
        toBlock: "0x" + to.toString(16),
        topics: [TRANSFER_TOPIC, null, topicAddr(payeeAddr)],
      }])) || [];
      for (const lg of logs) await handleLog(lg, payeeAddr === CANONICAL ? "canonical" : "legacy");
      store.state[cursorField] = to;
      store.save();
      from = to + 1;
      if (head - to > 200000) break; // safety: never chase more than ~200k blocks per pass
    }
    return store.state[cursorField];
  }

  const canonicalCursor = await scanPayee(CANONICAL, "cursor");
  // Stranding watch runs AFTER the canonical pass so a canonical-side failure
  // (RPC blip) never suppresses the legacy scan in the same pass, and vice versa.
  let legacyCursor;
  try {
    legacyCursor = await scanPayee(LEGACY_PAYEE, "legacy_cursor");
  } catch (e) {
    console.error("legacy-watch scan failed (will retry next pass):", e.message);
    legacyCursor = store.state.legacy_cursor;
  }
  return { head, scanned, matched, cursor: canonicalCursor, legacy_cursor: legacyCursor };
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
<li>Go to the RAEN contract-eye endpoint: <code>POST https://contract-eye-x402.fly.dev/api/analyze-contract</code></li>
<li>Header: <code>X-RAEN-CREDIT: ${claimCode(order.tx)}</code> — this replaces the x402 payment header for prepaid purchases (the service verifies + burns the credit for you).</li>
<li>Body: <code>{"text": "&lt;50-50000 chars of the full contract text&gt;"}</code></li>
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
<li>Endpoint: <code>POST https://tradingagents-x402.fly.dev/api/analyze-ticker</code></li>
<li>Header: <code>X-RAEN-CREDIT: ${claimCode(order.tx)}</code> — this replaces the x402 payment header for prepaid purchases (the service verifies + burns the credit for you).</li>
<li>Body: <code>{"ticker": "AAPL"}</code> (optional <code>"date": "YYYY-MM-DD"</code>)</li>
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

// ---------- manual-pay recovery: payment-verified credit confirmation ----------
// For external payers whose on-chain transfer matched no catalog price (e.g. a
// manual EIP-3009 settle without the second X-PAYMENT call). Honest by
// construction: states exactly what was verified, that nothing was delivered,
// and the self-serve paths forward. Never promises fulfillment that did not happen.
function creditEmailHtml(order, claim) {
  const receipt = `Transaction: ${order.tx}\nBlock: ${order.block}\nPayer: ${order.payer}\nAmount: ${order.amount_usd} USDC\nNetwork: Base mainnet\nClaim code: ${claim}`;
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Payment verified — RAEN</title><style>
body{font-family:Georgia,serif;background:#faf8f4;color:#222;margin:0;padding:1.5rem;line-height:1.5}
.wrap{max-width:720px;margin:0 auto;background:#fff;border:1px solid #ddd;padding:2rem;border-radius:8px}
h1{font-size:1.4rem}h1 span{color:#8a1538}
.receipt{background:#f5efe9;border:1px solid #e5dcd2;border-radius:6px;padding:1rem;font-family:monospace;font-size:.8rem;white-space:pre-wrap}
code{background:#f0ece6;padding:2px 6px;border-radius:4px}
.foot{margin-top:1.5rem;font-size:.75rem;color:#999;font-family:system-ui,sans-serif}
</style></head><body><div class="wrap">
<h1><span>RAEN</span> — Payment verified</h1>
<div class="receipt">RECEIPT (on-chain, Base mainnet)
${esc(receipt)}</div>
<p>Your USDC payment was verified on-chain. It did not match one of our catalog product prices, so <strong>no product was auto-delivered</strong> — we will not pretend otherwise.</p>
<p>If you were making a per-call <strong>x402 API request</strong>: a standalone on-chain transfer cannot complete the protocol. The signed payment must ride along as the <code>X-PAYMENT</code> header on your second HTTP request. The 60-second flow: <a href="https://bshelby88.github.io/x402-cookbook/recipes/02-python-client/">cookbook recipe 02</a>. Your call will then settle and return normally.</p>
<p>Otherwise, your payment is recorded under claim code <code>${esc(claim)}</code>. Reply to this email (ops@royalruby.io) to apply it toward a purchase or a per-call fulfillment, or browse the catalog at <a href="https://settlement-collector.fly.dev/storefront/catalog">/storefront/catalog</a>.</p>
<div class="foot">Delivered automatically by the RAEN autonomous storefront. Payment verified on-chain before this email was composed. RAEN — managed AI agent operations.</div>
</div></body></html>`;
}

async function sendCreditConfirmation(order, email, apiKey) {
  const claim = order.claim_code || claimCode(order.tx);
  return sendHtmlEmail(apiKey, {
    to: email,
    subject: `RAEN — payment verified ($${order.amount_usd} USDC, claim ${claim})`,
    html: creditEmailHtml(order, claim),
    listUnsubscribe: false, // transactional: this thread IS the human contact path
  });
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
  // PLAN 9: free-tier registration also arms the 3-email onboarding sequence.
  armOnboarding(store, email, { product: "dispute-starter-free", tx, source: "free-tier" });
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

// ---------- PLAN 9 onboarding sequence (Erica, Nov 15) ----------
// Fires on ANY buyer email registration — purchase (/storefront/register),
// free tier (/storefront/free-tier), or claim/credit capture. Three emails:
//   T+0   receipt + a ready-to-paste curl for THEIR SKU + the 402→200 flow
//   T+1   verified-tx follow-up — adjacent-SKU suggestions, volume economics
//   T+7   week-one value ledger — usage summary, receipts, what's next
// TRUTH rule (ERICA MAX EFFORT §3, binding): every email contains ONLY facts
// read back from chain/telemetry. A buyer whose settlement cannot be verified
// gets NO revenue claim — just the integration help.

const STATUS_URL = "https://settlement-collector.fly.dev/stats";
const VALUE_LEDGER_URL = "https://bshelby88.github.io/raen-agent-directory/value-ledger.html";
const FAQ_URL = "https://bshelby88.github.io/raen-agent-directory/faq.html";
const DISCUSSIONS_URL = "https://github.com/orgs/x402/discussions";

// Per-SKU T0 curl + routing notes. Email-delivered products get the matching
// live API surface so the buyer always has a machine-callable route in hand.
const ONBOARD_ROUTES = {
  "dispute-package": {
    label: "Dispute Package ($49 — delivered by email)",
    note: "Your package itself arrives by email. The curl below is the live per-call API surface for the same dispute-letter engine, if you want machine access.",
    curl: `curl -i -X POST https://sentry-forge-x402.fly.dev/api/dispute-pack \\\n  -H "Content-Type: application/json" \\\n  -d '{"case_summary":"<your dispute facts>"}'`,
  },
  "dispute-starter-free": {
    label: "First 10 Dispute Letters — Free (delivered by email)",
    note: "Your letters arrive by email. The curl below hits the free example of the same engine live — no payment needed for the example.",
    curl: `curl -i https://sentry-forge-x402.fly.dev/api/dispute-pack/example`,
  },
  "contract-review": {
    label: "Contract Review credit ($19 — prepaid)",
    note: "You already paid, so use your credit header instead of an x402 payment — no second charge.",
    curl: `curl -i -X POST https://contract-eye-x402.fly.dev/api/analyze-contract \\\n  -H "Content-Type: application/json" \\\n  -H "X-RAEN-CREDIT: <your claim code from the delivery webhook>" \\\n  -d '{"text":"<50-50000 chars of the full contract text>"}'`,
  },
  "contract-watch": {
    label: "Contract Watch, 30-day ($19 — prepaid, recurring)",
    note: "Same endpoint as Contract Review; your weekly re-reviews ride on the same credit header.",
    curl: `curl -i -X POST https://contract-eye-x402.fly.dev/api/analyze-contract \\\n  -H "Content-Type: application/json" \\\n  -H "X-RAEN-CREDIT: <your claim code from the delivery webhook>" \\\n  -d '{"text":"<50-50000 chars of the full contract text>"}'`,
  },
  "market-intel-pack": {
    label: "Market Intel Pack ($29 — prepaid)",
    note: "You already paid, so use your credit header instead of an x402 payment — no second charge.",
    curl: `curl -i -X POST https://tradingagents-x402.fly.dev/api/analyze-ticker \\\n  -H "Content-Type: application/json" \\\n  -H "X-RAEN-CREDIT: <your claim code from the delivery webhook>" \\\n  -d '{"ticker":"AAPL"}'`,
  },
};
// Skills are delivered inline by email — no API call. T0 still gives the
// catalog + status surfaces so a skill buyer can see the fleet is live.
ONBOARD_ROUTES["skill-steward-pattern"] =
ONBOARD_ROUTES["skill-charge-and-rebate"] =
ONBOARD_ROUTES["skill-x402-production"] =
ONBOARD_ROUTES["skill-bundle"] = {
  label: "RAEN skill (delivered by email)",
  note: "Your skill content is in your delivery email — no API call needed. The curl below is the catalog, so you can see everything live and priced.",
  curl: `curl -i https://settlement-collector.fly.dev/storefront/catalog`,
};

// Adjacent-SKU suggestions for T+1 — only live, purchasable surfaces.
const ADJACENT = {
  "dispute-package": ["contract-review", "market-intel-pack"],
  "dispute-starter-free": ["contract-review", "market-intel-pack"],
  "contract-review": ["market-intel-pack", "skill-x402-production"],
  "contract-watch": ["market-intel-pack", "skill-x402-production"],
  "market-intel-pack": ["contract-review", "skill-steward-pattern"],
  "skill-steward-pattern": ["skill-charge-and-rebate", "contract-review"],
  "skill-charge-and-rebate": ["skill-steward-pattern", "contract-review"],
  "skill-x402-production": ["skill-bundle", "contract-review"],
  "skill-bundle": ["contract-review", "market-intel-pack"],
};
const adjacentLine = (productId) =>
  (ADJACENT[productId] || []).map(id => CATALOG[id] ? `- **${CATALOG[id].name}** — $${CATALOG[id].price_usd}` : null).filter(Boolean).join("<br>") || "- browse the full catalog";

// armOnboarding: record/refresh a buyer's sequence state. One sequence per
// email; a later registration refreshes the SKU/tx facts but never re-sends
// a step already sent. MACHINE-FIRST LAW: the key may also be an https
// callback_url for machine buyers (channel 'webhook') — machines don't read
// email, so their T0/T1/T7 steps arrive as HTTP POSTs to callback_url.
function armOnboarding(store, key, { product, tx, source, channel, callback_url, agent_id }) {
  const k = String(key).trim().toLowerCase();
  const isEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(k);
  const isUrl = /^https:\/\/[^\s]+$/.test(k);
  if (!isEmail && !isUrl) return null;
  const now = Date.now();
  const DAY = 86400000;
  const cur = store.state.onboarding[k] || {
    registered_at: now, t0_sent_at: null, t1_sent_at: null, t7_sent_at: null,
    t0_due_at: now, t1_due_at: now + DAY, t7_due_at: now + 7 * DAY,
  };
  if (product && CATALOG[product]) { cur.product = product; cur.product_name = CATALOG[product].name; cur.price_usd = CATALOG[product].price_usd; }
  if (tx) cur.tx = String(tx).toLowerCase();
  if (source) cur.source = String(source).slice(0, 40);
  if (isUrl || callback_url) {
    cur.channel = "webhook";
    cur.callback_url = String(callback_url || key).trim();
    cur.agent_id = agent_id ? String(agent_id).slice(0, 120) : null;
  }
  store.state.onboarding[k] = cur;
  store.save();
  return cur;
}

// ---------- MACHINE-FIRST LAW: webhook delivery for machine buyers ----------
// Machines don't read email. When a registration carries callback_url, the
// receipt, product delivery, and onboarding steps arrive as HTTP POSTs.

const CATALOG_URL = "https://settlement-collector.fly.dev/storefront/catalog";
const CLAIMS_URL = "https://settlement-collector.fly.dev/storefront/claims";
const REGISTER_URL = "https://settlement-collector.fly.dev/storefront/register";

async function postWebhook(url, payload) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 10000);
  try {
    const r = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", "user-agent": "RAEN-settlement-collector/1.0 (x402 machine onboarding)" },
      body: JSON.stringify(payload),
      signal: ctrl.signal,
    });
    return { status: r.status };
  } finally { clearTimeout(t); }
}

// Product delivery receipt (same moment an email buyer gets the delivery mail).
async function deliverWebhookReceipt(order, product, reg, log = console.log) {
  const payload = {
    type: "delivery",
    tx: order.tx,
    product: product.id,
    product_name: product.name,
    amount_usd: order.amount_usd || null,
    claim_code: order.claim_code || null,
    catalog_url: CATALOG_URL,
    claims_url: CLAIMS_URL,
    agent_id: reg.agent_id || null,
    delivered_at: new Date().toISOString(),
  };
  const r = await postWebhook(reg.callback_url, payload);
  if (r.status >= 300) throw new Error(`delivery webhook got HTTP ${r.status}`);
  log(`delivery webhook POSTed to ${reg.agent_id || reg.callback_url} for tx ${order.tx}`);
}

// Onboarding steps T0/T1/T7 as webhooks — same facts, same TRUTH rule, no email.
async function sendOnboardingWebhook(store, key, ob, step, log = console.log) {
  const route = ONBOARD_ROUTES[ob.product] || {
    label: ob.product_name || "RAEN purchase (registered)",
    note: "Your receipt and integration route are in this payload.",
    curl: `curl -i ${CATALOG_URL}`,
  };
  const payload = {
    type: "onboarding",
    step,
    product: ob.product || null,
    product_name: ob.product_name || null,
    price_usd: ob.price_usd || null,
    tx: ob.tx || null,
    label: route.label,
    note: route.note,
    integration_curl: route.curl,
    catalog_url: CATALOG_URL,
    claims_url: CLAIMS_URL,
    register_url: REGISTER_URL,
    status_url: STATUS_URL,
    agent_id: ob.agent_id || null,
    sent_at: new Date().toISOString(),
  };
  const r = await postWebhook(ob.callback_url, payload);
  if (r.status >= 300) throw new Error(`onboarding ${step} webhook got HTTP ${r.status}`);
  log(`onboarding ${step} webhook delivered to ${ob.agent_id || ob.callback_url}`);
}

// Verified-tx check for the T+1 value-ledger proof line: the buyer's tx is
// only quoted when it exists as a real observed on-chain order. TRUTH: an
// unverified buyer gets no revenue claim — just integration help.
function verifiedTxFor(store, ob) {
  if (!ob.tx) return null;
  const o = store.findOrder(ob.tx);
  if (o && o.payer && (o.matched || o.classification === "external" || o.delivered)) {
    return { tx: o.tx, amount_usd: o.amount_usd, product: o.product_name || (ob.product_name || null), delivered: Boolean(o.delivered) };
  }
  return null;
}

async function sendOnboardingT0(store, email, ob, apiKey, log) {
  const r = ONBOARD_ROUTES[ob.product] || {
    label: ob.product_name || "RAEN purchase (registered)",
    note: "Your delivery email has the details; reply to this one if anything is unclear.",
    curl: `curl -i https://settlement-collector.fly.dev/storefront/catalog`,
  };
  const txLine = ob.tx && !ob.tx.startsWith("free-") ? `<br>Receipt key:<br><code>${esc(ob.tx)}</code>` : "";
  const html = storefrontShell("Your RAEN x402 receipt + the 90-second integration check", `
<p><b>What you registered for:</b> ${esc(r.label)}.${txLine}</p>
<h2>The 90-second integration check</h2>
<p>Ready-to-paste curl for your route:</p>
<pre>${esc(r.curl)}</pre>
<p>${esc(r.note)}</p>
<h2>The 402 → 200 flow, in 3 lines</h2>
<ol>
<li>Call the route with no payment header → you get an <code>HTTP 402</code> with a payment challenge (price, payTo, network).</li>
<li>Your client signs that challenge with USDC on Base and retries with the <code>X-PAYMENT</code> header.</li>
<li>The route returns <code>200</code> and your settlement lands on-chain — visible in the public <a href="${VALUE_LEDGER_URL}">value ledger</a>.</li>
</ol>
<h2>Fleet status, any time</h2>
<p><a href="${STATUS_URL}">settlement-collector /stats</a> — challenges, settlements, and failures, live. Answers to the seven questions every buyer asks: <a href="${FAQ_URL}">the RAEN answer library</a>.</p>
<p><b>One ask:</b> reply with your use case — we tune the service around real buyers. Direct line: <code>ops@royalruby.io</code>.</p>`);
  const out = await sendHtmlEmail(apiKey, {
    to: email,
    subject: "Your RAEN x402 receipt + the 90-second integration check",
    html,
  });
  log(`onboarding T0 (receipt + integration check) sent to ${email} (${ob.product || "no product hint"})`);
  return out;
}

async function sendOnboardingT1(store, email, ob, apiKey, log) {
  const vt = verifiedTxFor(store, ob);
  const proof = vt
    ? `<p>Value-ledger proof: your settlement <code>${esc(vt.tx)}</code>${vt.product ? ` for ${esc(vt.product)}` : ""} (${vt.amount_usd} USDC) is verified on-chain. ${vt.delivered ? "Your delivery is confirmed sent." : ""} See it in the public <a href="${VALUE_LEDGER_URL}">value ledger</a>.</p>`
    : `<p>We have not yet observed a verified settlement to quote on-chain for your registration${ob.tx ? ` (<code>${esc(ob.tx)}</code> is not yet an observed on-chain payment — if you already paid, reply with the tx and we will reconcile it the same day)` : " — if you pay per-call, your first settled tx will appear here as proof"}.</p>`;
  const html = storefrontShell("Your first call — what to buy next", `
${proof}
<h2>What to buy next (matched to what you registered for)</h2>
<p>${adjacentLine(ob.product)}</p>
<h2>Volume economics</h2>
<p>Everything above is per-call USDC on Base — no subscription required. At 100 calls/day, a $0.05 route costs $5/day. If your volume is steady, reply and tell us — pack pricing is on the roadmap queue and real buyer volume is what moves it.</p>
<p>Direct line: <code>ops@royalruby.io</code>.</p>`);
  const out = await sendHtmlEmail(apiKey, {
    to: email,
    subject: "Your first call worked — here's what to buy next",
    html,
  });
  log(`onboarding T1 (verified-tx follow-up${vt ? ", proof included" : ", no verified tx — honest no-claim variant"}) sent to ${email}`);
  return out;
}

async function sendOnboardingT7(store, email, ob, apiKey, log) {
  // Week-one usage summary — only from recorded state, never invented.
  const regs = Object.entries(store.state.registrations)
    .filter(([, r]) => r && r.email && r.email.toLowerCase() === email)
    .map(([k, r]) => ({ key: k, ...r }));
  const myOrders = store.state.orders
    .filter(o => regs.some(r => r.key === o.tx) || (o.tx && regs.some(r => r.key === o.tx)))
    .map(o => ({ tx: o.tx, product: o.product_name, amount_usd: o.amount_usd, delivered: o.delivered, at: o.seen_at }));
  const usageRows = myOrders.length
    ? myOrders.slice(-10).map(o => `<li><code>${esc(o.tx.slice(0, 20))}…</code> — ${esc(o.product || "registered")} — $${o.amount_usd} — ${o.delivered ? "delivered" : "on record"}</li>`).join("")
    : "<li>No calls recorded from this email in week one — that's useful signal too; reply and tell us what blocked you.</li>";
  const html = storefrontShell("Your week with RAEN: usage, receipts, and what we built from your feedback", `
<h2>Week-one usage (from our recorded ledger, nothing invented)</h2>
<ul>${usageRows}</ul>
<h2>What we're building next (from live demand data)</h2>
<ul>
<li>Pack pricing for steady-volume buyers — queued, driven by real volume reports (see day-1 email).</li>
<li>Better free discovery on every SKU — free example routes like <code>/api/dispute-pack/example</code> are the pattern we're extending.</li>
</ul>
<h2>If you serve clients</h2>
<p>The connector program is live: white-label dispute letters at $0.75/letter, 20% lifetime commission on referred spend, no minimums — <a href="https://settlement-collector.fly.dev/connector">join here</a>, terms verified on the <a href="https://settlement-collector.fly.dev/connector/stats">public connector ledger</a>.</p>
<h2>Community</h2>
<p>Buyers debug x402 challenges in public at the <a href="${DISCUSSIONS_URL}">x402 GitHub discussions</a> — we answer there daily. The standing answers to the seven most common questions: <a href="${FAQ_URL}">answer library</a>.</p>
<p>Direct line: <code>ops@royalruby.io</code>.</p>`);
  const out = await sendHtmlEmail(apiKey, {
    to: email,
    subject: "Your week with RAEN: usage, receipts, and what we built from your feedback",
    html,
  });
  log(`onboarding T7 (week-one value ledger) sent to ${email} (${myOrders.length} recorded orders)`);
  return out;
}

// Daily pass (extends the Rico followup-cron pattern): walk every armed
// onboarding state and send whichever step is due, strictly in order.
async function checkOnboarding(store, apiKey, log = console.log) {
  const out = { checked: 0, t0: 0, t1: 0, t7: 0, failed: 0 };
  const now = Date.now();
  for (const [key, ob] of Object.entries(store.state.onboarding)) {
    out.checked++;
    try {
      const isWebhook = ob.channel === "webhook" && ob.callback_url;
      if (!ob.t0_sent_at && ob.t0_due_at <= now) {
        if (isWebhook) await sendOnboardingWebhook(store, key, ob, "T0", log);
        else await sendOnboardingT0(store, key, ob, apiKey, log);
        ob.t0_sent_at = Date.now();
        out.t0++;
        store.save();
      } else if (ob.t0_sent_at && !ob.t1_sent_at && ob.t1_due_at <= now) {
        if (isWebhook) await sendOnboardingWebhook(store, key, ob, "T1", log);
        else await sendOnboardingT1(store, key, ob, apiKey, log);
        ob.t1_sent_at = Date.now();
        out.t1++;
        store.save();
      } else if (ob.t1_sent_at && !ob.t7_sent_at && ob.t7_due_at <= now) {
        if (isWebhook) await sendOnboardingWebhook(store, key, ob, "T7", log);
        else await sendOnboardingT7(store, key, ob, apiKey, log);
        ob.t7_sent_at = Date.now();
        out.t7++;
        store.save();
      }
    } catch (e) {
      out.failed++;
      log(`onboarding step failed for ${key}: ${e.message}`);
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
function wireStorefront(app, { isInternalPayer, collectorKey }) {
  const store = makeStore();
  const log = (...a) => console.log("[storefront]", ...a);

  // RECOVERY-FUNNEL helpers (Staci, 2026-11-22)
  const isTxHash = (s) => /^0x[0-9a-fA-F]{64}$/.test(s);
  const isAddr = (s) => /^0x[0-9a-fA-F]{40}$/.test(s);
  // lookup rate limit: 30 requests / 60s per IP (claims-by-tx/payer only)
  const lookupHits = new Map();
  function lookupRateLimit(req) {
    const ip = String(req.ip || (req.headers && req.headers["x-forwarded-for"]) || "unknown").split(",")[0].trim();
    const now = Date.now();
    const arr = (lookupHits.get(ip) || []).filter(t => now - t < 60000);
    if (arr.length >= 30) { lookupHits.set(ip, arr); return false; }
    arr.push(now);
    lookupHits.set(ip, arr);
    return true;
  }
  // find an order by tx hash OR (for 20-hex keys) by payer address, newest first
  function findOrderFor(key) {
    const k = String(key || "").trim().toLowerCase();
    if (!k) return undefined;
    const byTx = store.state.orders.find(o => String(o.tx).toLowerCase() === k);
    if (byTx) return byTx;
    if (isAddr(k)) {
      const byPayer = store.state.orders.filter(o => String(o.payer || "").toLowerCase() === k);
      if (byPayer.length) return byPayer[byPayer.length - 1];
    }
    return undefined;
  }
  // re-delivery: an order flagged delivered (maybe to a stale/test target) gets
  // delivery re-armed when a NEW registration target arrives. The delivery
  // record's target is the dedupe key — same target never re-sends.
  function armRedelivery(order, newTarget) {
    if (!order || !order.delivered || !newTarget) return;
    const rec = store.state.delivered[order.tx];
    const priorTarget = rec ? String(rec.email || rec.callback_url || "") : "";
    if (priorTarget.toLowerCase() !== String(newTarget).toLowerCase()) {
      order.delivered = false;
      log(`re-delivery armed for tx ${order.tx} — new registration target differs from the recorded delivery target`);
    }
  }

  // CLAIMS DELIVERY-BINDING (Staci, 2026-11-24): the /events ingest hands every
  // tx-bearing settled/delivery event here. Service-side telemetry (key-gated)
  // on a SERVICE's own route = delivery evidence; facilitator telemetry and
  // buyer-presented data never flip delivered (TRUTH law). See the module-level
  // delivery-binding block for the evidence model.
  function recordDeliveryEvidence(ev) {
    try {
      if (!ev || !ev.tx) return null;
      const et = String(ev.event || "");
      if (et !== "settled" && et !== "delivery") return null;
      const service = String(ev.service || "").toLowerCase();
      if (!isDeliveryRouteEvidence(service, ev.route)) return null; // facilitator routes prove payment, not delivery
      if (et === "settled" && Number(ev.status) >= 400) return null; // a failed "settled" never binds
      return upsertDeliveryEvidence(store, {
        service, route: String(ev.route || ""), price_usd: Number(ev.price_usd) || null,
        payer: ev.payer ? String(ev.payer).toLowerCase() : null, tx: ev.tx,
        product_digest: ev.product_digest || null,
        digest_source: et === "delivery" ? "service-telemetry" : null,
        service_attested: true,
      });
    } catch (e) {
      console.error("[storefront] delivery-evidence ingest failed:", e.message);
      return null;
    }
  }
  store.recordDeliveryEvidence = recordDeliveryEvidence;


  // Manual-pay recovery backfill (Bean, Nov 15): external orders already seen
  // before this feature get claim codes so their payers can self-serve too.
  // Dust below the claim floor (scatter transfers) is excluded.
  let backfilled = 0;
  for (const o of store.state.orders) {
    if (o.classification === "external" && !o.delivered && !o.credit_recorded && !o.claim_code &&
        BigInt(o.amount_atomic || "0") >= CLAIM_MIN_ATOMIC) {
      o.claim_code = claimCode(o.tx);
      backfilled++;
      log(`backfill claim ${o.claim_code} for external tx ${o.tx}`);
    }
  }
  if (backfilled) store.save();

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
      const claimable = order.matched ||
        (order.classification === "external" && BigInt(order.amount_atomic || "0") >= CLAIM_MIN_ATOMIC);
      if (claimable) {
        order.claim_code = order.claim_code || claimCode(order.tx);
        log(`unresolved product for tx ${order.tx} — claim ${order.claim_code} published`);
      }
      // Manual-pay recovery (Bean, Nov 15): an external payer who registered an
      // email but whose transfer matches no catalog price gets a ONE-TIME
      // payment-verified credit confirmation. delivered stays false (TRUTH:
      // no product was delivered) — credit_recorded marks it handled.
      if (claimable && order.classification === "external" && !order.delivered && !order.credit_recorded) {
        const email0 = store.lookupEmail(order);
        if (email0) {
          try {
            await sendCreditConfirmation(order, email0, process.env.RESEND_API_KEY);
            order.credit_recorded = true;
            store.markDelivered(order.tx, { email: email0, kind: "credit-confirmation", sent_at: Date.now(), email_id: null });
            log(`credit confirmation sent to ${email0} for tx ${order.tx}`);
          } catch (e) {
            console.error("credit confirmation failed:", e.message, "tx", order.tx);
          }
        }
      }
      store.save();
      return;
    }
    order.product = product.id;
    order.product_name = product.name;
    // MACHINE-FIRST LAW: webhook-channel registration → receipt POSTs to the
    // machine's callback_url; no email is ever involved.
    const reg = store.lookupRegistration(order);
    if (reg && reg.channel === "webhook" && reg.callback_url) {
      if (order.delivered) return;
      await deliverWebhookReceipt(order, product, reg, log);
      order.delivered = true;
      store.markDelivered(order.tx, { email: null, channel: "webhook", callback_url: reg.callback_url, sent_at: Date.now(), email_id: null });
      store.save();
      log(`delivered ${product.name} to machine ${reg.agent_id || reg.callback_url} via webhook for tx ${order.tx}`);
      const refW = store.lookupRef(order);
      if (refW && product.price_usd > 0) {
        const commissionW = Math.round(product.price_usd * 0.20 * 10000) / 10000;
        store.addCommission({ tx: order.tx, connector: refW, order_usd: product.price_usd, commission_usd: commissionW, at: Date.now() });
      }
      return;
    }
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
    res.json({
      pay_to: CANONICAL, network: "base", token: "USDC", usdc_contract: USDC_BASE, products: CATALOG,
      // CLAIMS DISCOVERABILITY (Staci, 2026-11-22): every machine-facing surface
      // carries the claims desk + tx-lookup instructions, so a stranded buyer
      // who paid but got nothing always finds the self-serve path here.
      claims_help: {
        paid_but_nothing_delivered: "If you paid USDC but received nothing, do not pay again.",
        lookup_your_payment: "GET /storefront/claims?tx=<your tx hash>  OR  GET /storefront/claims?payer=<your wallet address>",
        claim_code_note: "Your claim code is derivable from your tx: RAEN-<sha256(tx)[:10] uppercase> — lookup confirms it.",
        register_for_delivery: "POST /storefront/register {tx_or_intent: <tx or payer address>, email: <you> | callback_url: <https machine endpoint>, product?: <catalog id>}",
        credits_verify: "GET /storefront/credits/<claim code> — verify a prepaid credit (one-time redeem: POST /storefront/credits/<code>/redeem)",
        legacy_payee_watch: "Transfers to the retired treasury 0x7861db4efc14a1ed5dd8c96c528a3796560f1393 are ALSO tracked (auto order + claim code) — see /storefront/legacy-watch",
        contact: "ops@royalruby.io",
      },
    }));

  app.get("/storefront/deliveries", (_req, res) => res.json({ deliveries: store.state.delivered }));

  app.get("/storefront/orders", (_req, res) => {
    const orders = store.state.orders.slice().reverse().slice(-200).map(o => ({
      ...o,
      payee: o.payee || "canonical", // pre-watch orders are canonical by construction
      email: o.email || (store.lookupEmail(o) || null),
    }));
    res.json({ total_orders: store.state.orders.length, matched: store.state.orders.filter(o => o.matched).length, orders });
  });

  app.get("/storefront/claims", (req, res) => {
    // RECOVERY-FUNNEL (Staci, 2026-11-22): claims lookup by tx hash OR payer
    // address — knowledge-of-tx becomes proof-of-payer. The payer's tx hash is
    // the one artifact every x402 client retains; the claim code is derivable
    // from it anyway (RAEN-<sha256(tx)[:10]>), so returning it for a tx the
    // caller already knows adds no leak. No PII: emails/callback URLs are never
    // returned. Light per-IP rate limit below stops enumeration hammering.
    const q = req.query || {};
    const txQ = String(q.tx || "").trim();
    const payerQ = String(q.payer || "").trim();
    if (txQ || payerQ) {
      if (!lookupRateLimit(req)) return res.status(429).json({ error: "rate limit — slow down" });
      if (txQ && !/^0x[0-9a-fA-F]{64}$/.test(txQ))
        return res.status(400).json({ error: "tx must be a 0x-prefixed 32-byte hash" });
      if (payerQ && !/^0x[0-9a-fA-F]{40}$/.test(payerQ))
        return res.status(400).json({ error: "payer must be a 0x-prefixed 20-byte address" });
      const txL = txQ.toLowerCase();
      const payerL = payerQ.toLowerCase();
      const matchedOrders = store.state.orders.filter(o =>
        (txL && String(o.tx).toLowerCase() === txL) ||
        (payerL && String(o.payer).toLowerCase() === payerL));
      const claims = matchedOrders.map(o => {
        const rec = store.state.delivered[o.tx] || null;
        const claimable = Boolean(o.matched || o.claim_code ||
          (o.classification === "external" && Number(o.amount_usd || 0) >= 0.01));
        const state = o.credit_redeemed ? "credit_redeemed"
          : o.delivered ? "delivered"
          : claimable ? "open"
          : "recorded";
        return {
          tx: o.tx, block: o.block || null, payer: o.payer,
          payee: o.payee || "canonical",
          amount_usd: o.amount_usd,
          product: o.product || null, product_name: o.product_name || null,
          product_digest: o.product_digest || null, // claims delivery-binding (Staci, 2026-11-24)
          delivery_evidence: o.delivery_evidence || null, // {source,service,route,product_digest,digest_source,at}
          claim_code: o.claim_code || (claimable ? claimCode(o.tx) : null),
          matched: Boolean(o.matched), ambiguous: Boolean(o.ambiguous),
          delivered: Boolean(o.delivered),
          delivery_channel: rec ? (rec.channel || (rec.kind === "route-delivery" ? "route-delivery" : (rec.kind === "credit-confirmation" ? "credit-confirmation" : "email"))) : (o.delivered ? "email" : null),
          credit_redeemed: Boolean(o.credit_redeemed),
          registered: Boolean(store.lookupEmail(o) || (store.lookupRegistration(o) || {}).callback_url),
          state, seen_at: o.seen_at,
        };
      });
      return res.json({
        found: claims.length > 0, lookup_by: txQ ? "tx" : "payer",
        claims,
        note: "open claim? register at /storefront/register with your tx hash (or payer address) + email, or callback_url for machine delivery. redeemed credits are one-time.",
        resolve_url: "https://settlement-collector.fly.dev/storefront/register",
        credits_verify_url: "https://settlement-collector.fly.dev/storefront/credits/<code>",
        legacy_payee_watch: "transfers to the retired payee 0x7861db4efc14a1ed5dd8c96c528a3796560f1393 are tracked too — if your challenge named that wallet, your payment is still here; this lookup finds it.",
      });
    }
    const claims = store.state.orders
      .filter(o => (o.matched || o.claim_code) && !o.delivered && !o.credit_recorded && !store.lookupEmail(o))
      .map(o => ({
        tx: o.tx, product: o.product_name, amount_usd: o.amount_usd, payer: o.payer,
        payee: o.payee || "canonical",
        claim_code: o.claim_code, ambiguous: Boolean(o.ambiguous), candidates: o.candidates || [],
        seen_at: o.seen_at,
      }))
      .reverse();
    res.json({
      open_claims: claims.length, claims, resolve_url: "https://settlement-collector.fly.dev/storefront/register",
      lookup_hint: "have a tx hash or wallet address? GET /storefront/claims?tx=… or ?payer=… for your specific payment",
      legacy_payee_watch: "retired-payee transfers are watched too — status at /storefront/legacy-watch",
    });
  });

  // POST /storefront/delivery-digest — buyer-presented delivery receipt
  // (CLAIMS DELIVERY-BINDING, Staci 2026-11-24). The buyer (or any machine that
  // received the payload) presents the sha256 of the delivered bytes for a tx
  // it paid. TRUTH: this NEVER sets delivered:true by itself — it attaches the
  // payload proof to the delivery-evidence record; delivery flips only when the
  // service's own key-gated telemetry attests the paid 2xx for that tx.
  app.post("/storefront/delivery-digest", (req, res) => {
    if (!lookupRateLimit(req)) return res.status(429).json({ error: "rate limit — slow down" });
    const b = req.body || {};
    const tx = String(b.tx || "").trim().toLowerCase();
    const rawDigest = String(b.product_digest || "").trim().toLowerCase();
    if (!/^0x[0-9a-f]{64}$/.test(tx))
      return res.status(400).json({ error: "tx must be a 0x-prefixed 32-byte hash" });
    const digestHex = rawDigest.startsWith("sha256:") ? rawDigest.slice(7) : rawDigest;
    if (!/^[0-9a-f]{64}$/.test(digestHex))
      return res.status(400).json({ error: "product_digest must be a sha256 hex digest (64 hex chars, optional sha256: prefix)" });
    const product_digest = "sha256:" + digestHex;
    const trusted = Boolean(collectorKey && req.get("x-collector-key") === collectorKey);
    const evd = upsertDeliveryEvidence(store, {
      tx, product_digest,
      digest_source: trusted ? "service-telemetry" : "buyer-presented",
      service_attested: false,
    });
    const order = store.findOrder(tx);
    res.json({
      ok: true, tx, product_digest,
      service_attested: Boolean(evd && evd.service_attested),
      delivery_state: order ? (order.delivered ? "delivered" : "recorded-awaiting-service-attestation") : "no-order-yet",
      note: "digest recorded. delivered flips only on the service's own key-gated telemetry (paid 2xx with this tx) — a buyer-presented digest alone never marks delivery.",
    });
  });

  // LEGACY-PAYEE STRANDING WATCH status (Staci, 2026-11-22): public, honest
  // counts. The watch is live when legacy_cursor advances with the chain head;
  // zero orders is an honest zero (no transfers to the retired payee in the
  // scanned window), never a silent watcher.
  app.get("/storefront/legacy-watch", (_req, res) => {
    const legacyOrders = store.state.orders.filter(o => o.payee === "legacy");
    const last = legacyOrders.length ? legacyOrders[legacyOrders.length - 1] : null;
    res.json({
      payee: LEGACY_PAYEE,
      status: "watching",
      what: "USDC transfers to this retired-but-still-served payee auto-create an order + claim code (RAEN-<sha256(tx)[:10]>) so legacy-named settles are never invisible",
      cursor: store.state.legacy_cursor,
      watch_orders: legacyOrders.length,
      last_order: last ? {
        tx: last.tx, payer: last.payer, amount_usd: last.amount_usd,
        claim_code: last.claim_code, matched: Boolean(last.matched),
        delivered: Boolean(last.delivered), seen_at: last.seen_at,
      } : null,
      claims_url: "https://settlement-collector.fly.dev/storefront/claims",
      register_url: "https://settlement-collector.fly.dev/storefront/register",
      note: "zero orders = no transfers to the retired payee in the scanned window (honest zero). Lookup by tx/payer at /storefront/claims.",
    });
  });

  // POST /storefront/register {tx_or_intent, email, product?, ref?}
  // MACHINE-FIRST LAW: machines may register with {tx_or_intent, agent_id?,
  // callback_url?} — when callback_url (https) is present, the receipt +
  // claims URL are delivered by HTTP POST instead of email, and the T0/T1/T7
  // onboarding sequence fires as webhooks. Same ledger, same TRUTH rule.
  app.post("/storefront/register", (req, res) => {
    const b = req.body || {};
    const email = String(b.email || "").trim();
    const key = String(b.tx_or_intent || "").trim();
    const callbackUrl = String(b.callback_url || "").trim();
    const agentId = String(b.agent_id || "").trim().slice(0, 120);
    const hint = String(b.product || "").trim() && CATALOG[String(b.product || "").trim()] ? String(b.product).trim() : null;
    const ref = store.state.connectors[String(b.ref || "").trim()] ? String(b.ref).trim() : null;
    if (!key) return res.status(400).json({ error: "tx_or_intent required" });
    if (callbackUrl) {
      if (!/^https:\/\/[^\s]+$/.test(callbackUrl)) return res.status(400).json({ error: "callback_url must be an https URL" });
      store.addRegistration(key, null, hint, ref, { channel: "webhook", callback_url: callbackUrl, agent_id: agentId || null });
      armOnboarding(store, callbackUrl, { product: hint, tx: key, source: "machine-register", callback_url: callbackUrl, agent_id: agentId || null });
      setImmediate(() => checkOnboarding(store, process.env.RESEND_API_KEY, log)
        .catch(e => log(`onboarding pass failed: ${e.message}`)));
      const mOrder = findOrderFor(key);
      armRedelivery(mOrder, callbackUrl);
      if (mOrder && (mOrder.matched || mOrder.classification === "external") && !mOrder.delivered) {
        deliver(mOrder, null).catch(e => log(`machine delivery failed: ${e.message}`));
      }
      return res.json({
        ok: true,
        channel: "webhook",
        delivered: Boolean(mOrder && mOrder.delivered),
        delivery_pending: Boolean(mOrder && (mOrder.matched || mOrder.classification === "external") && !mOrder.delivered),
        agent_id: agentId || null,
        callback_url: callbackUrl,
        catalog_url: CATALOG_URL,
        claims_url: CLAIMS_URL,
        note: "Receipt + delivery notices POST to callback_url. Onboarding steps (T+0 receipt, T+1 verified-tx follow-up, T+7 value ledger) arrive as webhooks. Machines never need to read email.",
      });
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "valid email required (or pass callback_url for machine registration)" });
    store.addRegistration(key, email, hint, ref);
    // PLAN 9: any registration — purchase, claim, or pre-payment capture —
    // arms the 3-email onboarding sequence and fires T0 immediately
    // (best-effort; the daily cron retries if the send fails).
    armOnboarding(store, email, { product: hint, tx: key, source: "register" });
    setImmediate(() => checkOnboarding(store, process.env.RESEND_API_KEY, log)
      .catch(e => log(`onboarding pass failed: ${e.message}`)));
    // if this tx already arrived, deliver immediately
    // RECOVERY-FUNNEL (Staci, 2026-11-22): tx_or_intent may also be a PAYER
    // ADDRESS — a payer who retained only their wallet can still bind delivery.
    const order = findOrderFor(key);
    armRedelivery(order, email);
    if (order && order.payer) { // bind email to the payer's subscription if it lacks one
      const sub = store.findSubByPayer(order.payer);
      if (sub && !sub.email) { sub.email = email; store.upsertSubscription(sub); }
    }
    if (order && (order.matched || order.classification === "external") && !order.delivered) {
      deliver(order, null)
        .then(() => res.json({ ok: true, delivered: Boolean(order.delivered), credit_recorded: Boolean(order.credit_recorded), product: order.product_name || null, claim_code: order.claim_code || null }))
        .catch(e => res.status(502).json({ ok: true, delivered: false, error: e.message.slice(0, 160) }));
      return;
    }
    res.json({ ok: true, delivered: false, note: "email registered — delivery fires when the matching on-chain payment is observed" });
  });

  // Credit authority (Bean, 2026-11-16): the claim code (RAEN-<sha256(tx)[:10]>) is
  // the prepaid SKU's machine redemption key. GET verifies; POST redeems one-time.
  // The paid services (contract-eye, tradingagents) call redeem here before serving
  // a prepaid call instead of collecting x402 payment — this is what makes the
  // delivery copy's X-RAEN-CREDIT promise REAL instead of fiction.
  function orderByCode(code) {
    const want = String(code || "").trim().toUpperCase();
    if (!/^RAEN-[0-9A-F]{10}$/.test(want)) return null;
    return store.state.orders.find(o => o.claim_code === want || claimCode(o.tx) === want) || null;
  }
  app.get("/storefront/credits/:code", (req, res) => {
    const order = orderByCode(req.params.code);
    if (!order) return res.status(404).json({ ok: false, error: "unknown credit code" });
    if (!order.delivered && !order.matched && order.classification !== "external")
      return res.status(404).json({ ok: false, error: "credit not backed by a paid order" });
    res.json({ ok: true, product: order.product || order.product_name || null, product_name: order.product_name || null,
      amount_usd: order.amount_usd || null, tx: order.tx, redeemed: Boolean(order.credit_redeemed) });
  });
  app.post("/storefront/credits/:code/redeem", (req, res) => {
    const order = orderByCode(req.params.code);
    if (!order) return res.status(404).json({ ok: false, error: "unknown credit code" });
    if (!order.delivered && !order.matched && order.classification !== "external")
      return res.status(404).json({ ok: false, error: "credit not backed by a paid order" });
    if (order.credit_redeemed)
      return res.status(409).json({ ok: false, error: "credit already redeemed", redeemed_at: order.credit_redeemed_at, product: order.product || order.product_name || null });
    order.credit_redeemed = true;
    order.credit_redeemed_at = new Date().toISOString();
    store.save();
    log(`credit ${claimCode(order.tx)} redeemed (${order.product || order.product_name || "unresolved"}) by ${String((req.body || {}).agent_id || "unknown").slice(0, 80)}`);
    res.json({ ok: true, product: order.product || order.product_name || null, product_name: order.product_name || null, tx: order.tx });
  });

  // POST /storefront/ops/credit-rearm — fleet-ops only (X-COLLECTOR-KEY).
  // RECOVERY-FUNNEL (Staci, 2026-11-22): a one-time credit consumed by an
  // internal TEST (not by the payer) is a stranding, not a sale. This reverses
  // a misredemption with a full audit trail — the redemption record moves into
  // order.credit_history and the payer's redemption right is restored.
  // It NEVER touches delivered flags, amounts, or classification (TRUTH law).
  app.post("/storefront/ops/credit-rearm", (req, res) => {
    if (!collectorKey || req.get("x-collector-key") !== collectorKey)
      return res.status(401).json({ ok: false, error: "collector key required" });
    const tx = String((req.body || {}).tx || "").trim().toLowerCase();
    const reason = String((req.body || {}).reason || "").slice(0, 300);
    if (!/^0x[0-9a-f]{64}$/.test(tx)) return res.status(400).json({ ok: false, error: "tx must be a 0x-prefixed 32-byte hash" });
    if (!reason) return res.status(400).json({ ok: false, error: "reason required (audit trail)" });
    const order = store.state.orders.find(o => String(o.tx).toLowerCase() === tx);
    if (!order) return res.status(404).json({ ok: false, error: "tx not found" });
    if (!order.credit_redeemed) return res.status(409).json({ ok: false, error: "credit is not redeemed" });
    order.credit_history = order.credit_history || [];
    order.credit_history.push({ redeemed_at: order.credit_redeemed_at || null, rearmed_at: new Date().toISOString(), reason });
    order.credit_redeemed = false;
    delete order.credit_redeemed_at; // preserved in credit_history
    store.save();
    log(`credit re-arm for tx ${tx}: ${reason}`);
    res.json({ ok: true, tx, claim_code: order.claim_code || claimCode(order.tx), credit_history: order.credit_history });
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
    // Hardening (ARC-67): a stale/legacy record without a code must never reach the
    // buyer-facing email — regenerate instead of rendering "Code: undefined".
    if (!rec || !rec.code || !store.state.connectors[rec.code]) {
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

  // ---- PLAN 9 onboarding sequence (Erica, Nov 15) ----
  // GET /storefront/onboarding — sequence status per registered buyer.
  app.get("/storefront/onboarding", (_req, res) => {
    const seq = Object.entries(store.state.onboarding).map(([email, ob]) => ({
      email,
      source: ob.source || null,
      product: ob.product || null,
      product_name: ob.product_name || null,
      tx: ob.tx || null,
      registered_at: ob.registered_at,
      steps: {
        t0: { due_at: ob.t0_due_at, sent_at: ob.t0_sent_at },
        t1: { due_at: ob.t1_due_at, sent_at: ob.t1_sent_at },
        t7: { due_at: ob.t7_due_at, sent_at: ob.t7_sent_at },
      },
    }));
    res.json({
      total: seq.length,
      t0_sent: seq.filter(s => s.steps.t0.sent_at).length,
      t1_sent: seq.filter(s => s.steps.t1.sent_at).length,
      t7_sent: seq.filter(s => s.steps.t7.sent_at).length,
      sequences: seq,
    });
  });

  // GET /storefront/onboarding-cron?secret=... — cron-friendly onboarding pass
  // (same pattern as followup-cron: T0/T+1/T+7, strictly in order).
  app.get("/storefront/onboarding-cron", (req, res) => {
    if (process.env.REPORT_CRON_SECRET && req.query.secret !== process.env.REPORT_CRON_SECRET)
      return res.status(401).json({ error: "bad secret" });
    checkOnboarding(store, process.env.RESEND_API_KEY, log)
      .then(r => res.json({ ok: true, ...r }))
      .catch(e => res.status(502).json({ error: e.message }));
  });

  // daily internal onboarding pass (same belt-and-braces pattern as subscriptions
  // and the Rico followups)
  let obBusy = false;
  const obIv = setInterval(() => {
    if (obBusy) return;
    obBusy = true;
    checkOnboarding(store, process.env.RESEND_API_KEY, log)
      .catch(e => console.error("[storefront] onboarding cron failed:", e.message))
      .finally(() => { obBusy = false; });
  }, 24 * 60 * 60 * 1000);
  if (obIv.unref) obIv.unref();

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

module.exports = { wireStorefront, CATALOG, CANONICAL, matchProduct, claimCode, buildDeliverable, scanOnce, makeStore, upsertContractWatch, checkSubscriptions, armOnboarding, checkOnboarding, verifiedTxFor, ONBOARD_ROUTES, ADJACENT };
