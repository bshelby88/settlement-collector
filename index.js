// settlement-collector — RAEN fleet paid-route telemetry (STACI, Nov 2026)
// Receives settlement-funnel events from every x402 service, keeps an
// in-memory + on-disk (volume) ring buffer, exposes aggregate funnel stats:
// challenges (402), settled (200-on-paid), failures, latency, revenue estimate.
// Auth: services send X-COLLECTOR-KEY (set via fly secrets). Read endpoints public.
const express = require("express");
const fs = require("fs");
const path = require("path");
const { loadConfig, buildReport, renderHtml, sendViaResend } = require("./report");
const { wireStorefront } = require("./storefront");

const PORT = process.env.PORT || 3000;
const COLLECTOR_KEY = process.env.COLLECTOR_KEY || "";
const REPORT_CRON_SECRET = process.env.REPORT_CRON_SECRET || "";
const DATA_FILE = path.join("/data", "events.jsonl").replace(/\//g, require("path").sep);
const MAX_EVENTS = 20000;

const app = express();
app.use(express.json({ limit: "256kb" }));

app.get("/status", (_req, res) => res.sendFile(path.join(__dirname, "status.html")));

app.get("/proof", (_req, res) => res.sendFile(path.join(__dirname, "proof.html")));

app.get("/ledger.html", (_req, res) => res.sendFile(path.join(__dirname, "ledger.html")));

// Canonical short link — 302 to the canonical /ledger.html page.
app.get("/ledger", (_req, res) => res.redirect(302, "/ledger.html"));

// Public sales ledger (JSON) — straight from our own settlement records.
// TRUTH FIX (Nov 11): external vs internal split. RAEN-owned wallets
// (raen-wallets.json) are internal test traffic: they never inflate
// distinct_external_payers or total_external_usdc.
const RAEN_WALLETS_FILE = path.join(__dirname, "raen-wallets.json");
function loadInternalWallets() {
  const fallback = [
    "0xfbc0eb7811d477e55261d956df39f0046e192240", // AgentCash payer
    "0x9b8a27e03ceba490953cda8ab1291c0e13baf72f", // RAEN treasury (payTo)
  ];
  try {
    const cfg = JSON.parse(fs.readFileSync(RAEN_WALLETS_FILE, "utf8"));
    if (Array.isArray(cfg.internal_wallets) && cfg.internal_wallets.length) {
      return { set: new Set(cfg.internal_wallets.map(w => String(w).toLowerCase())), labels: cfg._labels || {} };
    }
  } catch (e) { console.error("raen-wallets.json load failed, using fallback:", e.message); }
  return { set: new Set(fallback), labels: {} };
}
const internalWallets = loadInternalWallets();
const isInternalPayer = (p) => Boolean(p) && internalWallets.set.has(String(p).toLowerCase());

app.get("/ledger.json", (_req, res) => {
  const settled = events.filter(e => e.event === "settled");
  const verified = settled.filter(e => Boolean(e.tx)); // TRUTH (Nov 11): only tx-bearing settles count as revenue
  const perService = {};
  const externalPayers = new Set();
  let totalExternal = 0, totalInternal = 0, internalCount = 0, unattributed = 0, totalUnattributed = 0, totalUnverified = 0;
  let lastUpdated = 0;
  const transactions = [];
  for (const e of verified) {
    const amount = e.price_usd || 0;
    if (e.ts > lastUpdated) lastUpdated = e.ts;
    const internal = isInternalPayer(e.payer);
    const attributed = Boolean(e.payer && e.tx);
    if (!attributed) unattributed += 1;
    if (internal) { totalInternal += amount; internalCount += 1; }
    else if (attributed) {
      totalExternal += amount;
      externalPayers.add(e.payer.toLowerCase());
    } else totalUnattributed += amount;
    const s = (perService[e.service] ||= { settled_tx_count: 0, settled_usdc: 0, settled_usdc_external: 0, settled_usdc_internal: 0 });
    s.settled_tx_count += 1;
    s.settled_usdc = Math.round((s.settled_usdc + amount) * 10000) / 10000;
    if (internal) s.settled_usdc_internal = Math.round((s.settled_usdc_internal + amount) * 10000) / 10000;
    else if (attributed) s.settled_usdc_external = Math.round((s.settled_usdc_external + amount) * 10000) / 10000;
    else s.settled_usdc_unattributed = Math.round(((s.settled_usdc_unattributed || 0) + amount) * 10000) / 10000;
    transactions.push({
      service: e.service, route: e.route, ts: e.ts,
      amount_usd: amount, payer: e.payer || null, tx: e.tx || null,
      classification: internal ? "internal" : (attributed ? "external" : "unattributed"),
      payer_label: internal
        ? (internalWallets.labels[String(e.payer).toLowerCase()] || "RAEN test")
        : (attributed ? "external" : "unattributed"),
    });
  }
  // unverified = settled events with no tx hash (telemetry-only, not on-chain money)
  let unverifiedCount = 0, unverifiedUsd = 0;
  for (const e of settled) {
    if (!e.tx) { unverifiedCount += 1; unverifiedUsd += e.price_usd || 0; }
  }
  const r4 = (n) => Math.round(n * 10000) / 10000;
  res.json({
    total_settled_usdc: r4(totalExternal + totalInternal + totalUnattributed),
    total_external_usdc: r4(totalExternal),
    total_internal_usdc: r4(totalInternal),
    total_unattributed_usdc: r4(totalUnattributed),
    total_unverified_usdc: r4(unverifiedUsd),
    settled_tx_count: verified.length,
    settled_tx_count_unverified: unverifiedCount,
    settled_tx_count_external: verified.length - internalCount - unattributed,
    settled_tx_count_internal: internalCount,
    settled_tx_count_unattributed: unattributed,
    note: "Counts include only settlements with an on-chain tx hash (TRUTH law, 2026-11-11).",
    distinct_payers: externalPayers.size,
    distinct_external_payers: externalPayers.size,
    per_service: perService,
    transactions: transactions.slice(-500).reverse(),
    last_updated: lastUpdated || null,
  });
});

let events = [];
// restore from disk if volume is mounted
try {
  if (fs.existsSync(DATA_FILE)) {
    events = fs.readFileSync(DATA_FILE, "utf8").split("\n").filter(Boolean)
      .slice(-MAX_EVENTS).map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean);
  }
} catch (e) { console.error("restore failed:", e.message); }

let flushTimer = null;
function persist(ev) {
  try {
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    fs.appendFileSync(DATA_FILE, JSON.stringify(ev) + "\n");
  } catch (e) { /* volume absent — memory-only, fine */ }
}

app.get("/health", (_req, res) => res.json({
  status: "ok", service: "settlement-collector", events: events.length,
}));

app.post("/events", (req, res) => {
  if (COLLECTOR_KEY && req.get("x-collector-key") !== COLLECTOR_KEY) {
    return res.status(401).json({ error: "bad collector key" });
  }
  const b = req.body || {};
  if (!b.service || !b.event) return res.status(400).json({ error: "service and event required" });
  const ev = {
    service: String(b.service).slice(0, 64),
    event: String(b.event).slice(0, 32),        // challenge | settled | failed | retry
    route: String(b.route || "").slice(0, 128),
    price_usd: Number(b.price_usd) || 0,
    status: Number(b.status) || 0,
    error_class: String(b.error_class || "").slice(0, 64),
    latency_ms: Number(b.latency_ms) || 0,
    tx: String(b.tx || "").slice(0, 80),
    payer: String(b.payer || "").slice(0, 64),
    ts: Date.now(),
  };
  events.push(ev);
  if (events.length > MAX_EVENTS) events = events.slice(-MAX_EVENTS);
  persist(ev);
  res.json({ ok: true });
});

// Aggregates: overall + per-service funnel and revenue estimate
app.get("/stats", (_req, res) => {
  const day = Date.now() - 86400000;
  function agg(list) {
    const byEvent = {};
    for (const e of list) byEvent[e.event] = (byEvent[e.event] || 0) + 1;
    const settled = byEvent.settled || 0;
    const challenges = byEvent.challenge || 0;
    const verified = list.filter(e => e.event === "settled" && e.tx); // TRUTH: revenue = tx-verified only
    const revenue = verified.reduce((s, e) => s + (e.price_usd || 0), 0);
    const unverifiedRevenue = list.filter(e => e.event === "settled" && !e.tx)
      .reduce((s, e) => s + (e.price_usd || 0), 0);
    const lat = list.filter(e => e.event === "settled" && e.latency_ms > 0)
      .map(e => e.latency_ms).sort((a, b) => a - b);
    return {
      challenges, settled, settled_verified: verified.length,
      settled_unverified: settled - verified.length,
      failed: byEvent.failed || 0, retries: byEvent.retry || 0,
      convert_pct: challenges + settled ? Math.round(100 * settled / (challenges + settled)) : null,
      revenue_usd: Math.round(revenue * 10000) / 10000,
      revenue_unverified_usd: Math.round(unverifiedRevenue * 10000) / 10000,
      p50_ms: lat.length ? lat[Math.floor(lat.length / 2)] : null,
      p95_ms: lat.length ? lat[Math.ceil(lat.length * 0.95) - 1] : null,
    };
  }
  const byService = {};
  for (const e of events) (byService[e.service] ||= { all: [], day: [] });
  for (const e of events) { byService[e.service].all.push(e); if (e.ts >= day) byService[e.service].day.push(e); }
  const out = {};
  for (const [svc, l] of Object.entries(byService)) out[svc] = { "24h": agg(l.day), all: agg(l.all) };
  res.json({ fleet_24h: agg(events.filter(e => e.ts >= day)), services: out });
});

// recent failures for debugging paid routes
app.get("/failures", (_req, res) => {
  res.json(events.filter(e => e.event === "failed" || (e.event === "challenge" && e.status >= 500)).slice(-100).reverse());
});
// ---- client value-ledger reports (retention weapon, REVENUE-STRATEGY-V2) ----
const valueConfig = loadConfig();

function wantHtml(req) {
  return req.query.format === "html" || (req.headers.accept || "").includes("text/html");
}

// GET /report?client=X&period=week  (default json; ?format=html for the human email page)
app.get("/report", (req, res) => {
  const client = String(req.query.client || "");
  const period = String(req.query.period || "week");
  const report = buildReport(events, valueConfig, client, period);
  if (!report) return res.status(404).json({ error: `unknown client '${client}'` });
  if (wantHtml(req)) return res.type("html").send(renderHtml(report));
  res.json(report);
});

// POST /report/send {email} — render the HTML report and email it via Resend
app.post("/report/send", async (req, res) => {
  const b = req.body || {};
  const client = String(b.client || "");
  const period = String(b.period || "week");
  const email = String(b.email || "").trim();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: "valid email required" });
  const report = buildReport(events, valueConfig, client, period);
  if (!report) return res.status(404).json({ error: `unknown client '${client}'` });
  try {
    const out = await sendViaResend(report, renderHtml(report), email, process.env.RESEND_API_KEY);
    res.json({ ok: true, sent_to: email, email_id: out.id || null });
  } catch (e) {
    console.error("report/send failed:", e.message);
    res.status(502).json({ error: e.message });
  }
});

// GET /report/cron?secret=... — cron-friendly: emails the weekly report to each client's recipients
app.get("/report/cron", async (req, res) => {
  if (!REPORT_CRON_SECRET || req.query.secret !== REPORT_CRON_SECRET) return res.status(401).json({ error: "bad secret" });
  const results = [];
  for (const [client, cfg] of Object.entries(valueConfig.clients || {})) {
    for (const email of cfg.email_recipients || []) {
      const report = buildReport(events, valueConfig, client, "week");
      try {
        const out = await sendViaResend(report, renderHtml(report), email, process.env.RESEND_API_KEY);
        results.push({ client, email, ok: true, email_id: out.id || null });
      } catch (e) {
        results.push({ client, email, ok: false, error: e.message.slice(0, 120) });
      }
    }
  }
  res.json({ ok: true, sent: results });
});

// ---- page-hit telemetry (ARC-48): one-line beacon endpoint ----
const HIT_LOG = path.join("/data", "hit-log.jsonl");
app.get("/hit", (req, res) => {
  try {
    const row = {
      ts: new Date().toISOString(),
      src: String(req.query.src || "unknown").slice(0, 80),
      ref: String(req.get("referer") || "").slice(0, 300),
      ua: String(req.get("user-agent") || "").slice(0, 200),
    };
    fs.appendFileSync(HIT_LOG, JSON.stringify(row) + "\n");
  } catch (_e) { /* never fail the beacon */ }
  res.set("Cache-Control", "no-store, max-age=0");
  res.set("Access-Control-Allow-Origin", "*");
  res.type("gif").send(Buffer.from("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7", "base64"));
});
app.get("/hits.json", (_req, res) => {
  try {
    const lines = fs.existsSync(HIT_LOG) ? fs.readFileSync(HIT_LOG, "utf8").trim().split("\n").filter(Boolean) : [];
    const rows = lines.map((l) => { try { return JSON.parse(l); } catch (_e) { return null; } }).filter(Boolean);
    const bySrc = {};
    for (const r of rows) bySrc[r.src] = (bySrc[r.src] || 0) + 1;
    res.json({ ok: true, total: rows.length, by_src: bySrc, last: rows.slice(-20).reverse() });
  } catch (e) { res.status(500).json({ ok: false, error: String(e) }); }
});
app.get("/events/recent", (_req, res) => res.json(events.slice(-200).reverse()));

// ---- THE TRANSPARENT COMPANY — live page + agent action feed (Bean, Nov 12) ----
app.get("/live", (_req, res) => res.sendFile(path.join(__dirname, "live.html")));

// Live-feed: append-only JSONL at /data/live-feed.jsonl. All fleet agents
// (steward/crown/closer/tiffany/franklin/bean) POST one-line events here —
// key-gated like /events. Boot-seeded from the bundled backfill so the
// public record starts with the real Nov 10-12 ops history.
const FEED_FILE = path.join("/data", "live-feed.jsonl").replace(/\//g, require("path").sep);
const FEED_BACKFILL_FILE = path.join(__dirname, "live-feed-backfill.jsonl");
const MAX_FEED = 10000;
let liveFeed = [];
function loadFeedFrom(file, quiet) {
  try {
    if (!fs.existsSync(file)) return 0;
    const lines = fs.readFileSync(file, "utf8").split("\n").filter(Boolean);
    let n = 0;
    for (const l of lines) {
      try { liveFeed.push(JSON.parse(l)); n++; } catch { /* skip bad line */ }
    }
    return n;
  } catch (e) { if (!quiet) console.error("live-feed load failed:", e.message); return 0; }
}
if (fs.existsSync(DATA_FILE)) { // /data volume mounted: restore, else seed from backfill
  if (fs.existsSync(FEED_FILE)) loadFeedFrom(FEED_FILE, true);
  if (!liveFeed.length) {
    const n = loadFeedFrom(FEED_BACKFILL_FILE);
    if (n) { try { fs.mkdirSync(path.dirname(FEED_FILE), { recursive: true }); fs.copyFileSync(FEED_BACKFILL_FILE, FEED_FILE); } catch (e) { console.error("feed seed copy failed:", e.message); } }
    console.log(`live-feed seeded with ${n} backfill events`);
  }
}
function feedPersist(ev) {
  try {
    fs.mkdirSync(path.dirname(FEED_FILE), { recursive: true });
    fs.appendFileSync(FEED_FILE, JSON.stringify(ev) + "\n");
  } catch (e) { /* volume absent - memory-only */ }
}
const FEED_AGENTS = new Set(["steward", "crown", "closer", "tiffany", "franklin", "bean", "staci", "hermes", "owner"]);

app.get("/live-feed.json", (req, res) => {
  const n = Math.min(Number(req.query.limit) || 50, 500);
  res.json({ total: liveFeed.length, events: liveFeed.slice(-n).reverse() });
});

app.post("/live-feed", (req, res) => {
  if (COLLECTOR_KEY && req.get("x-collector-key") !== COLLECTOR_KEY) {
    return res.status(401).json({ error: "bad collector key" });
  }
  const b = req.body || {};
  const agent = String(b.agent || "").toLowerCase().trim();
  const action = String(b.action || "").trim();
  if (!FEED_AGENTS.has(agent)) return res.status(400).json({ error: "agent must be one of " + [...FEED_AGENTS].join(",") });
  if (!action) return res.status(400).json({ error: "action required" });
  const ev = {
    agent,
    action: action.slice(0, 300),
    evidence_url: String(b.evidence_url || b.evidenceUrl || "").slice(0, 400),
    ts: b.ts ? new Date(b.ts).toISOString() : new Date().toISOString(),
  };
  liveFeed.push(ev);
  if (liveFeed.length > MAX_FEED) liveFeed = liveFeed.slice(-MAX_FEED);
  feedPersist(ev);
  res.json({ ok: true, total: liveFeed.length });
});

// Fleet health: server-side probe of all 13 live services, cached 30s.
const FLEET = [
  "royal-ruby-x402", "tradingagents-x402", "dispute-forge-x402", "nanobanana-x402",
  "nft-alpha-x402", "escrow-x402", "vault-pro-x402", "dispatch-x402",
  "contract-eye-x402", "briefsnap-x402", "sentry-forge-x402", "settlement-collector", "raen-mcp-gateway",
];
let fleetCache = { at: 0, data: null };
async function probeService(name) {
  const t0 = Date.now();
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 5000);
    const r = await fetch(`https://${name}.fly.dev/health`, { signal: ctl.signal });
    clearTimeout(timer);
    return { name, healthy: r.status === 200, latency_ms: Date.now() - t0 };
  } catch (e) {
    return { name, healthy: false, latency_ms: Date.now() - t0 };
  }
}
app.get("/fleet.json", async (_req, res) => {
  if (fleetCache.data && Date.now() - fleetCache.at < 30000) return res.json(fleetCache.data);
  const services = await Promise.all(FLEET.map(probeService));
  const data = { checked_at: new Date().toISOString(), green: services.filter(s => s.healthy).length, total: services.length, services };
  fleetCache = { at: Date.now(), data };
  res.json(data);
});

// ---- fully-autonomous USDC storefront (Bean, Nov 11) ----
wireStorefront(app, { isInternalPayer });

// Connector program: terms page + signup capture (JUANTERRY, Nov 2026).
// Signups append-only on the volume; page is public, capture is write-only.
const CONNECTOR_FILE = path.join("/data", "connector-signups.jsonl").replace(/\//g, path.sep);
app.get("/connector", (_req, res) => res.sendFile(path.join(__dirname, "connector.html")));
app.post("/connector-signup", (req, res) => {
  const { name, org, email, audience } = req.body || {};
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || !name) {
    return res.status(400).json({ ok: false, error: "name and valid email required" });
  }
  const rec = {
    ts: Date.now(),
    name: String(name).slice(0, 120),
    org: String(org || "").slice(0, 200),
    email: String(email).slice(0, 200),
    audience: String(audience || "").slice(0, 300),
  };
  try {
    fs.mkdirSync(path.dirname(CONNECTOR_FILE), { recursive: true });
    fs.appendFileSync(CONNECTOR_FILE, JSON.stringify(rec) + "\n");
  } catch (e) {
    return res.status(500).json({ ok: false, error: "could not persist signup" });
  }
  res.json({ ok: true, next: "connector terms + tracked link sent within one business day" });
});

app.listen(PORT, () => console.log(`settlement-collector on ${PORT}, ${events.length} events restored`));
