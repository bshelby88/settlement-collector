// settlement-collector — RAEN fleet paid-route telemetry (STACI, Nov 2026)
// Receives settlement-funnel events from every x402 service, keeps an
// in-memory + on-disk (volume) ring buffer, exposes aggregate funnel stats:
// challenges (402), settled (200-on-paid), failures, latency, revenue estimate.
// Auth: services send X-COLLECTOR-KEY (set via fly secrets). Read endpoints public.
const express = require("express");
const fs = require("fs");
const path = require("path");
const { loadConfig, buildReport, renderHtml, sendViaResend } = require("./report");

const PORT = process.env.PORT || 3000;
const COLLECTOR_KEY = process.env.COLLECTOR_KEY || "";
const REPORT_CRON_SECRET = process.env.REPORT_CRON_SECRET || "";
const DATA_FILE = path.join("/data", "events.jsonl").replace(/\//g, require("path").sep);
const MAX_EVENTS = 20000;

const app = express();
app.use(express.json({ limit: "256kb" }));

app.get("/status", (_req, res) => res.sendFile(path.join(__dirname, "status.html")));

app.get("/proof", (_req, res) => res.sendFile(path.join(__dirname, "proof.html")));

app.get("/ledger", (_req, res) => res.sendFile(path.join(__dirname, "ledger.html")));

// Public sales ledger (JSON) — straight from our own settlement records.
app.get("/ledger.json", (_req, res) => {
  const settled = events.filter(e => e.event === "settled");
  const perService = {};
  const payers = new Set();
  let total = 0;
  let lastUpdated = 0;
  for (const e of settled) {
    total += e.price_usd || 0;
    if (e.ts > lastUpdated) lastUpdated = e.ts;
    if (e.payer) payers.add(e.payer.toLowerCase());
    const s = (perService[e.service] ||= { settled_tx_count: 0, settled_usdc: 0 });
    s.settled_tx_count += 1;
    s.settled_usdc = Math.round((s.settled_usdc + (e.price_usd || 0)) * 10000) / 10000;
  }
  res.json({
    total_settled_usdc: Math.round(total * 10000) / 10000,
    settled_tx_count: settled.length,
    distinct_payers: payers.size,
    per_service: perService,
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
    const revenue = list.filter(e => e.event === "settled")
      .reduce((s, e) => s + (e.price_usd || 0), 0);
    const lat = list.filter(e => e.event === "settled" && e.latency_ms > 0)
      .map(e => e.latency_ms).sort((a, b) => a - b);
    return {
      challenges, settled, failed: byEvent.failed || 0, retries: byEvent.retry || 0,
      convert_pct: challenges + settled ? Math.round(100 * settled / (challenges + settled)) : null,
      revenue_usd: Math.round(revenue * 10000) / 10000,
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

app.get("/events/recent", (_req, res) => res.json(events.slice(-200).reverse()));

app.listen(PORT, () => console.log(`settlement-collector on ${PORT}, ${events.length} events restored`));
