// report.js — client value-ledger report builder (STACI, Nov 11 2026)
// Repurposes collector telemetry into Phil's "value ledger": per-period rows of
// settled calls with an assigned $ value and estimated hours saved.
// Reads config from env VALUE_CONFIG, then ../ops/value-config.json, then ./value-config.json.
const fs = require("fs");
const path = require("path");

function loadConfig() {
  const candidates = [
    process.env.VALUE_CONFIG,
    path.join(__dirname, "value-config.json"),
    path.join(__dirname, "..", "ops", "value-config.json"),
  ].filter(Boolean);
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, "utf8"));
    } catch (e) { console.error("value-config load failed:", p, e.message); }
  }
  return { services: {}, clients: {}, defaults: { value_per_call: 1, hours_per_call: 0.25, task_type: "agent task" } };
}

const PERIODS = { day: 1, week: 7, month: 30 };

function svcCfg(config, service) {
  return { ...config.defaults, ...(config.services[service] || {}) };
}

function clientEvents(events, config, client) {
  const c = config.clients[client];
  if (!c) return null;
  const services = new Set(c.services || []);
  const payers = new Set((c.payers || []).map(p => String(p).toLowerCase()));
  return events.filter(e =>
    e.event === "settled" && (services.has(e.service) || (e.payer && payers.has(e.payer.toLowerCase())))
  );
}

// Build the report: rows grouped per day per service within the period window.
function buildReport(events, config, client, period = "week") {
  const days = PERIODS[period] || 7;
  const since = Date.now() - days * 86400000;
  const list = clientEvents(events, config, client);
  if (!list) return null;
  const rows = {};
  for (const e of list) {
    if (e.ts < since) continue;
    const day = new Date(e.ts).toISOString().slice(0, 10);
    const key = `${day}|${e.service}`;
    const r = (rows[key] ||= { date: day, service: e.service, calls: 0, settlements: 0, revenue_usd: 0, last_ts: 0 });
    r.calls += 1;
    r.settlements += 1;
    r.revenue_usd += e.price_usd || 0;
    if (e.ts > r.last_ts) r.last_ts = e.ts;
  }
  const out = Object.values(rows).map(r => {
    const cfg = svcCfg(config, r.service);
    return {
      ...r,
      revenue_usd: Math.round(r.revenue_usd * 10000) / 10000,
      last_ts: r.last_ts ? new Date(r.last_ts).toISOString() : null,
      task_type: cfg.task_type,
      assigned_value_usd: Math.round(r.settlements * cfg.value_per_call * 100) / 100,
      estimated_hours_saved: Math.round(r.settlements * cfg.hours_per_call * 100) / 100,
    };
  }).sort((a, b) => a.date < b.date ? -1 : a.date > b.date ? 1 : a.service < b.service ? -1 : 1);
  const totals = out.reduce((t, r) => ({
    calls: t.calls + r.calls,
    settlements: t.settlements + r.settlements,
    revenue_usd: Math.round((t.revenue_usd + r.revenue_usd) * 10000) / 10000,
    assigned_value_usd: Math.round((t.assigned_value_usd + r.assigned_value_usd) * 100) / 100,
    estimated_hours_saved: Math.round((t.estimated_hours_saved + r.estimated_hours_saved) * 100) / 100,
  }), { calls: 0, settlements: 0, revenue_usd: 0, assigned_value_usd: 0, estimated_hours_saved: 0 });
  return {
    client,
    client_name: config.clients[client].name || client,
    period,
    window_days: days,
    since: new Date(since).toISOString(),
    generated_at: new Date().toISOString(),
    rows: out,
    totals,
  };
}

function esc(s) { return String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

function renderHtml(report) {
  const rows = report.rows.map(r => `
    <tr><td>${esc(r.date)}</td><td>${esc(r.service)}</td><td>${esc(r.task_type)}</td>
    <td class="n">${r.calls}</td><td class="n">${r.settlements}</td><td class="n">$${r.revenue_usd.toFixed(2)}</td>
    <td class="n strong">$${r.assigned_value_usd.toFixed(2)}</td><td class="n">${r.estimated_hours_saved.toFixed(2)} h</td>
    <td class="ts">${esc(r.last_ts || "")}</td></tr>`).join("") ||
    `<tr><td colspan="9" class="empty">No settled activity in this period.</td></tr>`;
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Value Ledger — ${esc(report.client_name)} (${esc(report.period)})</title>
<style>
body{font-family:Georgia,serif;background:#faf8f4;color:#222;margin:0;padding:2rem}
.wrap{max-width:960px;margin:0 auto;background:#fff;border:1px solid #ddd;padding:2rem;border-radius:8px}
h1{font-size:1.5rem;margin:0 0 .25rem}h1 span{color:#8a1538}
.sub{color:#666;font-size:.9rem;margin-bottom:1.5rem}
table{width:100%;border-collapse:collapse;font-size:.85rem}
th{background:#8a1538;color:#fff;text-align:left;padding:.5rem;font-family:system-ui,sans-serif;font-size:.72rem;text-transform:uppercase;letter-spacing:.05em}
td{padding:.5rem;border-bottom:1px solid #eee}
td:first-child,td:nth-child(2),.ts{white-space:nowrap}
.n{text-align:right;font-family:system-ui,monospace}
.strong{font-weight:bold;color:#1a7a3a}
.ts{color:#999;font-size:.72rem}
.tot td{font-weight:bold;background:#f5efe9;border-top:2px solid #8a1538}
.empty{color:#888;text-align:center;padding:2rem}
.foot{margin-top:1.5rem;font-size:.75rem;color:#999;font-family:system-ui,sans-serif}
</style></head><body><div class="wrap">
<h1>Weekly <span>Value Ledger</span> — ${esc(report.client_name)}</h1>
<div class="sub">Period: ${esc(report.period)} (last ${report.window_days} days, since ${esc(report.since.slice(0, 10))}) · Generated ${esc(report.generated_at.slice(0, 16))}Z · From real settlement telemetry</div>
<table><thead><tr><th>Date</th><th>Service</th><th>Task type</th><th>Settled calls</th><th>Settlements</th><th>Paid (USDC)</th><th>Assigned value</th><th>Est. hours saved</th><th>Last activity</th></tr></thead>
<tbody>${rows}
<tr class="tot"><td colspan="3">TOTALS</td><td class="n">${report.totals.settlements}</td><td class="n">${report.totals.settlements}</td><td class="n">$${report.totals.revenue_usd.toFixed(2)}</td><td class="n strong">$${report.totals.assigned_value_usd.toFixed(2)}</td><td class="n">${report.totals.estimated_hours_saved.toFixed(2)} h</td><td></td></tr>
</tbody></table>
<div class="foot">Assigned value = per-service $-value per settled call (RAEN managed-agent rate card). Hours saved estimated from task-type benchmarks. RAEN — managed AI agent operations.</div>
</div></body></html>`;
}

// POST /report/send {email} — render + send via Resend (ops@royalruby.io)
async function sendViaResend(report, html, email, apiKey) {
  if (!apiKey) throw new Error("RESEND_API_KEY not configured");
  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from: "RAEN Value Ledger <ops@royalruby.io>",
      to: [email],
      subject: `Your Value Ledger — ${report.client_name} (${report.period} ending ${report.generated_at.slice(0, 10)})`,
      html,
    }),
  });
  if (!r.ok) throw new Error(`resend ${r.status}: ${(await r.text()).slice(0, 200)}`);
  return r.json();
}

module.exports = { loadConfig, buildReport, renderHtml, sendViaResend, PERIODS };
