// certify.js — RAEN Compliance Feed (Bean, Nov 15 2026)
// Signed, machine-readable compliance certificates for the crawler economy.
// GET /certify/{service} (x402-paid, $0.01) returns a certificate built from
// REAL state only — TRUTH law: no inflated verdicts, no invented fields.
//
// Certificate fields, every one grounded in a live probe or the collector's
// own settlement records:
//   manifest    — sha256 of the service's LIVE /.well-known/x402.json body
//   quote       — the advertised price/network/payTo, validity-checked
//   health      — live probe of /health with latency
//   settlement  — last tx-verified settle event from the collector ledger
//   delivery    — tx-verified settled/200 counts from the collector ledger
//   claims      — fleet-wide open storefront claims (claims are not
//                 per-service attributable; the field is labeled as such)
//   verdict     — live | degraded | down, derived only from probe outcomes
// Signature: Ed25519 over the canonical JSON of the payload. Public key is
// published free at /certify/.well-known/pubkey.pem for offline verification.

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const CANONICAL_PAY_TO = "0x9b8a27e03ceba490953cda8ab1291c0e13baf72f";
const KEY_FILE = path.join(process.env.CERTIFY_KEY_FILE_DIR || "/data", "cert-signing-key.json");
const PROBE_TIMEOUT_MS = Number(process.env.CERTIFY_PROBE_TIMEOUT_MS || 6000);
const CACHE_TTL_MS = Number(process.env.CERTIFY_CACHE_TTL_MS || 30000);

// ---------- signing key (Ed25519, node crypto) ----------
function loadOrCreateSigningKey() {
  try {
    if (fs.existsSync(KEY_FILE)) {
      const k = JSON.parse(fs.readFileSync(KEY_FILE, "utf8"));
      if (k.private_key_pem && k.public_key_pem) return k;
    }
  } catch (e) { console.error("certify key restore failed:", e.message); }
  const { privateKey, publicKey } = crypto.generateKeyPairSync("ed25519");
  const out = {
    private_key_pem: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
    public_key_pem: publicKey.export({ type: "spki", format: "pem" }).toString(),
    created_at: new Date().toISOString(),
  };
  try {
    fs.mkdirSync(path.dirname(KEY_FILE), { recursive: true });
    fs.writeFileSync(KEY_FILE, JSON.stringify(out));
  } catch (e) { /* volume absent — memory-only, regenerated per boot */ }
  return out;
}

// stable JSON stringify (sorted keys) so signature verification is reproducible
function canonicalJson(value) {
  if (Array.isArray(value)) return "[" + value.map(canonicalJson).join(",") + "]";
  if (value && typeof value === "object") {
    return "{" + Object.keys(value).sort()
      .map((k) => JSON.stringify(k) + ":" + canonicalJson(value[k])).join(",") + "}";
  }
  return JSON.stringify(value);
}

// ---------- probes ----------
async function probe(url, init) {
  const t0 = Date.now();
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), PROBE_TIMEOUT_MS);
    const r = await fetch(url, { ...(init || {}), signal: ctl.signal });
    clearTimeout(timer);
    const body = await r.text();
    const headers = {};
    r.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });
    return { ok: r.status >= 200 && r.status < 300, status: r.status, body, headers, latency_ms: Date.now() - t0 };
  } catch (e) {
    return { ok: false, status: 0, body: "", latency_ms: Date.now() - t0, error: String(e.message || e).slice(0, 120) };
  }
}

function isHexAddress(s) { return /^0x[a-fA-F0-9]{40}$/.test(String(s || "")); }

async function probeManifestAndQuote(svc) {
  const r = await probe(`https://${svc}.fly.dev/.well-known/x402.json`);
  const manifest = { url: `https://${svc}.fly.dev/.well-known/x402.json`, fetched: r.ok, sha256: null };
  const quote = { advertised_price_usd: null, network: null, payTo: null, source: null, valid: false };
  if (r.ok) {
    manifest.sha256 = crypto.createHash("sha256").update(r.body).digest("hex");
    try {
      const j = JSON.parse(r.body);
      const eps = j.endpoints || {};
      const first = Object.values(eps)[0] || {};
      const a = first.accepts || {};
      quote.network = a.network || null;
      quote.payTo = a.payTo || null;
      const priceStr = String(a.price || "");
      if (priceStr) {
        const priceUsd = Number(priceStr.replace(/[^0-9.]/g, ""));
        quote.advertised_price_usd = Number.isFinite(priceUsd) ? priceUsd : null;
      }
    } catch (e) { manifest.fetched = false; manifest.error = "unparseable manifest"; }
  }

  // LIVE 402 quote probe — the authoritative quote is the challenge the service
  // actually returns, not what its manifest claims. v2 services may omit price
  // from the manifest entirely (royal-ruby does), so probe the paid route.
  if (manifest.fetched && quote.network) {
    try {
      const j2 = JSON.parse(r.body);
      const eps = j2.endpoints || {};
      const [route, cfg] = Object.entries(eps)[0] || [null, null];
      if (route) {
        const method = (cfg && cfg.method) || "GET";
        const probeUrl = `https://${svc}.fly.dev${route}`;
        const pr = await probe(probeUrl, method === "POST" ? { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } } : { method });
        if (pr.status === 402 && pr.body) {
          // v2 services return the challenge base64-encoded in the
          // PAYMENT-REQUIRED header (empty body); some return it in the body.
          const raw = pr.headers["payment-required"] || pr.body;
          let ch = null;
          try { ch = JSON.parse(Buffer.from(raw, "base64").toString("utf8")); } catch (e) { /* not a v2 challenge */ }
          const acc = ch && ch.accepts && ch.accepts[0];
          if (acc && acc.amount) {
            const usd = Number(BigInt(acc.amount)) / 1e6;
            quote.advertised_price_usd = usd;
            quote.network = acc.network || quote.network;
            quote.payTo = acc.payTo || quote.payTo;
            quote.source = "live-402-challenge";
          } else {
            quote.source = "402-no-challenge";
          }
        } else if (pr.status === 402 && quote.advertised_price_usd !== null) {
          quote.source = "manifest";
        }
      }
    } catch (e) { quote.source = quote.source || "probe-error"; }
  }

  const hasPrice = quote.advertised_price_usd !== null && quote.advertised_price_usd > 0;
  quote.valid = Boolean(
    hasPrice &&
    quote.network && /^eip155:\d+$/.test(quote.network) &&
    isHexAddress(quote.payTo)
  );
  return { manifest, quote };
}

// ---------- certificate builder ----------
function buildCertificate(service, events, storefrontStore, fleetNames) {
  const svcEvents = events.filter((e) => e.service === service);
  const settledVerified = svcEvents.filter((e) => e.event === "settled" && e.tx);
  const delivered = settledVerified.filter((e) => e.status === 200);
  const lastSettle = settledVerified.length
    ? settledVerified.reduce((a, b) => (b.ts > a.ts ? b : a))
    : null;

  const openClaims = storefrontStore && storefrontStore.state
    ? storefrontStore.state.orders.filter((o) =>
        (o.matched || o.claim_code) && !o.delivered && !o.credit_recorded)
    : [];

  return { service, svcEvents, settledVerified, delivered, lastSettle, openClaims, fleetNames };
}

async function assembleCertificate(parts, probeResults) {
  const { service, settledVerified, delivered, lastSettle, openClaims, fleetNames } = parts;
  const { health, manifestProbe } = probeResults;
  const r4 = (n) => Math.round(n * 10000) / 10000;

  let verdict = "down";
  if (health.ok && manifestProbe.manifest.fetched && manifestProbe.quote.valid) verdict = "live";
  else if (health.ok || manifestProbe.manifest.fetched) verdict = "degraded";

  const payload = {
    certificate: "raen-compliance-feed/v1",
    service,
    issuer: "settlement-collector (RAEN fleet settlement telemetry)",
    issued_at: new Date().toISOString(),
    verdict, // live | degraded | down — derived ONLY from probe outcomes
    health: {
      checked: true,
      healthy: health.ok,
      http_status: health.status,
      latency_ms: health.latency_ms,
    },
    manifest: manifestProbe.manifest,
    quote: manifestProbe.quote,
    settlement_evidence: lastSettle
      ? {
          tx: lastSettle.tx,
          payer: lastSettle.payer || null,
          amount_usd: r4(lastSettle.price_usd || 0),
          settled_at: new Date(lastSettle.ts).toISOString(),
          tx_verified: true,
        }
      : { tx: null, tx_verified: false, note: "no tx-verified settlement recorded for this service" },
    delivery_evidence: {
      settled_tx_count: settledVerified.length,
      delivered_200_count: delivered.length,
      note: "counts from the collector's own settlement ledger, tx-verified only (TRUTH law 2026-11-11)",
    },
    claims: {
      fleet_open_claim_count: openClaims.length,
      scope: "fleet-wide — storefront claims are not per-service attributable",
    },
    known_services: fleetNames,
  };
  return payload;
}

module.exports = {
  CANONICAL_PAY_TO, KEY_FILE, PROBE_TIMEOUT_MS, CACHE_TTL_MS,
  loadOrCreateSigningKey, canonicalJson, probe, probeManifestAndQuote,
  buildCertificate, assembleCertificate, isHexAddress,
};
