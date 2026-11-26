// chain-truth.js — authoritative settled amounts for every revenue surface.
// CROWN 2026-11-26 (backlog LEDGER-AMOUNT-CHAIN-DRIFT, P1).
//
// Telemetry reports the LIST price quoted at challenge time. The money truth is
// the ERC-20 Transfer value in the on-chain receipt (chain = money truth). Where
// a reconciliation proved the two diverge, a chain-verified correction overrides
// the telemetry figure everywhere revenue is published: /ledger.json, /stats,
// /certify settlement evidence, and client value-ledger reports.
//
// Seed evidence: FRANKLIN chain-truth reconciliation 2026-11-22
// (ops/franklin-work-20261122/reconcile-results.json), independently re-verified
// 2026-11-26 by CROWN via eth_getTransactionReceipt on base-rpc.publicnode.com:
// both contract-eye /api/analyze-contract receipts carried a 0.05 USDC Transfer
// to the canonical treasury while telemetry recorded $1.00.

const fs = require("fs");
const path = require("path");
const CORR_FILE = path.join("/data", "chain-corrections.json").replace(/\//g, path.sep);

const SEED = {
  "0xc1258329d3e99bc3f452a6a2df2a9bccd68f19c3f39514dc5dd827a19c759f63": {
    amount_usd: 0.05, block: 60293436,
    evidence: "receipt base-rpc.publicnode.com: 0.05 USDC Transfer to canonical treasury; telemetry said $1.00 (reconciled 2026-11-22, re-verified CROWN 2026-11-26)",
  },
  "0xfad918310b7af50355d6a7e5c752af85707b66e8f705d51d12fbaebcb5ec9f66": {
    amount_usd: 0.05, block: 60292235,
    evidence: "receipt base-rpc.publicnode.com: 0.05 USDC Transfer to canonical treasury; telemetry said $1.00 (reconciled 2026-11-22, re-verified CROWN 2026-11-26)",
  },
};

let corrections = { ...SEED };
try {
  if (fs.existsSync(CORR_FILE)) {
    const saved = JSON.parse(fs.readFileSync(CORR_FILE, "utf8"));
    if (saved && typeof saved === "object" && !Array.isArray(saved)) {
      corrections = { ...SEED, ...saved }; // seed cannot be silently overwritten away
    }
  }
} catch (e) { console.error("chain-truth load failed:", e.message); }

const norm = (tx) => String(tx || "").toLowerCase();

function chainCorrection(e) {
  if (!e || !e.tx) return null;
  return corrections[norm(e.tx)] || null;
}

// Every revenue aggregation must call this, never e.price_usd directly.
function settledAmount(e) {
  if (!e) return 0;
  const c = chainCorrection(e);
  return c && Number.isFinite(c.amount_usd) ? c.amount_usd : (Number(e.price_usd) || 0);
}

function listCorrections() {
  return JSON.parse(JSON.stringify(corrections));
}

// Key-gated ops write: record a receipt-verified amount for a tx without a deploy.
function setCorrection(tx, amount_usd, evidence) {
  const t = norm(tx);
  if (!/^0x[0-9a-f]{64}$/.test(t)) return { ok: false, error: "tx must be 0x + 64 hex chars" };
  const amt = Number(amount_usd);
  if (!Number.isFinite(amt) || amt < 0) return { ok: false, error: "amount_usd must be a non-negative number" };
  corrections[t] = {
    amount_usd: amt,
    evidence: String(evidence || "").slice(0, 300),
    at: Date.now(),
  };
  try {
    fs.mkdirSync(path.dirname(CORR_FILE), { recursive: true });
    fs.writeFileSync(CORR_FILE, JSON.stringify(corrections, null, 2));
  } catch (e) {
    console.error("chain-truth persist failed:", e.message);
  }
  return { ok: true, tx: t, amount_usd: amt };
}

module.exports = { settledAmount, chainCorrection, setCorrection, listCorrections };
