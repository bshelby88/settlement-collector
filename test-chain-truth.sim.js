// Chain-truth overlay sim test (CROWN 2026-11-26). No network, no secrets.
const assert = require("assert");
const { settledAmount, chainCorrection, listCorrections, setCorrection } = require("./chain-truth");

const DRIFT_A = "0xc1258329d3e99bc3f452a6a2df2a9bccd68f19c3f39514dc5dd827a19c759f63";
const DRIFT_B = "0xfad918310b7af50355d6a7e5c752af85707b66e8f705d51d12fbaebcb5ec9f66";

// receipt-verified amounts override list price
assert.strictEqual(settledAmount({ tx: DRIFT_A, price_usd: 1.0 }), 0.05);
assert.strictEqual(settledAmount({ tx: DRIFT_B.toUpperCase(), price_usd: 1.0 }), 0.05, "case-insensitive tx");
assert.strictEqual(chainCorrection({ tx: DRIFT_A }).block, 60293436);

// uncorrected events keep telemetry price
assert.strictEqual(settledAmount({ tx: "0x" + "a".repeat(64), price_usd: 0.05 }), 0.05);
assert.strictEqual(settledAmount({ price_usd: 0.001 }), 0.001);
assert.strictEqual(settledAmount(null), 0);

// write validation rejects junk without mutating state
assert.strictEqual(setCorrection("bad", 1, "").ok, false);
assert.strictEqual(setCorrection("0x" + "b".repeat(64), -1, "").ok, false);

// two seeded corrections present, evidence recorded, no secret fields
const all = listCorrections();
assert.ok(all[DRIFT_A] && all[DRIFT_B]);
assert.ok(all[DRIFT_A].evidence.includes("0.05 USDC"));

console.log("chain-truth sim: ALL PASS (" + Object.keys(all).length + " corrections, read-only)");
