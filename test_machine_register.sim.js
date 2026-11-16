// Live test driver: machine (webhook) registration path.
// Spawns node index.js with PORT + scratch STOREFRONT_DATA_FILE, runs the
// assertion sequence, prints PASS/FAIL lines, always kills the server.
// Run: node test_machine_register.sim.js
const { spawn } = require("child_process");
const os = require("os");
const path = require("path");

const PORT = 4599;
const base = `http://127.0.0.1:${PORT}`;

const server = spawn("node", ["index.js"], {
  env: { ...process.env, PORT: String(PORT), STOREFRONT_DATA_FILE: path.join(os.tmpdir(), `sf_test_${Date.now()}.json`) },
  stdio: ["ignore", "pipe", "pipe"],
});
let serverLog = "";
server.stdout.on("data", d => serverLog += d);
server.stderr.on("data", d => serverLog += d);

(async () => {
  let up = false;
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500));
    try { await fetch(base + "/health"); up = true; break; } catch (_) {}
  }
  if (!up) { console.log("SERVER FAILED TO BOOT\n" + serverLog.slice(-800)); server.kill(); process.exit(1); }

  const j = async (r) => ({ status: r.status, body: await r.json().catch(() => null) });

  // 1. https callback_url required — http must be rejected
  let r = await j(await fetch(base + "/storefront/register", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tx_or_intent: "machtest-http-reject", callback_url: "http://127.0.0.1:4598/hook" }),
  }));
  console.log("http-callback rejected:", r.status === 400 ? "PASS" : `FAIL ${r.status} ${JSON.stringify(r.body)}`);

  // 2. machine registration with https receiver — unreachable host, so the T0
  // webhook send fails and stays unsent for cron retry (honest behavior);
  // assert the register response shape.
  r = await j(await fetch(base + "/storefront/register", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tx_or_intent: "machtest-intent-001", agent_id: "test-agent-7", callback_url: "https://example.invalid/hook" }),
  }));
  const okReg = r.status === 200 && r.body && r.body.ok === true && r.body.channel === "webhook"
    && r.body.catalog_url && r.body.claims_url && typeof r.body.delivery_pending === "boolean";
  console.log("machine register response:", okReg ? "PASS" : `FAIL ${r.status} ${JSON.stringify(r.body)}`);

  // 3. email path regression
  r = await j(await fetch(base + "/storefront/register", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tx_or_intent: "machtest-email-001", email: "human@example.com" }),
  }));
  console.log("email path regression:", r.status === 200 && r.body.ok ? "PASS" : `FAIL ${r.status} ${JSON.stringify(r.body)}`);

  // 4. no tx and no email/callback → 400
  r = await j(await fetch(base + "/storefront/register", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
  console.log("missing key rejected:", r.status === 400 ? "PASS" : `FAIL ${r.status}`);

  server.kill();
  setTimeout(() => process.exit(0), 300);
})().catch(e => { console.error("TEST ERROR", e); server.kill(); process.exit(1); });
