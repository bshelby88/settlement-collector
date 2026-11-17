// Minimal isolated runner for storefront claim-code testing (no external network dependencies)
const express = require("express");
const { wireStorefront } = require("./storefront");

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => res.json({ ok: true, status: "healthy" }));

const store = wireStorefront(app, { isInternalPayer: () => false });

const port = parseInt(process.env.PORT || "4788", 10);
const server = app.listen(port, "127.0.0.1", () => {
  console.log(`CLAIM_TEST_SERVER_READY:${port}`);
});

process.on("SIGTERM", () => server.close());
process.on("SIGINT", () => server.close());
