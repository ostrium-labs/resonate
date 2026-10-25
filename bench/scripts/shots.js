// SigNoz screenshots for the results README (playwright-core 1.63.0, its Chromium).
//   node bench/scripts/shots.js <out-dir> <spec.json>
// spec.json: [{"name": "file-stem", "path": "/trace/<id>" | "/services" | ..., "wait": ms}]
// SIGNOZ_URL (default http://127.0.0.1:13301), SIGNOZ_EMAIL, SIGNOZ_PASSWORD_FILE.
const fs = require("fs");
const path = require("path");
const { chromium } = require(process.env.PLAYWRIGHT_CORE || "playwright-core");

(async () => {
  const [out, specFile] = process.argv.slice(2);
  const spec = JSON.parse(fs.readFileSync(specFile, "utf8"));
  const base = process.env.SIGNOZ_URL || "http://127.0.0.1:13301";
  const email = process.env.SIGNOZ_EMAIL || "rbench@localhost.local";
  const password = fs.readFileSync(process.env.SIGNOZ_PASSWORD_FILE, "utf8").trim();
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 }, colorScheme: "light" });
  async function login() {
    await page.goto(`${base}/login`, { waitUntil: "networkidle" });
    await page.fill('input[type="email"], input#email, input[name="email"]', email);
    const next = page.getByRole("button", { name: /next|continue/i });
    if (await next.count()) await next.first().click();
    await page.waitForSelector('input[type="password"]', { timeout: 30000 });
    await page.fill('input[type="password"]', password);
    await page.getByRole("button", { name: /login|sign in|log in/i }).first().click();
    await page.waitForURL((u) => !u.toString().includes("/login"), { timeout: 60000 });
    await page.waitForLoadState("networkidle");
  }
  await login();
  for (const s of spec) {
    await page.setViewportSize(s.viewport || { width: 1600, height: 1000 });
    await page.goto(`${base}${s.path}`, { waitUntil: "networkidle" });
    if (page.url().includes("/login")) {
      await login();
      await page.goto(`${base}${s.path}`, { waitUntil: "networkidle" });
    }
    await page.waitForTimeout(s.wait || 4000);
    const file = path.join(out, `${s.name}.png`);
    await page.screenshot({ path: file, fullPage: false });
    console.log(file);
  }
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
