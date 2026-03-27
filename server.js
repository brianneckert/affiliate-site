const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

const ARTICLES_PATH = path.join(
  __dirname,
  "../affiliate_os/runs/manual_tests/espresso_grinders"
);

function readJson(name) {
  const file = path.join(ARTICLES_PATH, name);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function renderHome(content, compliance) {
  const approved = compliance && compliance.passed === true;

  return `
  <!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Affiliate OS</title>
    <style>
      body {
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #eef2f7;
        color: #0f172a;
      }
      .wrap {
        max-width: 1100px;
        margin: 0 auto;
        padding: 48px 32px;
      }
      h1 {
        font-size: 56px;
        line-height: 1;
        margin: 0 0 12px;
        font-weight: 800;
      }
      .sub {
        font-size: 18px;
        color: #475569;
        margin-bottom: 32px;
      }
      .card {
        background: #fff;
        border: 1px solid #dbe2ea;
        border-radius: 24px;
        padding: 28px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
      }
      .eyebrow {
        font-size: 14px;
        font-weight: 700;
        letter-spacing: 0.12em;
        color: #2563eb;
        text-transform: uppercase;
        margin-bottom: 10px;
      }
      .title {
        font-size: 28px;
        font-weight: 800;
        margin: 0 0 14px;
      }
      .summary {
        font-size: 18px;
        line-height: 1.6;
        color: #334155;
        margin: 0 0 24px;
      }
      .btn {
        display: inline-block;
        background: #0f172a;
        color: #fff;
        text-decoration: none;
        padding: 14px 18px;
        border-radius: 14px;
        font-weight: 700;
      }
      .muted {
        color: #64748b;
      }
      .warn {
        background: #fff7ed;
        border: 1px solid #fdba74;
        color: #9a3412;
        padding: 16px;
        border-radius: 14px;
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <h1>Affiliate OS</h1>
      <div class="sub">Local approved-article frontend.</div>

      ${
        approved && content
          ? `
          <div class="card">
            <div class="eyebrow">Approved article</div>
            <div class="title">${content.title}</div>
            <p class="summary">${content.summary}</p>
            <a class="btn" href="/article/espresso-grinders">Read article</a>
          </div>
        `
          : `
          <div class="warn">
            No approved article is currently available for public display.
          </div>
        `
      }
    </div>
  </body>
  </html>
  `;
}

function renderArticle(content, compliance) {
  if (!content || !compliance || compliance.passed !== true) {
    return `
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <title>Article unavailable</title>
    </head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:40px;background:#eef2f7;">
      <a href="/" style="color:#2563eb;text-decoration:none;font-weight:700;">← Back</a>
      <h1>Article unavailable</h1>
      <p>This article is not approved for display.</p>
    </body>
    </html>
    `;
  }

  const rows = (content.comparison || [])
    .map(
      (p) => `
      <tr>
        <td>${p.name}</td>
        <td>${p.price_tier}</td>
        <td>${p.best_for}</td>
        <td>${p.total_score}</td>
        <td>${(p.notable_features || []).join(", ")}</td>
        <td><a class="shop-btn" href="#" onclick="return false;">Shop on Amazon</a></td>
      </tr>
    `
    )
    .join("");

  const who = (content.sections?.who_is_this_for || [])
    .map((x) => `<li><strong>${x.product}</strong>: ${x.best_for}</li>`)
    .join("");

  const guide = (content.sections?.buying_guide || [])
    .map((x) => `<li>${x}</li>`)
    .join("");

  return `
  <!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>${content.title}</title>
    <style>
      body {
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #eef2f7;
        color: #0f172a;
      }
      .wrap {
        max-width: 1250px;
        margin: 0 auto;
        padding: 28px 32px 60px;
      }
      .back {
        display: inline-block;
        margin-bottom: 22px;
        color: #2563eb;
        text-decoration: none;
        font-weight: 700;
        font-size: 18px;
      }
      .card {
        background: #fff;
        border: 1px solid #dbe2ea;
        border-radius: 28px;
        padding: 42px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
      }
      h1 {
        font-size: 64px;
        line-height: 1.02;
        margin: 0 0 24px;
        font-weight: 800;
      }
      .summary {
        font-size: 24px;
        line-height: 1.55;
        color: #334155;
        margin-bottom: 34px;
      }
      .top-pick {
        background: #f3f4f6;
        border: 1px solid #d1d5db;
        border-radius: 22px;
        padding: 22px 26px;
        margin-bottom: 32px;
      }
      .eyebrow {
        font-size: 14px;
        font-weight: 700;
        letter-spacing: 0.12em;
        color: #6b7280;
        text-transform: uppercase;
        margin-bottom: 10px;
      }
      .top-name {
        font-size: 34px;
        font-weight: 800;
      }
      h3 {
        font-size: 16px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #6b7280;
        margin: 28px 0 18px;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        background: #fff;
      }
      th {
        text-align: left;
        font-size: 14px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #4b5563;
        background: #f3f4f6;
        padding: 16px;
      }
      td {
        vertical-align: top;
        padding: 18px 16px;
        border-top: 1px solid #e5e7eb;
        font-size: 18px;
      }
      .shop-btn {
        display: inline-block;
        background: #0f172a;
        color: #fff;
        text-decoration: none;
        padding: 14px 18px;
        border-radius: 14px;
        font-weight: 700;
        white-space: nowrap;
      }
      ul {
        font-size: 20px;
        line-height: 1.6;
        color: #334155;
      }
      p.final {
        font-size: 22px;
        line-height: 1.6;
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <a class="back" href="/">← Back</a>

      <div class="card">
        <h1>${content.title}</h1>
        <div class="summary">${content.summary}</div>

        <div class="top-pick">
          <div class="eyebrow">Top pick</div>
          <div class="top-name">${content.top_pick}</div>
        </div>

        <h3>Comparison</h3>
        <table>
          <tr>
            <th>Product</th>
            <th>Price Tier</th>
            <th>Best For</th>
            <th>Score</th>
            <th>Notable Features</th>
            <th>Shop</th>
          </tr>
          ${rows}
        </table>

        <h3>Who is this for</h3>
        <ul>${who}</ul>

        <h3>Buying Guide</h3>
        <ul>${guide}</ul>

        <h3>Final Verdict</h3>
        <p class="final">${content.sections?.final_verdict || ""}</p>
      </div>
    </div>
  </body>
  </html>
  `;
}

app.get("/", (req, res) => {
  const content = readJson("contentproduction.json");
  const compliance = readJson("compliance.json");
  res.send(renderHome(content, compliance));
});

app.get("/article/espresso-grinders", (req, res) => {
  const content = readJson("contentproduction.json");
  const compliance = readJson("compliance.json");
  res.send(renderArticle(content, compliance));
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
