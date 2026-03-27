const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const ARTICLES_PATH = path.join(__dirname, 'data/articles');
const REGISTRY_PATH = path.join(ARTICLES_PATH, 'registry.json');
const ANALYTICS_PATH = path.join(__dirname, 'data/analytics/events.json');

function readJson(name) {
  const file = path.join(ARTICLES_PATH, name);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function readRegistry() {
  if (!fs.existsSync(REGISTRY_PATH)) return { articles: [] };
  return JSON.parse(fs.readFileSync(REGISTRY_PATH, 'utf8'));
}

function getPublishedArticles() {
  return (readRegistry().articles || []).filter((item) => item.publish_status === 'published');
}

function readArticleBundle(articleSlug) {
  const registry = readRegistry();
  const entry = (registry.articles || []).find((item) => item.article_slug === articleSlug);
  if (!entry) return null;
  const baseDir = path.join(__dirname, entry.article_dir);
  const readBundleJson = (name) => {
    const file = path.join(baseDir, name);
    if (!fs.existsSync(file)) return null;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  };
  return {
    entry,
    content: readBundleJson('contentproduction.json'),
    compliance: readBundleJson('compliance.json'),
    intelligence: readBundleJson('productintelligence.json')
  };
}

function ensureAnalyticsStore() {
  const dir = path.dirname(ANALYTICS_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(ANALYTICS_PATH)) fs.writeFileSync(ANALYTICS_PATH, '[]\n');
}

function readEvents() {
  ensureAnalyticsStore();
  return JSON.parse(fs.readFileSync(ANALYTICS_PATH, 'utf8'));
}

function writeEvents(events) {
  ensureAnalyticsStore();
  fs.writeFileSync(ANALYTICS_PATH, JSON.stringify(events, null, 2));
}

function logEvent(event) {
  const events = readEvents();
  events.push({ ...event, timestamp: event.timestamp || new Date().toISOString() });
  writeEvents(events);
}

function buildAnalyticsSummary(events) {
  const summary = { articles: {}, products: {}, categories: {} };
  for (const event of events) {
    const articleSlug = event.article_slug || 'unknown-article';
    if (!summary.articles[articleSlug]) {
      summary.articles[articleSlug] = { total_views: 0, total_clicks: 0, ctr: 0, category: event.category || null };
    }
    const category = event.category || summary.articles[articleSlug].category || 'unknown-category';
    summary.articles[articleSlug].category = category;
    if (!summary.categories[category]) {
      summary.categories[category] = { total_views: 0, total_clicks: 0, ctr: 0, articles: {} };
    }
    if (!summary.categories[category].articles[articleSlug]) {
      summary.categories[category].articles[articleSlug] = { total_views: 0, total_clicks: 0 };
    }
    if (event.type === 'article_view') {
      summary.articles[articleSlug].total_views += 1;
      summary.categories[category].total_views += 1;
      summary.categories[category].articles[articleSlug].total_views += 1;
    }
    if (event.type === 'outbound_click') {
      summary.articles[articleSlug].total_clicks += 1;
      summary.categories[category].total_clicks += 1;
      summary.categories[category].articles[articleSlug].total_clicks += 1;
      const productName = event.product_name || 'unknown-product';
      if (!summary.products[productName]) {
        summary.products[productName] = { article_slug: articleSlug, category, clicks: 0, asin: event.asin || null };
      }
      summary.products[productName].clicks += 1;
    }
  }

  for (const article of Object.values(summary.articles)) {
    article.ctr = article.total_views ? Number((article.total_clicks / article.total_views).toFixed(4)) : 0;
  }
  for (const category of Object.values(summary.categories)) {
    category.ctr = category.total_views ? Number((category.total_clicks / category.total_views).toFixed(4)) : 0;
  }

  return summary;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function hasValidAffiliateUrl(row) {
  return /^https?:\/\//i.test(String(row?.affiliate_url || ''));
}

function isDisplayableCompliance(compliance) {
  if (!compliance) return false;
  if (compliance.passed === true) return true;
  const errors = Array.isArray(compliance.errors) ? compliance.errors : [];
  return errors.length === 1 && errors[0] === 'no_external_urls';
}

function buildSearchIndex() {
  return getPublishedArticles().flatMap((entry) => {
    const bundle = readArticleBundle(entry.article_slug);
    const content = bundle?.content;
    const compliance = bundle?.compliance;
    if (!content || !isDisplayableCompliance(compliance)) return [];
    const comparison = Array.isArray(content.comparison) ? content.comparison : [];
    return [{
      route: `/article/${entry.article_slug}`,
      article_title: content.title || entry.title || entry.article_slug,
      summary: content.summary || '',
      top_pick: content.top_pick || '',
      category: entry.category || content.category || '',
      products: comparison.map(item => item.name),
      search_text: [
        content.title || '',
        content.summary || '',
        content.top_pick || '',
        entry.category || '',
        ...comparison.map(item => item.name || '')
      ].join(' ').toLowerCase()
    }];
  });
}

function renderHome() {
  const articleIndex = buildSearchIndex();
  const searchData = JSON.stringify(articleIndex);
  const publishedCount = articleIndex.length;
  return `<!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Affiliate Site</title>
    <style>
      :root {
        --bg1:#060b16;
        --bg2:#0b1220;
        --text:#f8fafc;
        --muted:#b9c4d6;
        --panel:rgba(255,255,255,.06);
        --panelBorder:rgba(255,255,255,.12);
        --shadow:0 20px 60px rgba(0,0,0,.35);
      }
      * { box-sizing:border-box; }
      body {
        margin:0;
        min-height:100vh;
        font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
        color:var(--text);
        background:
          radial-gradient(circle at 20% 20%, rgba(96,165,250,.18), transparent 28%),
          radial-gradient(circle at 80% 10%, rgba(94,234,212,.12), transparent 22%),
          linear-gradient(180deg, var(--bg1), var(--bg2));
      }
      .wrap { max-width:1180px; margin:0 auto; padding:56px 20px 80px; }
      .hero { padding:72px 28px 38px; text-align:center; }
      .eyebrow {
        display:inline-block;
        padding:8px 12px;
        border:1px solid rgba(255,255,255,.14);
        border-radius:999px;
        font-size:12px;
        letter-spacing:.1em;
        text-transform:uppercase;
        color:#dbeafe;
        background:rgba(255,255,255,.04);
        backdrop-filter:blur(12px);
        margin-bottom:18px;
      }
      h1 {
        margin:0 auto 14px;
        max-width:900px;
        font-size:clamp(40px,6vw,72px);
        line-height:1.02;
        letter-spacing:-.03em;
      }
      .sub {
        max-width:780px;
        margin:0 auto;
        color:var(--muted);
        font-size:clamp(16px,2vw,20px);
        line-height:1.7;
      }
      .search-shell {
        max-width:920px;
        margin:34px auto 0;
        background:linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.05));
        border:1px solid rgba(255,255,255,.14);
        border-radius:26px;
        padding:18px;
        box-shadow:var(--shadow);
        backdrop-filter:blur(20px);
      }
      .search-row {
        display:flex;
        align-items:center;
        gap:14px;
        background:rgba(6,11,22,.72);
        border:1px solid rgba(255,255,255,.1);
        border-radius:20px;
        padding:14px 18px;
      }
      .search-icon { font-size:22px; opacity:.9; }
      .search-input {
        width:100%;
        background:transparent;
        border:0;
        outline:0;
        color:var(--text);
        font-size:22px;
        font-weight:500;
      }
      .search-input::placeholder { color:#91a0b8; }
      .assist { margin:14px 6px 4px; color:#9fb0c8; font-size:14px; text-align:left; }
      .results { max-width:920px; margin:18px auto 0; display:grid; gap:14px; }
      .result {
        display:block;
        text-decoration:none;
        color:inherit;
        background:var(--panel);
        border:1px solid var(--panelBorder);
        border-radius:20px;
        padding:20px;
        box-shadow:var(--shadow);
        transition:transform .14s ease, border-color .14s ease, background .14s ease;
        backdrop-filter:blur(16px);
      }
      .result:hover {
        transform:translateY(-2px);
        border-color:rgba(94,234,212,.38);
        background:rgba(255,255,255,.09);
      }
      .result-title { margin:0 0 8px; font-size:24px; line-height:1.2; color:#ffffff; }
      .result-meta { font-size:14px; color:#9fdcf1; margin-bottom:10px; }
      .result-summary { margin:0 0 14px; color:#d4dce8; line-height:1.65; }
      .chips { display:flex; flex-wrap:wrap; gap:8px; }
      .chip {
        padding:8px 10px;
        border-radius:999px;
        background:rgba(255,255,255,.06);
        border:1px solid rgba(255,255,255,.12);
        color:#dbe7f7;
        font-size:13px;
      }
      .empty {
        max-width:920px;
        margin:18px auto 0;
        text-align:center;
        color:#9fb0c8;
        padding:28px;
        border:1px dashed rgba(255,255,255,.16);
        border-radius:18px;
        background:rgba(255,255,255,.03);
      }
      .footer-note { margin-top:26px; text-align:center; color:#7f91ac; font-size:13px; }
    </style>
  </head>
  <body>
    <main class="wrap">
      <section class="hero">
        <div class="eyebrow">Next-generation product research</div>
        <h1>Search products like Prime — filtered through a smarter research layer.</h1>
        <p class="sub">Find ranked buying guides, top picks, and comparison-driven recommendations from approved local article intelligence — faster, cleaner, and more focused than a raw marketplace search.</p>
        <div class="search-shell">
          <div class="search-row">
            <div class="search-icon">⌕</div>
            <input id="searchInput" class="search-input" type="text" placeholder="Search grinders, product names, top picks, buying guides..." autofocus>
          </div>
          <div class="assist">Published library: ${publishedCount} approved guides. Search across article titles, categories, top picks, and compared product names.</div>
        </div>
      </section>
      <section id="results" class="results"></section>
      <section id="empty" class="empty" style="display:none;">No matching approved article found. Try a product name, top pick, or category phrase.</section>
      <div class="footer-note">Local-only experience. Only compliance-approved article content is surfaced here.</div>
    </main>
    <script>
      const ARTICLE_INDEX = ${searchData};
      const input = document.getElementById('searchInput');
      const resultsEl = document.getElementById('results');
      const emptyEl = document.getElementById('empty');
      function renderResults(query) {
        const q = String(query || '').trim().toLowerCase();
        const matches = !q ? ARTICLE_INDEX : ARTICLE_INDEX.filter(item => item.search_text.includes(q));
        resultsEl.innerHTML = matches.map(function(item) {
          const chips = (item.products || []).map(function(name) {
            return '<span class="chip">' + name + '</span>';
          }).join('');
          return '<a class="result" href="' + item.route + '">' +
            '<h2 class="result-title">' + item.article_title + '</h2>' +
            '<div class="result-meta">Category: ' + (item.category || '—') + ' · Top pick: ' + (item.top_pick || '—') + '</div>' +
            '<p class="result-summary">' + (item.summary || '') + '</p>' +
            '<div class="chips">' + chips + '</div>' +
            '</a>';
        }).join('');
        emptyEl.style.display = matches.length ? 'none' : 'block';
      }
      input.addEventListener('input', function(e) { renderResults(e.target.value); });
      renderResults('');
    </script>
  </body>
  </html>`;
}

function renderArticle(content, compliance, entry = null) {
  if (!content || !isDisplayableCompliance(compliance)) {
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

  const productEntityMap = new Map((content.product_entities || []).map((item) => [item.product_name, item]));
  const relatedGuides = getPublishedArticles()
    .filter((article) => article.category === (entry?.category || content.category) && article.article_slug !== (entry?.article_slug || content.article_slug))
    .map((article) => `<a href="/article/${escapeHtml(article.article_slug)}">${escapeHtml(article.title)}</a>`)
    .join(' · ');

  const rows = (content.comparison || [])
    .filter(hasValidAffiliateUrl)
    .map(
      (p) => `
      <tr>
        <td>${escapeHtml(p.name)}</td>
        <td>${escapeHtml(p.price_tier)}</td>
        <td>${escapeHtml(p.best_for)}</td>
        <td>${escapeHtml(p.total_score)}</td>
        <td>${escapeHtml((p.notable_features || []).join(', '))}</td>
        <td><a class="shop-btn analytics-link" data-article-slug="${escapeHtml(content.article_slug || 'configured-article')}" data-category="${escapeHtml(content.category || 'configured category')}" data-product-name="${escapeHtml(p.name)}" data-asin="${escapeHtml(p.asin)}" data-affiliate-url="${escapeHtml(p.affiliate_url)}" href="${escapeHtml(p.affiliate_url)}" target="_blank" rel="noopener noreferrer">Shop on Amazon</a></td>
      </tr>
    `
    )
    .join('');

  const glance = (content.top_picks_at_a_glance || [])
    .map((item) => `
      <div class="mini-card">
        <div class="mini-title">${escapeHtml(item.product_name)}</div>
        <div><strong>Best for:</strong> ${escapeHtml(item.best_for)}</div>
        <div><strong>Price tier:</strong> ${escapeHtml(item.pricing_tier)}</div>
        <div><strong>Rating:</strong> ${escapeHtml(item.rating)} (${escapeHtml(item.review_count)} reviews)</div>
        <div style="margin-top:10px;"><a class="shop-btn analytics-link" data-article-slug="${escapeHtml(content.article_slug || 'configured-article')}" data-category="${escapeHtml(content.category || 'configured category')}" data-product-name="${escapeHtml(item.product_name)}" data-asin="${escapeHtml(productEntityMap.get(item.product_name)?.asin || '')}" data-affiliate-url="${escapeHtml(item.canonical_product_url)}" href="${escapeHtml(item.canonical_product_url)}" target="_blank" rel="noopener noreferrer">Shop on Amazon</a></div>
      </div>
    `)
    .join('');

  const productSections = (content.sections?.product_sections || content.product_entities || [])
    .map((item) => `
      <div class="product-card">
        <h4>${escapeHtml(item.product_name)}</h4>
        <p><strong>Best for:</strong> ${escapeHtml(item.best_for)}</p>
        <p><strong>Price position:</strong> ${escapeHtml(item.price_position)}</p>
        <p><strong>Rating:</strong> ${escapeHtml(item.rating)} (${escapeHtml(item.review_count)} reviews)</p>
        <p><strong>Prime eligible:</strong> ${escapeHtml(item.prime_eligible)}</p>
        <p><strong>ASIN:</strong> ${escapeHtml(item.asin)}</p>
        <p><strong>Category:</strong> ${escapeHtml(item.category)}</p>
        <p><strong>Summary:</strong> ${escapeHtml(item.short_factual_description)}</p>
        <p><strong>Key strengths:</strong> ${escapeHtml((item.key_strengths || []).join(', '))}</p>
        <p><strong>Drawbacks:</strong> ${escapeHtml((item.drawbacks || []).join(', '))}</p>
        <p><strong>Canonical product URL:</strong> <a class="analytics-link" data-article-slug="${escapeHtml(content.article_slug || 'configured-article')}" data-category="${escapeHtml(content.category || 'configured category')}" data-product-name="${escapeHtml(item.product_name)}" data-asin="${escapeHtml(item.asin)}" data-affiliate-url="${escapeHtml(item.canonical_product_url)}" href="${escapeHtml(item.canonical_product_url)}" target="_blank" rel="noopener noreferrer">View on Amazon</a></p>
      </div>
    `)
    .join('');

  const faq = (content.sections?.faq || [])
    .map((item) => `
      <div class="faq-item">
        <p><strong>${escapeHtml(item.question)}</strong></p>
        <p>${escapeHtml(item.answer)}</p>
      </div>
    `)
    .join('');

  const who = (content.sections?.who_is_this_for || [])
    .map((x) => `<li><strong>${escapeHtml(x.product)}</strong>: ${escapeHtml(x.best_for)}</li>`)
    .join('');

  const guide = (content.sections?.buying_guide || [])
    .map((x) => `<li>${escapeHtml(x)}</li>`)
    .join('');

  return `
  <!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>${escapeHtml(content.title)}</title>
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
      .glance-grid,
      .product-grid {
        display: grid;
        gap: 16px;
      }
      .glance-grid {
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        margin-bottom: 28px;
      }
      .product-grid {
        grid-template-columns: 1fr;
      }
      .mini-card,
      .product-card,
      .faq-item {
        background: #f8fafc;
        border: 1px solid #dbe2ea;
        border-radius: 18px;
        padding: 18px;
      }
      .mini-title,
      .product-card h4 {
        margin: 0 0 10px;
        font-size: 22px;
        font-weight: 800;
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
        <h1>${escapeHtml(content.title)}</h1>
        <div class="summary">${escapeHtml(content.summary)}</div>

        <div class="top-pick">
          <div class="eyebrow">Top pick</div>
          <div class="top-name">${escapeHtml(content.top_pick)}</div>
        </div>

        ${relatedGuides ? `<h3>Related Guides</h3><p>${relatedGuides}</p>` : ''}

        ${glance ? `<h3>Top Picks at a Glance</h3><div class="glance-grid">${glance}</div>` : ''}

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

        ${productSections ? `<h3>Product Details</h3><div class="product-grid">${productSections}</div>` : ''}

        <h3>Buying Guide</h3>
        <ul>${guide}</ul>

        ${faq ? `<h3>FAQ</h3><div class="product-grid">${faq}</div>` : ''}

        ${relatedGuides ? `<h3>More Air Purifier Guides</h3><p>${relatedGuides}</p>` : ''}

        <h3>Final Verdict</h3>
        <p class="final">${escapeHtml(content.sections?.final_verdict || '')}</p>
      </div>
    </div>
    <script>
      document.querySelectorAll('.analytics-link').forEach(function(link) {
        link.addEventListener('click', function() {
          const payload = {
            type: 'outbound_click',
            article_slug: link.dataset.articleSlug,
            category: link.dataset.category,
            product_name: link.dataset.productName,
            asin: link.dataset.asin,
            affiliate_url: link.dataset.affiliateUrl,
            timestamp: new Date().toISOString()
          };
          fetch('/analytics/click', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            keepalive: true
          }).catch(function() {});
        });
      });
    </script>
  </body>
  </html>
  `;
}

app.get('/', (req, res) => {
  res.send(renderHome());
});

app.get('/article/:slug', (req, res) => {
  const bundle = readArticleBundle(req.params.slug);
  if (!bundle || bundle.entry?.publish_status !== 'published') {
    return res.status(404).send(renderArticle(null, null));
  }
  const content = bundle.content;
  const compliance = bundle.compliance;
  if (content && isDisplayableCompliance(compliance)) {
    logEvent({
      type: 'article_view',
      article_slug: content.article_slug || req.params.slug,
      category: content.category || bundle?.entry?.category || 'configured category',
      timestamp: new Date().toISOString()
    });
  }
  res.send(renderArticle(content, compliance, bundle.entry));
});

app.post('/analytics/click', (req, res) => {
  const { article_slug, category, product_name, asin, affiliate_url, timestamp } = req.body || {};
  if (!article_slug || !product_name || !affiliate_url) {
    return res.status(400).json({ ok: false, error: 'missing_required_fields' });
  }
  logEvent({
    type: 'outbound_click',
    article_slug,
    category: category || 'configured category',
    product_name,
    asin: asin || null,
    affiliate_url,
    timestamp: timestamp || new Date().toISOString()
  });
  res.json({ ok: true });
});

app.get('/analytics/events', (req, res) => {
  res.json(readEvents());
});

app.get('/analytics/summary', (req, res) => {
  res.json(buildAnalyticsSummary(readEvents()));
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
