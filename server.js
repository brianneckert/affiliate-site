const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const createAnalytics = require('./analytics');
const createPaidRequests = require('./paid_requests');
const Stripe = require('stripe');

const app = express();
app.set('trust proxy', true);
const PORT = process.env.PORT || 3000;
const SITE_BASE_URL = (process.env.SITE_BASE_URL || 'https://www.bestofprime.online').replace(/\/$/, '');
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const BUNDLE_20_PRICE_IN_CENTS = Number(process.env.BUNDLE_20_PRICE_IN_CENTS || 1499);
const BUNDLE_100_PRICE_IN_CENTS = Number(process.env.BUNDLE_100_PRICE_IN_CENTS || 5900);
const ARTICLE_BUNDLES = {
  article_bundle_20: { code: 'article_bundle_20', credits: 20, unit_amount: BUNDLE_20_PRICE_IN_CENTS, label: '20 comparison articles', description: '20 comparison articles for $14.99 (about $0.75 each)' },
  article_bundle_100: { code: 'article_bundle_100', credits: 100, unit_amount: BUNDLE_100_PRICE_IN_CENTS, label: '100 comparison articles', description: '100 comparison articles for $59.00 (about $0.59 each)' }
};
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf; } }));

const ARTICLES_PATH = path.join(__dirname, 'data/articles');
const REGISTRY_PATH = path.join(ARTICLES_PATH, 'registry.json');
const analytics = createAnalytics({ rootDir: __dirname, registryPath: REGISTRY_PATH });
const paidRequests = createPaidRequests({ rootDir: __dirname });
const { execFile } = require('child_process');

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

function readEvents() {
  return analytics.readEvents();
}

function readSummary() {
  return analytics.readSummary();
}

function logEvent(event) {
  analytics.appendEvent(event);
}

function buildAnalyticsSummary(events) {
  return analytics.summarize(events);
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

function getSiteBaseUrl(req) {
  if (SITE_BASE_URL) return SITE_BASE_URL;
  return `${req.protocol}://${req.get('host')}`.replace(/\/$/, '');
}

function buildAbsoluteUrl(req, route = '/') {
  const base = getSiteBaseUrl(req);
  const normalizedRoute = route.startsWith('/') ? route : `/${route}`;
  return `${base}${normalizedRoute}`;
}

function slugifyQuery(value) {
  return String(value || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function getSocialImageUrl(req) {
  return buildAbsoluteUrl(req, '/social-share.jpg?v=20260330a');
}

function renderSocialMeta(meta) {
  return `
    <meta property="og:type" content="website" />
    <meta property="og:title" content="${escapeHtml(meta.title)}" />
    <meta property="og:description" content="${escapeHtml(meta.description)}" />
    <meta property="og:url" content="${escapeHtml(meta.canonicalUrl)}" />
    <meta property="og:image" content="${escapeHtml(meta.imageUrl)}" />
    <meta property="og:image:secure_url" content="${escapeHtml(meta.imageUrl)}" />
    <meta property="og:image:type" content="image/jpeg" />
    <meta property="og:image:width" content="1264" />
    <meta property="og:image:height" content="944" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="${escapeHtml(meta.title)}" />
    <meta name="twitter:description" content="${escapeHtml(meta.description)}" />
    <meta name="twitter:image" content="${escapeHtml(meta.imageUrl)}" />`;
}

function renderFaviconMarkup() {
  return '<link rel="icon" type="image/svg+xml" href="/favicon.svg" />';
}

function renderFaviconSvg() {
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <rect width="64" height="64" rx="16" fill="#2563EB"/>
  <rect x="16" y="12" width="6.5" height="40" rx="3.25" fill="#FF9900"/>
  <circle cx="35" cy="32" r="13.5" fill="none" stroke="#F8FAFC" stroke-width="5.5"/>
  <path d="M43 18l2.6 2.7-11.6 17.5-5.4-5.3 2.8-2.8 2.5 2.4z" fill="#FF9900"/>
</svg>`;
}


function renderInstantAnswerStatusPage(title, message) {
  return `<!doctype html><html><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width,initial-scale=1" /><title>${escapeHtml(title)}</title>${renderFaviconMarkup()}</head><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:40px;background:#eef2f7;color:#0f172a;"><a href="/" style="color:#2563eb;text-decoration:none;font-weight:700;">← Back</a><h1>${escapeHtml(title)}</h1><p>${escapeHtml(message)}</p></body></html>`;
}


function getClientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',').map((part) => part.trim()).filter(Boolean);
  const raw = forwarded[0] || req.headers['cf-connecting-ip'] || req.headers['x-real-ip'] || req.ip || req.socket?.remoteAddress || '';
  return String(raw || '').replace(/^::ffff:/, '').trim();
}

function getRequestCountry(req) {
  const country = String(req.headers['cf-ipcountry'] || req.headers['x-vercel-ip-country'] || req.headers['x-country-code'] || '').trim().toUpperCase();
  return country || null;
}

function hashIp(ip) {
  if (!ip) return null;
  return crypto.createHash('sha256').update(String(ip)).digest('hex');
}

function buildInstantAnswerAccess(req) {
  const ip = getClientIp(req);
  const ipHash = hashIp(ip);
  const country = getRequestCountry(req);
  const userKey = paidRequests.getUserKey({ ip_hash: ipHash });
  const userRecord = userKey ? paidRequests.getUserRecord(userKey, { ip_hash: ipHash, country }) : null;
  const freeArticlesUsed = Number(userRecord?.free_articles_used || 0);
  const paidBalance = Number(userRecord?.articles_remaining_balance || 0);
  const isUs = country === 'US';
  const freeRemaining = isUs ? Math.max(0, 3 - freeArticlesUsed) : 0;
  const hasFreeAccess = isUs && freeArticlesUsed < 3;
  const hasPaidBalance = paidBalance > 0;
  const canGenerate = hasFreeAccess || hasPaidBalance;
  const accessMode = hasFreeAccess ? 'free' : (hasPaidBalance ? 'bundle' : 'paid');

  return {
    ip,
    ipHash,
    userKey,
    country,
    isUs,
    userRecord,
    successfulGenerations: paidRequests.countSuccessfulGenerationsByIpHash(ipHash),
    freeArticlesUsed,
    paidBalance,
    freeRemaining,
    canGenerate,
    hasFreeAccess,
    hasPaidBalance,
    accessMode
  };
}

function kickOffInstantAnswerProcessing(requestId) {
  if (!requestId) return;
  execFile('node', [path.join(__dirname, 'scripts', 'process_paid_instant_answers.js'), '--request-id', requestId], { cwd: __dirname }, () => {});
}

function renderInstantAnswerSuccessPage(requestId) {
  return `<!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Building your Instant Answer</title>
    ${renderFaviconMarkup()}
    <style>
      :root {
        --bg1:#060b16;
        --bg2:#0b1220;
        --text:#f8fafc;
        --muted:#c8d3e6;
        --panel:rgba(255,255,255,.07);
        --panelBorder:rgba(255,255,255,.12);
        --green1:#22c55e;
        --green2:#16a34a;
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
      .wrap { max-width:860px; margin:0 auto; padding:56px 20px 80px; }
      .back { color:#93c5fd; text-decoration:none; font-weight:700; }
      .card {
        margin-top:18px;
        background:var(--panel);
        border:1px solid var(--panelBorder);
        border-radius:28px;
        padding:30px;
        backdrop-filter:blur(18px);
        box-shadow:0 20px 60px rgba(0,0,0,.35);
      }
      h1 { margin:0 0 10px; font-size:42px; line-height:1.05; }
      .sub { color:var(--muted); font-size:18px; line-height:1.7; }
      .bar-shell { margin:24px 0 18px; width:100%; height:16px; border-radius:999px; background:rgba(255,255,255,.10); overflow:hidden; }
      .bar { height:100%; width:8%; border-radius:999px; background:linear-gradient(90deg,var(--green1),var(--green2)); transition:width .35s ease; }
      .status-badge {
        display:inline-block; margin-top:12px; padding:8px 12px; border-radius:999px;
        background:rgba(34,197,94,.12); color:#bbf7d0; border:1px solid rgba(34,197,94,.24); font-size:13px; font-weight:700;
      }
      .steps { margin:22px 0 0; display:grid; gap:12px; }
      .step {
        display:flex; gap:12px; align-items:flex-start; padding:14px 16px; border-radius:18px;
        border:1px solid rgba(255,255,255,.10); background:rgba(255,255,255,.03);
      }
      .dot { width:12px; height:12px; margin-top:5px; border-radius:50%; background:rgba(255,255,255,.24); flex:0 0 12px; }
      .step.done .dot, .step.active .dot { background:#22c55e; box-shadow:0 0 0 6px rgba(34,197,94,.14); }
      .step-title { font-weight:800; margin-bottom:4px; }
      .step-copy { color:var(--muted); line-height:1.55; }
      .cta { margin-top:24px; display:none; }
      .cta a {
        display:inline-block; background:linear-gradient(180deg,var(--green1),var(--green2)); color:white; text-decoration:none;
        padding:16px 22px; border-radius:16px; font-weight:800; font-size:20px;
      }
      .small { margin-top:14px; color:#93a5c3; font-size:14px; }
    </style>
  </head>
  <body>
    <main class="wrap">
      <a class="back" href="/">← Back</a>
      <section class="card">
        <h1 id="title">Payment received. Building your Instant Answer now.</h1>
        <div id="sub" class="sub">We’re gathering data, comparing the top 5 Amazon options, and selecting a clear winner for you.</div>
        <div class="status-badge" id="badge">Working on it</div>
        <div class="bar-shell"><div class="bar" id="bar"></div></div>
        <div class="steps">
          <div class="step active" id="step-payment"><div class="dot"></div><div><div class="step-title">Payment confirmed</div><div class="step-copy">Your request is recorded and queued.</div></div></div>
          <div class="step" id="step-data"><div class="dot"></div><div><div class="step-title">Gathering Amazon product data</div><div class="step-copy">Finding the top 5 best matches for your search.</div></div></div>
          <div class="step" id="step-compare"><div class="dot"></div><div><div class="step-title">Comparing and selecting a winner</div><div class="step-copy">Ranking the strongest options and choosing a clear top pick.</div></div></div>
          <div class="step" id="step-publish"><div class="dot"></div><div><div class="step-title">Publishing your article</div><div class="step-copy">Preparing the final article page and redirecting you automatically.</div></div></div>
        </div>
        <div class="cta" id="cta"><a id="articleLink" href="#">Open your article</a></div>
        <div class="small" id="small">Request ID: ${escapeHtml(requestId || '')}</div>
      </section>
    </main>
    <script>
      const requestId = ${JSON.stringify(requestId || '')};
      const sessionId = new URLSearchParams(window.location.search).get('session_id') || '';
      const titleEl = document.getElementById('title');
      const subEl = document.getElementById('sub');
      const badgeEl = document.getElementById('badge');
      const barEl = document.getElementById('bar');
      const ctaEl = document.getElementById('cta');
      const articleLinkEl = document.getElementById('articleLink');
      const steps = {
        payment: document.getElementById('step-payment'),
        data: document.getElementById('step-data'),
        compare: document.getElementById('step-compare'),
        publish: document.getElementById('step-publish')
      };

      function setStepState(activeKey, doneKeys, progress, badge, sub) {
        Object.entries(steps).forEach(([key, el]) => {
          el.classList.remove('active', 'done');
          if (doneKeys.includes(key)) el.classList.add('done');
          if (key === activeKey) el.classList.add('active');
        });
        barEl.style.width = progress + '%';
        badgeEl.textContent = badge;
        if (sub) subEl.textContent = sub;
      }

      function applyRequestState(req) {
        const status = req?.request_status || '';
        const payment = req?.payment_status || '';
        const targetUrl = req?.published_url || (req?.published_slug ? '/article/' + req.published_slug : null);

        if (targetUrl && (status === 'published' || req?.publish_status === 'published')) {
          setStepState('publish', ['payment','data','compare','publish'], 100, 'Article ready', 'Your Instant Answer is ready. Redirecting you now...');
          titleEl.textContent = 'Your Instant Answer is ready.';
          ctaEl.style.display = 'block';
          articleLinkEl.href = targetUrl;
          setTimeout(() => { window.location.href = targetUrl; }, 1200);
          return true;
        }

        if (payment === 'paid' && (status === 'generating' || status === 'paid_pending')) {
          setStepState('data', ['payment'], 45, 'Gathering data', 'We are collecting the strongest Amazon matches for your query.');
        } else if (payment === 'paid' && status === 'validated') {
          setStepState('compare', ['payment','data'], 72, 'Comparing options', 'We have the candidates and are selecting the clearest winner now.');
        } else if (payment === 'paid') {
          setStepState('publish', ['payment','data','compare'], 88, 'Publishing article', 'We are finalizing and publishing your comparison page.');
        } else {
          setStepState('payment', [], 16, 'Waiting for payment confirmation', 'Your payment completed. We are waiting for the payment confirmation to finish syncing.');
        }
        return false;
      }

      async function poll() {
        if (!requestId) return;
        try {
          const qs = sessionId ? ('?session_id=' + encodeURIComponent(sessionId)) : '';
          const res = await fetch('/api/instant-answer/request/' + encodeURIComponent(requestId) + qs, { cache: 'no-store' });
          const data = await res.json();
          if (res.ok && data.request) {
            const done = applyRequestState(data.request);
            if (done) return;
          } else {
            badgeEl.textContent = 'Looking for your request';
          }
        } catch {
          badgeEl.textContent = 'Reconnecting';
        }
        setTimeout(poll, 2000);
      }

      poll();
    </script>
  </body>
  </html>`;
}

async function createInstantAnswerCheckoutSession({ raw_query, requested_by = null, notes = null, requestMeta = null, bundleCode = 'article_bundle_20' }) {
  if (!stripe) throw new Error('stripe_not_configured');
  const bundle = ARTICLE_BUNDLES[bundleCode];
  if (!bundle) throw new Error('invalid_bundle_code');
  const request = paidRequests.createPaidRequest({ raw_query, requested_by, notes, request_meta: { ...(requestMeta || {}), bundle_code: bundle.code, bundle_credits: bundle.credits } });
  const successUrl = `${SITE_BASE_URL}/instant-answer/success?request_id=${encodeURIComponent(request.request_id)}&session_id={CHECKOUT_SESSION_ID}`;
  const cancelUrl = `${SITE_BASE_URL}/instant-answer/cancel?request_id=${encodeURIComponent(request.request_id)}`;
  const session = await stripe.checkout.sessions.create({
    mode: 'payment',
    success_url: successUrl,
    cancel_url: cancelUrl,
    line_items: [{
      price_data: {
        currency: 'usd',
        unit_amount: bundle.unit_amount,
        product_data: {
          name: bundle.label,
          description: bundle.description
        }
      },
      quantity: 1
    }],
    metadata: {
      request_id: request.request_id,
      normalized_query: request.normalized_query,
      bundle_code: bundle.code,
      bundle_credits: String(bundle.credits),
      user_key: requestMeta?.user_key || '',
      ip_hash: requestMeta?.ip_hash || '',
      country: requestMeta?.country || ''
    }
  });
  const updated = paidRequests.updateRequestStatus(request.request_id, {
    stripe_checkout_session_id: session.id,
    stripe_payment_status: session.payment_status || 'unpaid',
    payment_status: 'awaiting_payment',
    request_status: 'awaiting_payment'
  });
  return { request: updated, session, bundle };
}

function buildHomeMeta(req) {
  return {
    title: 'Best of Amazon Prime - Every Time | Next Generation Product Research Tool',
    description: 'Best of Amazon Prime - Every Time | Next Generation Product Research Tool',
    canonicalUrl: buildAbsoluteUrl(req, '/'),
    imageUrl: getSocialImageUrl(req)
  };
}

function buildArticleMeta(req, content, entry) {
  const title = content?.title || entry?.title || entry?.article_slug || 'Affiliate article';
  const description = (content?.summary || `Comparison guide for ${title}`).slice(0, 160);
  const canonicalUrl = buildAbsoluteUrl(req, `/article/${entry?.article_slug || content?.article_slug || ''}`);
  return { title, description, canonicalUrl, imageUrl: getSocialImageUrl(req) };
}

function renderSitemapXml(baseUrl) {
  const urls = [
    `${baseUrl}/`,
    ...getPublishedArticles().map((entry) => `${baseUrl}/article/${entry.article_slug}`)
  ];
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.map((url) => `  <url>\n    <loc>${escapeHtml(url)}</loc>\n  </url>`).join('\n')}\n</urlset>\n`;
}

function renderRobotsTxt(baseUrl) {
  return `User-agent: *\nAllow: /\n\nSitemap: ${baseUrl}/sitemap.xml\n`;
}

function renderHome(req) {
  logEvent(analytics.buildPageViewEvent(req, 'home'));
  const articleIndex = buildSearchIndex();
  const searchData = JSON.stringify(articleIndex).replace(/</g, '\\u003c');
  const publishedCount = articleIndex.length;
  const meta = buildHomeMeta(req);
  return `<!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>${escapeHtml(meta.title)}</title>
    ${renderFaviconMarkup()}
    ${renderFaviconMarkup()}
    <meta name="description" content="${escapeHtml(meta.description)}" />
    <link rel="canonical" href="${escapeHtml(meta.canonicalUrl)}" />
    ${renderSocialMeta(meta)}
    ${renderSocialMeta(meta)}
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
        color:#dbe7f7;
        padding:30px 26px;
        border:1px solid rgba(255,255,255,.12);
        border-radius:22px;
        background:rgba(255,255,255,.04);
        box-shadow:var(--shadow);
      }
      .empty-title {
        font-size:24px;
        font-weight:800;
        color:#ffffff;
        margin-bottom:16px;
      }
      .instant-answer-btn {
        display:none;
        background:linear-gradient(180deg,#22c55e,#16a34a);
        color:#ffffff;
        border:0;
        border-radius:18px;
        padding:18px 28px;
        font-size:24px;
        font-weight:800;
        cursor:pointer;
        box-shadow:0 16px 34px rgba(34,197,94,.28);
      }
      .instant-answer-btn:hover { transform:translateY(-1px); }
      .empty-copy {
        max-width:760px;
        margin:18px auto 0;
        color:#c8d3e6;
        font-size:16px;
        line-height:1.7;
      }
      .empty-copy strong { color:#ffffff; }
      .footer-note { margin-top:26px; text-align:center; color:#7f91ac; font-size:13px; }
    </style>
  </head>
  <body>
    <main class="wrap">
      <section class="hero">
        <div class="eyebrow">Next-generation product research</div>
        <h1>Find the clear winner — every time.</h1>
        <p class="sub">We compare the top products so you don’t have to. Fast, focused, and built to help you buy with confidence.</p>
        <div class="search-shell">
          <div class="search-row">
            <div class="search-icon">⌕</div>
            <input id="searchInput" class="search-input" type="text" placeholder="Search products, categories, or comparisons (e.g. air fryers, espresso grinders, best blenders)…" autofocus>
          </div>
          <div class="assist">Search across ranked buying guides, top picks, and comparison-driven results.</div>
        </div>
      </section>
      <section id="results" class="results"></section>
      <section id="empty" class="empty" style="display:none;">
        <div class="empty-title">No matching approved article found.</div>
        <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;">
          <button id="instantAnswerBtn20" class="instant-answer-btn">Get Instant Answer</button>
          <button id="instantAnswerBtn100" class="instant-answer-btn" style="display:none;background:linear-gradient(180deg,#0ea5e9,#2563eb);">Buy 100-article bundle</button>
        </div>
        <div id="emptyText" class="empty-copy"></div>
      </section>
      <div class="footer-note">Local-only experience. Only compliance-approved article content is surfaced here.</div>
    </main>
    <script id="articleIndexData" type="application/json">${searchData}</script>
    <script>
      const ARTICLE_INDEX = JSON.parse(document.getElementById('articleIndexData').textContent || '[]');
      const input = document.getElementById('searchInput');
      const resultsEl = document.getElementById('results');
      const emptyEl = document.getElementById('empty');
      const emptyText = document.getElementById('emptyText');
      const instantAnswerBtn20 = document.getElementById('instantAnswerBtn20');
      const instantAnswerBtn100 = document.getElementById('instantAnswerBtn100');
      function escapeHtml(value) {
        return String(value || '')
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }
      function renderResults(query) {
        const raw = String(query || '').trim();
        const q = raw.toLowerCase();
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
        if (!matches.length && q) {
          emptyEl.style.display = 'block';
          instantAnswerBtn20.style.display = 'inline-block';
          instantAnswerBtn100.style.display = 'inline-block';
          instantAnswerBtn20.textContent = 'Get Instant Answer / Buy 20-article bundle';
          emptyText.innerHTML = "Your first 3 articles are free. After that, unlock prepaid bundles only:<br><br><strong>$5 value per article</strong><br>20 articles for <strong>$14.99</strong> (about $0.75 each)<br>100 articles for <strong>$59.00</strong> (about $0.59 each)<br><br><strong>What you'll get:</strong> a comparison of the top 5 <strong>" + escapeHtml(raw) + "</strong> available on Amazon.com with a clear winner selected + a link to the exact products we compare.<br><br>Let our deep learning AI model do the heavy lifting.";
        } else {
          emptyEl.style.display = matches.length ? 'none' : 'block';
          instantAnswerBtn20.style.display = 'none';
          instantAnswerBtn100.style.display = 'none';
          emptyText.innerHTML = '';
        }
      }
      const sessionId = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : 'sess-' + Math.random().toString(36).slice(2);
      function sendPresence(closed) {
        const payload = { session_id: sessionId, article_slug: 'home', path: location.pathname, closed_at: closed ? new Date().toISOString() : null };
        const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
        if (navigator.sendBeacon) navigator.sendBeacon('/analytics/presence', blob);
        else fetch('/analytics/presence', { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(payload), keepalive:true }).catch(function(){});
      }
      sendPresence(false);
      setInterval(function(){ sendPresence(false); }, 30000);
      window.addEventListener('pagehide', function(){ sendPresence(true); }, { once: true });
      let searchTimer = null;
      function sendSearchAnalytics(query) {
        const q = String(query || '').trim();
        if (!q) return;
        const matches = ARTICLE_INDEX.filter(item => item.search_text.includes(q.toLowerCase()));
        fetch('/analytics/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            event_type: 'search',
            query: q,
            results_count: matches.length,
            has_results: matches.length > 0,
            matched_article_slug: matches[0] ? String(matches[0].route || '').replace('/article/', '') : null,
            timestamp: new Date().toISOString()
          }),
          keepalive: true
        }).catch(function() {});
      }
      async function startInstantAnswer(bundleCode, buttonEl, busyText, idleText) {
        const q = String(input.value || '').trim();
        if (!q) return;
        buttonEl.disabled = true;
        buttonEl.textContent = busyText;
        try {
          const res = await fetch('/api/instant-answer/checkout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ raw_query: q, notes: 'search miss instant answer', bundle_code: bundleCode })
          });
          const data = await res.json();
          if (res.ok && data.checkout_url) {
            window.location.href = data.checkout_url;
            return;
          }
          if (res.ok && data.success_url) {
            window.location.href = data.success_url;
            return;
          }
          alert(data.error || 'Unable to create checkout right now.');
        } catch (err) {
          alert('Unable to create checkout right now.');
        } finally {
          buttonEl.disabled = false;
          buttonEl.textContent = idleText;
        }
      }
      instantAnswerBtn20.addEventListener('click', function() {
        startInstantAnswer('article_bundle_20', instantAnswerBtn20, 'Checking access…', 'Get Instant Answer / Buy 20-article bundle');
      });
      instantAnswerBtn100.addEventListener('click', function() {
        startInstantAnswer('article_bundle_100', instantAnswerBtn100, 'Creating checkout…', 'Buy 100-article bundle');
      });
      input.addEventListener('input', function(e) {
        renderResults(e.target.value);
        clearTimeout(searchTimer);
        searchTimer = setTimeout(function(){ sendSearchAnalytics(e.target.value); }, 500);
      });
      renderResults('');
    </script>
  </body>
  </html>`;
}

function renderArticle(req, content, compliance, entry = null) {
  if (!content || !isDisplayableCompliance(compliance)) {
    return `
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="robots" content="noindex,follow" />
      <title>Article unavailable</title>
      ${renderFaviconMarkup()}
    </head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:40px;background:#eef2f7;">
      <a href="/" style="color:#2563eb;text-decoration:none;font-weight:700;">← Back</a>
      <h1>Article unavailable</h1>
      <p>This article is not approved for display.</p>
    </body>
    </html>
    `;
  }

  const meta = buildArticleMeta(req, content, entry);
  const productEntityMap = new Map((content.product_entities || []).map((item) => [item.product_name, item]));
  const comparisonRankMap = new Map((content.comparison || []).map((item, index) => [item.name, index + 1]));
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
        <td><a class="shop-btn analytics-link" data-article-slug="${escapeHtml(content.article_slug || 'configured-article')}" data-category="${escapeHtml(content.category || 'configured category')}" data-product-name="${escapeHtml(p.name)}" data-asin="${escapeHtml(p.asin)}" data-affiliate-url="${escapeHtml(p.affiliate_url)}" data-position-in-article="${comparisonRankMap.get(p.name) || ''}" data-was-top-pick="${String((content.top_pick || '').trim() === (p.name || '').trim())}" href="${escapeHtml(p.affiliate_url)}" target="_blank" rel="noopener noreferrer">Shop on Amazon</a></td>
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
        <div style="margin-top:10px;"><a class="shop-btn analytics-link" data-article-slug="${escapeHtml(content.article_slug || 'configured-article')}" data-category="${escapeHtml(content.category || 'configured category')}" data-product-name="${escapeHtml(item.product_name)}" data-asin="${escapeHtml(productEntityMap.get(item.product_name)?.asin || '')}" data-affiliate-url="${escapeHtml(item.canonical_product_url)}" data-position-in-article="${comparisonRankMap.get(item.product_name) || ''}" data-was-top-pick="${String((content.top_pick || '').trim() === (item.product_name || '').trim())}" href="${escapeHtml(item.canonical_product_url)}" target="_blank" rel="noopener noreferrer">Shop on Amazon</a></div>
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
        <p><strong>Canonical product URL:</strong> <a class="analytics-link" data-article-slug="${escapeHtml(content.article_slug || 'configured-article')}" data-category="${escapeHtml(content.category || 'configured category')}" data-product-name="${escapeHtml(item.product_name)}" data-asin="${escapeHtml(item.asin)}" data-affiliate-url="${escapeHtml(item.canonical_product_url)}" data-position-in-article="${comparisonRankMap.get(item.product_name) || ''}" data-was-top-pick="${String((content.top_pick || '').trim() === (item.product_name || '').trim())}" href="${escapeHtml(item.canonical_product_url)}" target="_blank" rel="noopener noreferrer">View on Amazon</a></p>
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
    <title>${escapeHtml(meta.title)}</title>
    <meta name="description" content="${escapeHtml(meta.description)}" />
    <link rel="canonical" href="${escapeHtml(meta.canonicalUrl)}" />
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
            position_in_article: link.dataset.positionInArticle ? Number(link.dataset.positionInArticle) : null,
            was_top_pick: link.dataset.wasTopPick === 'true',
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
      const sessionId = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : 'sess-' + Math.random().toString(36).slice(2);
      function sendPresence(closed) {
        const payload = { session_id: sessionId, article_slug: ${JSON.stringify(content.article_slug || entry?.article_slug || req.params.slug)}, path: location.pathname, closed_at: closed ? new Date().toISOString() : null };
        const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
        if (navigator.sendBeacon) navigator.sendBeacon('/analytics/presence', blob);
        else fetch('/analytics/presence', { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(payload), keepalive:true }).catch(function(){});
      }
      sendPresence(false);
      setInterval(function(){ sendPresence(false); }, 30000);
      const pageStart = Date.now();
      function sendArticleView() {
        const payload = {
          event_type: 'article_view',
          article_slug: ${JSON.stringify(content.article_slug || entry?.article_slug || req.params.slug)},
          category: ${JSON.stringify(content.category || entry?.category || 'configured category')},
          time_on_page_ms: Date.now() - pageStart,
          timestamp: new Date().toISOString()
        };
        const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
        if (navigator.sendBeacon) {
          navigator.sendBeacon('/analytics/article-view', blob);
        } else {
          fetch('/analytics/article-view', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), keepalive: true }).catch(function() {});
        }
      }
      window.addEventListener('pagehide', function(){ sendArticleView(); sendPresence(true); }, { once: true });
    </script>
  </body>
  </html>
  `;
}

app.get('/favicon.svg', (req, res) => {
  res.type('image/svg+xml').send(renderFaviconSvg());
});

app.get('/favicon.ico', (req, res) => {
  res.redirect(302, '/favicon.svg');
});

app.get('/social-share.jpg', (req, res) => {
  res.sendFile(path.join(__dirname, 'assets', 'social-share.jpg'));
});

app.get('/robots.txt', (req, res) => {
  res.type('text/plain').send(renderRobotsTxt(getSiteBaseUrl(req)));
});

app.get('/sitemap.xml', (req, res) => {
  res.type('application/xml').send(renderSitemapXml(getSiteBaseUrl(req)));
});

app.get('/', (req, res) => {
  res.send(renderHome(req));
});

app.get('/article/:slug', (req, res) => {
  const bundle = readArticleBundle(req.params.slug);
  if (!bundle || bundle.entry?.publish_status !== 'published') {
    return res.status(404).send(renderArticle(req, null, null));
  }
  const content = bundle.content;
  const compliance = bundle.compliance;
  logEvent(analytics.buildPageViewEvent(req, req.params.slug));
  res.send(renderArticle(req, content, compliance, bundle.entry));
});

app.post('/analytics/click', (req, res) => {
  const { article_slug, category, product_name, asin, affiliate_url, position_in_article, was_top_pick, timestamp } = req.body || {};
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
    position_in_article: Number.isFinite(Number(position_in_article)) ? Number(position_in_article) : null,
    was_top_pick: Boolean(was_top_pick),
    timestamp: timestamp || new Date().toISOString()
  });
  res.json({ ok: true });
});


app.post('/analytics/presence', (req, res) => {
  const { session_id, article_slug, path: current_path, closed_at } = req.body || {};
  if (!session_id) return res.status(400).json({ ok: false, error: 'missing_session_id' });
  const payload = analytics.updatePresence({
    session_id,
    article_slug: article_slug || 'home',
    path: current_path || '/',
    closed_at: closed_at || null
  });
  res.json({ ok: true, current_viewers: payload.current_viewers });
});

app.get('/analytics/realtime', (req, res) => {
  res.json(analytics.readActiveSessions());
});

app.post('/analytics/search', (req, res) => {
  const { query, results_count, has_results, matched_article_slug, timestamp } = req.body || {};
  if (!String(query || '').trim()) return res.status(400).json({ ok: false, error: 'missing_query' });
  const cleanedQuery = String(query).trim();
  logEvent({
    event_type: 'search',
    query: cleanedQuery,
    results_count: Number(results_count || 0),
    has_results: Boolean(has_results),
    matched_article_slug: matched_article_slug || null,
    timestamp: timestamp || new Date().toISOString()
  });
  paidRequests.appendSearchQuery({ raw_query: cleanedQuery, matched_article_slug: matched_article_slug || null, timestamp: timestamp || new Date().toISOString() });
  res.json({ ok: true });
});

app.post('/analytics/article-view', (req, res) => {
  const { article_slug, category, time_on_page_ms, timestamp } = req.body || {};
  if (!article_slug) return res.status(400).json({ ok: false, error: 'missing_article_slug' });
  logEvent({
    event_type: 'article_view',
    article_slug,
    category: category || 'configured category',
    time_on_page_ms: Math.max(0, Number(time_on_page_ms || 0)),
    timestamp: timestamp || new Date().toISOString()
  });
  res.json({ ok: true });
});



async function syncRequestFromStripeSession(session, existingRequest = null) {
  if (!session) return existingRequest || null;
  const requestId = existingRequest?.request_id || session.metadata?.request_id || session.id;
  const alreadyPublished = existingRequest && (existingRequest.request_status === 'published' || existingRequest.publish_status === 'published');
  const patch = {
    request_id: requestId,
    raw_query: existingRequest?.raw_query || session.metadata?.raw_query || session.metadata?.normalized_query || requestId,
    normalized_query: existingRequest?.normalized_query || session.metadata?.normalized_query || session.metadata?.raw_query || requestId,
    created_at: existingRequest?.created_at || new Date((session.created || Math.floor(Date.now()/1000)) * 1000).toISOString(),
    requested_by: existingRequest?.requested_by || session.metadata?.requested_by || null,
    payment_status: session.payment_status === 'paid' ? 'paid' : (existingRequest?.payment_status || 'awaiting_payment'),
    request_status: alreadyPublished ? existingRequest.request_status : (session.payment_status === 'paid' ? (existingRequest?.request_status && existingRequest.request_status !== 'awaiting_payment' ? existingRequest.request_status : 'paid_pending') : (existingRequest?.request_status || 'awaiting_payment')),
    generated_article_slug: existingRequest?.generated_article_slug || null,
    fulfillment_status: existingRequest?.fulfillment_status || null,
    publish_status: existingRequest?.publish_status || null,
    published_at: existingRequest?.published_at || null,
    published_slug: existingRequest?.published_slug || null,
    published_url: existingRequest?.published_url || null,
    source_request_id: existingRequest?.source_request_id || null,
    content_hash: existingRequest?.content_hash || null,
    generation_attempts: existingRequest?.generation_attempts || 0,
    fulfillment_output_path: existingRequest?.fulfillment_output_path || null,
    notes: existingRequest?.notes || 'recovered_from_stripe_session',
    error: existingRequest?.error || null,
    stripe_checkout_session_id: session.id,
    stripe_payment_status: session.payment_status || null,
    paid_at: session.payment_status === 'paid' ? (existingRequest?.paid_at || new Date((session.created || Math.floor(Date.now()/1000)) * 1000).toISOString()) : (existingRequest?.paid_at || null)
  };
  return paidRequests.upsertRequest(patch);
}

async function recoverRequestFromStripe(requestId, sessionId = null) {
  if (!stripe || (!requestId && !sessionId)) return null;
  let session = null;
  if (sessionId) {
    try { session = await stripe.checkout.sessions.retrieve(sessionId); } catch {}
  }
  if (!session && requestId) {
    const sessions = await stripe.checkout.sessions.list({ limit: 50 });
    session = (sessions.data || []).find((s) => s.metadata && s.metadata.request_id === requestId) || null;
  }
  if (!session) return null;
  return await syncRequestFromStripeSession(session, paidRequests.getRequestById(requestId || session.metadata?.request_id || session.id));
}

app.post('/api/stripe/webhook', (req, res) => {
  if (!stripe || !STRIPE_WEBHOOK_SECRET) {
    return res.status(500).json({ ok: false, error: 'stripe_webhook_not_configured' });
  }
  const signature = req.headers['stripe-signature'];
  if (!signature) { console.log('[instant-answer webhook] missing signature'); return res.status(400).json({ ok: false, error: 'missing_stripe_signature' }); }

  let event;
  try {
    event = stripe.webhooks.constructEvent(req.rawBody, signature, STRIPE_WEBHOOK_SECRET);
  } catch (error) {
    console.log('[instant-answer webhook] invalid signature');
    return res.status(400).json({ ok: false, error: 'invalid_signature' });
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    let request = paidRequests.getRequestByStripeCheckoutSessionId(session.id) || paidRequests.getRequestById(session.metadata?.request_id || '');
    if (!request) {
      request = paidRequests.upsertRequest({
        request_id: session.metadata?.request_id || session.id,
        raw_query: session.metadata?.raw_query || session.metadata?.normalized_query || session.id,
        normalized_query: session.metadata?.normalized_query || session.metadata?.raw_query || session.id,
        created_at: new Date((session.created || Math.floor(Date.now()/1000)) * 1000).toISOString(),
        requested_by: session.metadata?.requested_by || null,
        payment_status: 'awaiting_payment',
        request_status: 'awaiting_payment',
        generated_article_slug: null,
        fulfillment_status: null,
        publish_status: null,
        published_at: null,
        published_slug: null,
        published_url: null,
        source_request_id: null,
        content_hash: null,
        generation_attempts: 0,
        fulfillment_output_path: null,
        notes: 'recovered_in_webhook',
        error: null,
        stripe_checkout_session_id: session.id,
        stripe_payment_status: session.payment_status || null,
        paid_at: null
      });
    }
    if (!request) {
      console.log('[instant-answer webhook] request not found', { session_id: session.id, request_id: session.metadata?.request_id || null });
      return res.json({ ok: true, handled: false, reason: 'request_not_found' });
    }
    if (request.payment_status === 'paid' || request.paid_at) {
      console.log('[instant-answer webhook] idempotent delivery', { request_id: request.request_id, session_id: session.id });
      return res.json({ ok: true, handled: true, idempotent: true, request_id: request.request_id });
    }
    const bundleCode = session.metadata?.bundle_code || request?.request_meta?.bundle_code || 'article_bundle_20';
    const bundle = ARTICLE_BUNDLES[bundleCode];
    const userKey = session.metadata?.user_key || request?.request_meta?.user_key || request?.request_meta?.ip_hash || null;
    const ipHash = session.metadata?.ip_hash || request?.request_meta?.ip_hash || null;
    const country = session.metadata?.country || request?.request_meta?.country || null;

    if (!bundle) {
      return res.status(400).json({ ok: false, handled: false, error: 'invalid_bundle_code' });
    }

    const creditResult = paidRequests.applyBundlePurchase({
      userKey,
      bundleSize: bundle.credits,
      stripeCustomerId: session.customer || null,
      checkoutSessionId: session.id,
      purchasedAt: new Date().toISOString(),
      ipHash,
      country
    });

    const updated = paidRequests.updateRequestStatus(request.request_id, {
      payment_status: 'paid',
      request_status: 'awaiting_bundle_use',
      paid_at: request.paid_at || new Date().toISOString(),
      stripe_payment_status: 'completed',
      stripe_last_event_id: event.id,
      request_meta: {
        ...(request.request_meta || {}),
        access_mode: 'paid',
        bundle_code: bundle.code,
        bundle_credits: bundle.credits,
        user_key: userKey,
        ip_hash: ipHash,
        country
      }
    });
    console.log('[instant-answer webhook] bundle credited', { request_id: updated.request_id, session_id: session.id, bundle_code: bundle.code, credits: bundle.credits, already_processed: creditResult?.alreadyProcessed === true });
    return res.json({ ok: true, handled: true, request_id: updated.request_id, bundle_code: bundle.code, credits_added: creditResult?.alreadyProcessed ? 0 : bundle.credits, already_processed: creditResult?.alreadyProcessed === true });
  }

  res.json({ ok: true, handled: false, ignored_event_type: event.type });
});

app.post('/api/instant-answer/checkout', async (req, res) => {
  const { raw_query, requested_by, notes, bundle_code } = req.body || {};
  if (!String(raw_query || '').trim()) return res.status(400).json({ ok: false, error: 'missing_raw_query' });

  const access = buildInstantAnswerAccess(req);
  const requestMeta = {
    user_key: access.userKey,
    ip_hash: access.ipHash,
    country: access.country,
    access_mode: access.accessMode,
    successful_generations_before_request: access.successfulGenerations,
    free_articles_used_before_request: access.freeArticlesUsed,
    articles_remaining_balance_before_request: access.paidBalance
  };

  try {
    if (access.hasFreeAccess) {
      paidRequests.upsertUserRecord(access.userKey, { ip_hash: access.ipHash, country: access.country });
      const request = paidRequests.createPaidRequest({
        raw_query: String(raw_query).trim(),
        requested_by: requested_by || null,
        notes: notes || null,
        request_meta: { ...requestMeta, access_mode: 'free' }
      });
      const updated = paidRequests.updateRequestStatus(request.request_id, {
        payment_status: 'paid',
        request_status: 'paid_pending',
        paid_at: new Date().toISOString(),
        stripe_payment_status: 'free_access'
      });
      kickOffInstantAnswerProcessing(updated.request_id);
      return res.json({
        ok: true,
        request_id: updated.request_id,
        access_mode: 'free',
        free_articles_remaining_after_this: Math.max(0, 3 - (access.freeArticlesUsed + 1)),
        success_url: `${SITE_BASE_URL}/instant-answer/success?request_id=${encodeURIComponent(updated.request_id)}`
      });
    }

    if (access.hasPaidBalance) {
      const request = paidRequests.createPaidRequest({
        raw_query: String(raw_query).trim(),
        requested_by: requested_by || null,
        notes: notes || null,
        request_meta: { ...requestMeta, access_mode: 'bundle' }
      });
      const updated = paidRequests.updateRequestStatus(request.request_id, {
        payment_status: 'paid',
        request_status: 'paid_pending',
        paid_at: new Date().toISOString(),
        stripe_payment_status: 'credit_balance'
      });
      kickOffInstantAnswerProcessing(updated.request_id);
      return res.json({
        ok: true,
        request_id: updated.request_id,
        access_mode: 'bundle',
        articles_remaining_balance: access.paidBalance,
        success_url: `${SITE_BASE_URL}/instant-answer/success?request_id=${encodeURIComponent(updated.request_id)}`
      });
    }

    const selectedBundleCode = ARTICLE_BUNDLES[bundle_code] ? bundle_code : 'article_bundle_20';
    const { request, session, bundle } = await createInstantAnswerCheckoutSession({
      raw_query: String(raw_query).trim(),
      requested_by: requested_by || null,
      notes: notes || null,
      requestMeta: { ...requestMeta, access_mode: 'paid', selected_bundle_code: selectedBundleCode },
      bundleCode: selectedBundleCode
    });
    res.json({
      ok: true,
      request_id: request.request_id,
      access_mode: 'paid',
      checkout_url: session.url,
      selected_bundle_code: bundle.code,
      selected_bundle_credits: bundle.credits,
      free_articles_remaining_after_this: access.freeRemaining,
      articles_remaining_balance: access.paidBalance
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || 'checkout_session_failed' });
  }
});

app.get('/instant-answer/success', (req, res) => {
  res.send(renderInstantAnswerSuccessPage(req.query.request_id || ''));
});

app.get('/instant-answer/cancel', (req, res) => {
  res.send(renderInstantAnswerStatusPage('Instant Answer checkout cancelled', 'Your request was saved, but payment is not complete. You can restart checkout later.'));
});

app.get('/api/instant-answer/request/:id', async (req, res) => {
  let request = paidRequests.getRequestById(req.params.id);
  const sessionId = String(req.query.session_id || '').trim() || null;
  if (!request) {
    request = await recoverRequestFromStripe(req.params.id, sessionId);
  } else if (stripe && (request.payment_status !== 'paid' || request.request_status === 'awaiting_payment')) {
    let session = null;
    if (sessionId) {
      try { session = await stripe.checkout.sessions.retrieve(sessionId); } catch {}
    }
    if (!session && request.stripe_checkout_session_id) {
      try { session = await stripe.checkout.sessions.retrieve(request.stripe_checkout_session_id); } catch {}
    }
    if (session) {
      request = await syncRequestFromStripeSession(session, request);
    }
  }
  if (!request) return res.status(404).json({ ok: false, error: 'request_not_found' });
  if (!request.generated_article_slug && !request.published_slug && ['validated', 'generating', 'paid_pending'].includes(request.request_status)) {
    const inferredSlug = slugifyQuery(request.normalized_query || request.raw_query);
    const registry = readRegistry();
    const published = (registry.articles || []).find((a) => a.article_slug === inferredSlug && a.publish_status === 'published');
    if (published) {
      request = paidRequests.updateRequestStatus(request.request_id, {
        generated_article_slug: inferredSlug,
        published_slug: inferredSlug,
        published_url: `${SITE_BASE_URL}/article/${inferredSlug}`,
        publish_status: 'published',
        request_status: 'published',
        fulfillment_status: 'completed',
        published_at: new Date().toISOString(),
        error: null
      }) || request;
    }
  }
  if (request.payment_status === 'paid' && ['paid_pending', 'validated'].includes(request.request_status)) {
    kickOffInstantAnswerProcessing(request.request_id);
  }
  res.json({ ok: true, request });
});

app.get('/analytics/events', (req, res) => {
  res.json(readEvents());
});

app.get('/analytics/summary', (req, res) => {
  res.json(readSummary());
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
