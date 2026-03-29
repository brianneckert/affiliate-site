#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const createPaidRequests = require('../paid_requests');

const ROOT = path.resolve(__dirname, '..');
const paidRequests = createPaidRequests({ rootDir: ROOT });
const registryPath = path.join(ROOT, 'data', 'articles', 'registry.json');
const outputsDir = path.join(ROOT, 'data', 'instant_answers');
const lockPath = path.join(ROOT, 'data', 'analytics', 'instant_answer_fulfillment.lock');
const syncScript = path.join(ROOT, 'scripts', 'sync_live_repo.py');
const sitemapScript = path.join(ROOT, 'scripts', 'generate_sitemap.py');

function readJson(file) { return JSON.parse(fs.readFileSync(file, 'utf8')); }
function writeJson(file, payload) { fs.writeFileSync(file, JSON.stringify(payload, null, 2)); }
function normalize(q) { return paidRequests.normalizeSearchQuery(q); }
function slugify(q) { return normalize(q).replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').replace(/-+/g, '-').replace(/^-|-$/g, ''); }

function loadPublishedArticles() {
  const reg = readJson(registryPath);
  return (reg.articles || []).filter((a) => a.publish_status === 'published').map((entry) => {
    const dir = path.join(ROOT, entry.article_dir);
    const content = JSON.parse(fs.readFileSync(path.join(dir, 'contentproduction.json'), 'utf8'));
    const intelligence = JSON.parse(fs.readFileSync(path.join(dir, 'productintelligence.json'), 'utf8'));
    return {
      entry,
      content,
      intelligence,
      article_slug: entry.article_slug,
      title: content.title || entry.title,
      summary: content.summary || '',
      top_pick: content.top_pick || '',
      category: entry.category || content.category || '',
      search_text: [content.title || '', content.summary || '', content.top_pick || '', entry.category || '', ...(content.comparison || []).map(x => x.name || '')].join(' ').toLowerCase()
    };
  });
}

function buildOutput(request, published) {
  const q = normalize(request.raw_query);
  const qTokens = q.split(' ').filter(Boolean);
  const matches = published.map((item) => {
    const score = qTokens.reduce((acc, token) => acc + (item.search_text.includes(token) ? 1 : 0), 0);
    return { ...item, score };
  }).filter((item) => item.score > 0).sort((a,b) => b.score - a.score).slice(0, 5);
  if (!matches.length) return { ok: false, error: 'no_relevant_content_found' };
  const products = [];
  const seen = new Set();
  for (const match of matches) {
    for (const p of (match.intelligence.products || [])) {
      const key = (p.product_name || p.name || '').trim().toLowerCase();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      products.push({
        product_name: p.product_name || p.name,
        affiliate_url: p.affiliate_url,
        notes: p.notes || '',
        why_it_won: p.why_it_won || '',
        source_article_slug: match.article_slug
      });
      if (products.length >= 5) break;
    }
    if (products.length >= 5) break;
  }
  if (!products.length) return { ok: false, error: 'no_valid_amazon_products_found' };
  return {
    ok: true,
    request_id: request.request_id,
    raw_query: request.raw_query,
    normalized_query: request.normalized_query,
    generated_at: new Date().toISOString(),
    top_matches: matches.map((m) => ({ article_slug: m.article_slug, title: m.title, score: m.score, top_pick: m.top_pick })),
    answer_summary: `Built from ${matches.length} existing published guide(s) using valid Amazon-linked products.`,
    products
  };
}

function ensurePublish(registry, request, output) {
  const slug = slugify(request.normalized_query || request.raw_query);
  const existing = registry.articles.find((a) => a.article_slug === slug);
  if (existing) {
    return { slug, existing: true, article_dir: existing.article_dir, published_url: `https://www.bestofprime.online/article/${slug}` };
  }
  const articleDirRel = `data/articles/${slug}`;
  const articleDir = path.join(ROOT, articleDirRel);
  fs.mkdirSync(articleDir, { recursive: true });
  const content = {
    article_slug: slug,
    title: output.raw_query.replace(/\b\w/g, c => c.toUpperCase()),
    summary: output.answer_summary,
    top_pick: output.products[0].product_name,
    comparison: output.products.map((p) => ({ name: p.product_name, why_it_won: p.why_it_won || `Referenced from ${p.source_article_slug}`, best_for: 'Instant answer request', keep_in_mind: p.notes || '' }))
  };
  const intelligence = { products: output.products };
  const compliance = { passed: true, mode: 'instant_answer_existing_content' };
  writeJson(path.join(articleDir, 'contentproduction.json'), content);
  writeJson(path.join(articleDir, 'productintelligence.json'), intelligence);
  writeJson(path.join(articleDir, 'compliance.json'), compliance);
  registry.articles.push({
    article_slug: slug,
    category: request.normalized_query,
    title: content.title,
    output_dir: articleDirRel,
    article_dir: articleDirRel,
    topic_family: request.normalized_query,
    article_family_position: 'instant_answer',
    source_topic_plan_date: new Date().toISOString().slice(0,10),
    generation_status: 'published',
    publish_status: 'published',
    validation_result: { passed: true },
    published_at: new Date().toISOString(),
    source_article_family: 'instant_answer_paid',
    related_articles: output.top_matches.map((x) => x.article_slug),
    duplicate_of: null,
    source_request_id: request.request_id
  });
  return { slug, existing: false, article_dir: articleDirRel, published_url: `https://www.bestofprime.online/article/${slug}` };
}

function processOne(request) {
  const published = loadPublishedArticles();
  if (request.request_status === 'published' || request.publish_status === 'published' || request.generated_article_slug) {
    return { request_id: request.request_id, status: 'idempotent', published_slug: request.generated_article_slug || request.published_slug || null };
  }
  paidRequests.updateRequestStatus(request.request_id, {
    fulfillment_status: 'processing',
    request_status: 'generating',
    generation_attempts: Number(request.generation_attempts || 0) + 1
  });
  const result = buildOutput(request, published);
  if (!result.ok) {
    paidRequests.updateRequestStatus(request.request_id, {
      fulfillment_status: 'failed',
      request_status: 'failed',
      error: result.error
    });
    return { request_id: request.request_id, status: 'failed', error: result.error };
  }
  if (!fs.existsSync(outputsDir)) fs.mkdirSync(outputsDir, { recursive: true });
  const outPath = path.join(outputsDir, `${request.request_id}.json`);
  writeJson(outPath, result);
  paidRequests.updateRequestStatus(request.request_id, { request_status: 'validated', fulfillment_output_path: path.relative(ROOT, outPath) });

  const registry = readJson(registryPath);
  const publish = ensurePublish(registry, request, result);
  writeJson(registryPath, registry);
  execFileSync('python3', [sitemapScript], { cwd: ROOT });
  execFileSync('python3', [syncScript, '--message', `publish paid instant answer: ${publish.slug}`, '--paths', registryPath, path.join(ROOT, publish.article_dir), path.join(ROOT, 'sitemap.xml')], { cwd: ROOT });

  const updated = paidRequests.updateRequestStatus(request.request_id, {
    fulfillment_status: 'completed',
    request_status: 'published',
    fulfillment_output_path: path.relative(ROOT, outPath),
    publish_status: 'published',
    published_at: new Date().toISOString(),
    published_slug: publish.slug,
    published_url: publish.published_url,
    generated_article_slug: publish.slug,
    source_request_id: request.request_id,
    content_hash: crypto.createHash('sha256').update(request.normalized_query).digest('hex').slice(0, 16),
    error: null
  });
  return { request_id: request.request_id, status: 'completed', published_slug: updated.published_slug, published_url: updated.published_url };
}

function main() {
  const requestIdArg = process.argv.includes('--request-id') ? process.argv[process.argv.indexOf('--request-id') + 1] : null;
  if (fs.existsSync(lockPath)) {
    console.log(JSON.stringify({ ok: false, error: 'lock_exists' }));
    process.exit(1);
  }
  fs.writeFileSync(lockPath, String(Date.now()));
  try {
    const all = paidRequests.readPaidRequests();
    const queue = all.filter((r) => (!requestIdArg || r.request_id === requestIdArg) && r.payment_status === 'paid' && r.request_status === 'paid_pending');
    const results = queue.map(processOne);
    console.log(JSON.stringify({ ok: true, processed: results.length, results }, null, 2));
  } finally {
    if (fs.existsSync(lockPath)) fs.unlinkSync(lockPath);
  }
}

main();
