#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const createPaidRequests = require('../paid_requests');

const ROOT = path.resolve(__dirname, '..');
const paidRequests = createPaidRequests({ rootDir: ROOT });
const registryPath = path.join(ROOT, 'data', 'articles', 'registry.json');
const outputsDir = path.join(ROOT, 'data', 'instant_answers');
const lockPath = path.join(ROOT, 'data', 'analytics', 'instant_answer_fulfillment.lock');

function readJson(file) { return JSON.parse(fs.readFileSync(file, 'utf8')); }
function normalize(q) { return paidRequests.normalizeSearchQuery(q); }

function loadPublishedArticles() {
  const reg = readJson(registryPath);
  return (reg.articles || []).filter((a) => a.publish_status === 'published').map((entry) => {
    const dir = path.join(ROOT, entry.article_dir);
    const content = JSON.parse(fs.readFileSync(path.join(dir, 'contentproduction.json'), 'utf8'));
    return {
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
  const matches = published.filter((item) => item.search_text.includes(q)).slice(0, 5);
  if (!matches.length) {
    return { ok: false, error: 'no_relevant_content_found' };
  }
  return {
    ok: true,
    request_id: request.request_id,
    raw_query: request.raw_query,
    normalized_query: request.normalized_query,
    generated_at: new Date().toISOString(),
    top_matches: matches,
    answer_summary: `Found ${matches.length} relevant guide(s) for \"${request.raw_query}\" based on existing published comparison content.`,
  };
}

function processOne(request) {
  const published = loadPublishedArticles();
  paidRequests.updateRequestStatus(request.request_id, {
    fulfillment_status: 'processing',
    generation_attempts: Number(request.generation_attempts || 0) + 1,
    request_status: request.request_status === 'paid' ? 'paid' : request.request_status
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
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
  paidRequests.updateRequestStatus(request.request_id, {
    fulfillment_status: 'completed',
    request_status: 'completed',
    fulfillment_output_path: path.relative(ROOT, outPath),
    error: null
  });
  return { request_id: request.request_id, status: 'completed', output_path: path.relative(ROOT, outPath) };
}

function main() {
  if (fs.existsSync(lockPath)) {
    console.log(JSON.stringify({ ok: false, error: 'lock_exists' }));
    process.exit(1);
  }
  fs.writeFileSync(lockPath, String(Date.now()));
  try {
    const queue = paidRequests.readPaidRequests().filter((r) => r.payment_status === 'paid' && (!r.fulfillment_status || r.fulfillment_status === 'queued'));
    const results = queue.map(processOne);
    console.log(JSON.stringify({ ok: true, processed: results.length, results }, null, 2));
  } finally {
    if (fs.existsSync(lockPath)) fs.unlinkSync(lockPath);
  }
}

main();
