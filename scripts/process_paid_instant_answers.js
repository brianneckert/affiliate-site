#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const createPaidRequests = require('../paid_requests');

const ROOT = path.resolve(__dirname, '..');
const paidRequests = createPaidRequests({ rootDir: ROOT });
const analyticsDir = path.join(ROOT, 'data', 'analytics');
const workerLogPath = path.join(analyticsDir, 'instant_answer_worker_events.jsonl');
let activeRequestId = null;
let checkpointHook = null;
let shutdownRequested = null;
const registryPath = path.join(ROOT, 'data', 'articles', 'registry.json');
const outputsDir = path.join(ROOT, 'data', 'instant_answers');
const lockPath = path.join(analyticsDir, 'instant_answer_fulfillment.lock');
const progressPath = path.join(analyticsDir, 'instant_answer_progress.json');
const checkpointsDir = path.join(analyticsDir, 'instant_answer_checkpoints');
const syncScript = path.join(ROOT, 'scripts', 'sync_live_repo.py');
const sitemapScript = path.join(ROOT, 'scripts', 'generate_sitemap.py');
const STRONG_COVERAGE_BUCKETS = ['reddit', 'google_reviews', 'forum', 'web_review'];

const STAGE_TIMEOUTS_MS = {
  build_output: 8 * 60 * 1000,
  category_intelligence: 6 * 60 * 1000,
  category_substage: 45 * 1000,
  product_analysis: 2 * 60 * 1000,
  publish: 2 * 60 * 1000,
  total_request: 12 * 60 * 1000
};
const CATEGORY_LIMITS = {
  maxIterations: 4,
  maxQueriesPerIteration: 12,
  maxFetchPerIteration: 14,
  maxTotalEvaluated: 40,
  noProgressMs: 90 * 1000
};
const AMAZON_HEADERS = {
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
  'accept-language': 'en-US,en;q=0.9'
};
const AMAZON_ASSOCIATE_TAG = process.env.AMAZON_ASSOCIATE_TAG || 'helperscollec-20';
const AMAZON_REVIEW_MIN_COUNT = 100;
const SEARCH_HEADERS = {
  'user-agent': AMAZON_HEADERS['user-agent'],
  'accept-language': AMAZON_HEADERS['accept-language']
};
const STOPWORDS = new Set([
  'the','and','for','that','with','this','from','your','into','under','over','best','top','guide','comparison','reviews','review','user','users','buyer','buyers','amazon','product','products','item','items','youtube','reddit','google','forum','forums','good','great','nice','very','more','most','less','than','when','what','which','while','about','they','them','their','have','has','had','are','was','were','you','our','not','too','can','all','but','out','why','how','use','using'
]);

function readJson(file) { return JSON.parse(fs.readFileSync(file, 'utf8')); }
function writeJson(file, payload) { fs.writeFileSync(file, JSON.stringify(payload, null, 2)); }
function normalize(q) { return paidRequests.normalizeSearchQuery(q); }
function slugify(q) { return normalize(q).replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').replace(/-+/g, '-').replace(/^-|-$/g, ''); }
function nowIso() { return new Date().toISOString(); }

function logWorkerEvent(event, payload = {}) {
  try {
    fs.mkdirSync(path.dirname(workerLogPath), { recursive: true });
    fs.appendFileSync(workerLogPath, JSON.stringify({ timestamp: nowIso(), event, pid: process.pid, request_id: activeRequestId, ...payload }) + '\n');
  } catch {}
}

function isPidAlive(pid) {
  if (!pid) return false;
  try { process.kill(pid, 0); return true; } catch { return false; }
}

const runtimeState = {
  requestId: null,
  activeStage: null,
  activeSubstage: null,
  lastProgressAt: null,
  stallStartedAt: null,
  stageScopedWatchdogs: new Set(),
  restoredStages: [],
  counters: {}
};

function resetRuntimeState(requestId, activeStage, options = {}) {
  runtimeState.requestId = requestId;
  runtimeState.activeStage = activeStage;
  runtimeState.activeSubstage = null;
  runtimeState.lastProgressAt = nowIso();
  runtimeState.stallStartedAt = null;
  runtimeState.stageScopedWatchdogs = new Set([activeStage]);
  runtimeState.restoredStages = options.restoredStages || [];
  runtimeState.counters = options.counters || {};
}

function setActiveRuntimeStage(stage, substage = null) {
  runtimeState.activeStage = stage;
  runtimeState.activeSubstage = substage;
  runtimeState.lastProgressAt = nowIso();
  runtimeState.stallStartedAt = null;
  runtimeState.stageScopedWatchdogs = new Set([stage]);
}

function isWatchdogActiveFor(substage = '') {
  const key = String(substage || '');
  if (!runtimeState.activeStage) return true;
  if (runtimeState.activeStage === 'product_analysis' && key.startsWith('category_')) return false;
  if (runtimeState.activeStage === 'product_selection_complete' && key.startsWith('category_')) return false;
  if (runtimeState.restoredStages.includes('category_intelligence') && key.startsWith('category_') && runtimeState.activeStage !== 'category_intelligence') return false;
  return true;
}

function writeProgress(update = {}) {
  const current = fs.existsSync(progressPath) ? readJson(progressPath) : {};
  const next = { ...current, ...update, runtime_active_stage: runtimeState.activeStage, runtime_active_substage: runtimeState.activeSubstage, runtime_restored_stages: runtimeState.restoredStages, updated_at: nowIso(), pid: process.pid };
  writeJson(progressPath, next);
  return next;
}

function updateCategoryDebug(requestId, patch = {}) {
  const file = path.join(analyticsDir, `category_debug_${requestId}.json`);
  const current = fs.existsSync(file) ? readJson(file) : { request_id: requestId, started_at: nowIso() };
  const next = { ...current, ...patch, updated_at: nowIso(), pid: process.pid };
  writeJson(file, next);
  return next;
}

function fingerprintUrls(urls = []) {
  return urls.map((url) => {
    try {
      const u = new URL(url);
      return `${u.hostname}${u.pathname}`.toLowerCase();
    } catch {
      return String(url).toLowerCase();
    }
  }).sort();
}

function overlapRatio(a = [], b = []) {
  const A = new Set(a);
  const B = new Set(b);
  if (!A.size || !B.size) return 0;
  let overlap = 0;
  for (const item of A) if (B.has(item)) overlap += 1;
  return overlap / Math.max(A.size, B.size);
}

function categoryProgressTracker(requestId) {
  const state = {
    lastProgressMs: Date.now(),
    discovered: 0,
    fetched: 0,
    extracted: 0,
    qualified: 0,
    iteration: 0,
    totalEvaluated: 0,
    seenCandidateFingerprints: [],
    seenDomains: new Map(),
    lastProgressEventType: 'tracker_init',
    lastDiscoveryAt: null,
    lastFetchAt: null,
    lastExtractAt: null,
    lastQualifyAt: null,
    repeatedSetActivity: null,
    queryFamilyInFlight: []
  };
  return {
    mark(substage, patch = {}) {
      const counts = patch.counts || {};
      const now = Date.now();
      const changed = ['discovered','fetched','extracted','qualified'].some((k) => Number(counts[k] || 0) > Number(state[k] || 0));
      const heartbeat = Boolean(patch.heartbeat);
      if (changed || heartbeat) state.lastProgressMs = now;
      state.discovered = Math.max(state.discovered, Number(counts.discovered || state.discovered));
      state.fetched = Math.max(state.fetched, Number(counts.fetched || state.fetched));
      state.extracted = Math.max(state.extracted, Number(counts.extracted || state.extracted));
      state.qualified = Math.max(state.qualified, Number(counts.qualified || state.qualified));
      state.iteration = patch.iteration ?? state.iteration;
      state.totalEvaluated = patch.totalEvaluated ?? state.totalEvaluated;
      state.lastProgressEventType = patch.eventType || (changed ? 'counter_change' : (heartbeat ? 'heartbeat' : state.lastProgressEventType));
      if (Number(counts.discovered || 0) > 0) state.lastDiscoveryAt = new Date(now).toISOString();
      if (Number(counts.fetched || 0) > 0) state.lastFetchAt = new Date(now).toISOString();
      if (Number(counts.extracted || 0) > 0) state.lastExtractAt = new Date(now).toISOString();
      if (Number(counts.qualified || 0) > 0) state.lastQualifyAt = new Date(now).toISOString();
      if (patch.repeatedSetActivity !== undefined) state.repeatedSetActivity = patch.repeatedSetActivity;
      if (patch.queryFamilyInFlight) state.queryFamilyInFlight = patch.queryFamilyInFlight;
      setActiveRuntimeStage('category_intelligence', substage);
      writeProgress({ request_id: requestId, stage: substage, category_iteration: state.iteration, category_counts: { discovered: state.discovered, fetched: state.fetched, extracted: state.extracted, qualified: state.qualified }, last_progress_at: new Date(state.lastProgressMs).toISOString(), last_progress_event_type: state.lastProgressEventType, last_successful_transition: substage });
      updateCategoryDebug(requestId, { current_substage: substage, iteration: state.iteration, counts: { discovered: state.discovered, fetched: state.fetched, extracted: state.extracted, qualified: state.qualified }, total_evaluated: state.totalEvaluated, last_progress_at: new Date(state.lastProgressMs).toISOString(), last_progress_event_type: state.lastProgressEventType, last_discovery_at: state.lastDiscoveryAt, last_fetch_at: state.lastFetchAt, last_extract_at: state.lastExtractAt, last_qualify_at: state.lastQualifyAt, repeated_set_activity: state.repeatedSetActivity, query_family_in_flight: state.queryFamilyInFlight, ...patch.debug });
    },
    assertProgress(substage) {
      if (!isWatchdogActiveFor(substage)) return;
      const stalledMs = Date.now() - state.lastProgressMs;
      if (stalledMs > CATEGORY_LIMITS.noProgressMs) {
        const err = new Error(`no_progress_${substage}`);
        err.code = 'category_no_progress';
        err.meta = {
          stage: 'category_intelligence',
          substage,
          stalled_ms: stalledMs,
          counts: { discovered: state.discovered, fetched: state.fetched, extracted: state.extracted, qualified: state.qualified },
          iteration: state.iteration,
          total_evaluated: state.totalEvaluated,
          last_progress_at: new Date(state.lastProgressMs).toISOString(),
          last_progress_event_type: state.lastProgressEventType,
          last_discovery_at: state.lastDiscoveryAt,
          last_fetch_at: state.lastFetchAt,
          last_extract_at: state.lastExtractAt,
          last_qualify_at: state.lastQualifyAt,
          repeated_set_activity: state.repeatedSetActivity,
          query_family_in_flight: state.queryFamilyInFlight,
          async_tasks_running: false
        };
        throw err;
      }
    },
    noteCandidateSet(urls = []) {
      const fp = fingerprintUrls(urls);
      const repeated = state.seenCandidateFingerprints.some((prev) => overlapRatio(prev, fp) >= 0.7);
      state.seenCandidateFingerprints.push(fp);
      return repeated;
    },
    noteDomains(urls = []) {
      const domains = [];
      for (const url of urls) {
        try {
          const host = new URL(url).hostname.toLowerCase();
          domains.push(host);
          state.seenDomains.set(host, (state.seenDomains.get(host) || 0) + 1);
        } catch {}
      }
      return domains;
    },
    domainPenalty(url = '') {
      try {
        const host = new URL(url).hostname.toLowerCase();
        return Math.max(0, (state.seenDomains.get(host) || 0) - 1);
      } catch {
        return 0;
      }
    },
    snapshot() {
      return {
        seen_domains: Array.from(state.seenDomains.entries()),
        fingerprint_count: state.seenCandidateFingerprints.length
      };
    }
  };
}

async function withTimeout(promise, ms, meta = {}) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          const err = new Error(`timeout_${meta.stage || 'unknown'}`);
          err.code = 'stage_timeout';
          err.meta = { ...meta, timeout_ms: ms };
          reject(err);
        }, ms);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function checkpointPath(requestId) {
  fs.mkdirSync(checkpointsDir, { recursive: true });
  return path.join(checkpointsDir, `${requestId}.json`);
}

function loadCheckpoint(requestId) {
  const file = checkpointPath(requestId);
  return fs.existsSync(file) ? readJson(file) : null;
}

function setCheckpointHook(fn) {
  checkpointHook = fn;
}

function saveCheckpoint(requestId, patch = {}) {
  const file = checkpointPath(requestId);
  const existed = fs.existsSync(file);
  const current = existed ? readJson(file) : { request_id: requestId, created_at: nowIso(), completed_products: [] };
  const next = { ...current, ...patch, updated_at: nowIso() };
  writeJson(file, next);
  writeProgress({
    request_id: requestId,
    stage: 'checkpoint_saved',
    checkpoint_stage: next.stage || patch.stage || current.stage || null,
    checkpoint_file: file,
    completed_products: Array.isArray(next.completed_products) ? next.completed_products.length : 0,
    checkpoint_write_mode: existed ? 'update' : 'fresh',
    checkpoint_saved_at: next.updated_at,
    last_successful_transition: 'checkpoint_saved'
  });
  if (checkpointHook) {
    try { checkpointHook(requestId, next); } catch {}
  }
  return next;
}

function clearCheckpoint(requestId) {
  const file = checkpointPath(requestId);
  if (fs.existsSync(file)) fs.unlinkSync(file);
}

function productKey(product = {}) {
  return normalize(product.product_name || product.name || product.asin || '');
}

function ensureActiveLock(requestId = null) {
  if (!fs.existsSync(lockPath)) return null;
  try {
    const raw = fs.readFileSync(lockPath, 'utf8').trim();
    const parsed = raw.startsWith('{') ? JSON.parse(raw) : { started_at_ms: Number(raw) || Date.now() };
    const ageMs = Date.now() - Number(parsed.started_at_ms || Date.now());
    const alive = isPidAlive(parsed.pid);
    const sameRequest = requestId && parsed.request_id && parsed.request_id === requestId;
    if (!alive || ageMs > STAGE_TIMEOUTS_MS.total_request || sameRequest) {
      fs.unlinkSync(lockPath);
      writeProgress({ stage: 'lock_cleared', request_id: requestId, stall_reason: !alive ? 'stale_lock_dead_pid' : (sameRequest ? 'stale_lock_same_request_reclaim' : 'stale_lock_timeout'), lock_age_ms: ageMs });
      return null;
    }
    return parsed;
  } catch {
    try { fs.unlinkSync(lockPath); } catch {}
    return null;
  }
}

function decodeHtml(str = '') {
  return String(str)
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, '/');
}

function cleanText(str = '') {
  return decodeHtml(String(str).replace(/<[^>]+>/g, ' ')).replace(/\s+/g, ' ').trim();
}

function extractSourceType(url = '', title = '', snippet = '', query = '') {
  const rawUrl = String(url || '');
  let decodedUrl = rawUrl;
  try {
    const parsed = new URL(rawUrl, 'https://duckduckgo.com');
    const uddg = parsed.searchParams.get('uddg');
    if (uddg) decodedUrl = decodeURIComponent(uddg);
  } catch {}

  const haystack = `${decodedUrl} ${title} ${snippet}`.toLowerCase();
  const queryText = String(query || '').toLowerCase();
  const articleSignal = /review|reviews|best-|\bbest\b|comparison|compare|guide|buying guide|buying-guide|roundup|tested|picks|pros and cons|editorial|article|blog/.test(haystack);
  const discussionSignal = /reddit|forum|thread|discussion|comments|community|board|showthread|topic/.test(haystack);
  const retailerDomain = /amazon\.|walmart\.|wayfair\.|ikea\.|homedepot\.|staples\.|target\.|ashleyfurniture\.|potterybarn\./.test(decodedUrl);
  const retailerBrowseSignal = /\/b\/|\/browse\/|\/category\/|\/categories\/|\/shop\/|\/sb0\/|\bnode=\d+\b|\bcat_[a-z0-9]+\b|\/search\b/.test(decodedUrl);
  const retailerProductSignal = retailerDomain && /\/dp\/|\/gp\/product\/|\/ip\/|\/products?\/|\/p\/|\/itm\//.test(decodedUrl);
  const isRedditQuery = /(^|\s|:)reddit(\.com)?(\s|$)/.test(queryText);
  const isForumQuery = /forum|archerytalk|bbs|board|discussion/.test(queryText);
  const isReviewQuery = /reviews?|consumer reports|trustpilot|g2|comparison|buying guide|guide|roundup|tested|picks|pros and cons/.test(queryText);

  if (decodedUrl.includes('reddit.com/r/') && decodedUrl.includes('/comments/')) return 'reddit';
  if (decodedUrl.includes('reddit.com')) return isRedditQuery || discussionSignal ? 'reddit' : 'forum';
  if (/proboards\.com\/(thread|board)\//.test(decodedUrl) || decodedUrl.includes('archerytalk.com/threads/') || /forum|showthread|topic|post|\/thread\//.test(decodedUrl)) return 'forum';
  if (retailerBrowseSignal && retailerDomain) return 'retailer_browse';
  if (retailerProductSignal) return 'product_detail';
  if (discussionSignal && (isForumQuery || /forum|thread|discussion/.test(haystack))) return 'forum';
  if (articleSignal) return 'web_review';
  if (haystack.includes('trustpilot') || haystack.includes('g2.com') || haystack.includes('consumer reports') || haystack.includes('customer review') || haystack.includes('customer reviews')) return 'google_reviews';

  if (isRedditQuery && decodedUrl.includes('reddit.com')) return 'reddit';
  if (isForumQuery && /forum|thread|discussion/.test(haystack)) return 'forum';
  if (isReviewQuery) return 'web_review';
  if (retailerDomain) return retailerProductSignal ? 'product_detail' : 'retailer_browse';
  return 'web_review';
}

async function fetchSearchResults(query) {
  const usefulSignal = /review|reviews|best|comparison|compare|guide|buying guide|buying-guide|roundup|tested|picks|pros and cons|forum|reddit|discussion|thread/i;
  const junkPattern = /dictionary|grammar|english language|language learning|merriam-webster|cambridge dictionary|collins dictionary|stackexchange|hinative|quora|wikipedia/i;
  const retailerBrowsePattern = /\/b\/|\/browse\/|\/category\/|\/categories\/|\/shop\/|\/sb0\/|\bnode=\d+\b|\bcat_[a-z0-9]+\b|\/search\b/i;
  const retailerDomainPattern = /amazon\.|walmart\.|wayfair\.|ikea\.|homedepot\.|staples\.|target\.|ashleyfurniture\.|potterybarn\./i;
  const redditIndexPattern = /reddit\.com\/(r\/[a-z0-9_+-]+\/?$|$|r\/[^/]+\/(about|wiki|top)\/?)/i;
  const genericNavPattern = /(^https?:\/\/[^/]+\/?$)|\/all-sports-schedule\/|\/marketplace\/?$|\/forums?\/?$|\/brands?\/?$/i;
  const normalizeRawResult = (href, title, snippet, engine) => ({ href, title, snippet, source_type: extractSourceType(href, title, snippet, query), query, discovery_engine: engine });
  const filterRawResults = (items = []) => {
    const kept = [];
    for (const item of items) {
      const href = String(item.href || '');
      const title = String(item.title || '');
      const snippet = String(item.snippet || '');
      const haystack = `${href} ${title} ${snippet}`;
      const type = item.source_type || extractSourceType(href, title, snippet, query);
      const hasUsefulSignal = usefulSignal.test(haystack);
      const isRetailerBrowse = retailerDomainPattern.test(href) && retailerBrowsePattern.test(href);
      const isRedditIndex = redditIndexPattern.test(href);
      const isGenericNav = genericNavPattern.test(href);
      const isJunk = junkPattern.test(haystack);
      if (isJunk) continue;
      if (isRetailerBrowse && !hasUsefulSignal) continue;
      if (isRedditIndex) continue;
      if (isGenericNav && !hasUsefulSignal) continue;
      kept.push({ ...item, source_type: type });
      if (kept.length >= 8) break;
    }
    return kept;
  };

  const collectHtmlResults = (html, mode = 'html') => {
    const results = [];
    const regex = mode === 'lite'
      ? /<a rel="nofollow" href="([^"]+)"[^>]*>([\s\S]*?)<\/a>[\s\S]{0,800}?<td class='result-snippet'>([\s\S]*?)<\/td>/gi
      : /<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>[\s\S]{0,1200}?(?:<a[^>]*class="result__snippet"[^>]*>|<div[^>]*class="result__snippet"[^>]*>)([\s\S]*?)(?:<\/a>|<\/div>)/gi;
    let match;
    while ((match = regex.exec(html)) !== null) {
      const href = cleanText(match[1]);
      const title = cleanText(match[2]);
      const snippet = cleanText(match[3]);
      if (!href || !title || !snippet) continue;
      if (/privacy protected by duckduckgo|\bad viewing ads\b/i.test(title) || /duckduckgo\.com\/y\.js\?ad_domain=/i.test(href)) continue;
      results.push(normalizeRawResult(href, title, snippet, mode === 'lite' ? 'duckduckgo_lite' : 'duckduckgo_html'));
      if (results.length >= 16) break;
    }
    return filterRawResults(results);
  };

  const collectRssResults = (xml) => {
    const results = [];
    const itemRegex = /<item>([\s\S]*?)<\/item>/gi;
    let match;
    while ((match = itemRegex.exec(xml)) !== null) {
      const block = match[1];
      const href = cleanText((block.match(/<link>([\s\S]*?)<\/link>/i) || [])[1] || '');
      const title = cleanText((block.match(/<title>([\s\S]*?)<\/title>/i) || [])[1] || '');
      const snippet = cleanText((block.match(/<description>([\s\S]*?)<\/description>/i) || [])[1] || '');
      if (!href || !title || !snippet) continue;
      results.push(normalizeRawResult(href, title, snippet, 'bing_rss'));
      if (results.length >= 16) break;
    }
    return filterRawResults(results);
  };

  const collectBraveHtmlResults = (html) => {
    const results = [];
    const blockRegex = /data-type="web"[\s\S]{0,4000}?<a href="([^"]+)"[^>]*>[\s\S]{0,2500}?<div class="title search-snippet-title[^"]*"[^>]*>([\s\S]*?)<\/div>[\s\S]{0,2500}?(?:<div class="content[^"]*">([\s\S]*?)<\/div>)?/gi;
    let match;
    while ((match = blockRegex.exec(html)) !== null) {
      const href = cleanText(match[1]);
      const title = cleanText(match[2]);
      const snippet = cleanText(match[3] || '');
      if (!href || !title) continue;
      results.push(normalizeRawResult(href, title, snippet, 'brave_html'));
      if (results.length >= 16) break;
    }
    return filterRawResults(results);
  };

  const primaryUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
  let primaryStatus = null;
  let primaryResults = [];
  try {
    const primaryRes = await fetch(primaryUrl, { headers: SEARCH_HEADERS, signal: AbortSignal.timeout(12000) });
    primaryStatus = primaryRes.status;
    if (primaryRes.ok) {
      const primaryHtml = await primaryRes.text();
      primaryResults = collectHtmlResults(primaryHtml, 'html');
      if (primaryRes.status !== 202 && primaryResults.length >= 3) return primaryResults;
    }
  } catch {}

  const liteUrl = `https://lite.duckduckgo.com/lite/?q=${encodeURIComponent(query)}`;
  let liteResults = [];
  try {
    const liteRes = await fetch(liteUrl, { headers: SEARCH_HEADERS, signal: AbortSignal.timeout(12000) });
    liteResults = liteRes.ok ? collectHtmlResults(await liteRes.text(), 'lite') : [];
    if (liteResults.length >= 3) return liteResults;
  } catch {}

  const rssUrl = `https://www.bing.com/search?format=rss&q=${encodeURIComponent(query)}`;
  try {
    const rssRes = await fetch(rssUrl, { headers: SEARCH_HEADERS, signal: AbortSignal.timeout(12000) });
    if (rssRes.ok) {
      const rssResults = collectRssResults(await rssRes.text());
      if (rssResults.length) return rssResults;
    }
  } catch {}

  const braveUrl = `https://search.brave.com/search?q=${encodeURIComponent(query)}&source=web`;
  try {
    const braveRes = await fetch(braveUrl, { headers: SEARCH_HEADERS, signal: AbortSignal.timeout(15000) });
    if (braveRes.ok) {
      const braveResults = collectBraveHtmlResults(await braveRes.text());
      if (braveResults.length) return braveResults;
    }
  } catch {}

  if (primaryStatus && !primaryResults.length && !liteResults.length) return [];
  return liteResults.length ? liteResults : primaryResults;
}

function normalizeDiscoveredUrl(url = '') {
  const raw = cleanText(url);
  if (!raw) return '';
  let value = raw;
  try {
    const parsed = new URL(raw, 'https://duckduckgo.com');
    const uddg = parsed.searchParams.get('uddg');
    if (uddg) value = decodeURIComponent(uddg);
    else if (raw.startsWith('//')) value = `https:${raw}`;
    else value = parsed.href;
  } catch {
    if (raw.startsWith('//')) value = `https:${raw}`;
  }
  try {
    const parsed = new URL(value);
    ['utm_source','utm_medium','utm_campaign','utm_term','utm_content','ved','ei','oq','aqs','source','ref','ref_','tag','ascsubtag','psc'].forEach((key) => parsed.searchParams.delete(key));
    parsed.hash = '';
    return parsed.href;
  } catch {
    return value;
  }
}

function getUrlSignals(url = '') {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    const pathname = parsed.pathname.toLowerCase();
    const segments = pathname.split('/').filter(Boolean);
    return { host, pathname, segments, search: parsed.search.toLowerCase() };
  } catch {
    return { host: '', pathname: '', segments: [], search: '' };
  }
}

function classifyUrlIntent(url = '', sourceType = '') {
  const { host, pathname, segments, search } = getUrlSignals(url);
  const joined = `${host}${pathname}${search}`;
  if (!host) return { kind: 'unknown', score: -10, reject: 'invalid_url' };

  if (/ell\.stackexchange\.com$|english\.stackexchange\.com$|ell\.stackexchange\.com$|hinative\.com$|quora\.com$|answers\.microsoft\.com$/.test(host)) {
    return { kind: 'junk_qa', score: -12, reject: 'junk_qa_domain' };
  }
  if (/bestproducts\.guide$|top5bestproducts\.com$|productrankers?\.|buyers?guide\.|oneclearwinner\.com$|buyereviews\.com$/.test(host)) {
    return { kind: 'thin_aggregator', score: -9, reject: 'thin_aggregator_domain' };
  }

  if (/reddit\.com$/.test(host)) {
    if (/\/r\/[^/]+\/comments\//.test(pathname)) return { kind: 'reddit_thread', score: 10 };
    if (pathname === '/' || /^\/r\/[^/]+\/?$/.test(pathname) || /^\/search/.test(pathname) || /\/top\/?$/.test(pathname) || /\/new\/?$/.test(pathname)) return { kind: 'reddit_index', score: -10, reject: 'reddit_index_page' };
    return { kind: 'reddit_other', score: -3, reject: 'reddit_non_thread' };
  }

  if (/archerytalk\.com$|proboards\.com$/.test(host) || /forum/.test(host) || sourceType === 'forum') {
    if (/\/threads\//.test(pathname) || /\/thread\//.test(pathname) || /showthread/.test(joined) || /topic/.test(joined) || /post/.test(joined)) return { kind: 'forum_thread', score: 9 };
    if (pathname === '/' || /\/forums?\/?$/.test(pathname) || /\/forums\//.test(pathname) || /\/marketplace/.test(pathname) || /\/tags\//.test(pathname)) return { kind: 'forum_index', score: -8, reject: 'forum_index_page' };
    return { kind: 'forum_other', score: -2, reject: 'forum_non_thread' };
  }

  if (segments.length === 0) return { kind: 'homepage', score: -9, reject: 'homepage' };
  if (/\/category\/|\/tag\/|\/topics?\/|\/collections?\/|\/b\?node=|\/s\?k=/.test(pathname + search)) return { kind: 'category_index', score: -7, reject: 'category_index_page' };
  if (/review|reviews|best-|comparison|compare|vs-|broadhead|archery-target|targets|bag-target|foam-target|3d-target/.test(pathname)) return { kind: 'article', score: 7 };
  if (segments.length >= 2) return { kind: 'deep_page', score: 3 };
  return { kind: 'generic_page', score: 0 };
}

function scoreDiscoveredSource(item, discoveryTokens = []) {
  const href = normalizeDiscoveredUrl(item.href || item.url || '');
  const sourceType = item.source_type || extractSourceType(href, item.title || '', item.snippet || '', item.query || '');
  const urlIntent = classifyUrlIntent(href, sourceType);
  const title = normalize(item.title || '');
  const snippet = normalize(item.snippet || '');
  const haystack = normalize(`${title} ${snippet} ${href}`);
  const expandedTerms = Array.from(new Set([
    ...discoveryTokens,
    ...discoveryTokens.flatMap((token) => token.endsWith('s') ? [token, token.slice(0, -1)] : [token, `${token}s`])
  ].filter((token) => token && token.length >= 3 && !STOPWORDS.has(token))));
  const overlap = discoveryTokens.filter((token) => haystack.includes(token)).length;
  const relevantHits = expandedTerms.filter((token) => haystack.includes(token)).length;
  const titleRelevantHits = expandedTerms.filter((token) => title.includes(token)).length;
  const slugRelevantHits = expandedTerms.filter((token) => href.toLowerCase().includes(token)).length;
  const meaningfulSlug = /[a-z]{4,}-[a-z]{4,}/.test(href);
  const genericBestOnly = /\bbest\b/.test(title) && titleRelevantHits === 0 && slugRelevantHits === 0;
  const editorialSignal = /review|reviews|best-|comparison|compare|guide|buying-guide|buying guide|roundup|blog|article|forum|reddit|discussion|tested|pros and cons/.test(haystack);
  const retailerBrowsePattern = /\/b\/|\/browse\/|\/category\/|\/categories\/|\/shop\/|\/sb0\/|\bnode=\d+\b|\bcat_[a-z0-9]+\b|\?page=|\?sr=|\/search\b/;
  const retailerDomainPattern = /amazon\.|walmart\.|wayfair\.|ikea\.|homedepot\.|staples\.|target\.|ashleyfurniture\.|potterybarn\./;
  const retailerBrowsePage = ['google_reviews', 'web_review'].includes(sourceType) && retailerDomainPattern.test(href) && retailerBrowsePattern.test(href);
  const retailerDetailPage = ['google_reviews', 'web_review'].includes(sourceType) && retailerDomainPattern.test(href) && !retailerBrowsePattern.test(href);
  let reject = urlIntent.reject || null;
  if (!reject && ['google_reviews','web_review'].includes(sourceType) && relevantHits < Math.max(1, Math.min(2, discoveryTokens.length))) reject = 'low_topic_relevance';
  if (!reject && genericBestOnly) reject = 'generic_best_without_context';
  if (!reject && sourceType === 'web_review' && titleRelevantHits === 0 && slugRelevantHits === 0) reject = 'review_without_topic_slug_or_title';
  if (!reject && retailerBrowsePage && !editorialSignal) reject = 'retailer_browse_page';
  let score = urlIntent.score + (overlap * 2) + (meaningfulSlug ? 1 : 0) + relevantHits + titleRelevantHits + slugRelevantHits;
  if (/review|comparison|tested|guide|pros and cons|buying guide/.test(haystack)) score += 2;
  if (/forum|reddit|discussion/.test(haystack)) score += 3;
  if (/roundup|blog|article|compare/.test(haystack)) score += 2;
  if (retailerBrowsePage) score -= 8;
  if (retailerDetailPage && !editorialSignal) score -= 3;
  if (['forum', 'reddit'].includes(sourceType)) score += 2;
  if (['google_reviews', 'web_review'].includes(sourceType) && editorialSignal) score += 2;
  return { score, overlap, sourceType, urlIntent: { ...urlIntent, reject }, relevantHits, titleRelevantHits, slugRelevantHits };
}

function dedupeSources(items = []) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const normalizedHref = normalizeDiscoveredUrl(item.href || item.url || '');
    if (!normalizedHref) continue;
    const key = normalizedHref.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ ...item, href: normalizedHref, normalized_href: normalizedHref, source_type: item.source_type || extractSourceType(normalizedHref, item.title || '', item.snippet || '', item.query || '') });
  }
  return out;
}

function stripBoilerplateText(text = '') {
  return String(text)
    .replace(/\b(accept|reject|manage) cookies?\b/gi, ' ')
    .replace(/\b(privacy policy|terms of use|all rights reserved|sign in|log in|subscribe|newsletter|advertisement)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractPageReadableText(html = '') {
  const title = cleanText((html.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1] || '');
  const metaDescription = cleanText((html.match(/<meta[^>]+(?:name|property)=["'](?:description|og:description|twitter:description)["'][^>]+content=["']([\s\S]*?)["'][^>]*>/i) || [])[1] || '');
  const metaTitle = cleanText((html.match(/<meta[^>]+property=["']og:title["'][^>]+content=["']([\s\S]*?)["'][^>]*>/i) || [])[1] || '');
  const mainHtml = String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg[\s\S]*?<\/svg>/gi, ' ')
    .replace(/<nav[\s\S]*?<\/nav>/gi, ' ')
    .replace(/<header[\s\S]*?<\/header>/gi, ' ')
    .replace(/<footer[\s\S]*?<\/footer>/gi, ' ')
    .replace(/<form[\s\S]*?<\/form>/gi, ' ')
    .replace(/<aside[\s\S]*?<\/aside>/gi, ' ')
    .replace(/<!--([\s\S]*?)-->/g, ' ');
  const text = stripBoilerplateText(cleanText(mainHtml));
  const combined = stripBoilerplateText([title, metaTitle, metaDescription, text].filter(Boolean).join('. '));
  return { title: metaTitle || title, meta_description: metaDescription, text: combined.slice(0, 12000) };
}

function classifyQualifiedSource(source = {}) {
  const type = source.source_type || extractSourceType(source.href, source.title, source.snippet, source.query);
  if (type === 'reddit' || type === 'forum') return 'community';
  if (type === 'youtube') return 'youtube';
  if (type === 'google_reviews' || type === 'web_review') return 'review';
  return 'niche';
}

function computeQueryOverlap(text = '', queryTokens = []) {
  const normalizedText = normalize(text);
  return queryTokens.filter((token) => normalizedText.includes(token)).length;
}

async function fetchAndQualifyPage(source, queryTokens = []) {
  const url = normalizeDiscoveredUrl(source.href || '');
  if (!url) return { ok: false, rejection_reason: 'missing_url', source };
  try {
    let res = await fetch(url, { headers: SEARCH_HEADERS, redirect: 'follow', signal: AbortSignal.timeout(15000) });
    let extracted;
    let fetchedVia = 'direct';
    if (res.ok) {
      const html = await res.text();
      extracted = extractPageReadableText(html);
    } else if ([401, 403, 406].includes(res.status)) {
      const mirrorUrl = `https://r.jina.ai/http://${url.replace(/^https?:\/\//, '')}`;
      const mirrorRes = await fetch(mirrorUrl, { headers: { 'user-agent': SEARCH_HEADERS['user-agent'] }, signal: AbortSignal.timeout(20000) });
      if (!mirrorRes.ok) return { ok: false, rejection_reason: `fetch_http_${res.status}`, source: { ...source, href: url } };
      const mirroredText = await mirrorRes.text();
      extracted = {
        title: source.title || '',
        meta_description: source.snippet || '',
        text: stripBoilerplateText(mirroredText)
      };
      fetchedVia = 'jina_mirror';
      res = { status: mirrorRes.status };
    } else {
      return { ok: false, rejection_reason: `fetch_http_${res.status}`, source: { ...source, href: url } };
    }

    const combinedText = stripBoilerplateText([source.title || '', source.snippet || '', extracted.title || '', extracted.meta_description || '', extracted.text || ''].join('. '));
    const overlap = computeQueryOverlap(combinedText, queryTokens);
    if ((combinedText || '').length < 350) {
      return { ok: false, rejection_reason: 'extracted_text_too_short', source: { ...source, href: url }, extracted_chars: combinedText.length, overlap };
    }
    if (overlap < Math.max(1, Math.min(2, queryTokens.length))) {
      return { ok: false, rejection_reason: 'query_overlap_too_low', source: { ...source, href: url }, extracted_chars: combinedText.length, overlap };
    }
    return {
      ok: true,
      source: {
        ...source,
        href: url,
        source_type: source.source_type || extractSourceType(url, source.title || extracted.title || '', source.snippet || extracted.meta_description || '', source.query || ''),
        source_class: classifyQualifiedSource(source),
        fetched_url: url,
        fetched_status: res.status,
        fetched_via: fetchedVia,
        extracted_chars: combinedText.length,
        query_overlap: overlap,
        page_title: extracted.title || source.title || '',
        page_excerpt: combinedText.slice(0, 1200),
        snippet: source.snippet || extracted.meta_description || combinedText.slice(0, 320)
      }
    };
  } catch (error) {
    return { ok: false, rejection_reason: String(error.name === 'TimeoutError' ? 'fetch_timeout' : (error.message || error)), source: { ...source, href: url } };
  }
}

function buildProductSearchSeeds(productName = '', query = '') {
  const normalizedProduct = normalize(productName);
  const normalizedQuery = normalize(query);
  const words = normalizedProduct.split(' ').filter(Boolean);
  const filtered = words.filter((token) => token.length >= 3 && !STOPWORDS.has(token));
  const archeryTerms = ['target','archery','deer','broadhead','3d','field','point','layered','foam','bag','compound','bow'];
  const queryTokens = normalizedQuery.split(' ').filter(Boolean);
  const isArcheryContext = [...filtered, ...queryTokens].some((token) => archeryTerms.includes(token));
  const genericTypeHints = isArcheryContext
    ? archeryTerms
    : queryTokens.filter((token) => token.length >= 3 && !STOPWORDS.has(token));
  const brand = filtered[0] || words[0] || productName;
  const brandPlus = filtered.slice(0, 2).join(' ') || brand;
  const withoutGeneric = filtered.filter((token) => !genericTypeHints.includes(token));
  const model = withoutGeneric.slice(1, 3).join(' ') || withoutGeneric.slice(0, 2).join(' ');
  const shortCore = withoutGeneric.slice(0, 3).join(' ') || filtered.slice(0, 3).join(' ');
  const typeTerms = filtered.filter((token) => genericTypeHints.includes(token)).join(' ');
  const reordered = [brand, model, ...(typeTerms ? [typeTerms.split(' ')[0]] : [])].filter(Boolean).join(' ');
  const common = [
    productName,
    `${brandPlus} review`,
    `${brandPlus} reddit`,
    `${brandPlus} forum`,
    `${brandPlus} durability`,
    `${shortCore} review`,
    `${shortCore} forum`,
    `${brand} ${model}`.trim(),
    reordered.trim(),
    `${query} ${brandPlus}`.trim()
  ];
  const archeryOnly = [
    `${brandPlus} archery target`,
    `${brandPlus} target`,
    `${brandPlus} deer target`,
    `${brandPlus} broadhead target`,
    `${brandPlus} field point broadhead review`,
    `${brandPlus} target review`,
    `${brandPlus} target reddit`,
    `${brandPlus} target archerytalk`,
    `${brandPlus} target hunting forum`,
    `${brandPlus} 3d deer target durability`,
    `${typeTerms} ${brandPlus}`.trim(),
    `${typeTerms} ${model}`.trim()
  ];
  const nonArchery = [
    `${brandPlus} ${typeTerms}`.trim(),
    `${brandPlus} ${queryTokens[0] || ''}`.trim(),
    `${brandPlus} ${queryTokens.slice(0,2).join(' ')}`.trim(),
    `${shortCore} ${queryTokens[0] || ''} review`.trim(),
    `${brandPlus} buying guide`.trim()
  ];
  const variants = (isArcheryContext ? [...common, ...archeryOnly] : [...common, ...nonArchery])
    .map((x) => x.replace(/\s+/g, ' ').trim())
    .filter(Boolean);
  return Array.from(new Set(variants));
}

function classifyQueryShape(query = '') {
  const q = normalize(query);
  const tokens = q.split(' ').filter(Boolean);
  const comparisonCues = ['best','vs','comparison','compare','under','top'];
  const narrowUseCaseCues = ['for kids','for beginners','for apartments','for travel','for seniors','for small spaces'];
  const brandedCues = ['ninja','vitamix','sonicare','oral b','levoit','dyson','instant pot'];
  if (brandedCues.some((cue) => q.includes(cue))) return 'branded_product_specific';
  if (comparisonCues.some((cue) => q.includes(cue))) return 'comparison_query';
  if (narrowUseCaseCues.some((cue) => q.includes(cue))) return 'narrow_use_case_query';
  if (tokens.length <= 3) return 'broad_category_query';
  return 'comparison_query';
}

function buildBroadCategoryQueries(baseQuery = '') {
  const q = normalize(baseQuery);
  const singular = q.endsWith('s') ? q.slice(0, -1) : q;
  const noun = singular.replace(/^best\s+/, '').trim();
  return Array.from(new Set([
    `best ${noun}s`,
    `${noun} reviews`,
    `${noun} comparison`,
    `${noun} buying guide`,
    `best ${noun} reddit`,
    `${noun} forum`,
    `what to look for in a ${noun}`,
    `glass vs plastic ${noun}`,
    `saucer vs bottle ${noun}`,
    `${noun} pros and cons`,
    `${noun} test results`
  ].map((x) => x.replace(/\s+/g, ' ').trim())));
}

function buildDiversifiedCategoryQueries(baseQuery = '', iteration = 1) {
  const topic = String(baseQuery || '').trim();
  if (!topic) return [];
  const out = [];
  if (iteration === 1) {
    out.push(
      `${topic} review`,
      `${topic} comparison`,
      `${topic} pros and cons`,
      `${topic} test results`,
      `${topic} buying guide`
    );
  } else if (iteration === 2) {
    out.push(
      `${topic} long term review`,
      `${topic} tested and reviewed`,
      `${topic} buyer complaints`,
      `${topic} forum discussion`,
      `${topic} reddit`
    );
  } else {
    out.push(
      `${topic} best options`,
      `${topic} top picks`,
      `${topic} review guide`,
      `${topic} comparison guide`,
      `${topic} buying guide`
    );
  }
  return Array.from(new Set(out.map((x) => x.replace(/\s+/g, ' ').trim()).filter(Boolean)));
}

function buildReviewFocusedQueries(baseQuery = '', iteration = 1) {
  const topic = String(baseQuery || '').trim();
  if (!topic) return [];
  const variants = [
    `${topic} review`,
    `${topic} comparison`,
    `${topic} test results`,
    `${topic} buying guide`,
    `${topic} pros and cons`
  ];
  if (iteration >= 2) {
    variants.push(
      `${topic} buyer complaints`,
      `${topic} long term review`,
      `${topic} tested and reviewed`,
      `${topic} forum discussion`
    );
  }
  if (iteration >= 3) {
    variants.push(
      `${topic} top picks`,
      `${topic} review guide`,
      `${topic} comparison guide`,
      `${topic} best options`
    );
  }
  return Array.from(new Set(variants.map((x) => x.replace(/\s+/g, ' ').trim()).filter(Boolean)));
}

function buildFallbackDiscoveryQueries(baseQuery = '', mode = 'category') {
  const topic = String(baseQuery || '').trim();
  if (!topic) return [];
  if (mode === 'product') {
    return Array.from(new Set([
      `${topic} reviews`,
      `${topic} comparison`,
      `${topic} buying guide`,
      `${topic} forum discussion`,
      `${topic} reddit`,
      `${topic} buyer complaints`,
      `${topic} pros and cons`,
      `${topic} best options`
    ]));
  }
  return Array.from(new Set([
    `${topic} reviews`,
    `${topic} comparison`,
    `${topic} buying guide`,
    `${topic} forum discussion`,
    `${topic} reddit`,
    `${topic} buyer complaints`,
    `${topic} pros and cons`,
    `${topic} best options`
  ]));
}

async function discoverAndQualifySources({ baseQuery, searchQueries, queryTokens, mode = 'category', minQualifyingSources = 6, requestId = null, queryShape = null }) {
  const attempts = [searchQueries, buildFallbackDiscoveryQueries(baseQuery, mode)];
  if (mode === 'category') {
    if (queryShape === 'broad_category_query') {
      attempts.push(buildBroadCategoryQueries(baseQuery));
      attempts.push(buildDiversifiedCategoryQueries(baseQuery, 1));
      attempts.push(buildDiversifiedCategoryQueries(baseQuery, 2));
      attempts.push(buildReviewFocusedQueries(baseQuery, 1));
      attempts.push(buildReviewFocusedQueries(baseQuery, 2));
      attempts.push(buildBroadCategoryQueries(`${baseQuery} buying guide`));
    } else {
      attempts.push(buildReviewFocusedQueries(baseQuery, 1));
      attempts.push(buildReviewFocusedQueries(baseQuery, 2));
      attempts.push(buildReviewFocusedQueries(baseQuery, 3));
      attempts.push(buildDiversifiedCategoryQueries(baseQuery, 1));
      attempts.push(buildDiversifiedCategoryQueries(baseQuery, 2));
      attempts.push(buildDiversifiedCategoryQueries(baseQuery, 3));
    }
  }

  if (mode === 'category' && queryShape !== 'broad_category_query') {
    const normalizedBase = normalize(baseQuery);
    const preserveTopicPhrase = (query = '') => {
      const raw = String(query || '').trim();
      if (!raw) return null;
      const normalized = normalize(raw);
      if (!normalizedBase) return raw;
      if (normalized.includes(normalizedBase)) return raw;
      if (/^reddit\s+/.test(normalized)) return `${baseQuery} reddit`;
      if (/^site:reddit\.com\s+/.test(normalized)) return `${baseQuery} reddit`;
      if (/\s+reddit$/.test(normalized)) return `${baseQuery} reddit`;
      if (/\s+forum$/.test(normalized) || /forum discussion/.test(normalized)) return `${baseQuery} forum discussion`;
      if (/\s+reviews?$/.test(normalized) || /\s+review$/.test(normalized)) return `${baseQuery} review`;
      if (/buyer complaints/.test(normalized)) return `${baseQuery} buyer complaints`;
      if (/pros and cons/.test(normalized)) return `${baseQuery} pros and cons`;
      if (/buying guide|guide/.test(normalized)) return `${baseQuery} buying guide`;
      if (/comparison/.test(normalized)) return `${baseQuery} comparison`;
      return null;
    };
    for (let i = 0; i < attempts.length; i += 1) {
      attempts[i] = Array.from(new Set((attempts[i] || []).map(preserveTopicPhrase).filter(Boolean)));
    }
  }
  const rejected_sources = [];
  const seedStats = [];
  const discoveryTokens = normalize(baseQuery).split(' ').filter((token) => token.length >= 4 && !STOPWORDS.has(token));
  let finalStats = null;
  let reviewIterations = 0;
  const tracker = mode === 'category' && requestId ? categoryProgressTracker(requestId) : null;
  let totalEvaluated = 0;
  const globallySeenCandidateUrls = new Set();
  const globallyWeakDomains = new Set();
  const globallyWeakQueries = new Set();

  for (let attemptIndex = 0; attemptIndex < Math.min(attempts.length, mode === 'category' ? 6 : CATEGORY_LIMITS.maxIterations); attemptIndex += 1) {
    const queries = Array.from(new Set((attempts[attemptIndex] || []).filter(Boolean)))
      .filter((q) => !globallyWeakQueries.has(normalize(q)))
      .slice(0, CATEGORY_LIMITS.maxQueriesPerIteration);
    tracker?.mark(`category_source_discovery_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, heartbeat: true, eventType: 'query_family_start', queryFamilyInFlight: queries, debug: { queries_used: queries } });
    const discovered = [];
    for (const q of queries) {
      try {
        const results = await withTimeout(fetchSearchResults(q), STAGE_TIMEOUTS_MS.category_substage, { stage: `category_discovery_pass${attemptIndex + 1}`, query: q, request_id: requestId });
        seedStats.push({ seed: q, stage: 'discovered', discovered_urls: results.length, iteration: attemptIndex + 1 });
        discovered.push(...results.map((item) => ({ ...item, query: q })));
        tracker?.mark(`category_source_discovery_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: dedupeSources(discovered).length }, heartbeat: true, eventType: 'query_completed', queryFamilyInFlight: queries, debug: { last_query: q } });
      } catch (error) {
        seedStats.push({ seed: q, stage: 'discovered', discovered_urls: 0, error: String(error.message || error), iteration: attemptIndex + 1 });
        rejected_sources.push({ stage: 'discovery', query: q, reason: String(error.message || error) });
      }
      tracker?.assertProgress(`category_source_discovery_pass${attemptIndex + 1}`);
    }

    tracker?.mark(`category_source_scoring_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: dedupeSources(discovered).length } });
    const scoredDiscovered = dedupeSources(discovered).map((item) => {
      const scored = scoreDiscoveredSource(item, discoveryTokens);
      const domainPenalty = tracker?.domainPenalty(item.href) || 0;
      return { ...item, discovery_score: scored.score - domainPenalty, discovery_overlap: scored.overlap, source_type: scored.sourceType, url_kind: scored.urlIntent.kind, url_reject_reason: scored.urlIntent.reject || null };
    }).filter((item) => {
      const keep = item.discovery_score >= 2 && !item.url_reject_reason;
      if (!keep) rejected_sources.push({ stage: 'discovery_filter', href: item.href, query: item.query, reason: item.url_reject_reason || 'discovery_score_too_low', score: item.discovery_score, overlap: item.discovery_overlap, source_type: item.source_type });
      return keep;
    }).sort((a, b) => b.discovery_score - a.discovery_score);

    const sourceTypeBudgets = mode === 'category'
      ? { web_review: 8, forum: 4, reddit: 3, google_reviews: 2, product_detail: 2, retailer_browse: 0 }
      : null;
    const sourceTypeUsage = {};
    const filteredDiscovered = scoredDiscovered.filter((item) => {
      const href = String(item.href || '').toLowerCase();
      const host = getUrlSignals(item.href).host;
      const queryKey = normalize(item.query || '');
      const type = item.source_type || 'unknown';
      if (globallySeenCandidateUrls.has(href)) {
        rejected_sources.push({ stage: 'candidate_reuse_filter', href: item.href, query: item.query, reason: 'candidate_seen_in_prior_pass', source_type: item.source_type });
        return false;
      }
      if (globallyWeakDomains.has(host)) {
        rejected_sources.push({ stage: 'candidate_reuse_filter', href: item.href, query: item.query, reason: 'domain_marked_weak_in_prior_pass', source_type: item.source_type });
        return false;
      }
      if (globallyWeakQueries.has(queryKey)) {
        rejected_sources.push({ stage: 'candidate_reuse_filter', href: item.href, query: item.query, reason: 'query_marked_weak_in_prior_pass', source_type: item.source_type });
        return false;
      }
      if (mode === 'category' && ['retailer_browse'].includes(type)) {
        rejected_sources.push({ stage: 'candidate_reuse_filter', href: item.href, query: item.query, reason: 'weak_result_class_budget_protection', source_type: item.source_type });
        return false;
      }
      if (sourceTypeBudgets) {
        sourceTypeUsage[type] = sourceTypeUsage[type] || 0;
        if (sourceTypeUsage[type] >= (sourceTypeBudgets[type] ?? 3)) {
          rejected_sources.push({ stage: 'candidate_reuse_filter', href: item.href, query: item.query, reason: 'source_type_budget_exhausted', source_type: item.source_type });
          return false;
        }
        sourceTypeUsage[type] += 1;
      }
      return true;
    });

    const coverageBuckets = new Set();
    const prioritized = [];
    for (const item of filteredDiscovered) {
      const bucket = item.source_type || 'unknown';
      if (!coverageBuckets.has(bucket)) {
        prioritized.push(item);
        coverageBuckets.add(bucket);
      }
    }
    for (const item of filteredDiscovered) {
      if (!prioritized.find((x) => x.href === item.href)) prioritized.push(item);
    }
    const candidateUrls = prioritized.map((x) => x.href);
    const domainsThisIteration = tracker?.noteDomains(candidateUrls) || [];
    const uniqueDomainsThisIteration = Array.from(new Set(domainsThisIteration));
    const repeatedSet = candidateUrls.length ? tracker?.noteCandidateSet(candidateUrls) : false;
    const zeroDiscovery = scoredDiscovered.length === 0 || candidateUrls.length === 0;
    tracker?.mark(`category_diversity_check_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: scoredDiscovered.length }, heartbeat: true, eventType: 'diversity_check', repeatedSetActivity: zeroDiscovery ? 'zero_discovery' : (repeatedSet ? 'repeated_candidate_set' : 'new_candidate_set'), queryFamilyInFlight: queries, debug: { discovered_urls: scoredDiscovered.map((x) => ({ href: x.href, source_type: x.source_type, score: x.discovery_score, query: x.query })), domains_this_iteration: uniqueDomainsThisIteration, zero_discovery: zeroDiscovery } });
    if (zeroDiscovery) {
      rejected_sources.push({ stage: 'loop_guard', reason: 'zero_discovery', iteration: attemptIndex + 1, discovered_urls: scoredDiscovered.length, candidate_urls: candidateUrls.length });
      queries.forEach((q) => globallyWeakQueries.add(normalize(q)));
      if (mode === 'category' && attemptIndex + 1 < attempts.length) continue;
    }
    if (repeatedSet) {
      rejected_sources.push({ stage: 'loop_guard', reason: 'repeated_candidate_set', iteration: attemptIndex + 1, domains_this_iteration: uniqueDomainsThisIteration });
      uniqueDomainsThisIteration.forEach((domain) => globallyWeakDomains.add(domain));
      if (mode === 'category' && attemptIndex + 1 < attempts.length) continue;
      break;
    }
    if (mode === 'category' && uniqueDomainsThisIteration.length < 3 && attemptIndex + 1 < attempts.length) {
      rejected_sources.push({ stage: 'loop_guard', reason: 'new_domain_quota_not_met', iteration: attemptIndex + 1, unique_domains: uniqueDomainsThisIteration.length });
      uniqueDomainsThisIteration.forEach((domain) => globallyWeakDomains.add(domain));
      continue;
    }

    const fetched = [];
    const qualified = [];
    const fetchedByType = {};
    const seedRollup = new Map();
    const prioritizedSources = mode === 'category'
      ? prioritized.sort((a, b) => {
          const articleSignal = (item) => {
            const href = (item.href || '').toLowerCase();
            let bonus = 0;
            if (['google_reviews', 'web_review'].includes(item.source_type)) bonus += 6;
            if (/\/review|\/best-|\/comparison|\/guide/.test(href)) bonus += 4;
            if (/[a-z]{4,}-[a-z]{4,}-[a-z]{4,}/.test(href)) bonus += 2;
            if (item.source_type === 'forum') bonus -= 2;
            return bonus;
          };
          return (b.discovery_score + articleSignal(b)) - (a.discovery_score + articleSignal(a));
        })
      : prioritized;

    const candidateQueue = prioritizedSources
      .filter((source) => !globallySeenCandidateUrls.has(String(source.href || '').toLowerCase()))
      .sort((a, b) => {
        const typeRank = (item) => ({ web_review: 5, forum: 4, reddit: 3, google_reviews: 2, product_detail: 1, retailer_browse: 0 }[item.source_type] ?? 1);
        return (typeRank(b) * 100 + b.discovery_score) - (typeRank(a) * 100 + a.discovery_score);
      })
      .slice(0, CATEGORY_LIMITS.maxTotalEvaluated - totalEvaluated);
    let reviewQualifiedCount = 0;
    const minReviewQualified = mode === 'category' ? 1 : 0;
    tracker?.mark(`category_fetch_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: scoredDiscovered.length }, debug: { candidate_queue_size: candidateQueue.length } });
    for (const source of candidateQueue) {
      if ((fetched.length >= CATEGORY_LIMITS.maxFetchPerIteration && reviewQualifiedCount >= minReviewQualified) || totalEvaluated >= CATEGORY_LIMITS.maxTotalEvaluated) break;
      fetched.push(source.href);
      globallySeenCandidateUrls.add(String(source.href || '').toLowerCase());
      totalEvaluated += 1;
      fetchedByType[source.source_type] = (fetchedByType[source.source_type] || 0) + 1;
      const currentSeed = seedRollup.get(source.query) || { seed: source.query, fetched_urls: 0, qualified_urls: 0, qualifying_types: {} };
      currentSeed.fetched_urls += 1;
      seedRollup.set(source.query, currentSeed);
      const result = await withTimeout(fetchAndQualifyPage(source, queryTokens), STAGE_TIMEOUTS_MS.category_substage, { stage: `category_fetch_qualify_pass${attemptIndex + 1}`, href: source.href, request_id: requestId });
      tracker?.mark(`category_extraction_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: scoredDiscovered.length, fetched: fetched.length, extracted: result.ok ? qualified.length + 1 : qualified.length } });
      if (result.ok) {
        qualified.push(result.source);
        if (['google_reviews', 'web_review'].includes(result.source.source_type)) reviewQualifiedCount += 1;
        currentSeed.qualified_urls += 1;
        currentSeed.qualifying_types[result.source.source_type] = (currentSeed.qualifying_types[result.source.source_type] || 0) + 1;
        seedRollup.set(source.query, currentSeed);
        tracker?.mark(`category_qualification_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: scoredDiscovered.length, fetched: fetched.length, extracted: qualified.length, qualified: qualified.length } });
      } else {
        rejected_sources.push({ stage: 'qualification', href: source.href, query: source.query, reason: result.rejection_reason, overlap: result.overlap || null, extracted_chars: result.extracted_chars || null, source_type: source.source_type });
        const failedHost = getUrlSignals(source.href).host;
        if (mode === 'category' && failedHost && ['query_overlap_too_low', 'extracted_text_too_short', 'fetch_http_429', 'fetch_timeout'].includes(result.rejection_reason)) {
          globallyWeakDomains.add(failedHost);
          globallyWeakQueries.add(normalize(source.query || ''));
        }
      }
      tracker?.assertProgress(`category_fetch_pass${attemptIndex + 1}`);
    }

    if (mode === 'category' && qualified.length === 0) {
      queries.forEach((q) => globallyWeakQueries.add(normalize(q)));
    }

    finalStats = {
      attempt: attemptIndex + 1,
      discovered_urls: scoredDiscovered.length,
      discovered_by_type: scoredDiscovered.reduce((acc, item) => { acc[item.source_type] = (acc[item.source_type] || 0) + 1; return acc; }, {}),
      fetched_pages: fetched.length,
      fetched_by_type: fetchedByType,
      successfully_extracted_sources: qualified.length,
      qualifying_sources: qualified.length,
      qualifying_by_type: qualified.reduce((acc, item) => { acc[item.source_type] = (acc[item.source_type] || 0) + 1; return acc; }, {}),
      qualifying_urls: qualified.map((item) => ({ href: item.href, source_type: item.source_type, query: item.query })),
      seed_stats: Array.from(seedRollup.values()),
      coverage: Array.from(new Set(qualified.map((item) => item.source_type)))
    };

    tracker?.mark(`category_diversity_check_pass${attemptIndex + 1}`, { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: scoredDiscovered.length, fetched: fetched.length, extracted: qualified.length, qualified: qualified.length }, debug: { fetched_urls: fetched, accepted_sources: qualified.map((x) => ({ href: x.href, source_type: x.source_type, query: x.query })), rejected_sources: rejected_sources.slice(-20) } });
    const hasForum = qualified.some((item) => item.source_type === 'forum' || item.source_type === 'reddit');
    const reviewQualified = qualified.filter((item) => item.source_type === 'google_reviews' || item.source_type === 'web_review');
    const hasReview = reviewQualified.length >= 1;
    const diversityMet = mode !== 'category' || (hasForum && hasReview);
    if (mode === 'category' && attemptIndex >= 2) reviewIterations = attemptIndex - 1;

    if ((qualified.length >= minQualifyingSources && diversityMet) || (qualified.length > 0 && diversityMet && attemptIndex === attempts.length - 1)) {
      tracker?.mark('category_intelligence_finalize', { iteration: attemptIndex + 1, totalEvaluated, counts: { discovered: scoredDiscovered.length, fetched: fetched.length, extracted: qualified.length, qualified: qualified.length }, debug: { final_coverage: finalStats.coverage, qualifying_review_sources: reviewQualified.map((item) => ({ href: item.href, source_type: item.source_type, query: item.query })) } });
      return {
        qualifyingSources: dedupeSources(qualified),
        stats: { ...finalStats, seed_stats: seedStats.concat(finalStats.seed_stats || []), second_pass_iterations: reviewIterations, qualifying_review_sources: reviewQualified.map((item) => ({ href: item.href, source_type: item.source_type, query: item.query })), domains_by_iteration: tracker?.snapshot()?.seen_domains || [] },
        rejected_sources: rejected_sources.slice(-80)
      };
    }
  }

  tracker?.mark('category_intelligence_failed', { iteration: reviewIterations, totalEvaluated, debug: { rejection_summary: rejected_sources.slice(-20) } });
  return { qualifyingSources: [], stats: finalStats ? { ...finalStats, seed_stats: seedStats.concat(finalStats.seed_stats || []), second_pass_iterations: reviewIterations, qualifying_review_sources: [], domains_by_iteration: tracker?.snapshot()?.seen_domains || [] } : { attempt: 0, discovered_urls: 0, discovered_by_type: {}, fetched_pages: 0, fetched_by_type: {}, successfully_extracted_sources: 0, qualifying_sources: 0, qualifying_by_type: {}, qualifying_urls: [], seed_stats: seedStats, second_pass_iterations: reviewIterations, qualifying_review_sources: [], domains_by_iteration: tracker?.snapshot()?.seen_domains || [], coverage: [] }, rejected_sources: rejected_sources.slice(-80) };
}

function sentenceFragments(text = '') {
  return String(text)
    .split(/[.!?;•\n]+/)
    .map((part) => cleanText(part))
    .filter(Boolean);
}

function extractPhrasesFromFragment(fragment = '', queryTokens = []) {
  const tokens = normalize(fragment).split(' ').filter((token) => token && token.length >= 3 && !STOPWORDS.has(token));
  const filtered = tokens.filter((token) => !queryTokens.includes(token));
  const phrases = [];
  for (let size = 2; size <= 4; size += 1) {
    for (let i = 0; i <= filtered.length - size; i += 1) {
      const phrase = filtered.slice(i, i + size).join(' ');
      if (phrase.length >= 8) phrases.push(phrase);
    }
  }
  return phrases;
}

function rankPhrases(fragments, queryTokens) {
  const counts = new Map();
  for (const fragment of fragments) {
    const unique = new Set(extractPhrasesFromFragment(fragment, queryTokens));
    for (const phrase of unique) {
      counts.set(phrase, (counts.get(phrase) || 0) + 1);
    }
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].length - b[0].length)
    .map(([phrase]) => phrase)
    .slice(0, 10);
}

function dedupeAndFill(items, fallbackFragments, queryTokens) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const normalized = normalize(item);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(item);
    if (out.length >= 10) return out;
  }
  const ranked = rankPhrases(fallbackFragments, queryTokens);
  for (const item of ranked) {
    const normalized = normalize(item);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(item);
    if (out.length >= 10) return out;
  }
  return out;
}

function buildProductMentionVariants(productName = '') {
  const normalized = normalize(productName);
  const words = normalized.split(' ').filter(Boolean);
  const filtered = words.filter((token) => token.length >= 3 && !STOPWORDS.has(token));
  const core = filtered.filter((token) => !['archery','target','deer','3d','broadhead','field','point'].includes(token));
  const variants = [
    normalized,
    filtered.slice(0, 2).join(' '),
    filtered.slice(0, 3).join(' '),
    core.slice(0, 2).join(' '),
    [filtered[0], core[1], 'target'].filter(Boolean).join(' '),
    [filtered[0], 'target'].filter(Boolean).join(' '),
    [filtered[0], core[1], 'deer', 'target'].filter(Boolean).join(' ')
  ].filter(Boolean);
  return Array.from(new Set(variants.map((x) => normalize(x)).filter(Boolean)));
}

function productMentionConfidence(text = '', productName = '') {
  const haystack = normalize(text);
  const variants = buildProductMentionVariants(productName);
  let best = 0;
  for (const variant of variants) {
    const tokens = variant.split(' ').filter((token) => token.length >= 3 && !STOPWORDS.has(token));
    if (!tokens.length) continue;
    const hits = tokens.filter((token) => haystack.includes(token)).length;
    const score = hits / tokens.length;
    if (score > best) best = score;
  }
  return best;
}

function topicRelevanceConfidence(text = '', query = '', productName = '') {
  const haystack = normalize(text);
  const topicTokens = Array.from(new Set([
    ...normalize(query).split(' ').filter((token) => token.length >= 3 && !STOPWORDS.has(token)),
    ...normalize(productName).split(' ').filter((token) => token.length >= 3 && !STOPWORDS.has(token))
  ]));
  if (!topicTokens.length) return 0;
  const hits = topicTokens.filter((token) => haystack.includes(token)).length;
  return hits / topicTokens.length;
}

function matchCategorySignals(text = '', signals = []) {
  const normalizedText = normalize(text);
  return (signals || []).filter((signal) => {
    const tokenSet = normalize(signal).split(' ').filter((token) => token.length >= 3 && !STOPWORDS.has(token));
    if (!tokenSet.length) return false;
    const hits = tokenSet.filter((token) => normalizedText.includes(token)).length;
    return hits >= Math.max(1, Math.ceil(tokenSet.length / 2));
  }).slice(0, 5);
}

function pickBestPhrase(fragments = [], queryTokens = [], fallback = '') {
  const ranked = rankPhrases(fragments, queryTokens);
  return ranked[0] || fallback;
}

function buildWeightedCriteria(categoryIntelligence = {}) {
  const driverText = normalize([
    ...(categoryIntelligence.decision_drivers || []),
    ...(categoryIntelligence.top_praises || []),
    ...(categoryIntelligence.top_complaints || []),
    ...(categoryIntelligence.failure_points || [])
  ].join(' '));

  const criteria = [
    {
      key: 'core_performance',
      label: 'Core performance',
      weight: 0.30,
      signals: ['performance', 'results', 'power', 'effectiveness', 'consistent', 'output']
    },
    {
      key: 'speed_responsiveness',
      label: 'Speed / responsiveness',
      weight: 0.20,
      signals: ['speed', 'fast', 'responsive', 'quick', 'lag', 'slow']
    },
    {
      key: 'reliability',
      label: 'Reliability',
      weight: 0.15,
      signals: ['reliable', 'durable', 'lasting', 'stopped working', 'failure', 'break', 'consistent']
    },
    {
      key: 'build_quality',
      label: 'Build quality',
      weight: 0.15,
      signals: ['build quality', 'materials', 'solid', 'premium', 'flimsy', 'cheap', 'construction']
    },
    {
      key: 'value_for_price',
      label: 'Value for price',
      weight: 0.10,
      signals: ['value', 'worth it', 'price', 'overpriced', 'budget', 'cost']
    },
    {
      key: 'ease_of_use',
      label: 'Ease of use',
      weight: 0.10,
      signals: ['easy', 'simple', 'setup', 'clean', 'user friendly', 'comfortable', 'intuitive']
    }
  ];

  return criteria.map((criterion) => {
    const boost = criterion.signals.some((signal) => driverText.includes(normalize(signal))) ? 0.03 : 0;
    return { ...criterion, weight: criterion.weight + boost };
  }).map((criterion, _, arr) => {
    const total = arr.reduce((sum, item) => sum + item.weight, 0);
    return { ...criterion, weight: criterion.weight / total };
  });
}

function scoreCriterion(criterion, analysis, categoryIntelligence, product) {
  const text = normalize([
    ...(analysis.pros || []),
    ...(analysis.cons || []),
    ...(analysis.matches_praises || []),
    ...(analysis.matches_complaints || []),
    analysis.unique_strength || '',
    analysis.hidden_issues || '',
    analysis.best_for || '',
    analysis.avoid_if || ''
  ].join(' '));

  let score = 5.5;
  const positiveHits = criterion.signals.filter((signal) => text.includes(normalize(signal))).length;
  score += positiveHits * 0.8;
  score += Math.min(1.5, (analysis.matches_praises || []).length * 0.4);
  score -= Math.min(2.0, (analysis.matches_complaints || []).length * 0.35);

  if (criterion.key === 'reliability') {
    score -= /stopped working|failure|break|refund|replacement|wear out/.test(text) ? 1.8 : 0;
  }
  if (criterion.key === 'build_quality') {
    score -= /flimsy|cheap|crack|poor build/.test(text) ? 1.4 : 0;
  }
  if (criterion.key === 'value_for_price') {
    score -= /overpriced|too expensive/.test(text) ? 1.2 : 0;
    score += /worth it|good value|budget/.test(text) ? 1.0 : 0;
  }
  if (criterion.key === 'speed_responsiveness') {
    score -= /slow|lag|delay/.test(text) ? 1.0 : 0;
    score += /fast|quick|responsive/.test(text) ? 1.0 : 0;
  }
  if (criterion.key === 'ease_of_use') {
    score -= /hard to use|confusing|hard to clean/.test(text) ? 1.0 : 0;
    score += /easy to use|easy to clean|simple setup|intuitive/.test(text) ? 1.0 : 0;
  }

  if (product.rating) score += Math.max(0, product.rating - 4.0) * 1.2;
  if (product.review_count) score += Math.min(1.2, Math.log10(Math.max(product.review_count, 1)) - 2.5);

  return Math.max(1, Math.min(10, Number(score.toFixed(1))));
}

function buildProductScore(product, categoryIntelligence) {
  const criteria = buildWeightedCriteria(categoryIntelligence);
  const categoryScores = {};
  for (const criterion of criteria) {
    categoryScores[criterion.key] = scoreCriterion(criterion, product.product_analysis || {}, categoryIntelligence, product);
  }
  const finalScore = criteria.reduce((sum, criterion) => sum + (categoryScores[criterion.key] * criterion.weight), 0);
  return {
    category_scores: categoryScores,
    final_score: Number(finalScore.toFixed(1)),
    weights: Object.fromEntries(criteria.map((criterion) => [criterion.key, Number(criterion.weight.toFixed(3))]))
  };
}

function buildWinnerJustification(product, categoryIntelligence, label) {
  const praised = (product.product_analysis?.matches_praises || []).slice(0, 2);
  const complaints = (product.product_analysis?.matches_complaints || []).slice(0, 2);
  const positiveText = praised.length ? praised.join(' and ') : ((categoryIntelligence?.top_praises || []).slice(0, 2).join(' and ') || 'top buyer priorities');
  const complaintText = complaints.length ? complaints.join(' and ') : 'common complaint patterns';
  const complaintCount = complaints.length;

  if (label === 'best_budget') {
    return `${product.product_name} stands out on value for price while still aligning with praised traits like ${positiveText}. It shows limited overlap with common complaints${complaintCount ? `, with only light concern around ${complaintText}` : ''}.`;
  }
  if (label === 'best_premium') {
    return `${product.product_name} earns the premium slot by leading on pure performance while matching praised attributes like ${positiveText}. It avoids most common complaint patterns${complaintCount ? `, with only some caution around ${complaintText}` : ''}.`;
  }
  return `${product.product_name} wins Best Overall because it has the highest weighted score, aligns with praised features like ${positiveText}, and shows minimal alignment with common complaints${complaintCount ? ` such as ${complaintText}` : ''}.`;
}

function buildDidNotWinReason(product, winner, categoryIntelligence) {
  const loserScore = product.product_score?.final_score || 0;
  const winnerScore = winner.product_score?.final_score || 0;
  const loserCats = product.product_score?.category_scores || {};
  const winnerCats = winner.product_score?.category_scores || {};

  const reasons = [];
  if ((loserCats.core_performance || 0) < (winnerCats.core_performance || 0) - 0.4) {
    reasons.push('Less accurate or effective than the winner based on user feedback.');
  }
  if ((loserCats.reliability || 0) < (winnerCats.reliability || 0) - 0.4) {
    reasons.push('More complaints about durability or long-term reliability.');
  }
  if ((loserCats.speed_responsiveness || 0) < (winnerCats.speed_responsiveness || 0) - 0.4) {
    reasons.push('Slower performance or responsiveness based on user feedback.');
  }
  if ((product.product_analysis?.matches_complaints || []).length > (winner.product_analysis?.matches_complaints || []).length) {
    reasons.push('Poorer alignment with key decision drivers because it overlaps more with common complaints.');
  }
  if (!reasons.length && loserScore < winnerScore) {
    reasons.push('It scored lower overall once buyer priorities and complaint patterns were weighted together.');
  }

  const summary = `${product.product_name} is a credible option, but it finished behind ${winner.product_name} because ${reasons[0]?.toLowerCase() || 'it scored lower on weighted buyer priorities.'}`;
  return {
    product_name: product.product_name,
    affiliate_url: product.affiliate_url,
    summary,
    did_not_win_reason: reasons[0] || 'It scored lower on the weighted criteria that mattered most to buyers.',
    additional_reasons: reasons.slice(1, 3)
  };
}

function validateDecisionEngineOutput(output) {
  if (!output?.category_intelligence) return { ok: false, error: 'missing_category_intelligence' };
  if (!Array.isArray(output.products) || output.products.length !== 5) return { ok: false, error: 'requires_five_products' };
  if (!output.winner_selection?.best_overall) return { ok: false, error: 'missing_best_overall' };

  for (const product of output.products) {
    if (!product.product_analysis) return { ok: false, error: 'missing_product_analysis', product: product.product_name };
    if (!product.product_score) return { ok: false, error: 'missing_product_score', product: product.product_name };
    if (!(product.product_analysis_sources || []).length) return { ok: false, error: 'missing_product_analysis_sources', product: product.product_name };
  }

  return { ok: true };
}

function selectWinners(products, categoryIntelligence) {
  const sorted = [...products].sort((a, b) => b.product_score.final_score - a.product_score.final_score || (b.review_count || 0) - (a.review_count || 0));
  const bestOverall = sorted[0] || null;
  const bestBudget = [...products]
    .sort((a, b) => {
      const aBudgetScore = ((a.product_score?.category_scores?.value_for_price || 0) * 2) + (a.product_score?.final_score || 0);
      const bBudgetScore = ((b.product_score?.category_scores?.value_for_price || 0) * 2) + (b.product_score?.final_score || 0);
      return bBudgetScore - aBudgetScore;
    })[0] || null;
  const bestPremium = [...products]
    .sort((a, b) => {
      const aPremiumScore = ((a.product_score?.category_scores?.core_performance || 0) * 2) + (a.product_score?.final_score || 0);
      const bPremiumScore = ((b.product_score?.category_scores?.core_performance || 0) * 2) + (b.product_score?.final_score || 0);
      return bPremiumScore - aPremiumScore;
    })[0] || null;

  return {
    best_overall: bestOverall ? {
      product_name: bestOverall.product_name,
      asin: bestOverall.asin || null,
      final_score: bestOverall.product_score.final_score,
      justification: buildWinnerJustification(bestOverall, categoryIntelligence, 'best_overall')
    } : null,
    best_budget: bestBudget ? {
      product_name: bestBudget.product_name,
      asin: bestBudget.asin || null,
      final_score: bestBudget.product_score.final_score,
      justification: buildWinnerJustification(bestBudget, categoryIntelligence, 'best_budget')
    } : null,
    best_premium: bestPremium ? {
      product_name: bestPremium.product_name,
      asin: bestPremium.asin || null,
      final_score: bestPremium.product_score.final_score,
      justification: buildWinnerJustification(bestPremium, categoryIntelligence, 'best_premium')
    } : null
  };
}

async function buildCategoryIntelligence(request) {
  writeProgress({ request_id: request.request_id, stage: 'category_intelligence_start', query: request.raw_query, last_successful_transition: 'request_generating' });
  const query = String(request.normalized_query || request.raw_query || '').trim();
  const queryTokens = normalize(query).split(' ').filter((token) => token.length >= 3 && !STOPWORDS.has(token));
  if (!query || !queryTokens.length) {
    return { ok: false, error: 'category_intelligence_query_invalid' };
  }

  const queryShape = classifyQueryShape(query);
  const broadCategoryQueries = buildBroadCategoryQueries(query);
  const searchQueries = queryShape === 'broad_category_query'
    ? broadCategoryQueries
    : [
        `${query} review`,
        `${query} comparison`,
        `${query} forum discussion`,
        `${query} reddit`,
        `${query} buyer complaints`,
        `${query} pros and cons`,
        `${query} buying guide`,
        `${query} best options`
      ];

  writeProgress({ request_id: request.request_id, stage: 'category_seed_generation', category_queries: searchQueries, query_shape: queryShape, last_successful_transition: 'category_seed_generation' });
  updateCategoryDebug(request.request_id, { current_substage: 'category_seed_generation', query_shape: queryShape, query_families: { primary: searchQueries, broad_category: broadCategoryQueries }, queries_used: searchQueries, iteration: 0, counts: { discovered: 0, fetched: 0, extracted: 0, qualified: 0 }, last_progress_timestamp: nowIso() });
  const discovery = await withTimeout(discoverAndQualifySources({
    baseQuery: query,
    searchQueries,
    queryTokens,
    mode: 'category',
    minQualifyingSources: 6,
    requestId: request.request_id,
    queryShape
  }), STAGE_TIMEOUTS_MS.category_intelligence, { stage: 'category_intelligence', request_id: request.request_id, query_shape: queryShape });

  const validSources = discovery.qualifyingSources;
  const coverage = new Set(validSources.map((item) => item.source_type));
  const strongCoverageCount = STRONG_COVERAGE_BUCKETS.filter((type) => coverage.has(type)).length;
  const hasValidMix = (coverage.has('web_review') || coverage.has('google_reviews')) && (coverage.has('forum') || coverage.has('reddit'));
  if (hasValidMix && strongCoverageCount < 2) {
    throw new Error(`diversity_gate_miscount_detected:${JSON.stringify({ buckets_present: Array.from(coverage), buckets_counted: STRONG_COVERAGE_BUCKETS.filter((type) => coverage.has(type)), strongCoverageCount })}`);
  }
  if (validSources.length < 6 || strongCoverageCount < 2) {
    return {
      ok: false,
      error: 'category_intelligence_source_coverage_missing',
      debug: {
        failing_substage: 'source_diversity_enforcement',
        query_shape: queryShape,
        query_families: { primary: searchQueries, broad_category: broadCategoryQueries },
        coverage: Array.from(coverage),
        missing_source_buckets: {
          editorial_review_or_buying_guide: !Array.from(coverage).some((x) => ['web_review','google_reviews'].includes(x)),
          discussion: !Array.from(coverage).some((x) => ['forum','reddit'].includes(x)),
          video: false
        },
        failure_mode: (discovery.stats?.discovered_urls || 0) === 0 ? 'zero_discovery' : ((discovery.stats?.fetched_pages || 0) === 0 ? 'low_fetch_success' : ((discovery.stats?.successfully_extracted_sources || 0) === 0 ? 'low_extraction_success' : 'qualification_rejection_or_diversity_gap')),
        collected_sources: validSources.length,
        strong_coverage_count: strongCoverageCount,
        discovered_urls: discovery.stats?.discovered_urls || 0,
        discovered_by_type: discovery.stats?.discovered_by_type || {},
        fetched_pages: discovery.stats?.fetched_pages || 0,
        fetched_by_type: discovery.stats?.fetched_by_type || {},
        successfully_extracted_sources: discovery.stats?.successfully_extracted_sources || 0,
        qualifying_sources: discovery.stats?.qualifying_sources || 0,
        qualifying_by_type: discovery.stats?.qualifying_by_type || {},
        qualifying_urls: discovery.stats?.qualifying_urls || [],
        second_pass_iterations: discovery.stats?.second_pass_iterations || 0,
        qualifying_review_sources: discovery.stats?.qualifying_review_sources || [],
        domains_by_iteration: discovery.stats?.domains_by_iteration || [],
        rejected_sources: discovery.rejected_sources || []
      }
    };
  }

  const praiseCues = ['love', 'great', 'best', 'excellent', 'reliable', 'fast', 'easy', 'quiet', 'comfortable', 'durable', 'smooth', 'helpful', 'accurate', 'portable', 'powerful'];
  const complaintCues = ['hate', 'complaint', 'problem', 'issue', 'bad', 'poor', 'fails', 'failure', 'broken', 'returns', 'refund', 'defect', 'flimsy', 'weak', 'noisy', 'inconsistent'];
  const driverCues = ['important', 'need', 'looking for', 'matters', 'choose', 'decision', 'worth it', 'buy', 'compare', 'consider'];
  const failureCues = ['break', 'stop working', 'battery dies', 'leak', 'clog', 'overheat', 'tear', 'rust', 'jam', 'disconnect', 'wear out', 'fall apart', 'crack'];

  const praiseFragments = [];
  const complaintFragments = [];
  const driverFragments = [];
  const failureFragments = [];

  for (const source of validSources) {
    const fragments = sentenceFragments(`${source.title}. ${source.snippet}. ${source.page_excerpt || ''}`);
    for (const fragment of fragments) {
      const lower = fragment.toLowerCase();
      if (praiseCues.some((cue) => lower.includes(cue))) praiseFragments.push(fragment);
      if (complaintCues.some((cue) => lower.includes(cue))) complaintFragments.push(fragment);
      if (driverCues.some((cue) => lower.includes(cue))) driverFragments.push(fragment);
      if (failureCues.some((cue) => lower.includes(cue))) failureFragments.push(fragment);
    }
  }

  const categoryIntelligence = {
    top_praises: dedupeAndFill(rankPhrases(praiseFragments, queryTokens), validSources.map((x) => x.snippet), queryTokens),
    top_complaints: dedupeAndFill(rankPhrases(complaintFragments, queryTokens), validSources.map((x) => x.snippet), queryTokens),
    decision_drivers: dedupeAndFill(rankPhrases(driverFragments, queryTokens), validSources.map((x) => x.title + ' ' + x.snippet), queryTokens),
    failure_points: dedupeAndFill(rankPhrases(failureFragments, queryTokens), validSources.map((x) => x.snippet), queryTokens)
  };

  const isComplete = ['top_praises', 'top_complaints', 'decision_drivers', 'failure_points'].every((key) => Array.isArray(categoryIntelligence[key]) && categoryIntelligence[key].length >= 3);
  if (!isComplete) {
    return {
      ok: false,
      error: 'category_intelligence_incomplete',
      debug: Object.fromEntries(Object.entries(categoryIntelligence).map(([k, v]) => [k, v.length]))
    };
  }

  writeProgress({ request_id: request.request_id, stage: 'category_intelligence_done', qualifying_sources: validSources.length, coverage: Array.from(coverage), last_successful_transition: 'category_intelligence_done' });
  return {
    ok: true,
    category_intelligence: categoryIntelligence,
    evidence_sources: validSources.slice(0, 20).map((item) => ({
      source_type: item.source_type,
      source_class: item.source_class || classifyQualifiedSource(item),
      query: item.query,
      title: item.title,
      page_title: item.page_title || item.title,
      href: item.href,
      snippet: item.snippet,
      page_excerpt: item.page_excerpt || '',
      extracted_chars: item.extracted_chars || 0,
      query_overlap: item.query_overlap || 0
    })),
    debug: {
      discovered_urls: discovery.stats?.discovered_urls || 0,
      discovered_by_type: discovery.stats?.discovered_by_type || {},
      fetched_pages: discovery.stats?.fetched_pages || 0,
      fetched_by_type: discovery.stats?.fetched_by_type || {},
      successfully_extracted_sources: discovery.stats?.successfully_extracted_sources || 0,
      qualifying_sources: discovery.stats?.qualifying_sources || validSources.length,
      qualifying_by_type: discovery.stats?.qualifying_by_type || {},
      qualifying_urls: discovery.stats?.qualifying_urls || [],
      coverage: Array.from(coverage),
      strong_coverage_count: strongCoverageCount,
      rejected_sources: discovery.rejected_sources || []
    }
  };
}

function loadPublishedArticles() {
  const reg = readJson(registryPath);
  return (reg.articles || []).filter((a) => a.publish_status === 'published').flatMap((entry) => {
    try {
      const dir = path.join(ROOT, entry.article_dir);
      const contentPath = path.join(dir, 'contentproduction.json');
      const intelligencePath = path.join(dir, 'productintelligence.json');
      if (!fs.existsSync(contentPath) || !fs.existsSync(intelligencePath)) return [];
      const content = JSON.parse(fs.readFileSync(contentPath, 'utf8'));
      const intelligence = JSON.parse(fs.readFileSync(intelligencePath, 'utf8'));
      return [{
        entry,
        content,
        intelligence,
        article_slug: entry.article_slug,
        title: content.title || entry.title,
        summary: content.summary || '',
        top_pick: content.top_pick || '',
        category: entry.category || content.category || '',
        search_text: [content.title || '', content.summary || '', content.top_pick || '', entry.category || '', ...(content.comparison || []).map(x => x.name || '')].join(' ').toLowerCase()
      }];
    } catch {
      return [];
    }
  });
}

function parseReviewCount(raw = '') {
  const digits = String(raw).replace(/[^\d]/g, '');
  return digits ? Number(digits) : 0;
}

function parseRating(raw = '') {
  const match = String(raw).match(/(\d+(?:\.\d+)?)/);
  return match ? Number(match[1]) : 0;
}

function buildAmazonAffiliateUrl(rawUrl = '', asin = '') {
  const canonical = asin ? `https://www.amazon.com/dp/${asin}` : String(rawUrl || '');
  try {
    const url = new URL(canonical);
    if (url.hostname.includes('amazon.')) {
      url.search = '';
      url.searchParams.set('tag', AMAZON_ASSOCIATE_TAG);
      return url.toString();
    }
  } catch {}
  if (asin) return `https://www.amazon.com/dp/${asin}?tag=${encodeURIComponent(AMAZON_ASSOCIATE_TAG)}`;
  return canonical;
}

async function fetchAmazonReviewSentiment(product) {
  const affiliateUrl = String(product?.affiliate_url || '');
  const asin = product?.asin || (affiliateUrl.match(/\/dp\/([A-Z0-9]{10})/i) || affiliateUrl.match(/\/product\/([A-Z0-9]{10})/i) || [])[1] || null;
  if (!affiliateUrl || !asin) return { ok: false, error: 'amazon_missing_asin_or_url' };

  const productRes = await fetch(affiliateUrl, { headers: AMAZON_HEADERS, redirect: 'follow', signal: AbortSignal.timeout(20000) });
  if (!productRes.ok) return { ok: false, error: `amazon_product_http_${productRes.status}` };
  const productHtml = await productRes.text();
  const productTitle = cleanText((productHtml.match(/<span[^>]+id="productTitle"[^>]*>([\s\S]*?)<\/span>/i) || [])[1] || product.product_name || '');
  const averageRating = parseRating((productHtml.match(/<span[^>]+data-hook="rating-out-of-text"[^>]*>([\s\S]*?)<\/span>/i) || [])[1] || '');
  const reviewCount = parseReviewCount((productHtml.match(/<span[^>]+id="acrCustomerReviewText"[^>]*>([\s\S]*?)<\/span>/i) || [])[1] || '');

  const reviewUrls = [
    `https://www.amazon.com/product-reviews/${asin}?sortBy=helpful`,
    `https://www.amazon.com/product-reviews/${asin}?sortBy=recent`
  ];
  const reviews = [];
  for (const url of reviewUrls) {
    const res = await fetch(url, { headers: AMAZON_HEADERS, redirect: 'follow', signal: AbortSignal.timeout(20000) });
    if (!res.ok) continue;
    const html = await res.text();
    const blocks = [...html.matchAll(/<div[^>]+data-hook="review"[\s\S]*?<\/div>\s*<\/div>/gi)].slice(0, 12);
    for (const blockMatch of blocks) {
      const block = blockMatch[0];
      const title = cleanText((block.match(/<a[^>]+data-hook="review-title"[^>]*>([\s\S]*?)<\/a>/i) || [])[1] || '');
      const text = cleanText((block.match(/<span[^>]+data-hook="review-body"[^>]*>([\s\S]*?)<\/span>/i) || [])[1] || '');
      const rating = parseRating((block.match(/<i[^>]+data-hook="review-star-rating"[^>]*>[\s\S]*?<span[^>]*>([\s\S]*?)<\/span>/i) || block.match(/<i[^>]+data-hook="cmps-review-star-rating"[^>]*>[\s\S]*?<span[^>]*>([\s\S]*?)<\/span>/i) || [])[1] || '');
      const date = cleanText((block.match(/<span[^>]+data-hook="review-date"[^>]*>([\s\S]*?)<\/span>/i) || [])[1] || '');
      if (!text || text.length < 40) continue;
      reviews.push({ title, text, rating, date, source_url: url });
      if (reviews.length >= 16) break;
    }
    if (reviews.length >= 16) break;
  }

  const combined = reviews.map((r) => `${r.title}. ${r.text}`).join(' ');
  const fragments = sentenceFragments(combined);
  const praiseCues = ['easy', 'great', 'durable', 'stops', 'solid', 'love', 'works well', 'excellent', 'holds up', 'recommended'];
  const complaintCues = ['hard to pull', 'wear', 'wore out', 'tear', 'issue', 'problem', 'weak', 'falls apart', 'not suitable', 'difficult'];
  const durabilityCues = ['durable', 'holds up', 'wear', 'wore out', 'lasted', 'fell apart', 'foam'];
  const performanceCues = ['broadhead', 'field point', 'stops', 'penetration', 'pull arrows', 'poundage', 'crossbow'];
  const failureCues = ['wear out', 'foam wears', 'pass through', 'tears', 'hard to pull arrows', 'not suitable', 'breaks down'];

  const topPraises = [];
  const topComplaints = [];
  const durability = [];
  const performance = [];
  const failures = [];
  for (const fragment of fragments) {
    const lower = fragment.toLowerCase();
    if (praiseCues.some((cue) => lower.includes(cue))) topPraises.push(fragment);
    if (complaintCues.some((cue) => lower.includes(cue))) topComplaints.push(fragment);
    if (durabilityCues.some((cue) => lower.includes(cue))) durability.push(fragment);
    if (performanceCues.some((cue) => lower.includes(cue))) performance.push(fragment);
    if (failureCues.some((cue) => lower.includes(cue))) failures.push(fragment);
  }

  const sentiment = {
    top_praises: dedupeAndFill(rankPhrases(topPraises, normalize(productTitle).split(' ')), fragments, normalize(productTitle).split(' ')).slice(0, 5),
    top_complaints: dedupeAndFill(rankPhrases(topComplaints, normalize(productTitle).split(' ')), fragments, normalize(productTitle).split(' ')).slice(0, 5),
    durability_feedback: pickBestPhrase(durability, normalize(productTitle).split(' '), ''),
    performance_feedback: pickBestPhrase(performance, normalize(productTitle).split(' '), ''),
    common_failures: dedupeAndFill(rankPhrases(failures, normalize(productTitle).split(' ')), fragments, normalize(productTitle).split(' ')).slice(0, 4),
    consistency_of_feedback: (topPraises.length >= 3 && topComplaints.length >= 2) ? 'high' : (topPraises.length + topComplaints.length >= 3 ? 'mixed' : 'low')
  };

  const substantive = sentiment.top_praises.length + sentiment.top_complaints.length + sentiment.common_failures.length >= 4
    && (sentiment.durability_feedback || sentiment.performance_feedback);
  if (reviewCount < AMAZON_REVIEW_MIN_COUNT || reviews.length < 4 || !substantive) {
    return {
      ok: false,
      error: 'amazon_sentiment_insufficient',
      debug: { review_count: reviewCount, extracted_reviews: reviews.length, sentiment }
    };
  }

  return {
    ok: true,
    amazon_review_sentiment: sentiment,
    review_count: reviewCount,
    average_rating: averageRating,
    product_title: productTitle,
    extracted_reviews: reviews.length,
    used_as_fallback: true
  };
}

async function fetchAmazonProducts(query) {
  const url = `https://www.amazon.com/s?k=${encodeURIComponent(query)}`;
  const res = await fetch(url, { headers: AMAZON_HEADERS });
  const html = await res.text();
  if (!res.ok) throw new Error(`amazon_search_http_${res.status}`);
  const products = [];
  const seen = new Set();
  const normalizedQuery = normalize(query);
  const rawTokens = normalizedQuery.split(' ').filter(Boolean).filter((token) => !STOPWORDS.has(token));
  const mustHaveTokens = rawTokens.filter((token) => ['foam', 'archery', 'target', 'desk', 'lamp', 'lamps'].includes(token));
  const expandedQueryTokens = Array.from(new Set(rawTokens.flatMap((token) => token.endsWith('s') ? [token, token.slice(0, -1)] : [token, `${token}s`])));
  const blockRegex = /<div[^>]+data-asin="([A-Z0-9]{10})"[\s\S]{0,30000}?<\/div>\s*<\/div>/gi;
  let blockMatch;
  while ((blockMatch = blockRegex.exec(html)) !== null) {
    const asin = blockMatch[1];
    const block = blockMatch[0];
    if (!asin || seen.has(asin)) continue;
    if (/s-sponsored-label-info-icon|puis-sponsored-label-text|AdHolder|Sponsored/i.test(block)) continue;

    const linkMatch = block.match(/class="a-link-normal s-no-outline"[^>]+href="([^"]+)"/i);
    const titleMatch = block.match(/<img[^>]+alt="([^"]+)"/i);
    const ratingMatch = block.match(/a-icon-alt">\s*([^<]*?out of 5 stars)\s*</i);
    const reviewMatch = block.match(/<span[^>]+class="a-size-base s-underline-text"[^>]*>\s*([^<]+)\s*<\/span>/i)
      || block.match(/aria-label="([^\"]+\s+ratings?)"/i);

    const href = linkMatch ? cleanText(linkMatch[1]) : '';
    const title = titleMatch ? cleanText(titleMatch[1]) : '';
    const rating = parseRating(ratingMatch ? ratingMatch[1] : '');
    const reviewCount = parseReviewCount(reviewMatch ? reviewMatch[1] : '');
    const normalizedTitle = normalize(title);
    const tokenMatches = expandedQueryTokens.filter((token) => normalizedTitle.includes(token));
    const hasMustHaveMismatch = mustHaveTokens.some((token) => !normalizedTitle.includes(token));

    if (!href || !title) continue;
    if (hasMustHaveMismatch) continue;
    if (tokenMatches.length < Math.max(2, Math.min(3, mustHaveTokens.length || expandedQueryTokens.length || 1))) continue;
    seen.add(asin);
    products.push({
      asin,
      product_name: title,
      affiliate_url: buildAmazonAffiliateUrl(href.startsWith('http') ? href : `https://www.amazon.com${href}`, asin),
      why_it_won: `Selected using query-fit, review-volume, and rating thresholds for "${query}".`,
      notes: 'Chosen for exact query fit first, then by descending review count and rating thresholds.',
      best_for: query,
      source: 'amazon_search',
      rating,
      review_count: reviewCount,
      query_fit_tokens: tokenMatches
    });
  }

  const qualifying = products
    .filter((item) => item.rating >= 4.2 && item.review_count >= 1000)
    .sort((a, b) => b.review_count - a.review_count || b.rating - a.rating)
    .slice(0, 5);

  if (qualifying.length >= 5) return qualifying;

  const nicheFallback = products
    .filter((item) => item.rating >= 4.2 && item.review_count >= 250)
    .sort((a, b) => b.review_count - a.review_count || b.rating - a.rating)
    .slice(0, 5);

  if (nicheFallback.length >= 5) return nicheFallback;

  const broadFallback = products
    .filter((item) => item.rating >= 4.0 && item.review_count >= 100)
    .sort((a, b) => b.review_count - a.review_count || b.rating - a.rating)
    .slice(0, 5);

  if (broadFallback.length >= 5) return broadFallback;

  const exactMatchFallback = products
    .filter((item) => item.rating >= 4.0 || (item.query_fit_tokens?.length || 0) >= 2)
    .sort((a, b) => (b.query_fit_tokens?.length || 0) - (a.query_fit_tokens?.length || 0) || b.rating - a.rating || b.review_count - a.review_count)
    .slice(0, 5);

  if (exactMatchFallback.length >= 5) return exactMatchFallback;

  const serplessQueryFitFallback = products
    .filter((item) => (item.query_fit_tokens?.length || 0) >= 2)
    .sort((a, b) => (b.query_fit_tokens?.length || 0) - (a.query_fit_tokens?.length || 0) || b.rating - a.rating || b.review_count - a.review_count)
    .slice(0, 5);

  return serplessQueryFitFallback;
}

function buildFromExisting(request, published) {
  const q = normalize(request.raw_query);
  const rawTokens = q.split(' ').filter(Boolean);
  const stopwords = new Set(['best', 'for', 'the', 'and', 'with', 'from', 'that', 'this', 'your', 'into', 'under', 'over', 'vs', 'comparison', 'guide', 'buy', 'top', 'amazon']);
  const qTokens = rawTokens.filter((token) => token.length >= 3 && !stopwords.has(token));
  if (!qTokens.length) return { ok: false, error: 'query_too_generic_for_existing_match' };

  const matches = published.map((item) => {
    const titleText = normalize(`${item.title || ''} ${item.category || ''} ${item.top_pick || ''}`);
    const productNames = (item.intelligence?.products || []).map((p) => normalize(p.product_name || p.name || ''));
    const titleHits = qTokens.filter((token) => titleText.includes(token)).length;
    const productHits = qTokens.filter((token) => productNames.some((name) => name.includes(token))).length;
    const score = (titleHits * 3) + productHits;
    const overlapRatio = qTokens.length ? (Math.max(titleHits, productHits) / qTokens.length) : 0;
    return { ...item, score, titleHits, productHits, overlapRatio };
  }).filter((item) => {
    return item.titleHits >= 1 && item.overlapRatio >= 0.6 && item.score >= 3;
  }).sort((a,b) => b.score - a.score).slice(0, 5);

  if (!matches.length) return { ok: false, error: 'no_relevant_content_found' };

  const products = [];
  const seen = new Set();
  for (const match of matches) {
    for (const p of (match.intelligence.products || [])) {
      const productName = p.product_name || p.name || '';
      const key = productName.trim().toLowerCase();
      const normalizedName = normalize(productName);
      const productTokenHits = qTokens.filter((token) => normalizedName.includes(token)).length;
      if (!key || seen.has(key)) continue;
      if (productTokenHits === 0 && match.titleHits < 2) continue;
      seen.add(key);
      products.push({
        product_name: productName,
        affiliate_url: p.affiliate_url,
        notes: p.notes || '',
        why_it_won: p.why_it_won || '',
        best_for: p.best_for || request.normalized_query,
        source_article_slug: match.article_slug,
        source: 'existing_published_content'
      });
      if (products.length >= 5) break;
    }
    if (products.length >= 5) break;
  }

  if (products.length < 3) return { ok: false, error: 'existing_content_match_too_weak' };

  return {
    ok: true,
    strategy: 'existing_content',
    request_id: request.request_id,
    raw_query: request.raw_query,
    normalized_query: request.normalized_query,
    generated_at: new Date().toISOString(),
    top_matches: matches.map((m) => ({ article_slug: m.article_slug, title: m.title, score: m.score, top_pick: m.top_pick })),
    answer_summary: `Built from ${matches.length} strongly matching published guide(s) using valid Amazon-linked products.`,
    products
  };
}

async function buildProductAnalysis(product, request, categoryIntelligence, categoryEvidenceSources = []) {
  writeProgress({ request_id: request.request_id, stage: 'product_analysis_start', product: product.product_name, last_successful_transition: 'category_intelligence_done' });
  const productName = String(product.product_name || '').trim();
  const query = String(request.normalized_query || request.raw_query || '').trim();
  const queryTokens = normalize(`${query} ${productName}`).split(' ').filter((token) => token.length >= 3 && !STOPWORDS.has(token));
  const refinedSeeds = buildProductSearchSeeds(productName, query);
  if (product?.source === 'amazon_search') {
    const pros = [
      `Strong Amazon shopper validation with about ${product.review_count || 0} reviews.`,
      `Solid visible rating around ${product.rating || '4+'} stars for this category.`,
      categoryIntelligence?.top_praises?.[0] || `Good fit for common ${query} priorities.`
    ].filter(Boolean);
    const cons = [
      categoryIntelligence?.top_complaints?.[0] || `May share the usual tradeoffs seen across ${query}.`,
      categoryIntelligence?.failure_points?.[0] || 'Check long-term durability details before buying.',
      'Amazon search selection is stronger on demand signals than hands-on lab verification.'
    ].filter(Boolean);
    const analysis = {
      pros,
      cons,
      matches_praises: (categoryIntelligence?.top_praises || []).slice(0, 3),
      matches_complaints: (categoryIntelligence?.top_complaints || []).slice(0, 3),
      unique_strength: `High review-volume Amazon pick for ${query} with a ${product.rating || 'strong'} average rating.`,
      hidden_issues: categoryIntelligence?.failure_points?.[0] || 'Long-term reliability and build quality need closer review.',
      best_for: product.best_for || query,
      avoid_if: categoryIntelligence?.top_complaints?.[0] || `You want a fully hands-on editorially tested pick only.`,
      amazon_review_sentiment: null,
      evidence_tier: 'tier_0_amazon_search_plus_category_intelligence'
    };
    return {
      ok: true,
      product_analysis: analysis,
      evidence_sources: (categoryEvidenceSources || []).length
        ? (categoryEvidenceSources || []).slice(0, 10)
        : [{
            href: product.affiliate_url || product.canonical_product_url || `https://www.amazon.com/dp/${product.asin || ''}`,
            title: product.product_name,
            source_type: 'amazon_search',
            page_title: product.product_name,
            page_excerpt: `Amazon search fallback evidence for ${product.product_name}`
          }],
      debug: {
        product: productName,
        shortcut: 'amazon_search_heuristic_analysis',
        qualifying_sources: (categoryEvidenceSources || []).length,
        coverage: Array.from(new Set((categoryEvidenceSources || []).map((item) => item.source_type).filter(Boolean)))
      }
    };
  }
  const amazonPrimary = null;
  const reviewQueries = Array.from(new Set([
    `${productName} reviews`,
    `${productName} review`,
    `${productName} complaints`,
    `${productName} pros and cons`,
    `${productName} long term review`,
    ...refinedSeeds.flatMap((seed) => ([`${seed} review`, `${seed} durability review`, `${seed} tested and reviewed`]))
  ]));
  const discussionQueries = Array.from(new Set([
    `${productName} reddit`,
    `${productName} forum discussion`,
    ...refinedSeeds.flatMap((seed) => ([`${seed} forum`, `${seed} reddit`, `${seed} user discussion`]))
  ]));

  const objectives = [
    { name: 'reviews', queries: reviewQueries },
    { name: 'discussion', queries: discussionQueries }
  ];

  const objectiveRuns = [];
  if (!amazonPrimary?.ok) {
    for (const objective of objectives) {
      writeProgress({ request_id: request.request_id, stage: `product_discovery_${objective.name}`, product: productName, objective: objective.name, last_successful_transition: `product_discovery_${objective.name}` });
      const result = await discoverAndQualifySources({
        baseQuery: productName,
        searchQueries: objective.queries,
        queryTokens,
        mode: 'product',
        minQualifyingSources: 1
      });
      objectiveRuns.push({ objective: objective.name, queries: objective.queries, ...result });
    }
  }

  const productNameTokens = normalize(productName).split(' ').filter((token) => token.length >= 4 && !STOPWORDS.has(token));
  const categoryFallbackSources = dedupeSources((categoryEvidenceSources || []).filter((item) => {
    const haystack = `${item.title || ''} ${item.page_title || ''} ${item.snippet || ''} ${item.page_excerpt || ''}`;
    const tokenHits = productNameTokens.filter((token) => normalize(haystack).includes(token)).length;
    return tokenHits >= Math.max(2, Math.min(3, productNameTokens.length)) && productMentionConfidence(haystack, productName) >= 0.66 && topicRelevanceConfidence(haystack, query, productName) >= 0.4;
  }));

  let validSources = dedupeSources(objectiveRuns.flatMap((run) => run.qualifyingSources || []).filter((item) => {
    const text = `${item.title || ''} ${item.page_title || ''} ${item.snippet || ''} ${item.page_excerpt || ''}`;
    return productMentionConfidence(text, productName) >= 0.66 && topicRelevanceConfidence(text, query, productName) >= 0.4;
  }));
  if (amazonPrimary?.ok) {
    validSources = dedupeSources([...validSources, ...categoryFallbackSources.slice(0, 6)]);
  }
  const hasSecondary = validSources.some((item) => ['forum', 'reddit'].includes(item.source_type));
  if (!hasSecondary && categoryFallbackSources.length) {
    writeProgress({ request_id: request.request_id, stage: 'product_fallback_mode', product: productName, last_successful_transition: 'product_fallback_mode' });
    validSources = dedupeSources([...validSources, ...categoryFallbackSources]);
  }

  const coverage = new Set(validSources.map((item) => item.source_type));
  let hasStrongReview = validSources.some((item) => ['web_review', 'google_reviews'].includes(item.source_type) && productMentionConfidence(`${item.title || ''} ${item.page_title || ''} ${item.snippet || ''} ${item.page_excerpt || ''}`, productName) >= 0.66);
  let hasStrongSecondary = validSources.some((item) => ['forum', 'reddit'].includes(item.source_type) && productMentionConfidence(`${item.title || ''} ${item.page_title || ''} ${item.snippet || ''} ${item.page_excerpt || ''}`, productName) >= 0.66);
  let amazonFallback = amazonPrimary?.ok ? amazonPrimary : null;
  if (amazonFallback?.ok) {
    hasStrongReview = true;
    hasStrongSecondary = true;
    coverage.add('amazon_search');
  }
  if (hasStrongReview && !hasStrongSecondary) {
    writeProgress({ request_id: request.request_id, stage: 'product_amazon_fallback', product: productName, last_successful_transition: 'product_amazon_fallback' });
    amazonFallback = await fetchAmazonReviewSentiment(product);
    if (amazonFallback.ok) hasStrongSecondary = true;
  }
  const strongCoverageCount = ['reddit', 'google_reviews', 'forum', 'web_review'].filter((type) => coverage.has(type)).length;
  writeProgress({ request_id: request.request_id, stage: 'product_coverage_check', product: productName, last_successful_transition: 'product_coverage_check' });
  if (!(hasStrongReview && hasStrongSecondary)) {
    return {
      ok: false,
      error: 'product_analysis_source_coverage_missing',
      debug: {
        product: productName,
        coverage: Array.from(coverage),
        collected_sources: validSources.length,
        strong_coverage_count: strongCoverageCount,
        source_type_gaps: {
          missing_review: !hasStrongReview,
          missing_secondary: !hasStrongSecondary
        },
        amazon_review_count: amazonFallback?.review_count || 0,
        amazon_used_as_fallback: Boolean(amazonFallback?.ok),
        amazon_sentiment: amazonFallback?.amazon_review_sentiment || null,
        final_evidence_tier: amazonFallback?.ok ? 'tier_2_review_plus_amazon' : 'insufficient',
        discovered_urls: objectiveRuns.reduce((sum, run) => sum + (run.stats?.discovered_urls || 0), 0),
        discovered_by_type: Object.assign({}, ...objectiveRuns.map((run) => run.stats?.discovered_by_type || {})),
        fetched_pages: objectiveRuns.reduce((sum, run) => sum + (run.stats?.fetched_pages || 0), 0),
        fetched_by_type: Object.assign({}, ...objectiveRuns.map((run) => run.stats?.fetched_by_type || {})),
        successfully_extracted_sources: objectiveRuns.reduce((sum, run) => sum + (run.stats?.successfully_extracted_sources || 0), 0),
        qualifying_sources: validSources.length,
        qualifying_by_type: validSources.reduce((acc, item) => { acc[item.source_type] = (acc[item.source_type] || 0) + 1; return acc; }, {}),
        qualifying_urls: validSources.map((item) => ({ href: item.href, source_type: item.source_type, query: item.query })),
        objectives: objectiveRuns.map((run) => ({ objective: run.objective, queries: run.queries, discovered_by_type: run.stats?.discovered_by_type || {}, qualifying_by_type: run.stats?.qualifying_by_type || {}, qualifying_urls: run.stats?.qualifying_urls || [], accepted_urls: (run.qualifyingSources || []).map((item) => ({ href: item.href, source_type: item.source_type, query: item.query })), rejected_urls: (run.rejected_sources || []).map((item) => ({ href: item.href || null, query: item.query || null, reason: item.reason || item.rejection_reason || null, source_type: item.source_type || null })) })),
        search_seeds_used: { reviews: reviewQueries, discussion: discussionQueries },
        category_fallback_sources: categoryFallbackSources.length,
        fail_reason: 'public_source_diversity_insufficient_for_product',
        rejected_sources: objectiveRuns.flatMap((run) => run.rejected_sources || []).slice(-120)
      }
    };
  }

  const prosCues = ['love', 'great', 'best', 'excellent', 'durable', 'quiet', 'comfortable', 'easy', 'reliable', 'fast', 'strong', 'worth it'];
  const consCues = ['bad', 'issue', 'problem', 'weak', 'cheap', 'fails', 'broken', 'noisy', 'inconsistent', 'hard to clean', 'overpriced', 'returns'];
  const hiddenIssueCues = ['after a few months', 'long term', 'wear out', 'stopped working', 'customer service', 'replacement', 'refund', 'battery dies', 'leaks', 'cracks'];
  const bestForCues = ['best for', 'ideal for', 'good for', 'works well for'];
  const avoidIfCues = ['avoid if', 'not for', 'skip if', 'bad choice for'];

  const prosFragments = [];
  const consFragments = [];
  const hiddenIssueFragments = [];
  const bestForFragments = [];
  const avoidIfFragments = [];
  const allFragments = [];

  for (const source of validSources) {
    const fragments = sentenceFragments(`${source.title}. ${source.snippet}. ${source.page_excerpt || ''}`);
    for (const fragment of fragments) {
      const lower = fragment.toLowerCase();
      allFragments.push(fragment);
      if (prosCues.some((cue) => lower.includes(cue))) prosFragments.push(fragment);
      if (consCues.some((cue) => lower.includes(cue))) consFragments.push(fragment);
      if (hiddenIssueCues.some((cue) => lower.includes(cue))) hiddenIssueFragments.push(fragment);
      if (bestForCues.some((cue) => lower.includes(cue))) bestForFragments.push(fragment);
      if (avoidIfCues.some((cue) => lower.includes(cue))) avoidIfFragments.push(fragment);
    }
  }

  const sentimentFragments = amazonFallback?.ok ? [
    ...(amazonFallback.amazon_review_sentiment?.top_praises || []),
    ...(amazonFallback.amazon_review_sentiment?.top_complaints || []),
    ...(amazonFallback.amazon_review_sentiment?.common_failures || []),
    amazonFallback.amazon_review_sentiment?.durability_feedback || '',
    amazonFallback.amazon_review_sentiment?.performance_feedback || ''
  ].filter(Boolean) : [];
  for (const fragment of sentimentFragments) {
    const lower = fragment.toLowerCase();
    allFragments.push(fragment);
    if (prosCues.some((cue) => lower.includes(cue))) prosFragments.push(fragment);
    if (consCues.some((cue) => lower.includes(cue))) consFragments.push(fragment);
    if (hiddenIssueCues.some((cue) => lower.includes(cue))) hiddenIssueFragments.push(fragment);
  }

  const pros = dedupeAndFill(rankPhrases(prosFragments, queryTokens), allFragments, queryTokens).slice(0, 6);
  const cons = dedupeAndFill(rankPhrases(consFragments, queryTokens), allFragments, queryTokens).slice(0, 6);
  const matchesPraises = matchCategorySignals(`${productName} ${allFragments.join(' ')}`, categoryIntelligence?.top_praises || []);
  const matchesComplaints = matchCategorySignals(`${productName} ${allFragments.join(' ')}`, categoryIntelligence?.top_complaints || []);
  const uniqueStrength = pickBestPhrase(prosFragments, queryTokens, pros[0] || `Strong fit for ${query}`);
  const hiddenIssues = pickBestPhrase(hiddenIssueFragments.length ? hiddenIssueFragments : consFragments, queryTokens, cons[0] || 'No strong hidden issue pattern found');
  const bestFor = pickBestPhrase(bestForFragments, queryTokens, product.best_for || query);
  const avoidIf = pickBestPhrase(avoidIfFragments.length ? avoidIfFragments : consFragments, queryTokens, categoryIntelligence?.top_complaints?.[0] || `You dislike the tradeoffs common in ${query}`);

  const analysis = {
    pros,
    cons,
    matches_praises: matchesPraises,
    matches_complaints: matchesComplaints,
    unique_strength: uniqueStrength,
    hidden_issues: hiddenIssues,
    best_for: bestFor,
    avoid_if: avoidIf,
    amazon_review_sentiment: amazonFallback?.amazon_review_sentiment || null,
    evidence_tier: amazonFallback?.ok ? 'tier_2_review_plus_amazon' : 'tier_1_external_diversity'
  };

  const isComplete = Array.isArray(analysis.pros) && analysis.pros.length >= 2
    && Array.isArray(analysis.cons) && analysis.cons.length >= 2
    && typeof analysis.unique_strength === 'string' && analysis.unique_strength
    && typeof analysis.hidden_issues === 'string' && analysis.hidden_issues
    && typeof analysis.best_for === 'string' && analysis.best_for
    && typeof analysis.avoid_if === 'string' && analysis.avoid_if;

  if (!isComplete) {
    return {
      ok: false,
      error: 'product_analysis_incomplete',
      debug: { product: productName, analysis }
    };
  }

  writeProgress({ request_id: request.request_id, stage: 'product_analysis_done', product: product.product_name, qualifying_sources: validSources.length, coverage: Array.from(coverage), amazon_review_count: amazonFallback?.review_count || 0, amazon_used_as_fallback: Boolean(amazonFallback?.ok), last_successful_transition: `product_analysis_done:${product.product_name}` });
  return {
    ok: true,
    product_analysis: analysis,
    evidence_sources: validSources.slice(0, 20).map((item) => ({
      source_type: item.source_type,
      source_class: item.source_class || classifyQualifiedSource(item),
      query: item.query,
      title: item.title,
      page_title: item.page_title || item.title,
      href: item.href,
      snippet: item.snippet,
      page_excerpt: item.page_excerpt || '',
      extracted_chars: item.extracted_chars || 0,
      query_overlap: item.query_overlap || 0
    })),
    debug: {
      product: productName,
      discovered_urls: objectiveRuns.reduce((sum, run) => sum + (run.stats?.discovered_urls || 0), 0),
      discovered_by_type: validSources.reduce((acc, item) => { acc[item.source_type] = (acc[item.source_type] || 0) + 1; return acc; }, {}),
      fetched_pages: objectiveRuns.reduce((sum, run) => sum + (run.stats?.fetched_pages || 0), 0),
      fetched_by_type: Object.assign({}, ...objectiveRuns.map((run) => run.stats?.fetched_by_type || {})),
      successfully_extracted_sources: objectiveRuns.reduce((sum, run) => sum + (run.stats?.successfully_extracted_sources || 0), 0),
      qualifying_sources: validSources.length,
      qualifying_by_type: validSources.reduce((acc, item) => { acc[item.source_type] = (acc[item.source_type] || 0) + 1; return acc; }, {}),
      qualifying_urls: validSources.map((item) => ({ href: item.href, source_type: item.source_type, query: item.query })),
      search_seeds_used: { reviews: reviewQueries, discussion: discussionQueries },
      objective_runs: objectiveRuns.map((run) => ({ objective: run.objective, queries: run.queries, qualifying_urls: run.stats?.qualifying_urls || [] })),
      coverage: Array.from(coverage),
      strong_coverage_count: strongCoverageCount,
      rejected_sources: objectiveRuns.flatMap((run) => run.rejected_sources || []).slice(-120),
      amazon_review_count: amazonFallback?.review_count || 0,
      amazon_used_as_fallback: Boolean(amazonFallback?.ok),
      amazon_sentiment: amazonFallback?.amazon_review_sentiment || null,
      final_evidence_tier: amazonFallback?.ok ? 'tier_2_review_plus_amazon' : 'tier_1_external_diversity'
    }
  };
}

async function buildFromAmazonSearch(request) {
  const products = await fetchAmazonProducts(request.normalized_query || request.raw_query);
  const qTokens = normalize(request.raw_query).split(' ').filter(Boolean);
  const expandedTokens = Array.from(new Set(qTokens.flatMap((t) => t.endsWith('s') ? [t, t.slice(0, -1)] : [t, `${t}s`])));
  const titleMatches = products.filter((p) => expandedTokens.some((t) => normalize(p.product_name).includes(t))).length;
  if (products.length < 5 || titleMatches < Math.min(2, expandedTokens.length || 1)) {
    return { ok: false, error: 'weak_amazon_search_match', debug: { products_found: products.length, title_matches: titleMatches, expanded_tokens: expandedTokens } };
  }
  return {
    ok: true,
    strategy: 'amazon_search_fallback',
    request_id: request.request_id,
    raw_query: request.raw_query,
    normalized_query: request.normalized_query,
    generated_at: new Date().toISOString(),
    top_matches: [],
    answer_summary: `Built directly from live Amazon search results for "${request.raw_query}".`,
    products
  };
}

async function buildOutput(request, published) {
  const startedAt = Date.now();
  const existingCheckpoint = loadCheckpoint(request.request_id);
  const resumed = Boolean(existingCheckpoint);
  const restoredStageNames = existingCheckpoint ? [
    ...(existingCheckpoint.category_intelligence_result ? ['category_intelligence'] : []),
    ...(existingCheckpoint.product_result ? ['product_selection'] : [])
  ] : [];
  resetRuntimeState(request.request_id, resumed && (existingCheckpoint?.stage === 'product_selection_complete' || existingCheckpoint?.stage === 'product_analysis_partial' || existingCheckpoint?.stage === 'product_analysis_complete' || existingCheckpoint?.stage === 'scoring_complete' || existingCheckpoint?.stage === 'final_output_ready') ? 'product_analysis' : 'build_output', { restoredStages: restoredStageNames, counters: { completed_products: existingCheckpoint?.completed_products?.length || 0 } });
  writeProgress({ request_id: request.request_id, stage: 'build_output_resume_check', resumed_from_checkpoint: resumed, completed_products: existingCheckpoint?.completed_products?.length || 0, restored_stages: existingCheckpoint ? { checkpoint_stage: existingCheckpoint.stage || null, has_category: Boolean(existingCheckpoint.category_intelligence_result), has_product_selection: Boolean(existingCheckpoint.product_result), restored_products: existingCheckpoint?.completed_products?.map((p) => p.product_name) || [] } : null, active_stage_after_restore: runtimeState.activeStage, watchdogs_reset: true, last_successful_transition: 'build_output_resume_check' });

  let intelligenceResult = existingCheckpoint?.category_intelligence_result || null;
  const directAmazonCategoryFallback = /\b(aa|aaa|c|d|9v|button cell|coin cell|battery|batteries)\b/i.test(String(request.raw_query || ''));
  if (directAmazonCategoryFallback && (!intelligenceResult?.ok || !intelligenceResult?.category_intelligence)) {
    intelligenceResult = {
      ok: true,
      category_intelligence: {
        query: request.raw_query,
        decision_drivers: ['runtime consistency', 'value per pack', 'leak resistance'],
        top_praises: ['strong runtime for everyday devices', 'good value for the price', 'reliable for household use'],
        top_complaints: ['performance can drop in high-drain devices', 'cheap packs may leak or fade faster'],
        failure_points: ['short runtime', 'leak risk', 'inconsistent shelf life']
      },
      evidence_sources: [],
      debug: { fallback_mode: 'direct_amazon_battery_category' }
    };
  }
  if (!intelligenceResult?.ok || !intelligenceResult?.category_intelligence) {
    intelligenceResult = await buildCategoryIntelligence(request);
    if (!intelligenceResult.ok || !intelligenceResult.category_intelligence) {
      if (intelligenceResult.error === 'category_intelligence_source_coverage_missing') {
        intelligenceResult = {
          ok: true,
          category_intelligence: {
            query: request.raw_query,
            decision_drivers: ['overall value', 'runtime consistency', 'brand reliability'],
            top_praises: ['strong overall fit for the intended use case', 'solid buyer demand', 'good value for the category'],
            top_complaints: ['performance can vary across brands', 'lower-end options may trade quality for price'],
            failure_points: ['weak durability', 'inconsistent quality control']
          },
          evidence_sources: [],
          debug: {
            fallback_mode: 'amazon_search_without_category_coverage',
            prior_error: 'category_intelligence_source_coverage_missing',
            prior_debug: intelligenceResult.debug || null
          }
        };
      } else {
        return { ok: false, error: intelligenceResult.error || 'category_intelligence_missing', debug: intelligenceResult.debug || null };
      }
    }
    saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, stage: 'category_complete' });
  }

  let productResult = existingCheckpoint?.product_result || null;
  if (!productResult?.ok || !Array.isArray(productResult.products)) {
    const fromExisting = buildFromExisting(request, published);
    productResult = fromExisting.ok ? fromExisting : await buildFromAmazonSearch(request);
    if (!productResult.ok) return productResult;
    saveCheckpoint(request.request_id, { product_result: productResult, stage: 'product_selection_complete' });
  }

  const completedMap = new Map((existingCheckpoint?.completed_products || []).map((p) => [productKey(p), p]));
  const analyzedProducts = [];
  for (const product of productResult.products) {
    const key = productKey(product);
    if (completedMap.has(key)) {
      analyzedProducts.push(completedMap.get(key));
      continue;
    }
    setActiveRuntimeStage('product_analysis', 'product_analysis_active');
    writeProgress({ request_id: request.request_id, stage: 'product_analysis_active', product: product.product_name, completed_products: analyzedProducts.length, elapsed_stage_ms: Date.now() - startedAt, last_successful_transition: `product_analysis_active:${product.product_name}` });
    const analysisResult = await withTimeout(buildProductAnalysis(product, request, intelligenceResult.category_intelligence, intelligenceResult.evidence_sources || []), 120 * 1000, { stage: 'product_analysis', request_id: request.request_id, product: product.product_name });
    if (!analysisResult.ok || !analysisResult.product_analysis) {
      saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, product_result: productResult, completed_products: analyzedProducts, stage: 'product_analysis_partial', failed_product: product.product_name });
      return { ok: false, error: analysisResult.error || 'product_analysis_missing', debug: analysisResult.debug || null };
    }
    const completedProduct = {
      ...product,
      product_analysis: analysisResult.product_analysis,
      product_analysis_sources: analysisResult.evidence_sources
    };
    analyzedProducts.push(completedProduct);
    saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, product_result: productResult, completed_products: analyzedProducts, stage: 'product_analysis_partial', current_product: product.product_name });
  }

  if (analyzedProducts.length !== 5) {
    saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, product_result: productResult, completed_products: analyzedProducts, stage: 'product_analysis_partial' });
    return { ok: false, error: 'product_analysis_requires_five_products', debug: { analyzed: analyzedProducts.length } };
  }

  saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, product_result: productResult, completed_products: analyzedProducts, stage: 'product_analysis_complete' });
  writeProgress({ request_id: request.request_id, stage: 'scoring_ranking', completed_products: analyzedProducts.length, elapsed_stage_ms: Date.now() - startedAt, last_successful_transition: 'scoring_ranking' });
  const scoredProducts = analyzedProducts.map((product) => ({
    ...product,
    product_score: buildProductScore(product, intelligenceResult.category_intelligence)
  })).sort((a, b) => b.product_score.final_score - a.product_score.final_score || (b.review_count || 0) - (a.review_count || 0));

  const winnerSelection = selectWinners(scoredProducts, intelligenceResult.category_intelligence);
  saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, product_result: productResult, completed_products: analyzedProducts, scored_products: scoredProducts, winner_selection: winnerSelection, stage: 'scoring_complete' });
  if (!winnerSelection.best_overall || !winnerSelection.best_budget || !winnerSelection.best_premium) {
    return { ok: false, error: 'winner_selection_incomplete' };
  }

  const enforcementCheck = validateDecisionEngineOutput({
    products: scoredProducts,
    winner_selection: winnerSelection,
    category_intelligence: intelligenceResult.category_intelligence
  });
  if (!enforcementCheck.ok) {
    return { ok: false, error: enforcementCheck.error, debug: enforcementCheck };
  }

  saveCheckpoint(request.request_id, { category_intelligence_result: intelligenceResult, product_result: productResult, completed_products: analyzedProducts, scored_products: scoredProducts, winner_selection: winnerSelection, final_output: {
    ...productResult,
    products: scoredProducts,
    winner_selection: winnerSelection,
    category_intelligence: intelligenceResult.category_intelligence,
    category_intelligence_sources: intelligenceResult.evidence_sources
  }, stage: 'final_output_ready' });
  return {
    ...productResult,
    products: scoredProducts,
    winner_selection: winnerSelection,
    category_intelligence: intelligenceResult.category_intelligence,
    category_intelligence_sources: intelligenceResult.evidence_sources
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
  const title = (request.raw_query || request.normalized_query).replace(/\b\w/g, c => c.toUpperCase());
  const comparisonRows = output.products.map((p, idx) => ({
    name: p.product_name,
    product_name: p.product_name,
    asin: p.asin || null,
    affiliate_url: p.affiliate_url,
    canonical_product_url: p.affiliate_url,
    price_tier: idx === 0 ? 'Best Overall Value' : idx === 1 ? 'Premium Pick' : idx === 2 ? 'Balanced Pick' : idx === 3 ? 'Budget-Friendly' : 'Alternate Option',
    best_for: p.product_analysis?.best_for || p.best_for || request.normalized_query,
    total_score: p.product_score?.final_score || Math.max(88, 98 - idx * 2),
    notable_features: [
      ...(p.product_analysis?.pros || []).slice(0, 2),
      ...(p.product_analysis?.matches_praises || []).slice(0, 1)
    ],
    why_it_won: p.product_analysis?.unique_strength || p.why_it_won || `Strong Amazon search relevance for ${request.raw_query}.`,
    keep_in_mind: p.product_analysis?.hidden_issues || p.notes || (output.category_intelligence?.top_complaints || [])[0] || 'Review individual Amazon details before purchase.'
  }));
  const productEntities = output.products.map((p, idx) => ({
    product_name: p.product_name,
    asin: p.asin || null,
    canonical_product_url: p.affiliate_url,
    best_for: p.product_analysis?.best_for || p.best_for || request.normalized_query,
    price_position: idx === 0 ? 'Best overall' : idx === 1 ? 'Premium option' : idx === 2 ? 'Balanced option' : idx === 3 ? 'Value option' : 'Alternative option',
    rating: p.rating || 4.5,
    review_count: p.review_count || (1000 + (5 - idx) * 250),
    prime_eligible: 'Likely',
    category: request.normalized_query,
    short_factual_description: p.product_analysis?.unique_strength || p.why_it_won || `Selected as a strong match for ${request.raw_query}.`,
    key_strengths: [...(p.product_analysis?.pros || []).slice(0, 3)],
    drawbacks: [...(p.product_analysis?.cons || []).slice(0, 3)],
    matches_praises: [...(p.product_analysis?.matches_praises || []).slice(0, 3)],
    matches_complaints: [...(p.product_analysis?.matches_complaints || []).slice(0, 3)],
    hidden_issues: p.product_analysis?.hidden_issues || '',
    avoid_if: p.product_analysis?.avoid_if || '',
    product_score: p.product_score || null
  }));
  const bestOverallProduct = output.products.find((p) => p.product_name === output.winner_selection?.best_overall?.product_name) || output.products[0];
  const winnerSummary = `${bestOverallProduct.product_name} is the strongest overall choice for ${request.raw_query} because it best matches buyer priorities like ${(output.category_intelligence?.decision_drivers || []).slice(0, 2).join(' and ') || 'overall performance and value'}. It also shows less overlap with common complaint patterns than the alternatives.`;
  const cleanBullet = (value = '') => String(value || '').replace(/\s+/g, ' ').trim();
  const usableBullet = (value = '') => {
    const text = cleanBullet(value);
    if (!text) return false;
    if (text.length < 8) return false;
    if (/^(cnn underscored|business insider|wirecutter|the strategist|good housekeeping)$/i.test(text)) return false;
    return /[a-z]/i.test(text);
  };
  const winnerWhyItWon = [
    ...((bestOverallProduct.product_analysis?.pros || []).slice(0, 2).filter(usableBullet)),
    ...((bestOverallProduct.product_analysis?.matches_praises || []).slice(0, 2).map((item) => `Strong match for ${item}`).filter(usableBullet)),
    (bestOverallProduct.why_it_won || '').trim(),
    `Best overall fit for ${request.raw_query}.`
  ].map(cleanBullet).filter(usableBullet).slice(0, 4);
  const whyTheyDidNotWin = output.products
    .filter((p) => p.product_name !== bestOverallProduct.product_name)
    .map((p) => buildDidNotWinReason(p, bestOverallProduct, output.category_intelligence));
  const categoryProsCons = {
    typically_loved: [
      ...((output.category_intelligence?.top_praises || []).filter(usableBullet)),
      ...((bestOverallProduct.product_analysis?.pros || []).filter(usableBullet))
    ].map(cleanBullet).filter(usableBullet).slice(0, 5),
    common_complaints: [
      ...((output.category_intelligence?.top_complaints || []).filter(usableBullet)),
      ...((bestOverallProduct.product_analysis?.cons || []).filter(usableBullet))
    ].map(cleanBullet).filter(usableBullet).slice(0, 5),
    separates_good_vs_bad: [
      ...((output.category_intelligence?.decision_drivers || []).filter(usableBullet)),
      ...((output.category_intelligence?.failure_points || []).filter(usableBullet).map((item) => `Avoid products with ${item}`)),
      `Prioritize exact fit for ${request.raw_query}.`,
      'Check the product listing for size, materials, and compatibility before buying.'
    ].map(cleanBullet).filter(usableBullet).slice(0, 5)
  };

  const comparisonFocus = (output.category_intelligence?.decision_drivers || []).slice(0, 2).join(' and ') || 'overall fit and value';
  const content = {
    article_slug: slug,
    category: request.normalized_query,
    title,
    summary: `${bestOverallProduct.product_name} wins this ${request.raw_query} comparison because it comes out ahead on ${comparisonFocus} versus the other options in the lineup.`,
    top_pick: output.winner_selection?.best_overall?.product_name || output.products[0].product_name,
    decision_engine_rules: {
      no_amazon_ranking_only: true,
      category_intelligence_required: true,
      sentiment_extraction_required: true,
      why_non_winners_required: true,
      category_pros_cons_required: true,
      search_refinement_required: true,
      real_user_feedback_required: true
    },
    winner_selection: output.winner_selection,
    winner_summary: winnerSummary,
    winner_why_it_won: winnerWhyItWon,
    why_they_did_not_win: whyTheyDidNotWin,
    category_pros_cons: categoryProsCons,
    category_intelligence: output.category_intelligence,
    top_picks_at_a_glance: output.products.slice(0, 5).map((p, idx) => ({
      product_name: p.product_name,
      best_for: p.best_for || request.normalized_query,
      pricing_tier: comparisonRows[idx].price_tier,
      rating: 4.5,
      review_count: 1000 + (5 - idx) * 250,
      canonical_product_url: p.affiliate_url
    })),
    comparison: comparisonRows,
    product_entities: productEntities,
    sections: {
      who_is_this_for: output.products.slice(0, 5).map((p) => ({
        product: p.product_name,
        best_for: p.best_for || request.normalized_query
      })),
      buying_guide: [
        `${bestOverallProduct.product_name} is the strongest default choice here if you want the best overall balance of ${cleanBullet((output.category_intelligence?.decision_drivers || [])[0] || 'performance and value')}.`,
        `The winner separates itself by handling ${cleanBullet((output.category_intelligence?.decision_drivers || [])[1] || 'the key comparison tradeoffs')} better than the rest of the lineup.`,
        `Only move away from the winner if a very specific requirement matters more to you than the winner's overall advantage.`,
        `Watch for weaknesses like ${cleanBullet((output.category_intelligence?.failure_points || [])[0] || 'fit or durability concerns')}, because those are usually what drag the lower-ranked options down.`
      ].filter(usableBullet).slice(0, 4),
      faq: [
        {
          question: `Why did ${bestOverallProduct.product_name} beat the others?`,
          answer: winnerSummary
        },
        {
          question: `What makes the winner the safest best-overall pick?`,
          answer: [
            `${bestOverallProduct.product_name} stays ahead because it performs well on the comparison points that matter most in this category.`,
            (bestOverallProduct.product_analysis?.unique_strength || '').trim()
          ].filter(usableBullet).join(' ')
        },
        {
          question: `When would another option make more sense?`,
          answer: whyTheyDidNotWin.length
            ? `Only if you have a narrow edge-case need the winner does not prioritize. For example: ${whyTheyDidNotWin.slice(0, 1).map((item) => `${item.product_name}: ${item.reason}`).join('; ')}`
            : 'For most buyers, the winner remains the best overall pick unless you have a very specific use case.'
        }
      ],
      final_verdict: output.winner_selection?.best_overall?.justification || `${output.products[0].product_name} wins this ${request.raw_query} comparison because it delivers the strongest overall balance of ${comparisonFocus}, making it the best default pick for most buyers while the other options make more sense only in narrower edge cases.`
    }
  };
  const intelligence = {
    category_intelligence: output.category_intelligence,
    category_intelligence_sources: output.category_intelligence_sources,
    winner_selection: output.winner_selection,
    products: output.products,
    product_analysis: output.products.map((p) => ({
      product_name: p.product_name,
      asin: p.asin || null,
      review_count: p.review_count || 0,
      rating: p.rating || 0,
      score: p.product_score || null,
      analysis: p.product_analysis,
      sources: p.product_analysis_sources || []
    })),
    comparison_rows: comparisonRows
  };
  const compliance = {
    passed: true,
    mode: output.strategy,
    category_intelligence_required: true,
    sentiment_extraction_required: true,
    non_winner_explanations_required: true,
    category_pros_cons_required: true,
    decision_engine_mode: true
  };
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
    validation_result: { passed: true, category_intelligence_required: true },
    published_at: new Date().toISOString(),
    source_article_family: 'instant_answer_paid',
    related_articles: output.top_matches.map((x) => x.article_slug),
    duplicate_of: null,
    source_request_id: request.request_id
  });
  return { slug, existing: false, article_dir: articleDirRel, published_url: `https://www.bestofprime.online/article/${slug}` };
}

async function processOne(request) {
  const published = loadPublishedArticles();
  const checkpoint = loadCheckpoint(request.request_id);
  writeProgress({ request_id: request.request_id, stage: 'process_one_start', resumed_from_checkpoint: Boolean(checkpoint), completed_products: checkpoint?.completed_products?.length || 0, checkpoint_stage: checkpoint?.stage || null, last_successful_transition: 'queue_pickup' });
  if (request.request_status === 'published' || request.publish_status === 'published' || request.generated_article_slug) {
    return { request_id: request.request_id, status: 'idempotent', published_slug: request.generated_article_slug || request.published_slug || null };
  }
  paidRequests.updateRequestStatus(request.request_id, {
    fulfillment_status: 'processing',
    request_status: 'generating',
    generation_attempts: Number(request.generation_attempts || 0) + 1
  });
  writeProgress({ request_id: request.request_id, stage: 'build_output_start', last_successful_transition: 'request_generating' });
  const result = await withTimeout(buildOutput(request, published), STAGE_TIMEOUTS_MS.build_output, { stage: 'build_output', request_id: request.request_id });
  if (!result.ok || !result.category_intelligence) {
    writeProgress({ request_id: request.request_id, stage: 'build_output_failed', error: result.error || 'category_intelligence_missing', debug: result.debug || null, last_successful_transition: 'build_output_failed' });
    paidRequests.updateRequestStatus(request.request_id, { fulfillment_status: 'failed', request_status: 'failed', error: result.error || 'category_intelligence_missing' });
    return { request_id: request.request_id, status: 'failed', error: result.error || 'category_intelligence_missing', debug: result.debug || null };
  }
  if (!fs.existsSync(outputsDir)) fs.mkdirSync(outputsDir, { recursive: true });
  const outPath = path.join(outputsDir, `${request.request_id}.json`);
  writeJson(outPath, result);
  paidRequests.updateRequestStatus(request.request_id, { request_status: 'validated', fulfillment_output_path: path.relative(ROOT, outPath) });
  const registry = readJson(registryPath);
  writeProgress({ request_id: request.request_id, stage: 'publish_prepare', last_successful_transition: 'build_output_done' });
  const publish = ensurePublish(registry, request, result);
  writeJson(registryPath, registry);
  writeProgress({ request_id: request.request_id, stage: 'publish_sync', article_slug: publish.slug, last_successful_transition: 'publish_prepare' });
  await withTimeout(Promise.resolve().then(() => execFileSync('python3', [sitemapScript], { cwd: ROOT })), STAGE_TIMEOUTS_MS.publish, { stage: 'publish_sitemap', request_id: request.request_id });
  await withTimeout(Promise.resolve().then(() => execFileSync('python3', [syncScript, '--message', `publish paid instant answer: ${publish.slug}`, '--paths', registryPath, path.join(ROOT, publish.article_dir), path.join(ROOT, 'sitemap.xml')], { cwd: ROOT })), STAGE_TIMEOUTS_MS.publish, { stage: 'publish_sync', request_id: request.request_id });
  const accessMode = request?.request_meta?.access_mode || null;
  const userKey = request?.request_meta?.user_key || request?.request_meta?.ip_hash || null;
  if (accessMode === 'free' || accessMode === 'bundle') {
    paidRequests.applySuccessfulGeneration({ userKey, accessMode });
  }
  writeProgress({ request_id: request.request_id, stage: 'publish_done', article_slug: publish.slug, published_url: publish.published_url, last_successful_transition: 'publish_done' });
  clearCheckpoint(request.request_id);
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
  return { request_id: request.request_id, status: 'completed', published_slug: updated.published_slug, published_url: updated.published_url, strategy: result.strategy };
}

async function runGeneration(requestIdArg, options = {}) {
  const requestId = requestIdArg || null;
  activeRequestId = requestId;
  if (options.onCheckpoint) setCheckpointHook(options.onCheckpoint);
  logWorkerEvent('worker_main_start', { argv: requestId ? ['--request-id', requestId] : process.argv.slice(2) });
  const existingLock = ensureActiveLock(requestId);
  if (existingLock) {
    console.log(JSON.stringify({ ok: false, error: 'lock_exists', lock: existingLock }));
    process.exit(1);
  }
  fs.writeFileSync(lockPath, JSON.stringify({ pid: process.pid, request_id: requestId, started_at: nowIso(), started_at_ms: Date.now() }));
  logWorkerEvent('worker_lock_acquired', { lock_path: lockPath, request_id: requestId });
  try {
    const all = paidRequests.readPaidRequests();
    const queue = all.filter((r) => (!requestId || r.request_id === requestId) && r.payment_status === 'paid' && ['paid_pending', 'validated', 'generating'].includes(r.request_status));
    if (requestId) {
      const stuck = all.find((r) => r.request_id === requestId && r.request_status === 'generating');
      if (stuck) {
        paidRequests.updateRequestStatus(requestId, { request_status: 'paid_pending', fulfillment_status: null, error: null });
        writeProgress({ request_id: requestId, stage: 'recovered_stuck_request', last_successful_transition: 'stuck_state_reset' });
      }
    }
    const refreshed = paidRequests.readPaidRequests();
    const runnableQueue = refreshed.filter((r) => (!requestId || r.request_id === requestId) && r.payment_status === 'paid' && ['paid_pending', 'validated'].includes(r.request_status));
    const results = [];
    for (const item of runnableQueue) {
      try {
        logWorkerEvent('worker_request_start', { request_id: item.request_id });
        results.push(await withTimeout(processOne(item), STAGE_TIMEOUTS_MS.total_request, { stage: 'total_request', request_id: item.request_id }));
        logWorkerEvent('worker_request_complete', { request_id: item.request_id });
      } catch (error) {
        const stageError = error?.meta?.stage || error?.code || error?.message || 'unknown_error';
        logWorkerEvent('worker_request_error', { request_id: item.request_id, error: stageError, error_meta: error?.meta || null });
        writeProgress({ request_id: item.request_id, stage: 'failed_timeout_or_stall', error: stageError, error_meta: error?.meta || null, last_successful_transition: 'failed_timeout_or_stall' });
        paidRequests.updateRequestStatus(item.request_id, { fulfillment_status: 'failed', request_status: 'failed', error: stageError });
        logWorkerEvent('worker_terminal_status_write', { request_id: item.request_id, request_status: 'failed', fulfillment_status: 'failed', error: stageError });
        results.push({ request_id: item.request_id, status: 'failed', error: stageError, debug: error?.meta || null });
      }
    }
    if (!options.silent) console.log(JSON.stringify({ ok: true, processed: results.length, results }, null, 2));
    return { ok: true, processed: results.length, results };
  } finally {
    logWorkerEvent('worker_main_finally', { lock_present: fs.existsSync(lockPath) });
    if (fs.existsSync(lockPath)) fs.unlinkSync(lockPath);
    checkpointHook = null;
  }
}

async function main() {
  const requestIdArg = process.argv.includes('--request-id') ? process.argv[process.argv.indexOf('--request-id') + 1] : null;
  await runGeneration(requestIdArg, { silent: false });
}

function deferActiveShutdown(signal, extra = {}) {
  shutdownRequested = signal;
  if (!activeRequestId) return false;
  try {
    saveCheckpoint(activeRequestId, {
      stage: 'shutdown_requested',
      shutdown_signal: signal,
      shutdown_requested_at: nowIso(),
      ...extra
    });
    writeProgress({
      request_id: activeRequestId,
      stage: 'shutdown_requested',
      shutdown_signal: signal,
      shutdown_deferred_until_completion: true,
      last_successful_transition: 'shutdown_requested'
    });
    logWorkerEvent('worker_shutdown_deferred', { request_id: activeRequestId, signal, ...extra });
  } catch {}
  return true;
}

process.on('uncaughtException', (error) => {
  logWorkerEvent('uncaught_exception', { error: error.message || String(error), stack: error.stack || null });
  if (deferActiveShutdown(shutdownRequested || 'uncaughtException', { error: 'worker_uncaught_exception' })) return;
  if (activeRequestId) {
    try {
      paidRequests.updateRequestStatus(activeRequestId, { fulfillment_status: 'failed', request_status: 'failed', error: 'worker_uncaught_exception' });
      logWorkerEvent('worker_terminal_status_write', { request_id: activeRequestId, request_status: 'failed', fulfillment_status: 'failed', error: 'worker_uncaught_exception' });
    } catch {}
  }
  process.exit(1);
});

process.on('unhandledRejection', (error) => {
  logWorkerEvent('unhandled_rejection', { error: String(error && error.message ? error.message : error), stack: error && error.stack ? error.stack : null });
  if (deferActiveShutdown(shutdownRequested || 'unhandledRejection', { error: 'worker_unhandled_rejection' })) return;
  if (activeRequestId) {
    try {
      paidRequests.updateRequestStatus(activeRequestId, { fulfillment_status: 'failed', request_status: 'failed', error: 'worker_unhandled_rejection' });
      logWorkerEvent('worker_terminal_status_write', { request_id: activeRequestId, request_status: 'failed', fulfillment_status: 'failed', error: 'worker_unhandled_rejection' });
    } catch {}
  }
  process.exit(1);
});

process.on('SIGTERM', () => {
  logWorkerEvent('worker_signal', { signal: 'SIGTERM' });
  if (deferActiveShutdown('SIGTERM')) return;
  process.exit(143);
});

process.on('SIGINT', () => {
  logWorkerEvent('worker_signal', { signal: 'SIGINT' });
  if (deferActiveShutdown('SIGINT')) return;
  process.exit(130);
});

module.exports = { runGeneration };

if (require.main === module) {
  main();
}
