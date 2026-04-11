const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

module.exports = function createPaidRequests({ rootDir }) {
  const analyticsDir = path.join(rootDir, 'data', 'analytics');
  const paidRequestsStoreDir = process.env.PAID_REQUESTS_STORE_DIR ? path.resolve(process.env.PAID_REQUESTS_STORE_DIR) : analyticsDir;
  const searchQueriesPath = path.join(analyticsDir, 'search_queries.json');
  const paidRequestsPath = path.join(paidRequestsStoreDir, 'paid_generation_requests.json');
  const userBalancesPath = path.join(paidRequestsStoreDir, 'article_user_balances.json');

  function ensureStores() {
    if (!fs.existsSync(analyticsDir)) fs.mkdirSync(analyticsDir, { recursive: true });
    if (!fs.existsSync(paidRequestsStoreDir)) fs.mkdirSync(paidRequestsStoreDir, { recursive: true });
    if (!fs.existsSync(searchQueriesPath)) fs.writeFileSync(searchQueriesPath, '[]\n');
    if (!fs.existsSync(paidRequestsPath)) fs.writeFileSync(paidRequestsPath, '[]\n');
    if (!fs.existsSync(userBalancesPath)) fs.writeFileSync(userBalancesPath, '{}\n');
  }

  function readJson(filePath) {
    ensureStores();
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }

  function writeJson(filePath, payload) {
    ensureStores();
    fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
  }

  function normalizeSearchQuery(query) {
    return String(query || '')
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9\s-]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function guessQueryType(query) {
    const q = normalizeSearchQuery(query);
    if (!q) return 'unclear';
    const categoryHints = ['best', 'for', 'under', 'vs', 'comparison', 'guide'];
    const productHints = ['vitamix', 'oral-b', 'sonicare', 'blueair', 'levoit', 'ninja', 'instant pot'];
    if (productHints.some((x) => q.includes(x))) return 'product';
    if (categoryHints.some((x) => q.includes(x)) || q.split(' ').length >= 2) return 'category';
    return 'unclear';
  }

  function appendSearchQuery({ raw_query, matched_article_slug = null, timestamp = null }) {
    const rows = readJson(searchQueriesPath);
    const normalized = normalizeSearchQuery(raw_query);
    const entry = {
      raw_query,
      normalized_query: normalized,
      timestamp: timestamp || new Date().toISOString(),
      match_found: Boolean(matched_article_slug),
      matched_article_slug: matched_article_slug || null,
      query_type_guess: guessQueryType(raw_query),
    };
    rows.push(entry);
    writeJson(searchQueriesPath, rows);
    return entry;
  }

  function readUserBalances() {
    const payload = readJson(userBalancesPath);
    return payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : {};
  }

  function writeUserBalances(payload) {
    writeJson(userBalancesPath, payload);
  }

  function getUserKey(input = {}) {
    return input.user_key || input.ip_hash || null;
  }

  function buildDefaultUserRecord(userKey, patch = {}) {
    return {
      user_key: userKey,
      ip_hash: patch.ip_hash || userKey,
      country: patch.country || null,
      free_articles_used: Number(patch.free_articles_used || 0),
      articles_remaining_balance: Number(patch.articles_remaining_balance || 0),
      total_articles_purchased: Number(patch.total_articles_purchased || 0),
      total_articles_generated: Number(patch.total_articles_generated || 0),
      total_paid_articles_consumed: Number(patch.total_paid_articles_consumed || 0),
      total_free_articles_consumed: Number(patch.total_free_articles_consumed || 0),
      last_purchase_at: patch.last_purchase_at || null,
      stripe_customer_id: patch.stripe_customer_id || null,
      processed_checkout_sessions: Array.isArray(patch.processed_checkout_sessions) ? patch.processed_checkout_sessions : [],
      created_at: patch.created_at || new Date().toISOString(),
      updated_at: patch.updated_at || new Date().toISOString()
    };
  }

  function getUserRecord(userKey, defaults = {}) {
    if (!userKey) return null;
    const users = readUserBalances();
    const existing = users[userKey];
    if (existing) return existing;
    return buildDefaultUserRecord(userKey, defaults);
  }

  function upsertUserRecord(userKey, patch = {}) {
    if (!userKey) return null;
    const users = readUserBalances();
    const existing = users[userKey] || buildDefaultUserRecord(userKey, patch);
    const merged = {
      ...existing,
      ...patch,
      user_key: userKey,
      ip_hash: patch.ip_hash || existing.ip_hash || userKey,
      processed_checkout_sessions: Array.isArray(patch.processed_checkout_sessions)
        ? patch.processed_checkout_sessions
        : (existing.processed_checkout_sessions || []),
      updated_at: new Date().toISOString()
    };
    users[userKey] = merged;
    writeUserBalances(users);
    return merged;
  }

  function applySuccessfulGeneration({ userKey, accessMode }) {
    if (!userKey) return null;
    const users = readUserBalances();
    const existing = users[userKey] || buildDefaultUserRecord(userKey);
    const next = { ...existing };

    if (accessMode === 'free') {
      next.free_articles_used = Number(next.free_articles_used || 0) + 1;
      next.total_free_articles_consumed = Number(next.total_free_articles_consumed || 0) + 1;
    } else if (accessMode === 'bundle') {
      const currentBalance = Number(next.articles_remaining_balance || 0);
      if (currentBalance <= 0) {
        throw new Error('insufficient_bundle_balance');
      }
      next.articles_remaining_balance = currentBalance - 1;
      next.total_paid_articles_consumed = Number(next.total_paid_articles_consumed || 0) + 1;
    }

    next.total_articles_generated = Number(next.total_articles_generated || 0) + 1;
    next.updated_at = new Date().toISOString();
    users[userKey] = next;
    writeUserBalances(users);
    return next;
  }

  function applyBundlePurchase({ userKey, bundleSize, stripeCustomerId = null, checkoutSessionId = null, purchasedAt = null, ipHash = null, country = null }) {
    if (!userKey) return null;
    const users = readUserBalances();
    const existing = users[userKey] || buildDefaultUserRecord(userKey, { ip_hash: ipHash || userKey, country });
    const processed = Array.isArray(existing.processed_checkout_sessions) ? existing.processed_checkout_sessions : [];

    if (checkoutSessionId && processed.includes(checkoutSessionId)) {
      return { user: existing, alreadyProcessed: true };
    }

    const next = { ...existing };
    next.articles_remaining_balance = Number(next.articles_remaining_balance || 0) + Number(bundleSize || 0);
    next.total_articles_purchased = Number(next.total_articles_purchased || 0) + Number(bundleSize || 0);
    next.last_purchase_at = purchasedAt || new Date().toISOString();
    next.stripe_customer_id = stripeCustomerId || next.stripe_customer_id || null;
    next.ip_hash = ipHash || next.ip_hash || userKey;
    next.country = country || next.country || null;
    next.processed_checkout_sessions = checkoutSessionId ? [...processed, checkoutSessionId] : processed;
    next.updated_at = new Date().toISOString();
    users[userKey] = next;
    writeUserBalances(users);
    return { user: next, alreadyProcessed: false };
  }

  function upsertRequest(record) {
    const rows = readJson(paidRequestsPath);
    const idx = rows.findIndex((x) => x.request_id === record.request_id);
    if (idx >= 0) rows[idx] = { ...rows[idx], ...record };
    else rows.push(record);
    writeJson(paidRequestsPath, rows);
    return idx >= 0 ? rows[idx] : record;
  }

  function buildRequestRecord({ raw_query, requested_by = null, notes = null, request_meta = null }) {
    const normalized = normalizeSearchQuery(raw_query);
    return {
      request_id: crypto.randomUUID(),
      raw_query,
      normalized_query: normalized,
      created_at: new Date().toISOString(),
      requested_by,
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
      notes: notes || null,
      error: null,
      stripe_checkout_session_id: null,
      stripe_payment_status: null,
      paid_at: null,
      request_meta: request_meta || null,
    };
  }

  function createPaidRequest(input) {
    const rows = readJson(paidRequestsPath);
    const record = buildRequestRecord(input);
    rows.push(record);
    writeJson(paidRequestsPath, rows);
    return record;
  }

  function countSuccessfulGenerationsByIpHash(ipHash) {
    if (!ipHash) return 0;
    const user = getUserRecord(ipHash);
    return Number(user?.free_articles_used || 0) + Number(user?.total_paid_articles_consumed || 0);
  }

  function updateRequestStatus(requestId, patch) {
    const rows = readJson(paidRequestsPath);
    const idx = rows.findIndex((x) => x.request_id === requestId);
    if (idx === -1) return null;
    rows[idx] = { ...rows[idx], ...patch };
    writeJson(paidRequestsPath, rows);
    return rows[idx];
  }

  function getRequestById(requestId) {
    return readJson(paidRequestsPath).find((x) => x.request_id === requestId) || null;
  }

  function getRequestByStripeCheckoutSessionId(sessionId) {
    return readJson(paidRequestsPath).find((x) => x.stripe_checkout_session_id === sessionId) || null;
  }

  function readSearchQueries() {
    return readJson(searchQueriesPath);
  }

  function readPaidRequests() {
    return readJson(paidRequestsPath);
  }

  ensureStores();

  return {
    normalizeSearchQuery,
    guessQueryType,
    appendSearchQuery,
    createPaidRequest,
    updateRequestStatus,
    getRequestById,
    getRequestByStripeCheckoutSessionId,
    upsertRequest,
    paths: {
      analyticsDir,
      paidRequestsStoreDir,
      searchQueriesPath,
      paidRequestsPath,
      userBalancesPath
    },
    readSearchQueries,
    readPaidRequests,
    readUserBalances,
    getUserRecord,
    upsertUserRecord,
    applySuccessfulGeneration,
    applyBundlePurchase,
    getUserKey,
    countSuccessfulGenerationsByIpHash,
    paidRequestsPath,
    userBalancesPath,
  };
};
