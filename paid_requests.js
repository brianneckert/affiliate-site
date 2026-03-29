const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

module.exports = function createPaidRequests({ rootDir }) {
  const analyticsDir = path.join(rootDir, 'data', 'analytics');
  const searchQueriesPath = path.join(analyticsDir, 'search_queries.json');
  const paidRequestsPath = path.join(analyticsDir, 'paid_generation_requests.json');

  function ensureStores() {
    if (!fs.existsSync(analyticsDir)) fs.mkdirSync(analyticsDir, { recursive: true });
    if (!fs.existsSync(searchQueriesPath)) fs.writeFileSync(searchQueriesPath, '[]\n');
    if (!fs.existsSync(paidRequestsPath)) fs.writeFileSync(paidRequestsPath, '[]\n');
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

  function buildRequestRecord({ raw_query, requested_by = null, notes = null }) {
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
      generation_attempts: 0,
      fulfillment_output_path: null,
      notes: notes || null,
      error: null,
      stripe_checkout_session_id: null,
      stripe_payment_status: null,
      paid_at: null,
    };
  }

  function createPaidRequest(input) {
    const rows = readJson(paidRequestsPath);
    const record = buildRequestRecord(input);
    rows.push(record);
    writeJson(paidRequestsPath, rows);
    return record;
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
    readSearchQueries,
    readPaidRequests,
  };
};
