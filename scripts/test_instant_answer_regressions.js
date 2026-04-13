#!/usr/bin/env node
const assert = require('assert');
const { buildDistinctiveTokenSet, shouldReuseExistingContentMatch, shouldMarkRequestOrphaned } = require('../lib/instant_answer_guardrails');

function testDistinctiveTokenGuard() {
  const qTokens = ['30x20', 'air', 'filter', 'conditioner'];
  const distinctive = buildDistinctiveTokenSet(qTokens);
  assert.deepStrictEqual(distinctive, ['30x20']);

  const badPurifierReuse = shouldReuseExistingContentMatch({
    titleHits: 3,
    productHits: 1,
    overlapRatio: 0.75,
    score: 14,
    distinctiveTitleHits: 0,
    distinctiveProductHits: 0,
    hasDistinctiveTokens: true
  });
  assert.strictEqual(badPurifierReuse, false, 'purifier-style generic overlap should not be reusable');

  const goodHvacReuse = shouldReuseExistingContentMatch({
    titleHits: 3,
    productHits: 2,
    overlapRatio: 0.75,
    score: 18,
    distinctiveTitleHits: 1,
    distinctiveProductHits: 1,
    hasDistinctiveTokens: true
  });
  assert.strictEqual(goodHvacReuse, true, 'size-specific HVAC overlap should remain reusable');
}

function testOrphanGraceGuard() {
  const freshRequest = shouldMarkRequestOrphaned({
    requestStatus: 'generating',
    fulfillmentStatus: 'processing',
    runtimeOrphaned: true,
    requestAgeMs: 60 * 1000,
    orphanGraceMs: 5 * 60 * 1000
  });
  assert.strictEqual(freshRequest, false, 'fresh generating requests should not be orphaned immediately');

  const staleRequest = shouldMarkRequestOrphaned({
    requestStatus: 'generating',
    fulfillmentStatus: 'processing',
    runtimeOrphaned: true,
    requestAgeMs: 10 * 60 * 1000,
    orphanGraceMs: 5 * 60 * 1000
  });
  assert.strictEqual(staleRequest, true, 'stale orphaned requests should still fail');
}

testDistinctiveTokenGuard();
testOrphanGraceGuard();
console.log('instant-answer regression guards: ok');
