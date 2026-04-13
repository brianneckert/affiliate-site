function shouldMarkRequestOrphaned({ requestStatus, fulfillmentStatus, runtimeOrphaned, requestAgeMs, orphanGraceMs = 5 * 60 * 1000 }) {
  return requestStatus === 'generating'
    && fulfillmentStatus === 'processing'
    && runtimeOrphaned
    && Number(requestAgeMs || 0) > orphanGraceMs;
}

function buildDistinctiveTokenSet(tokens = []) {
  const genericCategoryTokens = new Set(['air', 'filter', 'filters', 'conditioner', 'conditioners', 'unit', 'units', 'system', 'systems']);
  return tokens.filter((token) => !genericCategoryTokens.has(token));
}

function shouldReuseExistingContentMatch({ titleHits = 0, productHits = 0, overlapRatio = 0, score = 0, distinctiveTitleHits = 0, distinctiveProductHits = 0, hasDistinctiveTokens = false }) {
  const productAnchoredMatch = productHits >= 1;
  const distinctiveMatchRequired = !hasDistinctiveTokens || distinctiveTitleHits >= 1 || distinctiveProductHits >= 1;
  return titleHits >= 1 && overlapRatio >= 0.6 && score >= 3 && productAnchoredMatch && distinctiveMatchRequired;
}

module.exports = {
  shouldMarkRequestOrphaned,
  buildDistinctiveTokenSet,
  shouldReuseExistingContentMatch
};
