#!/usr/bin/env node
const path = require('path');
const { ensurePublish, readJson, writeJson, registryPath, buildProductScore, selectWinners } = require('./process_paid_instant_answers');

const outputPath = process.argv[2];
const rawQuery = process.argv[3];
if (!outputPath || !rawQuery) {
  console.error('Usage: node scripts/regenerate_article_from_output.js <output-json> <raw-query>');
  process.exit(1);
}
const absOutput = path.resolve(outputPath);
const output = readJson(absOutput);
const registry = readJson(registryPath);
const normalized = String(output.normalized_query || rawQuery).trim();
if (Array.isArray(output.products) && output.category_intelligence) {
  output.products = output.products
    .map((product) => ({ ...product, product_score: buildProductScore(product, { ...output.category_intelligence, query: rawQuery }) }))
    .sort((a, b) => b.product_score.final_score - a.product_score.final_score || (b.review_count || 0) - (a.review_count || 0))
    .map((product, index, arr) => {
      if (index === 0) return product;
      const prev = arr[index - 1].product_score.final_score;
      const current = product.product_score.final_score;
      const adjusted = current >= prev - 0.2 ? Math.max(1, Number((prev - (0.3 + (index * 0.1))).toFixed(1))) : current;
      return adjusted === current ? product : { ...product, product_score: { ...product.product_score, final_score: adjusted } };
    });
  output.winner_selection = selectWinners(output.products, { ...output.category_intelligence, query: rawQuery });
}
const request = {
  raw_query: rawQuery,
  normalized_query: normalized,
  request_id: output.request_id || `regen-${Date.now()}`
};
const result = ensurePublish(registry, request, output, { forceRewrite: true });
writeJson(registryPath, registry);
console.log(JSON.stringify({ ok: true, result }, null, 2));
