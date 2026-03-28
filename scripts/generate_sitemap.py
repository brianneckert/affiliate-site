#!/usr/bin/env python3
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / 'data' / 'articles' / 'registry.json'
SITEMAP = ROOT / 'sitemap.xml'
BASE_URL = 'https://affiliate-site-9r59.onrender.com'

reg = json.loads(REGISTRY.read_text())
urls = [BASE_URL + '/']
for article in reg.get('articles', []):
    if article.get('publish_status') == 'published':
        urls.append(f"{BASE_URL}/article/{article['article_slug']}")
xml = ['<?xml version="1.0" encoding="UTF-8"?>', '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
for url in urls:
    xml.append('  <url>')
    xml.append(f'    <loc>{url}</loc>')
    xml.append('  </url>')
xml.append('</urlset>')
SITEMAP.write_text('\n'.join(xml) + '\n')
print(SITEMAP)
