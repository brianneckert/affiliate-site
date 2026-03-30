const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

module.exports = function createAnalytics({ rootDir, registryPath }) {
  const analyticsDir = path.join(rootDir, 'data/analytics');
  const REPORT_TIME_ZONE = 'America/Los_Angeles';
  const eventsPath = path.join(analyticsDir, 'events.json');
  const summaryPath = path.join(analyticsDir, 'summary.json');
  const activeSessionsPath = path.join(analyticsDir, 'active_sessions.json');
  let queue = [];
  let flushTimer = null;

  function ensureStores() {
    if (!fs.existsSync(analyticsDir)) fs.mkdirSync(analyticsDir, { recursive: true });
    if (!fs.existsSync(eventsPath)) fs.writeFileSync(eventsPath, '[]\n');
    if (!fs.existsSync(summaryPath)) fs.writeFileSync(summaryPath, JSON.stringify(defaultSummary(), null, 2));
    if (!fs.existsSync(activeSessionsPath)) fs.writeFileSync(activeSessionsPath, JSON.stringify({ generated_at: new Date().toISOString(), current_viewers: 0, sessions: [] }, null, 2));
  }

  function defaultSummary() {
    return {
      generated_at: new Date().toISOString(),
      traffic: {},
      sources: {},
      geography: {},
      content_performance: {},
      monetization: {},
      search_intelligence: {},
      charts: {},
      realtime: { current_viewers: 0, active_sessions: [] },
      events_count: 0,
    };
  }

  function readRegistry() {
    if (!fs.existsSync(registryPath)) return { articles: [] };
    return JSON.parse(fs.readFileSync(registryPath, 'utf8'));
  }

  function readEvents() {
    ensureStores();
    return JSON.parse(fs.readFileSync(eventsPath, 'utf8'));
  }

  function formatDayKey(dateLike) {
    const date = new Date(dateLike || Date.now());
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: REPORT_TIME_ZONE,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).formatToParts(date);
    const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));
    return `${map.year}-${map.month}-${map.day}`;
  }

  function getLastNDays(count = 30) {
    const days = [];
    const now = new Date();
    for (let i = count - 1; i >= 0; i -= 1) {
      days.push(formatDayKey(now.getTime() - i * 24 * 60 * 60 * 1000));
    }
    return days;
  }

  function readSummary() {
    ensureStores();
    const next = summarize(readEvents());
    fs.writeFileSync(summaryPath, JSON.stringify(next, null, 2));
    return next;
  }

  function readActiveSessions() {
    ensureStores();
    return JSON.parse(fs.readFileSync(activeSessionsPath, 'utf8'));
  }

  function hashIp(ip) {
    return crypto.createHash('sha256').update(String(ip || 'unknown')).digest('hex').slice(0, 16);
  }

  function truncateIp(ip) {
    const raw = String(ip || '').trim();
    if (!raw) return 'unknown';
    if (raw.includes(':')) return raw.split(':').slice(0, 4).join(':') + '::';
    const parts = raw.split('.');
    if (parts.length === 4) return `${parts[0]}.${parts[1]}.${parts[2]}.0`;
    return 'unknown';
  }

  function getClientIp(req) {
    const forwarded = String(req.headers['cf-connecting-ip'] || req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '').split(',')[0].trim();
    return forwarded || 'unknown';
  }

  function getCountry(req) {
    return String(req.headers['cf-ipcountry'] || req.headers['x-vercel-ip-country'] || req.headers['x-country-code'] || 'unknown').toUpperCase();
  }

  function getDeviceType(userAgent) {
    const ua = String(userAgent || '').toLowerCase();
    return /iphone|android|mobile|ipad|tablet/.test(ua) ? 'mobile' : 'desktop';
  }

  function getReferrerDomain(referrer) {
    try {
      return referrer ? new URL(referrer).hostname.replace(/^www\./, '') : null;
    } catch {
      return null;
    }
  }

  function classifyTrafficSource(referrer) {
    const domain = getReferrerDomain(referrer);
    if (!domain) return { traffic_source: 'direct', referrer_domain: null };
    if (domain.includes('google.')) return { traffic_source: 'google', referrer_domain: domain };
    if (/(bing|yahoo|duckduckgo|baidu|yandex)\./.test(domain)) return { traffic_source: 'search', referrer_domain: domain };
    if (/(facebook|instagram|tiktok|x\.com|twitter|linkedin|pinterest|reddit|youtube)\./.test(domain)) return { traffic_source: 'social', referrer_domain: domain };
    return { traffic_source: 'external_referral', referrer_domain: domain };
  }

  function buildPageViewEvent(req, articleSlug = 'home') {
    const ip = getClientIp(req);
    const fullUrl = `${req.protocol}://${req.get('host')}${req.originalUrl}`;
    const userAgent = req.get('user-agent') || '';
    const referrer = req.get('referer') || '';
    return {
      event_type: 'page_view',
      article_slug: articleSlug,
      full_url: fullUrl,
      referrer,
      user_agent: userAgent,
      ip_hash: hashIp(ip),
      ip_prefix: truncateIp(ip),
      country: getCountry(req),
      device_type: getDeviceType(userAgent),
      ...classifyTrafficSource(referrer),
      timestamp: new Date().toISOString(),
    };
  }


  function cleanupActiveSessions(payload) {
    const now = Date.now();
    const sessions = (payload.sessions || []).filter((s) => now - new Date(s.last_seen_at || 0).getTime() <= 90000 && !s.closed_at);
    return {
      generated_at: new Date().toISOString(),
      current_viewers: sessions.length,
      sessions,
    };
  }

  function updatePresence(session) {
    ensureStores();
    const payload = cleanupActiveSessions(readActiveSessions());
    const sessions = payload.sessions || [];
    const idx = sessions.findIndex((s) => s.session_id === session.session_id);
    if (session.closed_at) {
      if (idx >= 0) sessions.splice(idx, 1);
    } else if (idx >= 0) {
      sessions[idx] = { ...sessions[idx], ...session, last_seen_at: new Date().toISOString() };
    } else {
      sessions.push({ ...session, last_seen_at: new Date().toISOString() });
    }
    const next = cleanupActiveSessions({ sessions });
    fs.writeFileSync(activeSessionsPath, JSON.stringify(next, null, 2));
    return next;
  }

  function summarize(events) {
    const registry = readRegistry();
    const publishedArticles = (registry.articles || []).filter((x) => x.publish_status === 'published');
    const now = new Date();
    const dayAgo = new Date(now.getTime() - 24*60*60*1000);
    const weekAgo = new Date(now.getTime() - 7*24*60*60*1000);

    const trafficByDay = {};
    const clicksByDay = {};
    const last30Days = getLastNDays(30);
    const sourceCounts = {};
    const referrerCounts = {};
    const countryCounts = {};
    const articleViews = {};
    const articleClicks = {};
    const productClicks = {};
    const searches = {};
    const zeroSearches = {};
    const visitorsToday = new Set();
    const visitorsWeek = new Set();
    let pageViewsToday = 0;
    let pageViewsWeek = 0;
    let totalClicks = 0;

    for (const event of events) {
      const ts = new Date(event.timestamp || Date.now());
      const day = formatDayKey(ts);
      const slug = event.article_slug || 'home';
      const ipHash = event.ip_hash || 'unknown';

      const eventType = event.event_type || event.type;

      if (eventType === 'page_view' || eventType === 'article_view') {
        trafficByDay[day] = (trafficByDay[day] || 0) + 1;
        sourceCounts[event.traffic_source || 'unknown'] = (sourceCounts[event.traffic_source || 'unknown'] || 0) + 1;
        if (event.referrer_domain) referrerCounts[event.referrer_domain] = (referrerCounts[event.referrer_domain] || 0) + 1;
        countryCounts[event.country || 'unknown'] = (countryCounts[event.country || 'unknown'] || 0) + 1;
        if (ts >= dayAgo) { pageViewsToday += 1; visitorsToday.add(ipHash); }
        if (ts >= weekAgo) { pageViewsWeek += 1; visitorsWeek.add(ipHash); }
      }

      if (eventType === 'article_view') {
        if (!articleViews[slug]) articleViews[slug] = { article_slug: slug, views: 0, total_time_on_page_ms: 0, avg_time_on_page_seconds: 0 };
        articleViews[slug].views += 1;
        articleViews[slug].total_time_on_page_ms += Number(event.time_on_page_ms || 0);
      }

      if (eventType === 'outbound_click') {
        totalClicks += 1;
        clicksByDay[day] = (clicksByDay[day] || 0) + 1;
        if (!articleClicks[slug]) articleClicks[slug] = { article_slug: slug, clicks: 0 };
        articleClicks[slug].clicks += 1;
        const productKey = event.product_name || 'unknown-product';
        if (!productClicks[productKey]) productClicks[productKey] = {
          product_name: productKey,
          article_slug: slug,
          clicks: 0,
          position_in_article: event.position_in_article ?? null,
          was_top_pick: Boolean(event.was_top_pick),
          asin: event.asin || null,
        };
        productClicks[productKey].clicks += 1;
      }

      if (eventType === 'search') {
        const q = String(event.query || '').trim().toLowerCase();
        if (q) searches[q] = (searches[q] || 0) + 1;
        if (q && !event.has_results) zeroSearches[q] = (zeroSearches[q] || 0) + 1;
      }
    }

    const topArticles = Object.keys(articleViews).map((slug) => {
      const views = articleViews[slug].views;
      const clicks = articleClicks[slug]?.clicks || 0;
      const avgTime = views ? articleViews[slug].total_time_on_page_ms / views / 1000 : 0;
      return {
        article_slug: slug,
        views,
        clicks,
        ctr: views ? Number((clicks / views).toFixed(4)) : 0,
        avg_time_on_page_seconds: Number(avgTime.toFixed(2)),
      };
    }).sort((a, b) => b.views - a.views || b.clicks - a.clicks || a.article_slug.localeCompare(b.article_slug));

    const topProducts = Object.values(productClicks).map((p) => {
      const articleViewCount = articleViews[p.article_slug]?.views || 0;
      return {
        ...p,
        ctr_per_product: articleViewCount ? Number((p.clicks / articleViewCount).toFixed(4)) : 0,
      };
    }).sort((a, b) => b.clicks - a.clicks || a.product_name.localeCompare(b.product_name));

    for (const item of Object.values(articleViews)) {
      item.avg_time_on_page_seconds = item.views ? Number((item.total_time_on_page_ms / item.views / 1000).toFixed(2)) : 0;
    }

    for (const day of last30Days) {
      if (!(day in trafficByDay)) trafficByDay[day] = 0;
      if (!(day in clicksByDay)) clicksByDay[day] = 0;
    }

    const activeNow = cleanupActiveSessions(readActiveSessions());
    return {
      generated_at: new Date().toISOString(),
      events_count: events.length,
      traffic: {
        total_visitors_today: visitorsToday.size,
        total_visitors_week: visitorsWeek.size,
        page_views_today: pageViewsToday,
        page_views_week: pageViewsWeek,
        unique_visitors_today_approx: visitorsToday.size,
        unique_visitors_week_approx: visitorsWeek.size,
      },
      sources: {
        traffic_by_source: Object.entries(sourceCounts).sort((a,b)=>b[1]-a[1]).map(([source,count]) => ({ source, count })),
        top_referrers: Object.entries(referrerCounts).sort((a,b)=>b[1]-a[1]).slice(0,20).map(([referrer_domain,count]) => ({ referrer_domain, count })),
      },
      geography: {
        traffic_by_country: Object.entries(countryCounts).sort((a,b)=>b[1]-a[1]).map(([country,count]) => ({ country, count })),
      },
      content_performance: {
        articles_published: publishedArticles.length,
        most_viewed_articles: topArticles.slice(0, 20),
        average_ctr_per_article: topArticles.length ? Number((topArticles.reduce((sum, x) => sum + x.ctr, 0) / topArticles.length).toFixed(4)) : 0,
      },
      monetization: {
        total_clicks: totalClicks,
        clicks_per_article: Object.values(articleClicks).sort((a,b)=>b.clicks-a.clicks).slice(0,20),
        most_clicked_products: topProducts.slice(0,20),
      },
      search_intelligence: {
        top_search_queries: Object.entries(searches).sort((a,b)=>b[1]-a[1]).slice(0,20).map(([query,count]) => ({ query, count })),
        queries_with_no_results: Object.entries(zeroSearches).sort((a,b)=>b[1]-a[1]).slice(0,20).map(([query,count]) => ({ query, count })),
      },
      charts: {
        traffic_over_time: last30Days.map((date) => ({ date, page_views: trafficByDay[date] || 0 })),
        clicks_over_time: last30Days.map((date) => ({ date, clicks: clicksByDay[date] || 0 })),
      },
      realtime: {
        current_viewers: activeNow.current_viewers,
        active_sessions: activeNow.sessions
      }
    };
  }

  function flush() {
    flushTimer = null;
    ensureStores();
    const existing = readEvents();
    const merged = existing.concat(queue);
    queue = [];
    fs.writeFileSync(eventsPath, JSON.stringify(merged, null, 2));
    fs.writeFileSync(summaryPath, JSON.stringify(summarize(merged), null, 2));
  }

  function appendEvent(event) {
    queue.push({ ...event, timestamp: event.timestamp || new Date().toISOString() });
    if (!flushTimer) flushTimer = setTimeout(flush, 250);
  }

  ensureStores();
  try {
    fs.writeFileSync(summaryPath, JSON.stringify(summarize(readEvents()), null, 2));
  } catch {}

  return { ensureStores, readEvents, readSummary, readActiveSessions, appendEvent, summarize, buildPageViewEvent, classifyTrafficSource, getReferrerDomain, getDeviceType, updatePresence };
};
