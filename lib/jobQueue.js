const fs = require('fs');
const path = require('path');

const JOBS_PATH = path.join(__dirname, '..', 'data', 'jobs_queue.json');
const STALE_PROCESSING_MS = 15 * 60 * 1000;

function nowIso() {
  return new Date().toISOString();
}

function isPidAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function recoverStaleProcessingJobs(store) {
  const now = Date.now();
  for (const job of store.jobs || []) {
    if (job.status !== 'processing') continue;
    const updatedAtMs = Date.parse(job.updatedAt || job.startedAt || 0) || 0;
    const staleByTime = updatedAtMs > 0 && (now - updatedAtMs) > STALE_PROCESSING_MS;
    const staleByPid = job.workerPid && !isPidAlive(job.workerPid);
    if (!staleByTime && !staleByPid) continue;
    job.status = 'pending';
    job.updatedAt = nowIso();
    job.recoveredAt = job.updatedAt;
    job.recoveredFromStaleProcessing = true;
    job.lastRecoveryReason = staleByPid ? 'dead_worker_pid' : 'stale_processing_timeout';
    delete job.workerPid;
    delete job.workerStartedAt;
    delete job.startedAt;
  }
  return store;
}

function ensureStore() {
  fs.mkdirSync(path.dirname(JOBS_PATH), { recursive: true });
  if (!fs.existsSync(JOBS_PATH)) {
    fs.writeFileSync(JOBS_PATH, JSON.stringify({ jobs: [] }, null, 2));
  }
}

function readStore() {
  ensureStore();
  try {
    return JSON.parse(fs.readFileSync(JOBS_PATH, 'utf8'));
  } catch {
    return { jobs: [] };
  }
}

function atomicWrite(payload) {
  ensureStore();
  const temp = `${JOBS_PATH}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(payload, null, 2));
  fs.renameSync(temp, JOBS_PATH);
}

function mutate(mutator) {
  const store = readStore();
  const next = mutator(store) || store;
  atomicWrite(next);
  return next;
}

function enqueueJob(requestId) {
  const ts = nowIso();
  mutate((store) => {
    const existing = store.jobs.find((job) => job.requestId === requestId && !['completed'].includes(job.status));
    if (existing) {
      existing.updatedAt = ts;
      if (existing.status === 'failed') existing.status = 'pending';
      return store;
    }
    store.jobs.push({
      requestId,
      status: 'pending',
      createdAt: ts,
      updatedAt: ts,
      checkpoint: {}
    });
    return store;
  });
}

function getNextPendingJob() {
  const store = mutate((current) => recoverStaleProcessingJobs(current));
  const pending = (store.jobs || []).filter((job) => job.status === 'pending');
  if (!pending.length) return null;
  const sortByTime = (a, b, field) => (Date.parse(a[field] || 0) || 0) - (Date.parse(b[field] || 0) || 0);
  const freshPending = pending
    .filter((job) => !job.recoveredFromStaleProcessing)
    .sort((a, b) => sortByTime(a, b, 'createdAt') || sortByTime(a, b, 'updatedAt'));
  if (freshPending.length) return freshPending[0];
  const recoveredPending = pending
    .filter((job) => job.recoveredFromStaleProcessing)
    .sort((a, b) => sortByTime(a, b, 'recoveredAt') || sortByTime(a, b, 'createdAt'));
  return recoveredPending[0] || null;
}

function getJob(requestId) {
  const store = mutate((current) => recoverStaleProcessingJobs(current));
  return store.jobs.find((job) => job.requestId === requestId) || null;
}

function markJobProcessing(requestId, extra = {}) {
  const ts = nowIso();
  mutate((store) => {
    const job = store.jobs.find((x) => x.requestId === requestId);
    if (!job) return store;
    job.status = 'processing';
    job.updatedAt = ts;
    job.startedAt = job.startedAt || ts;
    Object.assign(job, extra);
    return store;
  });
}

function updateCheckpoint(requestId, checkpointData = {}) {
  const ts = nowIso();
  mutate((store) => {
    const job = store.jobs.find((x) => x.requestId === requestId);
    if (!job) return store;
    job.updatedAt = ts;
    job.checkpoint = { ...(job.checkpoint || {}), ...checkpointData, updatedAt: ts };
    return store;
  });
}

function markJobCompleted(requestId, extra = {}) {
  const ts = nowIso();
  mutate((store) => {
    const job = store.jobs.find((x) => x.requestId === requestId);
    if (!job) return store;
    job.status = 'completed';
    job.updatedAt = ts;
    job.completedAt = ts;
    Object.assign(job, extra);
    return store;
  });
}

function markJobFailed(requestId, error, extra = {}) {
  const ts = nowIso();
  mutate((store) => {
    const job = store.jobs.find((x) => x.requestId === requestId);
    if (!job) return store;
    job.status = 'failed';
    job.updatedAt = ts;
    job.error = error;
    Object.assign(job, extra);
    return store;
  });
}

module.exports = {
  JOBS_PATH,
  enqueueJob,
  getNextPendingJob,
  getJob,
  markJobProcessing,
  updateCheckpoint,
  markJobCompleted,
  markJobFailed
};
