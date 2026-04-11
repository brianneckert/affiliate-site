#!/usr/bin/env node
const path = require('path');
const queue = require('./lib/jobQueue');
const { runGeneration } = require('./scripts/process_paid_instant_answers');

let activeJob = null;
let shuttingDown = false;
let shutdownSignal = null;
let shutdownInFlight = null;

function nowIso() {
  return new Date().toISOString();
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processNextJob() {
  const job = queue.getNextPendingJob();
  if (!job) return false;
  activeJob = job.requestId;
  queue.markJobProcessing(job.requestId, { workerPid: process.pid, workerStartedAt: nowIso() });
  try {
    await runGeneration(job.requestId, {
      silent: true,
      onCheckpoint: (requestId, checkpoint) => {
        queue.updateCheckpoint(requestId, {
          stage: checkpoint.stage || null,
          completed_products: Array.isArray(checkpoint.completed_products) ? checkpoint.completed_products.length : 0,
          checkpoint_file: path.join(__dirname, 'data', 'analytics', 'instant_answer_checkpoints', `${requestId}.json`)
        });
      }
    });
    queue.markJobCompleted(job.requestId, { workerCompletedAt: nowIso() });
  } catch (error) {
    queue.markJobFailed(job.requestId, error.message || String(error), {
      workerFailedAt: nowIso(),
      errorMeta: error.meta || null
    });
  } finally {
    activeJob = null;
    if (shuttingDown) {
      process.exit(0);
    }
  }
  return true;
}

async function loop() {
  while (!shuttingDown) {
    const worked = await processNextJob();
    if (!worked) await sleep(3000);
  }
}

async function gracefulShutdown(signal) {
  if (shutdownInFlight) return shutdownInFlight;
  shuttingDown = true;
  shutdownSignal = signal;
  shutdownInFlight = (async () => {
    if (activeJob) {
      queue.updateCheckpoint(activeJob, {
        shutdownSignal: signal,
        shutdownAt: nowIso(),
        jobStatusOnShutdown: 'processing',
        shutdownDeferredUntilJobComplete: true
      });
      return;
    }
    process.exit(0);
  })();
  return shutdownInFlight;
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

loop().catch(async (error) => {
  if (activeJob) {
    queue.markJobFailed(activeJob, error.message || String(error), { workerFailedAt: nowIso() });
  }
  process.exit(1);
});
