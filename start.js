#!/usr/bin/env node
const { spawn } = require('child_process');
const path = require('path');

const children = new Map();
let shuttingDown = false;

function startProcess(name, script) {
  const child = spawn(process.execPath, [path.join(__dirname, script)], {
    cwd: __dirname,
    stdio: 'inherit',
    env: process.env
  });
  children.set(name, child);
  console.log(`[launcher] started ${name} pid=${child.pid} script=${script}`);

  child.on('exit', (code, signal) => {
    console.log(`[launcher] ${name} exited code=${code} signal=${signal}`);
    children.delete(name);
    if (!shuttingDown) {
      console.log(`[launcher] shutting down sibling processes because ${name} exited`);
      shutdown(code || (signal ? 1 : 0));
    }
  });

  child.on('error', (error) => {
    console.log(`[launcher] ${name} error=${error.message || error}`);
  });
}

function shutdown(exitCode = 0) {
  shuttingDown = true;
  for (const [name, child] of children.entries()) {
    try {
      console.log(`[launcher] sending SIGTERM to ${name} pid=${child.pid}`);
      child.kill('SIGTERM');
    } catch {}
  }
  setTimeout(() => process.exit(exitCode), 1500).unref();
}

process.on('SIGTERM', () => shutdown(0));
process.on('SIGINT', () => shutdown(0));

startProcess('web', 'server.js');
startProcess('worker', 'worker.js');
