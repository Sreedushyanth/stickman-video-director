#!/usr/bin/env node
import {createServer} from 'node:http';
import {randomUUID, timingSafeEqual} from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

const STATUSES = ['queued','running','qa','awaiting_approval','uploading','scheduled','published','failed','cancelled'];
const TYPES = ['ingest','render','export','publish'];
const NEXT = {
  queued:['running','cancelled'], running:['qa','failed','cancelled'],
  qa:['awaiting_approval','failed','cancelled'],
  awaiting_approval:['uploading','cancelled','queued'],
  uploading:['scheduled','published','failed'],
  scheduled:['published','failed','cancelled'],
  published:[], failed:['queued','cancelled'], cancelled:[],
};
const SECRET = /(token|secret|password|authorization|api[_-]?key|cookie|credential|refresh[_-]?token)/i;
const clone = value => value == null ? value : JSON.parse(JSON.stringify(value));
const validString = (value, name, max = 160) => {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new TypeError(name + ' must be a non-empty string (max ' + max + ')');
  return value.trim();
};
function safePayload(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('payload must be a JSON object');
  if (Buffer.byteLength(JSON.stringify(value)) > 32768) throw new RangeError('payload is too large');
  const walk = (item, prefix) => {
    if (Array.isArray(item)) return item.forEach((child, i) => walk(child, prefix + '[' + i + ']'));
    if (item && typeof item === 'object') for (const [key, child] of Object.entries(item)) {
      if (SECRET.test(key)) throw new TypeError(prefix + '.' + key + ' is not accepted; use a secret manager');
      walk(child, prefix + '.' + key);
    }
  };
  walk(value, 'payload');
  return clone(value);
}
async function atomicWrite(file, value) {
  await fs.mkdir(path.dirname(file), {recursive:true});
  const temp = file + '.' + process.pid + '.' + randomUUID() + '.tmp';
  const handle = await fs.open(temp, 'wx', 0o600);
  try { await handle.writeFile(JSON.stringify(value, null, 2) + '\n', 'utf8'); await handle.sync(); }
  finally { await handle.close(); }
  await fs.rename(temp, file);
}

export class JobStore {
  constructor(directory = path.resolve(process.cwd(), 'autopilot-work', 'control-plane')) {
    this.directory = path.resolve(directory);
    this.file = path.join(this.directory, 'state.json');
    this.state = null;
    this.tail = Promise.resolve();
  }
  async init() {
    await fs.mkdir(this.directory, {recursive:true});
    try {
      const state = JSON.parse(await fs.readFile(this.file, 'utf8'));
      if (state.version !== 1 || !Array.isArray(state.jobs) || typeof state.paused !== 'boolean') throw new Error('Invalid control-plane state; refusing to reset it');
      this.state = state;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      this.state = {version:1, paused:false, jobs:[], updatedAt:new Date().toISOString()};
      await atomicWrite(this.file, this.state);
    }
    return this;
  }
  async mutate(fn) {
    if (!this.state) throw new Error('Call init() before using JobStore');
    const previous = this.tail;
    let release;
    this.tail = new Promise(resolve => { release = resolve; });
    await previous;
    try {
      const next = clone(this.state);
      const result = fn(next);
      next.updatedAt = new Date().toISOString();
      await atomicWrite(this.file, next);
      this.state = next;
      return clone(result);
    } finally { release(); }
  }
  async enqueue({type, projectId = null, payload = {}, idempotencyKey = null} = {}) {
    if (!TYPES.includes(type)) throw new TypeError('type must be one of: ' + TYPES.join(', '));
    if (projectId !== null) projectId = validString(projectId, 'projectId', 120);
    if (idempotencyKey !== null) idempotencyKey = validString(idempotencyKey, 'idempotencyKey', 160);
    payload = safePayload(payload);
    return this.mutate(state => {
      const existing = idempotencyKey && state.jobs.find(job => job.idempotencyKey === idempotencyKey);
      if (existing) return {job:existing, created:false};
      const now = new Date().toISOString();
      const job = {id:randomUUID(), type, projectId, payload, idempotencyKey, status:'queued', attempts:0, workerId:null, note:null, createdAt:now, updatedAt:now, startedAt:null, completedAt:null};
      state.jobs.push(job);
      return {job, created:true};
    });
  }
  async claim(workerId) {
    workerId = validString(workerId, 'workerId', 120);
    return this.mutate(state => {
      if (state.paused) return null;
      const job = state.jobs.filter(item => item.status === 'queued').sort((a,b) => a.createdAt.localeCompare(b.createdAt))[0];
      if (!job) return null;
      job.status = 'running'; job.workerId = workerId; job.startedAt = new Date().toISOString();
      job.updatedAt = job.startedAt; job.attempts += 1;
      return job;
    });
  }
  async transition(id, status, {note = null, workerId = null} = {}) {
    id = validString(id, 'job id', 80);
    if (!STATUSES.includes(status)) throw new TypeError('Unknown job status');
    if (note !== null) note = validString(note, 'note', 500);
    if (workerId !== null) workerId = validString(workerId, 'workerId', 120);
    return this.mutate(state => {
      const job = state.jobs.find(item => item.id === id);
      if (!job) return null;
      if (!NEXT[job.status].includes(status)) throw new Error('Invalid job transition: ' + job.status + ' -> ' + status);
      job.status = status; job.updatedAt = new Date().toISOString();
      if (note) job.note = note;
      if (workerId) job.workerId = workerId;
      if (['published','failed','cancelled'].includes(status)) job.completedAt = job.updatedAt;
      return job;
    });
  }
  async setPaused(paused) {
    if (typeof paused !== 'boolean') throw new TypeError('paused must be boolean');
    return this.mutate(state => { state.paused = paused; return {paused}; });
  }
  async list({status = null, limit = 50} = {}) {
    await this.tail;
    if (status !== null && !STATUSES.includes(status)) throw new TypeError('Unknown job status');
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) throw new RangeError('limit must be 1–200');
    return clone(this.state.jobs.filter(job => status === null || job.status === status).sort((a,b) => b.createdAt.localeCompare(a.createdAt)).slice(0, limit));
  }
  async snapshot() {
    await this.tail;
    const counts = Object.fromEntries(STATUSES.map(status => [status, 0]));
    for (const job of this.state.jobs) counts[job.status] += 1;
    return {paused:this.state.paused, total:this.state.jobs.length, counts, updatedAt:this.state.updatedAt};
  }
}
function send(res, status, value) {
  const body = JSON.stringify(value);
  res.writeHead(status, {'content-type':'application/json; charset=utf-8','content-length':Buffer.byteLength(body),'cache-control':'no-store','x-content-type-options':'nosniff'});
  res.end(body);
}
async function body(req) {
  const chunks = []; let size = 0;
  for await (const chunk of req) { size += chunk.length; if (size > 65536) throw Object.assign(new Error('Request too large'), {statusCode:413}); chunks.push(chunk); }
  if (!size) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw Object.assign(new Error('Invalid JSON body'), {statusCode:400}); }
}
function authorized(req, token) {
  const match = /^Bearer\s+(.+)$/i.exec(req.headers.authorization || '');
  if (!match) return false;
  const a = Buffer.from(match[1]); const b = Buffer.from(token);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function createControlPlaneServer({store, token}) {
  if (!store) throw new TypeError('store is required');
  if (typeof token !== 'string' || token.length < 32) throw new Error('Control token must be at least 32 characters');
  return createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://127.0.0.1');
      if (req.method === 'GET' && url.pathname === '/health') return send(res, 200, {ok:true, service:'Stickman Video Director control plane'});
      if (!authorized(req, token)) return send(res, 401, {error:'Unauthorized'});
      if (req.method === 'GET' && url.pathname === '/api/queue') return send(res, 200, await store.snapshot());
      if (req.method === 'GET' && url.pathname === '/api/jobs') return send(res, 200, {jobs:await store.list({status:url.searchParams.get('status'), limit:Number(url.searchParams.get('limit') || 50)})});
      if (req.method === 'POST' && url.pathname === '/api/jobs') {
        const result = await store.enqueue(await body(req));
        return send(res, result.created ? 201 : 200, result);
      }
      if (req.method === 'POST' && url.pathname === '/api/jobs/claim') return send(res, 200, {job:await store.claim((await body(req)).workerId)});
      if (req.method === 'POST' && url.pathname === '/api/control/pause') { await body(req); return send(res, 200, await store.setPaused(true)); }
      if (req.method === 'POST' && url.pathname === '/api/control/resume') { await body(req); return send(res, 200, await store.setPaused(false)); }
      const match = /^\/api\/jobs\/([^/]+)\/transition$/.exec(url.pathname);
      if (req.method === 'POST' && match) {
        const data = await body(req); const job = await store.transition(decodeURIComponent(match[1]), data.status, data);
        return job ? send(res, 200, {job}) : send(res, 404, {error:'Job not found'});
      }
      return send(res, 404, {error:'Route not found'});
    } catch (error) {
      const status = error.statusCode || (error instanceof TypeError || error instanceof RangeError ? 400 : 409);
      return send(res, status, {error:error.message || 'Request failed'});
    }
  });
}
export async function startControlPlane({host = process.env.STICKMAN_CONTROL_HOST || '127.0.0.1', port = Number(process.env.STICKMAN_CONTROL_PORT || 4317), directory = process.env.STICKMAN_CONTROL_DIR, token = process.env.STICKMAN_CONTROL_TOKEN} = {}) {
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new RangeError('Port must be 1–65535');
  if (typeof token !== 'string' || token.length < 32) throw new Error('Set STICKMAN_CONTROL_TOKEN to a random value of at least 32 characters');
  const store = await new JobStore(directory).init();
  const server = createControlPlaneServer({store, token});
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); });
  return server;
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  if ((process.argv[2] || 'serve') !== 'serve') { console.error('Usage: node scripts/control-plane.mjs serve'); process.exitCode = 2; }
  else try {
    const server = await startControlPlane();
    const addr = server.address();
    console.log('Stickman Video Director control plane listening on ' + (typeof addr === 'object' ? addr.address + ':' + addr.port : addr));
    console.log('This starts the local job ledger only; renderer workers and scheduling are separate components.');
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
