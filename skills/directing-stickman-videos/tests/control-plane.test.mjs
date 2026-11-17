import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {JobStore, createControlPlaneServer} from '../scripts/control-plane.mjs';

async function fixture(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'stickman-control-'));
  const store = await new JobStore(dir).init();
  t.after(() => fs.rm(dir, {recursive:true, force:true}));
  return {dir, store};
}

test('persists jobs and deduplicates idempotency keys', async t => {
  const {dir, store} = await fixture(t);
  const first = await store.enqueue({type:'render', projectId:'demo', payload:{ratio:'16:9'}, idempotencyKey:'demo-1'});
  const duplicate = await store.enqueue({type:'render', projectId:'demo', payload:{ratio:'16:9'}, idempotencyKey:'demo-1'});
  assert.equal(first.created, true);
  assert.equal(duplicate.created, false);
  assert.equal(first.job.id, duplicate.job.id);
  assert.equal((await new JobStore(dir).init()).state.jobs.length, 1);
});

test('pause blocks claims and resume permits a worker claim', async t => {
  const {store} = await fixture(t);
  await store.enqueue({type:'render', payload:{project:'demo'}});
  await store.setPaused(true);
  assert.equal(await store.claim('worker-a'), null);
  await store.setPaused(false);
  const job = await store.claim('worker-a');
  assert.equal(job.status, 'running');
  assert.equal(job.workerId, 'worker-a');
  assert.equal(job.attempts, 1);
});

test('validates lifecycle and rejects credentials in job payloads', async t => {
  const {store} = await fixture(t);
  const {job} = await store.enqueue({type:'render', payload:{sceneCount:2}});
  await assert.rejects(() => store.transition(job.id, 'published'), /Invalid job transition/);
  await assert.rejects(() => store.enqueue({type:'publish', payload:{refresh_token:'do-not-store'}}), /secret manager/);
  await store.claim('worker-a');
  await store.transition(job.id, 'qa', {note:'Render complete'});
  await store.transition(job.id, 'awaiting_approval');
  assert.equal((await store.list())[0].status, 'awaiting_approval');
});

test('protects operator endpoints with bearer authentication', async t => {
  const {store} = await fixture(t);
  const token = 'test-' + 'x'.repeat(40);
  const server = createControlPlaneServer({store, token});
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = 'http://127.0.0.1:' + server.address().port;
  assert.equal((await fetch(base + '/health')).status, 200);
  assert.equal((await fetch(base + '/api/jobs')).status, 401);
  const response = await fetch(base + '/api/jobs', {
    method:'POST',
    headers:{'content-type':'application/json', authorization:'Bearer ' + token},
    body:JSON.stringify({type:'export', payload:{target:'local'}}),
  });
  assert.equal(response.status, 201);
  assert.equal((await response.json()).job.status, 'queued');
});
