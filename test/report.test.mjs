import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { simulateQueue, LIMITS, parseStrictJson } from '../src/index.mjs';

const event = (at, kind, id) => id === undefined ? { at, kind } : { at, kind, id };
const doc = (events = [event(0, 'enqueue', 'a'), event(1, 'connect')], policy = {}) => ({ schemaVersion: '1', complete: true, seed: 7, policy: { capacity: 2, ttlMs: 100, maxAttempts: 2, retryDelayMs: 10, successPercent: 100, duplicate: 'suppress', overflow: 'reject-new', ...policy }, events });
const run = (root, input) => spawnSync(process.execPath, ['bin/offline-queue-simulator.mjs', '--root', root, '--input', input], { cwd: new URL('..', import.meta.url), encoding: 'utf8', env: { ...process.env, NODE_OPTIONS: `--import=${new URL('./deny-network.mjs', import.meta.url).href}` } });

test('good offline enqueue reconnect delivers once in FIFO order without action', () => {
  const r = simulateQueue(doc([event(0, 'enqueue', 'a'), event(0, 'enqueue', 'b'), event(1, 'connect')]));
  assert.equal(r.status, 'pass'); assert.deepEqual(r.outcomes.map(x => x.state), ['delivered', 'delivered']); assert.deepEqual(r.outcomes.map(x => x.sourceOrdinal), [0, 1]); assert.deepEqual(r.findings, []);
});
test('reconnect burst respects capacity and records backpressure loss', () => {
  const r = simulateQueue(doc([event(0, 'enqueue', 'a'), event(0, 'enqueue', 'b'), event(0, 'enqueue', 'c'), event(1, 'connect')]));
  assert.equal(r.status, 'fail'); assert.equal(r.findings[0].ruleId, 'queue-overflow'); assert.deepEqual(r.outcomes.map(x => x.state), ['delivered', 'delivered', 'lost']);
});
test('duplicate suppression passes; allowed duplicate delivery fails', () => {
  const events = [event(0, 'enqueue', 'a'), event(0, 'enqueue', 'a'), event(1, 'connect')];
  const safe = simulateQueue(doc(events)); assert.equal(safe.status, 'pass'); assert.deepEqual(safe.outcomes.map(x => x.state), ['delivered', 'suppressed']);
  const allowed = simulateQueue(doc(events, { duplicate: 'allow' })); assert.equal(allowed.status, 'fail'); assert.ok(allowed.findings.some(x => x.ruleId === 'duplicate-delivered'));
});
test('TTL exact boundary expires; just before is delivered', () => {
  assert.equal(simulateQueue(doc([event(0, 'enqueue', 'a'), event(99, 'connect')])).status, 'pass');
  const old = simulateQueue(doc([event(0, 'enqueue', 'a'), event(100, 'connect')])); assert.equal(old.status, 'fail'); assert.equal(old.findings[0].ruleId, 'item-expired');
});
test('offline ticks expire queued items before later enqueues consume capacity', () => {
  const x = doc([event(0, 'enqueue', 'old'), event(100, 'tick'), event(100, 'enqueue', 'new'), event(101, 'connect')], { capacity: 1 });
  const r = simulateQueue(x);
  assert.equal(r.status, 'fail');
  assert.deepEqual(r.outcomes.map(item => item.state), ['lost', 'delivered']);
  assert.deepEqual(r.findings.map(f => f.ruleId), ['item-expired']);
});
test('retry policy records attempts, exhausts failures and seeded runs reproduce', () => {
  const events = [event(0, 'enqueue', 'a'), event(0, 'connect'), event(10, 'tick')];
  const x = doc(events, { successPercent: 0 }); const a = simulateQueue(x), b = simulateQueue(x);
  assert.equal(JSON.stringify(a), JSON.stringify(b)); assert.equal(a.status, 'fail'); assert.equal(a.outcomes[0].attempts, 2); assert.equal(a.findings[0].ruleId, 'retry-exhausted');
  assert.notEqual(JSON.stringify(simulateQueue({ ...x, seed: 7, policy: { ...x.policy, successPercent: 50 } })), JSON.stringify(simulateQueue({ ...x, seed: 8, policy: { ...x.policy, successPercent: 50 } })));
});
test('reconnect starts retry delay at reconnection time rather than offline enqueue time', () => {
  const x = doc([event(0, 'enqueue', 'a'), event(20, 'connect'), event(29, 'tick')], { successPercent: 0 });
  const pending = simulateQueue(x);
  assert.equal(pending.status, 'incomplete');
  assert.equal(pending.outcomes[0].attempts, 1);
  const exhausted = simulateQueue({ ...x, events: x.events.concat(event(30, 'tick')) });
  assert.equal(exhausted.status, 'fail');
  assert.equal(exhausted.outcomes[0].attempts, 2);
});
test('offline pending is incomplete, invalid events and partial export never pass', () => {
  assert.equal(simulateQueue(doc([event(0, 'enqueue', 'a')])).status, 'incomplete');
  assert.equal(simulateQueue({ ...doc(), complete: false }).status, 'incomplete');
  assert.equal(simulateQueue(doc([event(2, 'enqueue', 'a'), event(1, 'connect')])).status, 'incomplete');
  assert.equal(simulateQueue(doc([])).status, 'incomplete');
});
test('event, capacity, max-attempt and percent boundaries allow N, reject N+1', () => {
  const x = doc(Array.from({ length: LIMITS.events - 2 }, (_, i) => event(i, 'tick')).concat([event(LIMITS.events - 2, 'enqueue', 'a'), event(LIMITS.events - 1, 'connect')])); assert.equal(simulateQueue(x).status, 'pass'); x.events.push(event(LIMITS.events, 'tick')); assert.equal(simulateQueue(x).findings[0].ruleId, 'record-limit');
  assert.equal(simulateQueue(doc(undefined, { capacity: LIMITS.capacity })).status, 'pass'); assert.equal(simulateQueue(doc(undefined, { capacity: LIMITS.capacity + 1 })).status, 'incomplete');
  assert.equal(simulateQueue(doc(undefined, { maxAttempts: LIMITS.maxAttempts })).status, 'pass'); assert.equal(simulateQueue(doc(undefined, { maxAttempts: LIMITS.maxAttempts + 1 })).status, 'incomplete');
  assert.equal(simulateQueue(doc(undefined, { successPercent: 100 })).status, 'pass'); assert.equal(simulateQueue(doc(undefined, { successPercent: 101 })).status, 'incomplete');
});
test('byte, depth, injected time and seed boundaries allow N, reject N+1', () => {
  const x = doc(); x.events[0].note = ''; const overhead = Buffer.byteLength(JSON.stringify(x)); x.events[0].note = 'x'.repeat(LIMITS.bytes - overhead); assert.equal(simulateQueue(x).status, 'pass'); x.events[0].note += 'x'; assert.equal(simulateQueue(x).findings[0].ruleId, 'byte-limit');
  const deep = doc(); assert.equal(simulateQueue(deep).status, 'pass'); deep.events[0].note = { nested: 'x' }; assert.equal(simulateQueue(deep).findings[0].ruleId, 'depth-limit');
  assert.equal(simulateQueue(doc(), { now: (() => { let n=0; return () => n++ ? LIMITS.milliseconds : 0; })() }).status, 'pass'); assert.equal(simulateQueue(doc(), { now: (() => { let n=0; return () => n++ ? LIMITS.milliseconds + 1 : 0; })() }).findings[0].ruleId, 'time-limit');
  assert.equal(simulateQueue({ ...doc(), seed: 4294967295 }).status, 'pass'); assert.equal(simulateQueue({ ...doc(), seed: 4294967296 }).status, 'incomplete');
});
test('duplicate JSON keys including escaped spelling refused', () => assert.throws(() => parseStrictJson('{"complete":false,"complet\\u0065":true}'), /duplicate-key/));
test('CLI confines reads, requires strict UTF-8 and has empty usage stdout', async () => {
  const root = await mkdtemp(join(tmpdir(), 'queue-')); await writeFile(join(root, 'good.json'), JSON.stringify(doc())); assert.equal(run(root, 'good.json').status, 0);
  await writeFile(join(root, 'bad.json'), Buffer.from([0xff])); assert.equal(JSON.parse(run(root, 'bad.json').stdout).status, 'incomplete');
  await symlink(tmpdir(), join(root, 'escape')); const escaped = run(root, 'escape/nonexistent.json'); assert.equal(escaped.status, 2); assert.equal(JSON.parse(escaped.stdout).status, 'incomplete');
  const invalidRoot = run(join(root, 'no-directory'), 'good.json'); assert.equal(invalidRoot.status, 2); assert.equal(invalidRoot.stdout, '');
  const usage = spawnSync(process.execPath, ['bin/offline-queue-simulator.mjs', '--root', root, '--input', 'good.json', '--unknown'], { cwd: new URL('..', import.meta.url), encoding: 'utf8' }); assert.equal(usage.status, 2); assert.equal(usage.stdout, '');
});
