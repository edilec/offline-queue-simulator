export const TOOL_ID = 'offline-queue-simulator';
export const LIMITS = Object.freeze({ bytes: 1_048_576, events: 200, capacity: 100, maxAttempts: 10, depth: 3, milliseconds: 5000 });
export const RULE_SEVERITY = Object.freeze({
  'input-unreadable': 'error', 'input-invalid': 'error', 'duplicate-key': 'error', 'byte-limit': 'error', 'record-limit': 'error', 'depth-limit': 'error', 'time-limit': 'error', 'export-incomplete': 'error',
  'policy-invalid': 'error', 'event-invalid': 'error', 'no-items': 'error', 'queue-pending': 'error',
  'queue-overflow': 'error', 'item-expired': 'error', 'retry-exhausted': 'error', 'duplicate-delivered': 'error', 'duplicate-suppressed': 'info'
});
const UNKNOWN = new Set(['input-unreadable', 'input-invalid', 'duplicate-key', 'byte-limit', 'record-limit', 'depth-limit', 'time-limit', 'export-incomplete', 'policy-invalid', 'event-invalid', 'no-items', 'queue-pending']);
const object = v => v !== null && typeof v === 'object' && !Array.isArray(v) && Object.getPrototypeOf(v) === Object.prototype;
const order = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const id = v => typeof v === 'string' && v.length > 0 && v.length <= 128 && !/[\u0000-\u001f\u007f-\u009f\u200e\u200f\u202a-\u202e\u2066-\u2069]/u.test(v);
function finding(ruleId, pointer = '') {
  if (!Object.hasOwn(RULE_SEVERITY, ruleId)) throw new Error('Unknown rule');
  return { ruleId, severity: RULE_SEVERITY[ruleId], message: {
    'queue-overflow': 'A bounded queue rejected or displaced an item.',
    'item-expired': 'An item reached its TTL before delivery.',
    'retry-exhausted': 'The virtual retry budget was exhausted without acknowledgment.',
    'duplicate-delivered': 'The allowed duplicate was delivered again.',
    'duplicate-suppressed': 'A duplicate was suppressed by policy.',
    'queue-pending': 'An item remains queued without a conclusive delivery outcome.'
  }[ruleId] ?? 'Queue simulation evidence cannot be evaluated safely.', location: { file: '@export', pointer } };
}
function report(findings, outcomes = []) {
  findings.sort((a, b) => order(a.location.file, b.location.file) || order(a.location.pointer, b.location.pointer) || order(a.ruleId, b.ruleId));
  const errors = findings.filter(f => f.severity === 'error').length;
  return { schemaVersion: '1', tool: TOOL_ID, status: findings.some(f => UNKNOWN.has(f.ruleId)) ? 'incomplete' : errors ? 'fail' : 'pass', summary: { checked: outcomes.length, errors, warnings: 0 }, outcomes, findings };
}
export const incomplete = ruleId => report([finding(ruleId)]);
export function parseStrictJson(raw) {
  const value = JSON.parse(raw); let i = 0;
  const space = () => { while (/\s/u.test(raw[i] ?? '')) i++; };
  const token = () => { const start = i++; while (i < raw.length) { if (raw[i] === '\\') { i += 2; continue; } if (raw[i++] === '"') return JSON.parse(raw.slice(start, i)); } throw new Error('input-invalid'); };
  const walk = depth => { if (depth > LIMITS.depth) throw new Error('depth-limit'); space(); if (raw[i] === '{') { i++; space(); const keys = new Set(); while (raw[i] !== '}') { const key = token(); if (keys.has(key)) throw new Error('duplicate-key'); keys.add(key); space(); i++; walk(depth + 1); space(); if (raw[i] !== ',') break; i++; space(); } i++; return; } if (raw[i] === '[') { i++; space(); while (raw[i] !== ']') { walk(depth + 1); space(); if (raw[i] !== ',') break; i++; space(); } i++; return; } if (raw[i] === '"') { token(); return; } while (i < raw.length && !/[\s,}\]]/u.test(raw[i])) i++; };
  walk(0); return value;
}
function tooDeep(v, depth = 0) { return depth > LIMITS.depth || (v !== null && typeof v === 'object' && Object.values(v).some(child => tooDeep(child, depth + 1))); }
function validPolicy(p) {
  return object(p) && Object.keys(p).every(k => ['capacity', 'ttlMs', 'maxAttempts', 'retryDelayMs', 'successPercent', 'duplicate', 'overflow'].includes(k)) && Number.isSafeInteger(p.capacity) && p.capacity >= 1 && p.capacity <= LIMITS.capacity && Number.isSafeInteger(p.ttlMs) && p.ttlMs >= 1 && Number.isSafeInteger(p.maxAttempts) && p.maxAttempts >= 1 && p.maxAttempts <= LIMITS.maxAttempts && Number.isSafeInteger(p.retryDelayMs) && p.retryDelayMs >= 0 && Number.isSafeInteger(p.successPercent) && p.successPercent >= 0 && p.successPercent <= 100 && ['suppress', 'allow'].includes(p.duplicate) && ['reject-new', 'drop-oldest'].includes(p.overflow);
}
export function simulateQueue(document, { now = Date.now } = {}) {
  const start = now(), expired = () => now() - start > LIMITS.milliseconds;
  if (!object(document)) return incomplete('input-invalid');
  let bytes; try { bytes = Buffer.byteLength(JSON.stringify(document)); } catch { return incomplete('input-invalid'); }
  if (bytes > LIMITS.bytes) return incomplete('byte-limit');
  if (tooDeep(document)) return incomplete('depth-limit');
  if (expired()) return incomplete('time-limit');
  if (document.schemaVersion !== '1' || !object(document.policy) || !Array.isArray(document.events) || !Number.isInteger(document.seed) || document.seed < 0 || document.seed > 4294967295 || Object.keys(document).some(k => !['schemaVersion', 'complete', 'seed', 'policy', 'events'].includes(k))) return incomplete('input-invalid');
  if (document.complete !== true) return incomplete('export-incomplete');
  if (!validPolicy(document.policy)) return incomplete('policy-invalid');
  if (document.events.length > LIMITS.events) return incomplete('record-limit');
  let previousAt = -1;
  for (const [i, event] of document.events.entries()) {
    if (!object(event) || !Number.isSafeInteger(event.at) || event.at < 0 || event.at < previousAt || !['enqueue', 'connect', 'disconnect', 'tick'].includes(event.kind) || (event.kind === 'enqueue' ? !id(event.id) : event.id !== undefined) || (event.note !== undefined && typeof event.note !== 'string') || Object.keys(event).some(k => !['at', 'kind', 'id', 'note'].includes(k))) return report([finding('event-invalid', `/events/${i}`)]);
    previousAt = event.at;
  }
  const policy = document.policy, queue = [], outcomes = [], findings = [], seen = new Set(), delivered = new Set();
  let online = false, state = document.seed >>> 0;
  const roll = () => { state = (Math.imul(1664525, state) + 1013904223) >>> 0; return state % 100; };
  const loss = (item, ruleId) => { item.outcome.state = 'lost'; findings.push(finding(ruleId, `/events/${item.outcome.sourceOrdinal}`)); };
  const drain = time => {
    for (let i = 0; i < queue.length;) {
      if (expired()) return false;
      const item = queue[i];
      if (time - item.enqueuedAt >= policy.ttlMs) { loss(item, 'item-expired'); queue.splice(i, 1); continue; }
      while (item.due <= time && item.outcome.attempts < policy.maxAttempts) {
        if (expired()) return false;
        if (item.due - item.enqueuedAt >= policy.ttlMs) { loss(item, 'item-expired'); break; }
        item.outcome.attempts++;
        if (roll() < policy.successPercent) {
          item.outcome.state = 'delivered';
          if (delivered.has(item.id)) findings.push(finding('duplicate-delivered', `/events/${item.outcome.sourceOrdinal}`));
          delivered.add(item.id); break;
        }
        if (item.outcome.attempts >= policy.maxAttempts) { loss(item, 'retry-exhausted'); break; }
        item.due = time + policy.retryDelayMs;
      }
      if (item.outcome.state !== 'pending') queue.splice(i, 1);
      else i++;
    }
    return true;
  };
  for (const [ordinal, event] of document.events.entries()) {
    if (expired()) return incomplete('time-limit');
    for (let i = 0; i < queue.length;) {
      if (event.at - queue[i].enqueuedAt >= policy.ttlMs) {
        loss(queue[i], 'item-expired');
        queue.splice(i, 1);
      } else i++;
    }
    if (event.kind === 'connect') online = true;
    else if (event.kind === 'disconnect') online = false;
    else if (event.kind === 'enqueue') {
      const outcome = { sourceOrdinal: ordinal, state: 'pending', attempts: 0 };
      outcomes.push(outcome);
      if (policy.duplicate === 'suppress' && seen.has(event.id)) { outcome.state = 'suppressed'; findings.push(finding('duplicate-suppressed', `/events/${ordinal}`)); }
      else {
        seen.add(event.id);
        if (queue.length >= policy.capacity) {
          if (policy.overflow === 'drop-oldest') loss(queue.shift(), 'queue-overflow');
          else { outcome.state = 'lost'; findings.push(finding('queue-overflow', `/events/${ordinal}`)); }
        }
        if (outcome.state === 'pending') queue.push({ id: event.id, enqueuedAt: event.at, due: event.at, outcome });
      }
    }
    if (online && !drain(event.at)) return incomplete('time-limit');
  }
  if (!outcomes.length) return incomplete('no-items');
  for (const item of queue) findings.push(finding('queue-pending', `/events/${item.outcome.sourceOrdinal}`));
  if (expired()) return incomplete('time-limit');
  return report(findings, outcomes);
}
