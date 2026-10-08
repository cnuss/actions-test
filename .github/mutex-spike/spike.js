'use strict';

// Can the Actions cache be a mutex?
//   acquire = CreateCacheEntry (atomic) + PUT + FinalizeCacheEntryUpload
//   release = REST DELETE /actions/caches/{cache_id}
//
// handoff:  one job, N acquire/release cycles. Measures whether the same
//           key+version can be re-created after a delete, and how long it takes.
// contend:  matrix of workers sharing one key. Each records its critical
//           section; verify checks that no two overlap.

const fs = require('fs');
const https = require('https');
const crypto = require('crypto');
const { URL } = require('url');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const int = (name, dflt) => parseInt(process.env[name] || String(dflt), 10);

// Fresh socket per request: keep-alive pins a client to one cache replica.
function request(method, urlStr, headers, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const data = body == null ? null : Buffer.from(body);
    const req = https.request({
      method,
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      headers: { Connection: 'close', ...headers, ...(data ? { 'Content-Length': data.length } : {}) },
      agent: false,
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

function cacheService() {
  const token = process.env.ACTIONS_RUNTIME_TOKEN;
  const results = process.env.ACTIONS_RESULTS_URL;
  if (!token || !results) throw new Error('ACTIONS_RUNTIME_TOKEN / ACTIONS_RESULTS_URL missing');
  const base = `${results.replace(/\/$/, '')}/twirp/github.actions.results.api.v1.CacheService`;
  return async (method, body) => {
    const res = await request('POST', `${base}/${method}`, {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    }, JSON.stringify(body));
    let json = {};
    try { json = res.text ? JSON.parse(res.text) : {}; } catch { /* leave {} */ }
    return { status: res.status, json, text: res.text };
  };
}

const versionFor = (key) => crypto.createHash('sha256').update(`mutex-spike-v1:${key}`).digest('hex');

// { url } on a win, { code } otherwise. code is the twirp error code, or the
// HTTP status when there is none.
async function tryAcquire(svc, key, version) {
  const r = await svc('CreateCacheEntry', { key, version });
  const url = r.json.signed_upload_url || r.json.signedUploadUrl;
  if (url) return { url };
  return { code: r.json.code || `http_${r.status}`, text: r.text };
}

// Retries until won. Returns stats plus a histogram of non-winning codes.
async function acquire(svc, key, version, { delayMs, jitterMs, timeoutMs }) {
  const t0 = now();
  const codes = {};
  let attempts = 0;
  for (;;) {
    attempts += 1;
    const r = await tryAcquire(svc, key, version);
    if (r.url) return { url: r.url, attempts, waitMs: now() - t0, codes };
    codes[r.code] = (codes[r.code] || 0) + 1;
    if (r.code !== 'already_exists' && codes[r.code] === 1) console.log(`unexpected CreateCacheEntry reply: ${r.text}`);
    if (now() - t0 > timeoutMs) {
      throw new Error(`no re-acquire after ${attempts} attempts / ${now() - t0}ms: ${JSON.stringify(codes)}`);
    }
    await sleep(delayMs + Math.random() * jitterMs);
  }
}

// Returns the entry id from Finalize ('' if the service omits it).
async function finalize(svc, key, version, url, envelope) {
  const bytes = Buffer.from(JSON.stringify(envelope));
  const put = await request('PUT', url, {
    'x-ms-blob-type': 'BlockBlob',
    'Content-Type': 'application/octet-stream',
  }, bytes);
  if (put.status < 200 || put.status >= 300) throw new Error(`blob PUT HTTP ${put.status}: ${put.text}`);
  const f = await svc('FinalizeCacheEntryUpload', { key, version, size_bytes: bytes.length });
  if (f.json.ok !== true) throw new Error(`FinalizeCacheEntryUpload: ${f.text}`);
  return String(f.json.entry_id ?? f.json.entryId ?? '');
}

// Polls the REST list until the entry shows up; returns its id and the lag.
async function waitListed(github, context, key, timeoutMs) {
  const t0 = now();
  for (;;) {
    const { data } = await github.rest.actions.getActionsCacheList({
      ...context.repo, key, ref: process.env.GITHUB_REF, per_page: 100,
    });
    const hit = data.actions_caches.find((c) => c.key === key);
    if (hit) return { id: String(hit.id), listMs: now() - t0 };
    if (now() - t0 > timeoutMs) throw new Error(`entry ${key} never listed after ${timeoutMs}ms`);
    await sleep(500);
  }
}

async function release(github, context, cacheId) {
  const t0 = now();
  await github.rest.actions.deleteActionsCacheById({ ...context.repo, cache_id: Number(cacheId) });
  return now() - t0;
}

function runKey(name) {
  return `mutex-spike-${name}-${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}`;
}

async function handoff({ github, context, core }) {
  const svc = cacheService();
  const key = runKey('handoff');
  const version = versionFor(key);
  const cycles = int('CYCLES', 5);
  const retry = { delayMs: 250, jitterMs: 0, timeoutMs: int('REACQUIRE_TIMEOUT_SECONDS', 300) * 1000 };
  const rows = [];

  const first = await tryAcquire(svc, key, version);
  if (!first.url) throw new Error(`first CreateCacheEntry lost on a fresh key: ${first.text}`);
  let url = first.url;

  for (let i = 1; i <= cycles; i += 1) {
    const tF = now();
    const entryId = await finalize(svc, key, version, url, { cycle: i });
    const finalizeMs = now() - tF;
    const { id: restId, listMs } = await waitListed(github, context, key, 120_000);
    const deleteMs = await release(github, context, restId);
    const got = await acquire(svc, key, version, retry);
    url = got.url;
    const row = {
      cycle: i, finalizeMs, entryId, restId, idsMatch: entryId === restId,
      listMs, deleteMs, reacquireMs: got.waitMs, attempts: got.attempts, codes: JSON.stringify(got.codes),
    };
    core.info(JSON.stringify(row));
    rows.push(row);
  }

  // The last re-acquire is still an open reservation; publish and drop it.
  await finalize(svc, key, version, url, { cycle: 'cleanup' });
  const { id } = await waitListed(github, context, key, 120_000);
  await release(github, context, id);

  const cols = Object.keys(rows[0]);
  await core.summary
    .addHeading('Handoff: delete → re-create on the same key+version', 3)
    .addTable([cols.map((c) => ({ data: c, header: true })), ...rows.map((r) => cols.map((c) => String(r[c])))])
    .write();
}

async function contend({ github, context, core }) {
  const svc = cacheService();
  const key = runKey('contend');
  const version = versionFor(key);
  const worker = process.env.WORKER;
  const rounds = int('ROUNDS', 2);
  const holdMs = int('HOLD_SECONDS', 10) * 1000;
  const retry = { delayMs: 1000, jitterMs: 1000, timeoutMs: int('ACQUIRE_TIMEOUT_SECONDS', 900) * 1000 };
  const intervals = [];

  for (let round = 1; round <= rounds; round += 1) {
    const got = await acquire(svc, key, version, retry);
    const entryId = await finalize(svc, key, version, got.url, { worker, round, run_id: process.env.GITHUB_RUN_ID });
    const start = now();
    core.info(`[${worker}] round ${round}: acquired after ${got.attempts} attempts / ${got.waitMs}ms, entry ${entryId}`);
    await sleep(holdMs);
    const end = now();
    // Finalize's entry_id may not be the REST cache id; fall back to the list.
    try {
      if (!entryId) throw Object.assign(new Error('no entry_id'), { status: 404 });
      await release(github, context, entryId);
    } catch (err) {
      if (err.status !== 404) throw err;
      core.warning(`[${worker}] delete by entry_id ${entryId || '(none)'} failed; using the REST list`);
      await release(github, context, (await waitListed(github, context, key, 120_000)).id);
    }
    const released = now();
    core.info(`[${worker}] round ${round}: released`);
    intervals.push({ worker, round, attempts: got.attempts, waitMs: got.waitMs, codes: got.codes, start, end, released });
  }

  fs.writeFileSync(`intervals-${worker}.json`, JSON.stringify(intervals));
}

async function verify({ github, context, core }) {
  const dir = process.env.INTERVALS_DIR;
  const all = fs.readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .flatMap((f) => JSON.parse(fs.readFileSync(`${dir}/${f}`, 'utf8')))
    .sort((a, b) => a.start - b.start);

  const rows = [];
  const overlaps = [];
  all.forEach((iv, i) => {
    const prev = all[i - 1];
    // Handoff: previous release returned → this holder finished acquiring.
    const handoffMs = prev ? iv.start - prev.released : null;
    if (prev && iv.start < prev.end) overlaps.push(`${prev.worker}#${prev.round} and ${iv.worker}#${iv.round}`);
    rows.push([iv.worker, String(iv.round), new Date(iv.start).toISOString(), new Date(iv.end).toISOString(),
      String(iv.attempts), String(iv.waitMs), handoffMs == null ? '' : String(handoffMs), JSON.stringify(iv.codes)]);
  });

  // Leftovers from a failed job would otherwise sit in the cache.
  const prefix = `mutex-spike-`;
  const runSuffix = `-${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}`;
  const { data } = await github.rest.actions.getActionsCacheList({ ...context.repo, key: prefix, per_page: 100 });
  for (const c of data.actions_caches.filter((c) => c.key.endsWith(runSuffix))) {
    await github.rest.actions.deleteActionsCacheById({ ...context.repo, cache_id: c.id });
  }

  const headers = ['worker', 'round', 'start', 'end', 'attempts', 'wait ms', 'handoff ms', 'codes'];
  await core.summary
    .addHeading('Contention: critical sections in acquire order', 3)
    .addTable([headers.map((h) => ({ data: h, header: true })), ...rows])
    .addRaw(overlaps.length ? `**Overlaps:** ${overlaps.join(', ')}` : '**No overlapping critical sections.**')
    .write();

  if (overlaps.length) core.setFailed(`mutual exclusion violated: ${overlaps.join(', ')}`);
}

module.exports = { handoff, contend, verify };
