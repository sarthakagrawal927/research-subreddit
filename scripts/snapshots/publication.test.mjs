import assert from "node:assert/strict";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { sha256 } from "./archive.mjs";
import { archivePrefix, renewSource, replaceSource, refreshSourceExports, publishPrepared, EXPORT_PREFIX, mapSettledBatches, createLimiter, MAX_PUBLISH_CONCURRENCY, settleAll } from "./publication.mjs";

const source = { run: "12", attempt: "1", revision: "a".repeat(64) };
const bytes = gzipSync("{}");
const entry = { subreddit: "AI_Agents", date: "2026-10-01", source, key: `snapshots/2026-10-01/AI_Agents/12-${source.revision}.json.gz`, sha256: sha256(bytes), bytes: bytes.length };

test("archive-prefix batches are bounded and results retain catalog order", async () => {
  let active = 0, peak = 0;
  const results = await mapSettledBatches([0, 1, 2, 3, 4], 2, async value => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, (5 - value) * 2));
    active--;
    return value;
  });
  assert.equal(peak, 2);
  assert.deepEqual(results, [0, 1, 2, 3, 4]);
});

test("archive-prefix failure settles its active batch and does not start later groups", async () => {
  const completed = [], started = [];
  await assert.rejects(mapSettledBatches([0, 1, 2, 3], 2, async value => {
    started.push(value);
    await new Promise(resolve => setImmediate(resolve));
    completed.push(value);
    if (value === 0) throw new Error("source permission denied");
    return value;
  }), /source permission denied/);
  assert.deepEqual(started, [0, 1]);
  assert.deepEqual(completed.sort(), [0, 1]);
});

test("overlapping prefix work shares one bounded storage-read budget", async () => {
  let active = 0, peak = 0;
  const withLimit = createLimiter(8);
  await Promise.all(Array.from({ length: 24 }, () => withLimit(async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setImmediate(resolve));
    active--;
  })));
  assert.equal(peak, 8);
  assert.throws(() => createLimiter(0), /concurrency limit/);
});

test("the read limiter transfers a released slot to a queued caller", async () => {
  const withLimit = createLimiter(1);
  let active = 0, peak = 0, releaseFirst;
  const run = async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setImmediate(resolve));
    active--;
  };
  const first = withLimit(async () => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => { releaseFirst = resolve; });
    active--;
  });
  const second = withLimit(run);
  releaseFirst();
  await Promise.resolve();
  await Promise.resolve();
  const third = withLimit(run);
  await Promise.all([first, second, third]);
  assert.equal(peak, 1);
});

test("settled archive reads finish before the first failure is surfaced", async () => {
  const completed = [];
  await assert.rejects(settleAll([
    Promise.resolve().then(() => { throw new Error("archive permission denied"); }),
    new Promise(resolve => setTimeout(() => { completed.push("read"); resolve(); }, 5)),
  ]), /archive permission denied/);
  assert.deepEqual(completed, ["read"]);
});

test("publication rejects arbitrary archive identities and renews only unchanged authoritative receipts", () => {
  assert.equal(archivePrefix(entry), "reddit/v2/run=12/attempt=1/date=2026-10-01");
  assert.throws(() => archivePrefix({ ...entry, source: { ...source, run: "../x" } }));
  const manifest = Buffer.from(JSON.stringify({ objectPrefix: archivePrefix(entry) }));
  assert.equal(renewSource([entry], manifest), null);
  const matching = { ...entry, source: { ...source, revision: sha256(manifest) } };
  const renewed = renewSource([matching], manifest, 0);
  assert.equal(renewed[0].expiresAt, "1970-01-02T00:00:00.000Z");
  assert.equal(renewSource([matching], Buffer.from('{}')), null);
});
test("changed source replaces all its old community revisions while preserving other dates", () => {
  const other = { ...entry, date: "2026-09-30" };
  const prepared = [{ bytes, entry: { ...entry, source: { ...source, revision: "b".repeat(64) } } }];
  const result = replaceSource({ entries: [entry, other] }, prepared);
  assert.deepEqual(result.entries, [other, prepared[0].entry]);
});
test("catalog publication follows uploads and verification; either failure leaves the old catalog active", async () => {
  const prepared = [{ bytes, entry }], catalog = { schema: "reddit-insights.catalog.v1", entries: [entry] };
  const writes = [];
  await assert.rejects(publishPrepared(catalog, prepared, async key => { writes.push(key); }, async () => { throw new Error("readback failed"); }));
  assert.deepEqual(writes, [EXPORT_PREFIX + entry.key]);
  writes.length = 0;
  await assert.rejects(publishPrepared(catalog, prepared, async () => { throw new Error("upload failed"); }, async () => { writes.push("verified"); }));
  assert.deepEqual(writes, []);
  await publishPrepared(catalog, prepared, async key => { writes.push(key); }, async () => { writes.push("verified"); });
  assert.deepEqual(writes, [EXPORT_PREFIX + entry.key, "verified", EXPORT_PREFIX + "index.json"]);
});

test("snapshot uploads share an eight-write cap and commit the catalog after checksum readback", async () => {
  const items = Array.from({ length: 12 }, (_, index) => {
    const subreddit = index === 0 ? "AI_Agents" : `community_${index}`;
    const body = gzipSync(JSON.stringify({ index }));
    const itemEntry = { ...entry, subreddit,
      key: `snapshots/2026-10-01/${subreddit}/12-${source.revision}.json.gz`,
      sha256: sha256(body), bytes: body.length };
    return { bytes: body, entry: itemEntry };
  });
  const catalog = { schema: "reddit-insights.catalog.v1", entries: items.map(item => item.entry) };
  const stored = new Map(), started = [];
  let active = 0, peak = 0, verified = false;
  await publishPrepared(catalog, items, async (key, body) => {
    started.push(key);
    if (key === EXPORT_PREFIX + "index.json") {
      assert.equal(verified, true);
      assert.equal(stored.size, items.length);
    }
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 2));
    stored.set(key, Buffer.from(body));
    active--;
  }, async item => {
    const body = stored.get(EXPORT_PREFIX + item.entry.key);
    assert.equal(body?.length, item.entry.bytes);
    assert.equal(sha256(body), item.entry.sha256);
    verified = true;
  });
  assert.equal(MAX_PUBLISH_CONCURRENCY, 8);
  assert.equal(peak, 8);
  assert.deepEqual(started, [...items.map(item => EXPORT_PREFIX + item.entry.key), EXPORT_PREFIX + "index.json"]);
});

test("a missing expired derivative is rebuilt from its authoritative archive instead of blocking refresh", async () => {
  const now = Date.parse("2026-10-22T00:00:00Z");
  const manifest = Buffer.from(JSON.stringify({ objectPrefix: archivePrefix(entry) }));
  const snapshot = { subreddit: entry.subreddit, date: entry.date,
    source: { ...source, revision: sha256(manifest) }, capturedAt: "2026-10-01T00:00:00Z" };
  const expired = { ...entry, source: snapshot.source, storedAt: "2026-10-14T00:00:00Z" };
  let loads = 0;
  const refreshed = await refreshSourceExports([expired], manifest,
    async () => { loads++; return [snapshot]; }, async () => null, now);
  assert.equal(loads, 1);
  assert.equal(refreshed.prepared.length, 1);
  assert.equal(refreshed.entries[0].expiresAt, "2026-10-23T00:00:00.000Z");
  assert.equal(refreshed.entries[0].source.revision, sha256(manifest));
  assert.equal(refreshed.entries[0].storedAt, new Date(now).toISOString());
  assert.equal(expired.storedAt, "2026-10-14T00:00:00Z");
});

test("refresh never turns permission failures, corrupt exports or unavailable archives into lease renewal", async () => {
  const manifest = Buffer.from(JSON.stringify({ objectPrefix: archivePrefix(entry) }));
  const old = { ...entry, source: { ...source, revision: sha256(manifest) }, storedAt: "2026-10-01T00:00:00Z" };
  const now = Date.parse("2026-10-22T00:00:00Z");
  const load = async () => { throw new Error("source unavailable"); };
  await assert.rejects(refreshSourceExports([old], manifest, load,
    async () => { throw new Error("permission denied"); }, now), /permission denied/);
  await assert.rejects(refreshSourceExports([old], manifest, load,
    async () => Buffer.from("corrupt"), now), /checksum verification/);
  await assert.rejects(refreshSourceExports([old], manifest, load,
    async () => null, now), /source unavailable/);
  await assert.rejects(refreshSourceExports([{ ...old, storedAt: "invalid" }], manifest, load,
    async () => Buffer.from("corrupt"), now), /checksum verification/);
});

test("aged derivative verification overlaps at most four reads and preserves catalog order", async () => {
  const manifest = Buffer.from(JSON.stringify({ objectPrefix: archivePrefix(entry) }));
  const old = Array.from({ length: 9 }, (_, index) => ({ ...entry, subreddit: `community_${index}`,
    source: { ...source, revision: sha256(manifest) }, storedAt: "2026-10-01T00:00:00Z" }));
  let active = 0, peak = 0, completed = 0;
  const refreshed = await refreshSourceExports(old, manifest, async () => { throw new Error("unexpected rebuild"); },
    async () => {
      active++; peak = Math.max(peak, active);
      await new Promise(resolve => setImmediate(resolve));
      active--; completed++; return bytes;
    }, Date.parse("2026-10-22T00:00:00Z"));
  assert.equal(peak, 4);
  assert.equal(active, 0);
  assert.equal(completed, 9);
  assert.deepEqual(refreshed.prepared.map(item => item.entry.subreddit), old.map(item => item.subreddit));
  assert.ok(old.every(item => item.storedAt === "2026-10-01T00:00:00Z"));
});

test("a missing object cannot mask concurrent permission or checksum failures", async () => {
  const manifest = Buffer.from(JSON.stringify({ objectPrefix: archivePrefix(entry) }));
  const old = Array.from({ length: 4 }, (_, index) => ({ ...entry, subreddit: `community_${index}`,
    source: { ...source, revision: sha256(manifest) }, storedAt: "2026-10-01T00:00:00Z" }));
  let completed = 0;
  const load = async () => { throw new Error("unexpected rebuild"); };
  await assert.rejects(refreshSourceExports(old, manifest, load, async item => {
    await new Promise(resolve => setImmediate(resolve)); completed++;
    if (item.subreddit === "community_1") throw new Error("permission denied");
    return null;
  }, Date.parse("2026-10-22T00:00:00Z")), /permission denied/);
  assert.equal(completed, 4);
  await assert.rejects(refreshSourceExports(old, manifest, load,
    async item => item.subreddit === "community_1" ? Buffer.from("corrupt") : null,
    Date.parse("2026-10-22T00:00:00Z")), /checksum verification/);
});

test("explicit recovery rebuilds from verified sources without trusting existing derivatives", async () => {
  const manifest = Buffer.from(JSON.stringify({ objectPrefix: archivePrefix(entry) }));
  const snapshot = { subreddit: entry.subreddit, date: entry.date, source: { ...source, revision: sha256(manifest) } };
  const old = { ...entry, source: snapshot.source, storedAt: "2026-10-01T00:00:00Z" };
  const now = Date.parse("2026-10-22T00:00:00Z"), options = { rebuildExisting: true };
  const read = async () => { throw new Error("must not trust a previous derivative"); };
  const refreshed = await refreshSourceExports([old], manifest, async () => [snapshot], read, now, options);
  assert.equal(refreshed.prepared.length, 1);
  assert.equal(refreshed.entries[0].sha256, sha256(refreshed.prepared[0].bytes));
  assert.equal(refreshed.entries[0].source.revision, sha256(manifest));
  assert.equal(old.storedAt, "2026-10-01T00:00:00Z");
  await assert.rejects(refreshSourceExports([old], manifest, async () => { throw new Error("source unavailable"); }, read, now, options), /source unavailable/);
  for (const snapshots of [[], [{ ...snapshot, source }], [{ ...snapshot, date: "2026-10-02" }]]) {
    await assert.rejects(refreshSourceExports([old], manifest, async () => snapshots, read, now, options), /source verification/);
  }
});
