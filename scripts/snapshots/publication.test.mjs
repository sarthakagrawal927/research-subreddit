import assert from "node:assert/strict";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { sha256 } from "./archive.mjs";
import { archivePrefix, renewSource, replaceSource, refreshSourceExports, publishPrepared, EXPORT_PREFIX } from "./publication.mjs";

const source = { run: "12", attempt: "1", revision: "a".repeat(64) };
const bytes = gzipSync("{}");
const entry = { subreddit: "AI_Agents", date: "2026-10-01", source, key: `snapshots/2026-10-01/AI_Agents/12-${source.revision}.json.gz`, sha256: sha256(bytes), bytes: bytes.length };
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
  const other = { ...entry, date: "2026-09-30", key: entry.key.replace("2026-10-01", "2026-09-30") };
  const prepared = [{ bytes, entry: { ...entry, source: { ...source, revision: "b".repeat(64) }, key: entry.key.replace(source.revision, "b".repeat(64)) } }];
  const result = replaceSource({ schema: "reddit-insights.catalog.v1", entries: [entry, other] }, prepared);
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
