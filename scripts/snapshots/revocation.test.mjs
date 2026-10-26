import assert from "node:assert/strict";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { sha256 } from "./archive.mjs";
import { EXPORT_PREFIX, MAX_PENDING_DELETIONS, replaceSource, verifyCatalog, publishPrepared } from "./publication.mjs";

function fixture(count = 2) {
  const bytes = gzipSync("{}"), source = { run: "12", attempt: "1", revision: "a".repeat(64) };
  const entries = Array.from({ length: count }, (_, index) => ({
    subreddit: `Community_${index}`, date: "2026-10-01", source,
    key: `snapshots/2026-10-01/Community_${index}/12-${source.revision}.json.gz`, sha256: sha256(bytes), bytes: bytes.length,
  }));
  const previous = { schema: "reddit-insights.catalog.v1", entries };
  const prepared = entries.map(entry => ({ bytes, entry: { ...entry, source: { ...source, revision: "b".repeat(64) }, key: entry.key.replace(source.revision, "b".repeat(64)) } }));
  const catalog = replaceSource(previous, prepared);
  const objects = new Map([[EXPORT_PREFIX + "index.json", Buffer.from(JSON.stringify(previous))], ...entries.map(entry => [EXPORT_PREFIX + entry.key, bytes])]);
  const put = async (key, value) => { objects.set(key, value); };
  const readCatalog = async () => objects.get(EXPORT_PREFIX + "index.json");
  const current = () => JSON.parse(objects.get(EXPORT_PREFIX + "index.json"));
  const verify = async item => assert.equal(sha256(objects.get(EXPORT_PREFIX + item.entry.key)), item.entry.sha256);
  return { entries, prepared, catalog, objects, put, readCatalog, current, verify };
}

test("ordinary publication queues bounded content-free revocations without deleting objects", async () => {
  const f = fixture();
  const result = await publishPrepared(f.catalog, f.prepared, f.put, f.verify);
  assert.deepEqual(result.pendingDeletionKeys, f.entries.map(entry => entry.key));
  assert.deepEqual(f.current(), result);
  assert.ok(f.entries.every(entry => f.objects.has(EXPORT_PREFIX + entry.key)));
  assert.deepEqual(replaceSource(result, f.prepared).pendingDeletionKeys, result.pendingDeletionKeys);
});

test("failed deletions preserve a durable journal and retries tolerate already removed objects", async () => {
  const f = fixture(), deleted = [];
  const remove = async key => {
    if (key === EXPORT_PREFIX + f.entries[0].key) throw new Error("delete unavailable");
    deleted.push(key); f.objects.delete(key);
  };
  await assert.rejects(publishPrepared(f.catalog, f.prepared, f.put, f.verify, { remove, readCatalog: f.readCatalog }), /delete unavailable/);
  assert.equal(deleted.length, 1);
  assert.deepEqual(f.current().pendingDeletionKeys, f.entries.map(entry => entry.key));
  assert.ok(f.prepared.every(item => f.objects.has(EXPORT_PREFIX + item.entry.key)));
  const result = await publishPrepared(f.current(), [], f.put, f.verify, {
    remove: async key => { f.objects.delete(key); }, readCatalog: f.readCatalog,
  });
  assert.equal(result.pendingDeletionKeys, undefined);
  assert.deepEqual(f.current(), result);
  assert.ok(f.entries.every(entry => !f.objects.has(EXPORT_PREFIX + entry.key)));
});

test("upload, catalog publication and readback failures cannot start deletion", async () => {
  for (const failure of ["upload", "catalog", "readback"]) {
    const f = fixture(); let deletes = 0;
    const put = async (key, bytes) => {
      if ((failure === "upload" && key !== EXPORT_PREFIX + "index.json") || (failure === "catalog" && key === EXPORT_PREFIX + "index.json")) throw new Error(failure);
      await f.put(key, bytes);
    };
    await assert.rejects(publishPrepared(f.catalog, f.prepared, put, f.verify, {
      remove: async () => { deletes++; }, readCatalog: async () => failure === "readback" ? Buffer.from("wrong receipt") : f.readCatalog(),
    }));
    assert.equal(deletes, 0);
  }
});

test("journal rejects raw, active, malformed and excess keys before any writes", async () => {
  const f = fixture();
  const bad = [
    ["reddit/v2/run=12/attempt=1/date=2026-10-01/posts.jsonl.zst"],
    [f.prepared[0].entry.key],
    ["snapshots/../../index.json"],
    [f.entries[0].key, f.entries[0].key],
    Array.from({ length: MAX_PENDING_DELETIONS + 1 }, (_, index) => `snapshots/2026-10-01/Community_${index}/12-${"c".repeat(64)}.json.gz`),
  ];
  for (const pendingDeletionKeys of bad) {
    const catalog = { ...f.catalog, pendingDeletionKeys };
    assert.throws(() => verifyCatalog(catalog));
    let writes = 0, deletes = 0;
    await assert.rejects(publishPrepared(catalog, f.prepared, async () => { writes++; }, f.verify, {
      remove: async () => { deletes++; }, readCatalog: f.readCatalog,
    }));
    assert.equal(writes, 0); assert.equal(deletes, 0);
  }
});

test("deletions settle at most four at a time before committing completion", async () => {
  const f = fixture(9); let active = 0, peak = 0, completed = 0;
  await publishPrepared(f.catalog, f.prepared, f.put, f.verify, {
    remove: async key => { active++; peak = Math.max(peak, active); await new Promise(resolve => setImmediate(resolve)); f.objects.delete(key); active--; completed++; },
    readCatalog: f.readCatalog,
  });
  assert.equal(peak, 4); assert.equal(active, 0); assert.equal(completed, 9);
  assert.equal(f.current().pendingDeletionKeys, undefined);
});


test("cleanup requires readback and completion-write failure keeps retry evidence", async () => {
  const f = fixture(); let writes = 0;
  await assert.rejects(publishPrepared(f.catalog, f.prepared, async () => { writes++; }, f.verify, { remove: async () => {} }), /readback verification/);
  assert.equal(writes, 0);
  const put = async (key, bytes) => {
    if (key === EXPORT_PREFIX + "index.json" && ++writes === 2) throw new Error("completion write failed");
    await f.put(key, bytes);
  };
  await assert.rejects(publishPrepared(f.catalog, f.prepared, put, f.verify, {
    remove: async key => { f.objects.delete(key); }, readCatalog: f.readCatalog,
  }), /completion write failed/);
  assert.ok(f.entries.every(entry => !f.objects.has(EXPORT_PREFIX + entry.key)));
  assert.deepEqual(f.current().pendingDeletionKeys, f.entries.map(entry => entry.key));
  await publishPrepared(f.current(), [], f.put, f.verify, { remove: async key => { f.objects.delete(key); }, readCatalog: f.readCatalog });
  assert.equal(f.current().pendingDeletionKeys, undefined);
});
