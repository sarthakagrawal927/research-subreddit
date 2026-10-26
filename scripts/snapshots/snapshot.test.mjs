import assert from "node:assert/strict";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { projectArchive, verifyFile, sha256 } from "./archive.mjs";
import { csv, readBounded, serveSnapshot } from "./service.mjs";
import { EXPORT_PREFIX, prepareSnapshots, replaceSource, publishPrepared } from "./publication.mjs";
import { onRequest } from "../../functions/api/snapshots/[[path]].js";

const post = { id: "post1", subreddit: "Cloud", title: "=unsafe <script>text</script>", body: "A complete body\nwith commas, and \"quotes\".", permalink: "/r/Cloud/comments/post1/example/", author: "PRIVATE_AUTHOR", createdUtc: 1790784000, edited: false, score: 4, upvoteRatio: .8, commentCount: 3, flair: null, nsfw: false, spoiler: false, stickied: false, locked: false, archived: false, removalState: null, retrievedAt: "2026-10-01T00:20:00Z", rawPayloadHash: "PRIVATE_HASH" };
const comment = { id: "comment1", postId: "post1", parentId: "t3_post1", subreddit: "Cloud", author: "PRIVATE_COMMENT_AUTHOR", body: " @SUM(1,2)", createdUtc: 1790784010, edited: false, score: -1, controversiality: 0, stickied: false, collapsed: false, depth: 0, isSubmitter: false, removalState: null, retrievedAt: "2026-10-01T00:20:00Z", rawPayloadHash: "PRIVATE_HASH" };
function fixture({ status = "complete", empty = false, removed = false } = {}) {
  const fields = { posts: Object.keys(post), comments: Object.keys(comment) };
  const rows = { posts: empty ? [] : [{ ...post, removalState: removed ? "deleted" : null }], comments: empty ? [] : [{ ...comment, removalState: removed ? "deleted" : null }] };
  const bytes = kind => Buffer.from(rows[kind].map(row => JSON.stringify(fields[kind].map(field => row[field])) + "\n").join(""));
  const postsBytes = bytes("posts"), commentsBytes = bytes("comments");
  const manifest = { schema: "high-signal.reddit-daily-archive.v2", objectPrefix: "reddit/v2/run=123/attempt=1/date=2026-10-01", windowStart: "2026-09-30T00:17:49Z", windowEnd: "2026-10-01T00:17:49Z", generatedAt: "2026-10-01T00:30:00Z", postCount: rows.posts.length, commentCount: rows.comments.length, schemas: fields,
    results: [{ subreddit: "cloud", status, posts: rows.posts.length, comments: rows.comments.length, commentsSeen: empty ? 0 : 3, commentsDropped: empty ? 0 : 2, listingCapped: false, commentFailures: 0, unresolvedMore: 0 }] };
  const index = { archiveDate: "2026-10-01", communities: { cloud: { status, posts: { startLine: 0, count: rows.posts.length }, comments: { startLine: 0, count: rows.comments.length }, decodedRanges: { posts: { startByte: 0, bytes: postsBytes.length }, comments: { startByte: 0, bytes: commentsBytes.length } } } } };
  return { manifest, index, postsBytes, commentsBytes, revision: "a".repeat(64) };
}
function store(snapshot = projectArchive(fixture())[0]) {
  const bytes = gzipSync(JSON.stringify(snapshot));
  const key = `snapshots/${snapshot.date}/${snapshot.subreddit}/${snapshot.source.run}-${snapshot.source.revision}.json.gz`;
  const entry = { subreddit: snapshot.subreddit, date: snapshot.date, source: snapshot.source, coverage: snapshot.coverage, capturedAt: snapshot.capturedAt, key, sha256: sha256(bytes), bytes: bytes.length, expiresAt: "2026-10-03T00:00:00Z" };
  const catalog = { schema: "reddit-insights.catalog.v1", entries: [entry] };
  const objects = new Map([[key, bytes]]);
  const get = async name => name === "index.json" ? new Blob([JSON.stringify(catalog)]).stream() : objects.has(name) ? new Blob([objects.get(name)]).stream() : null;
  return { snapshot, catalog, objects, get, entry };
}
const request = (suffix = "2026-10-01/cloud", method = "GET") => new Request(`https://example.com/api/snapshots/${suffix}`, { method });
const now = Date.parse("2026-10-02T00:00:00Z");

test("project full posts/comments, preserve relationships, normalize subreddit case, omit private metadata", () => {
  const [snapshot] = projectArchive(fixture());
  assert.equal(snapshot.posts[0].body, post.body);
  assert.equal(snapshot.posts[0].subreddit, "cloud");
  assert.equal(snapshot.comments[0].parentId, "t3_post1");
  assert.deepEqual(snapshot.coverage.commentsSeen, 3);
  assert.doesNotMatch(JSON.stringify(snapshot), /PRIVATE_|author|rawPayloadHash|objectPrefix/);
});
test("reject mismatched checksums, schema, community partitions and archive counts", () => {
  assert.throws(() => verifyFile(Buffer.from("corrupt"), { bytes: 7, sha256: "a".repeat(64) }), /checksum/);
  const schema = fixture(); schema.manifest.schemas.posts.shift(); assert.throws(() => projectArchive(schema), /fields/);
  const ranges = fixture(); ranges.index.communities.cloud.decodedRanges.posts.startByte = 1; ranges.index.communities.cloud.decodedRanges.posts.bytes--; assert.throws(() => projectArchive(ranges));
  const count = fixture(); count.manifest.postCount++; assert.throws(() => projectArchive(count), /counts/);
  const community = fixture(); community.manifest.results[0].subreddit = "wrong"; assert.throws(() => projectArchive(community), /receipt/);
});
test("enforce half-open collection window", () => {
  const input = fixture();
  input.manifest.windowStart = "2026-09-29T00:17:49Z"; input.manifest.windowEnd = "2026-09-30T00:17:49Z";
  input.manifest.objectPrefix = "reddit/v2/run=123/attempt=1/date=2026-09-30"; input.index.archiveDate = "2026-09-30";
  assert.throws(() => projectArchive(input), /outside collection window/);
});
test("redacted source receipts cannot republish captured text", () => {
  const [snapshot] = projectArchive(fixture({ removed: true }));
  assert.equal(snapshot.posts[0].title, "[removed]"); assert.equal(snapshot.posts[0].body, "[removed]"); assert.equal(snapshot.comments[0].body, "[removed]");
});
test("empty, partial and failed receipts remain distinct", () => {
  for (const status of ["complete", "partial", "failed"]) {
    const [snapshot] = projectArchive(fixture({ status, empty: true }));
    assert.equal(snapshot.coverage.status, status); assert.equal(snapshot.coverage.posts, 0);
  }
});
test("JSON and downloads reconcile to the same pinned collection; catalog hides object paths", async () => {
  const { get, snapshot } = store();
  const response = await serveSnapshot(request("2026-10-01/Cloud?run=123&revision=" + snapshot.source.revision), get, now);
  assert.equal(response.status, 200); const data = await response.json(); assert.equal(data.posts.length, 1); assert.deepEqual(data.source, snapshot.source);
  assert.equal(response.headers.get("Cache-Control"), "no-store");
  assert.match(response.headers.get("Server-Timing"), /^storage;dur=\d+, total;dur=\d+$/);
  const download = await serveSnapshot(request("2026-10-01/cloud?format=json&download=1"), get, now);
  assert.equal(download.headers.get("Cache-Control"), "no-store");
  assert.match(download.headers.get("Server-Timing"), /^storage;dur=\d+, total;dur=\d+$/);
  assert.match(download.headers.get("Content-Disposition"), /attachment/); assert.deepEqual((await download.json()).posts, snapshot.posts);
  for (const format of ["posts.csv", "comments.csv"]) {
    const csvResponse = await serveSnapshot(request(`2026-10-01/cloud?format=${format}`), get, now);
    assert.equal(csvResponse.headers.get("Cache-Control"), "no-store");
    assert.match(csvResponse.headers.get("Server-Timing"), /^storage;dur=\d+, total;dur=\d+$/);
    assert.equal(csvResponse.status, 200); const text = await csvResponse.text(); assert.match(text, /"run","revision","windowStart","windowEnd","coverageStatus"/); assert.doesNotMatch(text, /PRIVATE_|author/);
  }
  const catalog = await serveSnapshot(request("catalog"), get, now); assert.doesNotMatch(await catalog.text(), /\.json\.gz|"key"|"sha256"|"bytes"/);
  assert.equal(catalog.headers.get("Cache-Control"), "public, max-age=60, s-maxage=300");
  assert.match(catalog.headers.get("Server-Timing"), /^storage;dur=\d+, total;dur=\d+$/);
});
test("catalog errors remain no-store without successful-response timing", async () => {
  for (const get of [null, async () => null, async () => { throw new Error("storage unavailable"); }, async () => new Blob(["invalid JSON"]).stream()]) {
    const response = await serveSnapshot(request("catalog"), get, now);
    assert.equal(response.status, 503);
    assert.equal(response.headers.get("Cache-Control"), "no-store");
    assert.equal(response.headers.get("Server-Timing"), null);
  }
});
test("missing, removed, expired, unavailable and corrupt responses do not become empty snapshots", async () => {
  const { get, catalog, objects, entry } = store();
  assert.equal((await serveSnapshot(request("2026-10-02/cloud"), get, now)).status, 404);
  assert.equal((await serveSnapshot(request("2026-10-01/cloud?run=123&revision=" + "b".repeat(64)), get, now)).status, 410);
  assert.equal((await serveSnapshot(request(), get, Date.parse(entry.expiresAt))).status, 410);
  assert.equal((await serveSnapshot(request(), null, now)).status, 503);
  objects.set(entry.key, Buffer.from("broken")); assert.equal((await serveSnapshot(request(), get, now)).status, 503);
  catalog.entries = []; assert.equal((await serveSnapshot(request("2026-10-01/cloud?run=123"), get, now)).status, 410);
});
test("removed revisions are not served even if the old file still exists", async () => {
  const { get, catalog, objects, entry } = store(); catalog.entries[0] = { ...entry, source: { ...entry.source, revision: "b".repeat(64) } };
  assert.ok(objects.has(entry.key));
  assert.equal((await serveSnapshot(request("2026-10-01/cloud?revision=" + "a".repeat(64)), get, now)).status, 410);
});
test("short versions resolve full identity, reject ambiguous prefixes and respect revocation", async () => {
  const { get, snapshot, catalog, entry } = store();
  const version = snapshot.source.revision.slice(0, 12);
  const response = await serveSnapshot(request(`2026-10-01/cloud?v=${version}`), get, now);
  assert.equal(response.status, 200); assert.deepEqual((await response.json()).source, snapshot.source);
  assert.equal((await serveSnapshot(request("2026-10-01/cloud?v=bad"), get, now)).status, 400);
  assert.equal((await serveSnapshot(request("2026-10-01/cloud?v=" + "b".repeat(12)), get, now)).status, 410);
  catalog.entries.push({ ...entry, source: { ...entry.source, revision: version + "b".repeat(52) } });
  assert.equal((await serveSnapshot(request(`2026-10-01/cloud?v=${version}`), get, now)).status, 409);
  catalog.entries = []; assert.equal((await serveSnapshot(request(`2026-10-01/cloud?v=${version}`), get, now)).status, 410);
});
test("CSV quotes multiline text and blocks formula prefixes without changing numeric negatives", () => {
  const { snapshot } = store();
  const text = csv([{ body: '\n=SUM(1,2) "quote"', score: -3 }], ["body", "score"], snapshot);
  assert.ok(text.includes('"\'\n=SUM(1,2) ""quote"""')); assert.ok(text.includes('"-3"'));
  for (const body of [" =1", "\t+1", "@SUM(1)", "-formula", "\rplain"]) assert.ok(csv([{ body }], ["body"], snapshot).includes('"\'' + body));
});
test("bounded decoding rejects large data and invalid paths cannot reach storage", async () => {
  await assert.rejects(() => readBounded(new Blob(["12345"]).stream(), 4), /size/);
  const get = () => { throw new Error("must not read storage"); };
  assert.equal((await serveSnapshot(request("2026-10-01/%2E%2E%2Fsecret"), get, now)).status, 400);
  assert.equal((await serveSnapshot(request("catalog", "POST"), get, now)).status, 405);
});
test("Pages adapter fails explicitly without a binding and uses only the derivative prefix", async () => {
  assert.equal((await onRequest({ request: request(), env: {} })).status, 503);
  const keys = []; const response = await onRequest({ request: request("catalog"), env: { SNAPSHOT_EXPORTS: { get: async key => { keys.push(key); return null; } } } });
  assert.equal(response.status, 503); assert.deepEqual(keys, ["reddit-insights/exports/v1/index.json"]);
});
test("Pages adapter serves the catalog from the colo cache after the first read", async t => {
  const { catalog } = store();
  const entries = new Map(); let reads = 0; const pending = [];
  const original = globalThis.caches;
  globalThis.caches = { default: { match: async key => entries.get(key.url)?.clone(), put: async (key, value) => { entries.set(key.url, value); } } };
  t.after(() => { globalThis.caches = original; });
  const env = { SNAPSHOT_EXPORTS: { get: async () => { reads++; return { body: new Blob([JSON.stringify(catalog)]).stream() }; } } };
  const context = suffix => ({ request: request(suffix), env, waitUntil: promise => pending.push(promise) });
  const miss = await onRequest(context("catalog"));
  await Promise.all(pending);
  assert.equal(miss.status, 200);
  assert.match(miss.headers.get("Server-Timing"), /^storage;dur=\d+, total;dur=\d+, cache;desc="MISS"$/);
  const hit = await onRequest(context("catalog"));
  assert.equal(hit.status, 200); assert.equal(reads, 1);
  assert.equal(hit.headers.get("Server-Timing"), 'cache;desc="HIT"');
  assert.equal(hit.headers.get("Cache-Control"), "public, max-age=60, s-maxage=300");
  assert.deepEqual((await hit.json()).entries.length, catalog.entries.length);
  await onRequest(context("catalog?v=abc"));
  await onRequest(context("2026-10-01/cloud"));
  assert.equal(entries.size, 1, "only the bare catalog URL is cached");
});


test("redacted source publication removes the revoked export bytes and denies pinned downloads", async () => {
  const original = prepareSnapshots(projectArchive(fixture()), now);
  const initial = { schema: "reddit-insights.catalog.v1", entries: original.map(item => item.entry) };
  const objects = new Map([[EXPORT_PREFIX + "index.json", Buffer.from(JSON.stringify(initial))],
    ...original.map(item => [EXPORT_PREFIX + item.entry.key, item.bytes])]);
  const rawKey = "reddit/v2/run=123/attempt=1/date=2026-10-01/posts.jsonl.zst";
  objects.set(rawKey, Buffer.from("raw archive remains outside derivative cleanup"));
  const read = async key => objects.has(EXPORT_PREFIX + key) ? new Blob([objects.get(EXPORT_PREFIX + key)]).stream() : null;
  const before = await serveSnapshot(request("2026-10-01/cloud?format=posts.csv"), read, now);
  assert.match(await before.text(), /A complete body/);
  const revised = prepareSnapshots(projectArchive({ ...fixture({ removed: true }), revision: "b".repeat(64) }), now);
  const catalog = replaceSource(initial, revised);
  await publishPrepared(catalog, revised,
    async (key, bytes) => { objects.set(key, bytes); },
    async item => assert.equal(sha256(objects.get(EXPORT_PREFIX + item.entry.key)), item.entry.sha256),
    { remove: async key => { objects.delete(key); }, readCatalog: async () => objects.get(EXPORT_PREFIX + "index.json") });
  assert.equal(objects.has(EXPORT_PREFIX + original[0].entry.key), false);
  assert.equal(objects.has(rawKey), true);
  const pinned = await serveSnapshot(request(`2026-10-01/cloud?revision=${"a".repeat(64)}&format=posts.csv`), read, now);
  assert.equal(pinned.status, 410);
  assert.equal(pinned.headers.get("Cache-Control"), "no-store");
  for (const format of ["json", "posts.csv", "comments.csv"]) {
    const response = await serveSnapshot(request(`2026-10-01/cloud?format=${format}`), read, now);
    assert.equal(response.status, 200);
    const body = await response.text();
    assert.match(body, /\[removed\]/);
    assert.doesNotMatch(body, /A complete body|@SUM/);
  }
});
