#!/usr/bin/env node
// Publishes minimized evidence only. Raw archive objects are never modified.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { parseArgs } from "node:util";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { loadArchive, sha256 } from "./snapshots/archive.mjs";
import { EXPORT_PREFIX, archivePrefix, prepareSnapshots, verifyCatalog, replaceSource, replaceSourceEntries, renewSource, refreshSourceExports, publishPrepared } from "./snapshots/publication.mjs";
import { createWranglerRunner } from "./snapshots/wrangler-client.mjs";

const exec = promisify(execFile);
const { values } = parseArgs({ options: {
  "archive-dir": { type: "string", multiple: true }, "refresh-only": { type: "boolean", default: false },
  "rebuild-existing": { type: "boolean", default: false },
  "delete-revoked": { type: "boolean", default: false },
  initialize: { type: "boolean", default: false }, bucket: { type: "string", default: "reddit-insights-archive" },
} });
if (!values["archive-dir"]?.length && !values["refresh-only"]) throw new Error("Provide --archive-dir or --refresh-only");
if (values["rebuild-existing"] && !values["refresh-only"]) throw new Error("Rebuild requires an explicit refresh-only recovery");
if (values.bucket !== "reddit-insights-archive") throw new Error("Publisher is scoped to the private Reddit Insights derivative bucket");
const work = await mkdtemp(join(tmpdir(), "reddit-snapshot-publication-"));
const now = Date.now();
let serial = 0, uploads = 0;
const runWrangler = createWranglerRunner(exec, work);
async function wrangler(args) {
  try { return await runWrangler(args); }
  catch (error) { throw new Error(`Cloudflare operation failed: ${(error.stderr || error.stdout || error.message).slice(-2000)}`); }
}
async function get(bucket, key, path, allowMissing = false) {
  try { await wrangler(["r2", "object", "get", `${bucket}/${key}`, "--file", path, "--remote"]); return await readFile(path); }
  catch (error) {
    if (allowMissing && /(?:NoSuchKey|does not exist|not found|\b10007\b)/i.test(error.message) && !/10042|authentication|permission/i.test(error.message)) return null;
    throw error;
  }
}
async function put(key, bytes) {
  const path = join(work, `upload-${serial++}`);
  await writeFile(path, bytes);
  await wrangler(["r2", "object", "put", `${values.bucket}/${key}`, "--file", path, "--remote", "--content-type", key.endsWith(".gz") ? "application/gzip" : "application/json"]);
  uploads++;
  if (uploads % 20 === 0) console.log(`Uploaded ${uploads} minimized objects`);
}
async function downloadArchive(prefix, directory, manifestBytes) {
  await mkdir(directory, { recursive: true });
  await writeFile(join(directory, "manifest.json"), manifestBytes);
  await Promise.all(["subreddits.index.json", "posts.jsonl.zst", "comments.jsonl.zst", "events.jsonl.zst"].map(file => get("high-signal-reddit-archive", `${prefix}/${file}`, join(directory, file))));
  return loadArchive(directory);
}

try {
  const existingBytes = await get(values.bucket, EXPORT_PREFIX + "index.json", join(work, "existing-catalog.json"), values.initialize);
  if (existingBytes && existingBytes.length > 4 * 1024 * 1024) throw new Error("Catalog exceeds serving limit");
  let catalog = verifyCatalog(existingBytes ? JSON.parse(existingBytes) : { schema: "reddit-insights.catalog.v1", entries: [] });
  const prepared = [], updated = new Set();
  for (const directory of values["archive-dir"] || []) {
    const snapshots = await loadArchive(resolve(directory));
    // Recheck the actual R2 manifest before publishing any locally prepared archive.
    const prefix = archivePrefix(snapshots[0]);
    const authoritative = await get("high-signal-reddit-archive", `${prefix}/manifest.json`, join(work, `source-${serial++}.json`));
    if (sha256(authoritative) !== snapshots[0].source.revision) throw new Error("Authoritative source changed; download and reverify it before publication");
    const items = prepareSnapshots(snapshots, now);
    catalog = replaceSource(catalog, items); prepared.push(...items); updated.add(prefix);
  }
  // Always discover the complete canonical latest run. A queued refresh/redaction
  // job can then recover a collection even if its own publish job was superseded.
  const latest = JSON.parse(await get("high-signal-reddit-archive", "reddit/v2/latest.json", join(work, "latest.json")));
  const match = /^reddit\/v2\/run=([1-9][0-9]*)\/attempt=([1-9][0-9]*)\/date=(\d{4}-\d{2}-\d{2})\/manifest.json$/.exec(latest.objects?.manifest || "");
  if (latest.status !== "complete" || !match) throw new Error("Invalid canonical archive pointer");
  const latestPrefix = archivePrefix({ source: { run: match[1], attempt: match[2] }, date: match[3] });
  if (!updated.has(latestPrefix)) {
    const directory = join(work, "latest-archive"); await mkdir(directory);
    const manifest = await get("high-signal-reddit-archive", latest.objects.manifest, join(directory, "manifest.json"));
    const entries = catalog.entries.filter(entry => archivePrefix(entry) === latestPrefix);
    if (values["rebuild-existing"] || !renewSource(entries, manifest, now)) {
      const items = prepareSnapshots(await downloadArchive(latestPrefix, directory, manifest), now);
      catalog = replaceSource(catalog, items); prepared.push(...items); updated.add(latestPrefix);
    }
  }
  const groups = new Map();
  for (const entry of catalog.entries) {
    const prefix = archivePrefix(entry);
    if (updated.has(prefix)) continue;
    if (!groups.has(prefix)) groups.set(prefix, []);
    groups.get(prefix).push(entry);
  }
  for (const [prefix, entries] of groups) {
    const directory = join(work, `archive-${serial++}`);
    await mkdir(directory);
    // A missing/unreadable source fails this publication; no lease is silently extended.
    const manifest = await get("high-signal-reddit-archive", `${prefix}/manifest.json`, join(directory, "manifest.json"));
    const refreshed = await refreshSourceExports(entries, manifest,
      () => downloadArchive(prefix, directory, manifest),
      entry => get(values.bucket, EXPORT_PREFIX + entry.key, join(work, `retouch-${serial++}`), true), now, { rebuildExisting: values["rebuild-existing"] });
    catalog = replaceSourceEntries(catalog, refreshed.entries);
    prepared.push(...refreshed.prepared);
  }
  catalog.generatedAt = new Date(now).toISOString();
  catalog.entries.sort((a, b) => b.date.localeCompare(a.date) || b.capturedAt.localeCompare(a.capturedAt) || a.subreddit.localeCompare(b.subreddit));
  catalog = await publishPrepared(catalog, prepared, put, async item => {
    const bytes = await get(values.bucket, EXPORT_PREFIX + item.entry.key, join(work, "verified-canary.json.gz"));
    if (bytes.length !== item.entry.bytes || sha256(bytes) !== item.entry.sha256) throw new Error("Published derivative readback failed");
  }, values["delete-revoked"] ? {
    remove: key => wrangler(["r2", "object", "delete", `${values.bucket}/${key}`, "--remote"]),
    readCatalog: () => get(values.bucket, EXPORT_PREFIX + "index.json", join(work, `deletion-receipt-${serial++}.json`)),
  } : {});
  const receipt = await get(values.bucket, EXPORT_PREFIX + "index.json", join(work, "verified-catalog.json"));
  if (!receipt.equals(Buffer.from(JSON.stringify(catalog)))) throw new Error("Published catalog readback failed");
  console.log(`Pending revoked derivative objects: ${catalog.pendingDeletionKeys?.length || 0}; physical cleanup requires --delete-revoked.`);
  console.log(`Verified ${catalog.entries.length} private snapshots; ${uploads} writes. Serving leases expire after24h without an authoritative refresh.`);
} finally {
  // This process-created scratch directory contains temporary packs only.
  await rm(work, { recursive: true, force: true });
}
