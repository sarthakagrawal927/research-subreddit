import { gzipSync } from "node:zlib";
import { MAX_SNAPSHOT_BYTES, sha256 } from "./archive.mjs";
import { validDate } from "./viewer/paths.mjs";

export const EXPORT_PREFIX = "reddit-insights/exports/v1/";
export const LEASE_MS = 86400000;
export const RETOUCH_MS = 5 * LEASE_MS;
export const MAX_PENDING_DELETIONS = 4096;
const MAX_CATALOG_BYTES = 4 * 1024 * 1024;

export function archivePrefix(entry) {
  if (!validDate(entry.date) || !/^[1-9][0-9]*$/.test(entry.source?.run || "") || !/^[1-9][0-9]*$/.test(entry.source?.attempt || "")) throw new Error("Invalid source archive identity");
  return `reddit/v2/run=${entry.source.run}/attempt=${entry.source.attempt}/date=${entry.date}`;
}

export function verifyCatalog(catalog) {
  if (catalog.schema !== "reddit-insights.catalog.v1" || !Array.isArray(catalog.entries)) throw new Error("Invalid export catalog");
  const keys = new Set();
  for (const entry of catalog.entries) {
    archivePrefix(entry);
    if (!/^[A-Za-z0-9_]{2,30}$/.test(entry.subreddit) || !/^[a-f0-9]{64}$/.test(entry.source.revision) || !/^[a-f0-9]{64}$/.test(entry.sha256)
      || entry.key !== `snapshots/${entry.date}/${entry.subreddit}/${entry.source.run}-${entry.source.revision}.json.gz`
      || !Number.isSafeInteger(entry.bytes) || entry.bytes <= 0 || entry.bytes > MAX_SNAPSHOT_BYTES || keys.has(entry.key)) throw new Error("Invalid catalogued export");
    keys.add(entry.key);
  }
  const pending = catalog.pendingDeletionKeys ?? [];
  if (!Array.isArray(pending) || pending.length > MAX_PENDING_DELETIONS || new Set(pending).size !== pending.length) throw new Error("Invalid pending deletion journal");
  for (const key of pending) {
    const match = typeof key === "string" && /^snapshots\/(\d{4}-\d{2}-\d{2})\/[A-Za-z0-9_]{2,30}\/[1-9][0-9]*-[a-f0-9]{64}\.json\.gz$/.exec(key);
    if (!match || !validDate(match[1]) || keys.has(key)) throw new Error("Deletion journal contains an invalid or active export key");
  }
  return catalog;
}

export function prepareSnapshots(snapshots, now = Date.now()) {
  return snapshots.map(snapshot => {
    const body = Buffer.from(JSON.stringify(snapshot));
    if (body.length > MAX_SNAPSHOT_BYTES) throw new Error(`Snapshot exceeds bounded export size: ${snapshot.subreddit}`);
    const bytes = gzipSync(body, { level: 9 });
    const key = `snapshots/${snapshot.date}/${snapshot.subreddit}/${snapshot.source.run}-${snapshot.source.revision}.json.gz`;
    return { bytes, entry: { subreddit: snapshot.subreddit, date: snapshot.date, source: snapshot.source,
      capturedAt: snapshot.capturedAt, coverage: snapshot.coverage, key, sha256: sha256(bytes), bytes: bytes.length,
      storedAt: new Date(now).toISOString(), expiresAt: new Date(now + LEASE_MS).toISOString() } };
  });
}

export function replaceSourceEntries(catalog, entries) {
  verifyCatalog(catalog);
  const prefixes = new Set(entries.map(archivePrefix));
  const next = [...catalog.entries.filter(entry => !prefixes.has(archivePrefix(entry))), ...entries];
  const active = new Set(next.map(entry => entry.key));
  const retired = catalog.entries.filter(entry => !active.has(entry.key)).map(entry => entry.key);
  const pending = [...new Set([...(catalog.pendingDeletionKeys || []), ...retired])];
  return verifyCatalog({ ...catalog, entries: next, ...(pending.length ? { pendingDeletionKeys: pending } : {}) });
}

export function replaceSource(catalog, prepared) {
  return replaceSourceEntries(catalog, prepared.map(item => item.entry));
}

/** Lease renewal is permitted only after rereading the authoritative manifest. */
export function renewSource(entries, manifestBytes, now = Date.now()) {
  const manifest = JSON.parse(manifestBytes);
  const revision = sha256(manifestBytes);
  if (!entries.length || entries.some(entry => archivePrefix(entry) !== manifest.objectPrefix || entry.source.revision !== revision)) return null;
  return entries.map(entry => ({ ...entry, expiresAt: new Date(now + LEASE_MS).toISOString() }));
}

/** Expired derived objects can be rebuilt only from the verified source archive. */
export async function refreshSourceExports(entries, manifestBytes, load, read, now = Date.now(), { rebuildExisting = false } = {}) {
  const rebuild = async () => {
    const snapshots = await load(), prefix = archivePrefix(entries[0]), revision = sha256(manifestBytes);
    if (!snapshots.length || snapshots.some(snapshot => archivePrefix(snapshot) !== prefix || snapshot.source.revision !== revision)) throw new Error("Rebuilt snapshots failed source verification");
    const prepared = prepareSnapshots(snapshots, now);
    return { entries: prepared.map(item => item.entry), prepared };
  };
  if (rebuildExisting) return rebuild();
  const renewed = renewSource(entries, manifestBytes, now);
  if (!renewed) return rebuild();
  const prepared = [];
  const aged = renewed.filter(entry => !(Date.parse(entry.storedAt) > now - RETOUCH_MS));
  for (let start = 0; start < aged.length; start += 4) {
    const batch = aged.slice(start, start + 4);
    // Settle every read before rebuilding or cleaning the publication scratch directory.
    const results = await Promise.allSettled(batch.map(entry => read(entry)));
    const failed = results.find(result => result.status === "rejected");
    if (failed) throw failed.reason;
    let missing = false;
    for (let index = 0; index < batch.length; index++) {
      const bytes = results[index].value, entry = batch[index];
      if (bytes === null) { missing = true; continue; }
      if (bytes.length !== entry.bytes || sha256(bytes) !== entry.sha256) throw new Error(`Existing derivative failed checksum verification: ${entry.key}`);
    }
    if (missing) return rebuild();
    for (let index = 0; index < batch.length; index++) {
      const entry = batch[index];
      entry.storedAt = new Date(now).toISOString();
      prepared.push({ entry, bytes: results[index].value });
    }
  }
  return { entries: renewed, prepared };
}

/** Catalog is committed last; interrupted object uploads cannot switch readers. */
export async function publishPrepared(catalog, prepared, put, verify, { remove, readCatalog } = {}) {
  verifyCatalog(catalog);
  const pending = catalog.pendingDeletionKeys || [];
  if (remove && pending.length && !readCatalog) throw new Error("Deletion requires catalog readback verification");
  const encode = value => {
    const bytes = Buffer.from(JSON.stringify(verifyCatalog(value)));
    if (bytes.length > MAX_CATALOG_BYTES) throw new Error("Catalog exceeds serving limit");
    return bytes;
  };
  const catalogBytes = encode(catalog);
  for (let start = 0; start < prepared.length; start += 4) {
    const results = await Promise.allSettled(prepared.slice(start, start + 4).map(item => put(EXPORT_PREFIX + item.entry.key, item.bytes)));
    const failed = results.find(result => result.status === "rejected");
    if (failed) throw failed.reason;
  }
  if (prepared.length) await verify(prepared.find(item => item.entry.subreddit === "AI_Agents") || prepared[0]);
  await put(EXPORT_PREFIX + "index.json", catalogBytes);
  if (!remove || !pending.length) return catalog;
  const receipt = await readCatalog();
  if (!receipt || !Buffer.from(receipt).equals(catalogBytes)) throw new Error("Published deletion journal readback failed");
  for (let start = 0; start < pending.length; start += 4) {
    const results = await Promise.allSettled(pending.slice(start, start + 4).map(key => remove(EXPORT_PREFIX + key)));
    const failed = results.find(result => result.status === "rejected");
    if (failed) throw failed.reason;
  }
  const clean = { ...catalog };
  delete clean.pendingDeletionKeys;
  const cleanBytes = encode(clean);
  await put(EXPORT_PREFIX + "index.json", cleanBytes);
  const finalReceipt = await readCatalog();
  if (!finalReceipt || !Buffer.from(finalReceipt).equals(cleanBytes)) throw new Error("Deletion completion readback failed");
  return clean;
}
