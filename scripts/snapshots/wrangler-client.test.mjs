import assert from "node:assert/strict";
import test from "node:test";
import { createWranglerRunner, createRevocationHooks } from "./wrangler-client.mjs";

test("concurrent storage calls share one pinned CLI resolution and version check", async () => {
  const calls = [], binary = "/tmp/pinned-wrangler/bin/wrangler", cwd = "/tmp/publication";
  const run = createWranglerRunner(async (command, args, options) => {
    calls.push({ command, args, options });
    if (command === "npx") return { stdout: `${binary}\n` };
    if (args[0] === "--version") return { stdout: "4.120.0\n" };
    return { stdout: args.join(" ") };
  }, cwd);
  const get = ["r2", "object", "get", "bucket/key", "--remote"];
  const put = ["r2", "object", "put", "bucket/key", "--file", "/tmp/body", "--remote"];
  const results = await Promise.all([run(get), run(put)]);
  assert.equal(calls.filter(call => call.command === "npx").length, 1);
  assert.deepEqual(calls[0].args, ["--yes", "--package=wrangler@4.120.0", "--call", "command -v wrangler"]);
  assert.equal(calls.filter(call => call.args[0] === "--version").length, 1);
  assert.deepEqual(calls.slice(2).map(call => call.args), [get, put]);
  assert.ok(calls.slice(1).every(call => call.command === binary));
  assert.ok(calls.every(call => call.options.cwd === cwd));
  assert.deepEqual(results.map(result => result.stdout), [get.join(" "), put.join(" ")]);
});

test("invalid paths, wrong versions and resolution errors cannot start storage operations", async () => {
  for (const mode of ["relative", "multiline", "version", "unavailable"]) {
    let operations = 0, resolutions = 0;
    const run = createWranglerRunner(async (command, args) => {
      if (command === "npx") {
        resolutions++;
        if (mode === "unavailable") throw new Error("CLI unavailable");
        return { stdout: mode === "relative" ? "wrangler" : mode === "multiline" ? "/tmp/wrangler\nextra" : "/tmp/wrangler" };
      }
      if (args[0] === "--version") return { stdout: "4.119.0\n" };
      operations++;
    }, "/tmp/publication");
    await assert.rejects(run(["r2", "object", "get"]));
    await assert.rejects(run(["r2", "object", "put"]));
    assert.equal(resolutions, 1);
    assert.equal(operations, 0);
  }
});


test("physical cleanup is opt-in and emits only scoped remote object deletion", async () => {
  const key = `reddit-insights/exports/v1/snapshots/2026-10-01/Cloud/12-${"a".repeat(64)}.json.gz`;
  const calls = [], readCatalog = async () => Buffer.from("receipt");
  const options = { bucket: "reddit-insights-archive", run: async args => { calls.push(args); }, readCatalog };
  assert.deepEqual(createRevocationHooks(false, options), {});
  assert.deepEqual(calls, []);
  const hooks = createRevocationHooks(true, options);
  assert.equal(hooks.readCatalog, readCatalog);
  await hooks.remove(key);
  assert.deepEqual(calls, [["r2", "object", "delete", `reddit-insights-archive/${key}`, "--remote"]]);
  assert.throws(() => createRevocationHooks(true, { ...options, bucket: "high-signal-reddit-archive" }), /derivative bucket/);
  for (const bad of ["reddit/v2/raw", "reddit-insights/exports/v1/index.json", "reddit-insights/exports/v1/snapshots/../../raw"]) await assert.rejects(hooks.remove(bad), /derivative export/);
});

test("cleanup retries accept only explicit missing-object errors and preserve authorization failures", async () => {
  const key = `reddit-insights/exports/v1/snapshots/2026-10-01/Cloud/12-${"a".repeat(64)}.json.gz`;
  for (const message of ["NoSuchKey", "Object missing [code: 10007]"]) {
    const hooks = createRevocationHooks(true, { bucket: "reddit-insights-archive", run: async () => { throw new Error(message); } });
    await hooks.remove(key);
  }
  for (const message of ["permission denied 10007", "authentication failed", "10042", "Network error", "bucket not found"]) {
    const hooks = createRevocationHooks(true, { bucket: "reddit-insights-archive", run: async () => { throw new Error(message); } });
    await assert.rejects(hooks.remove(key), error => error.message === message);
  }
});

// The current remote transport cannot exclude another catalog publisher.
test("production cleanup fails closed before provider access", async () => {
  const { spawnSync } = await import("node:child_process");
  const result = spawnSync(process.execPath, ["scripts/publish-snapshots.mjs", "--refresh-only", "--delete-revoked"], { encoding: "utf8", env: { PATH: "" } });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /enforced exclusive publication transport is required/);
});
