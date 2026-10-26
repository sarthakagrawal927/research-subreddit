import { isAbsolute } from "node:path";
import { EXPORT_PREFIX, validDeletionKey } from "./publication.mjs";

const VERSION = "4.120.0";

/** Resolve the pinned provider CLI once, rather than starting npm for every object. */
export function createWranglerRunner(exec, cwd) {
  const options = { cwd, maxBuffer: 1024 * 1024 };
  let executable;
  return async args => {
    executable ??= (async () => {
      const result = await exec("npx", ["--yes", `--package=wrangler@${VERSION}`, "--call", "command -v wrangler"], options);
      const path = result.stdout.trim();
      if (!isAbsolute(path) || /[\r\n]/.test(path)) throw new Error("Invalid pinned Wrangler executable");
      const version = await exec(path, ["--version"], options);
      if (version.stdout.trim() !== VERSION) throw new Error("Pinned Wrangler version mismatch");
      return path;
    })();
    return exec(await executable, args, options);
  };
}


export function createRevocationHooks(enabled, { bucket, run, readCatalog }) {
  if (!enabled) return {};
  if (bucket !== "reddit-insights-archive") throw new Error("Cleanup is scoped to the derivative bucket");
  return {
    readCatalog,
    remove: async key => {
      if (!key.startsWith(EXPORT_PREFIX) || !validDeletionKey(key.slice(EXPORT_PREFIX.length))) throw new Error("Cleanup requires a derivative export key");
      try { await run(["r2", "object", "delete", `${bucket}/${key}`, "--remote"]); }
      catch (error) {
        const message = [error.message, error.stderr, error.stdout].filter(Boolean).join(" ");
        if (/(?:NoSuchKey|\b10007\b)/.test(message) && !/10042|authentication|permission/i.test(message)) return;
        throw error;
      }
    },
  };
}
