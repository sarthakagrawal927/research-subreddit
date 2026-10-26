# Reddit Insights status

## Revoked derivative cleanup — October 26, 2026

Snapshot publication records up to 4,096 revoked derivative keys in the private
catalog. Normal publication preserves that content-free journal without deleting
objects. The `--delete-revoked` CLI fails before remote access: the current
Wrangler transport has no enforced cross-publisher exclusion. Catalog readback
alone cannot prevent a concurrent publisher from reactivating a key before deletion.
The cleanup engine requires an exclusive transport before production integration.
Its bounded checkpoint path drains a full journal before adding new replacements;
reactivated keys are removed from the journal. Without cleanup, overflow retains the first 4,096 keys and an explicit incomplete-
reconciliation marker. Publication and lease renewals continue; the CLI exits
nonzero after publication, and cleanup refuses an incomplete journal.
Unconditional concurrent catalog writes can also lose journal entries: complete
journal capture requires exclusive publication, not just safe deletion.

Synthetic exclusive-storage tests cover redacted source projection, replacement
publication, physical old-export removal, revoked pinned downloads, sanitized
JSON/CSV, failed readback/deletion, retries, and the journal limit. CLI tests prove
that production deletion fails closed. Production deletion is unavailable and the
48-hour deletion acceptance remains unqualified. Previously orphaned derivatives
are not discovered by this journal. Enforced publication exclusion, collector/raw-
archive ownership, historical redaction and authorization remain open in
[#44](https://github.com/sarthakagrawal927/research-subreddit/issues/44).

## Prior deployed snapshot release

Updated October 2, 2026. Tracking: [snapshot feature #21](https://github.com/High-Signal-App/research-subreddit/issues/21).

Snapshot Studio is live at https://reddit-insights.highsignal.app/. At final
release acceptance, the catalog contains 297 verified community/day snapshots:
99 communities across September 30, October 1 and October 2. Availability follows
the live catalog as subsequent collections publish and serving leases renew.
Anonymous lookup, clean and pinned links, captured discussion, insights and
JSON/posts CSV/comments CSV downloads pass public acceptance. The owner
authorized the existing private R2 binding, automated collection/redaction
refresh and seven-day cleanup of obsolete derivative objects.
[Release evidence](docs/release-2026-10-02.md) records source, CI, deployment and
the successful automated refresh.

The local snapshot viewer supports subreddit/collection-date selection,
available-date navigation, recorded posts and retained discussion, search/sort,
revision-pinned share links and JSON/posts CSV/comments CSV downloads.
URLs now use `/r/<subreddit>/<date>/`, with a short version suffix for exact
shares. Legacy query links normalize to that path. The insights disclosure
summarizes captured engagement, post labels and comparable prior post counts;
partial or mismatched collections do not produce daily-change claims. Discussion
scrolling, focus return and appended-post reading positions are preserved.
Its verified importer produces bounded private gzip derivatives; the shared API
enforces 24-hour expiry, catalog revocation and checksum validation.

Local qualification uses 99 communities on each of September 30 and October 1.
October 1 r/AI_Agents reconciles to 71 posts and 350 retained comments;
September 30 reconciles to 96 posts and 468 retained comments. Dates refer to
collection windows, not exact UTC publishing-day censuses.

Archive R2 access is restored and authoritative September 30/October 1 packs
are reverified. The Pages Function passes Cloudflare runtime checks at the
existing compatibility date. The checked-in Pages configuration binds the
private derivative bucket; the serving adapter has no raw archive access.
Publication uploads minimized gzip objects before switching the verified
catalog. Daily collection and redaction refresh source manifests, renew 24-hour
leases and replace changed revisions. Obsolete derivatives expire after seven
days; active exports are retouched after five days. Raw archive storage stays
private and unchanged by publication.

Local raw research files and embeddings were moved reversibly to Trash. Compact
display data and existing reports were preserved; old research need not be rerun
to use the new daily snapshot path.

The owner approved Snapshot Studio (A) after comparing three complete directions.
The homepage and daily reader now use its pale-paper/navy/cobalt Archivo system.
The homepage offers unexpired community/date choices and live captured evidence;
preview titles open the chosen discussion in the pinned collection. Empty,
failed, expired and unavailable collections keep their distinct states.
The local homepage is at http://127.0.0.1:7425/.

Snapshot Studio qualification: 23 snapshot tests, 66 full unit tests, 112 display
artifacts, 26 enrichment checks, Pages export and format/lint/typecheck passed.
Independent review 33/40, audit 16/20, lowest viewport purpose 89/100; no unresolved
P0/P1. Public phone/desktop routes, contained discussion scrolling and close/focus
return passed. Direction evidence and final screenshots remain under gitignored
artifacts/design/.
