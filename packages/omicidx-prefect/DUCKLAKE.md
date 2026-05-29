# DuckLake conventions (OmicIDX)

Standards adopted for landing OmicIDX data into the shared DuckLake
catalog. These emerged while building the `ducklake-load` flow and are
meant to be portable to other projects writing to the same lake (CMGD,
BugSigDB, ontologies — see omicidx#64). The general/cross-project subset
should be mirrored into `monode/infrastructure`.

## Catalog topology

- **Catalog metadata:** Postgres database `lake` (role `postgres`), host
  `pg_main` inside the docker network (`127.0.0.1` from the host),
  port 5432. Metadata tables are `ducklake_*` in the `public` schema.
- **Data files:** `r2://cdsci-lake/`. This bucket is **ducklake-only** —
  no raw extracts, consolidated parquet, or public exports ever land
  here. Raw inputs are read from `PUBLISH_ROOT` (`omicidx-test`) and
  public exports (if any) are written there, never to `cdsci-lake`.
- **Attach name:** `lake`. Connect with `ATTACH 'ducklake:lake'` once the
  `lake` secret exists, then reference `lake.<schema>.<table>`.
- **Schemas:** `omicidx` (production) per omicidx#64; siblings `cmgd`,
  `bugsigdb`, `ontologies`. New work lands in `<schema>_dev` first and is
  promoted to the production schema once validated.

## DuckDB / catalog version

The catalog uses the **1.5.x DuckLake format**. duckdb **>= 1.5.3**
(latest PyPI stable) is required; 1.4.x refuses to attach (`Only DuckLake
versions 0.1, 0.2, 0.3-dev1 and 0.3 are supported`). Pin it in
`pyproject.toml` and keep workers in sync with the catalog version.

## Connection / secrets

`get_ducklake_connection()` (in `config.py`) builds three **TEMPORARY**
secrets from env (`DUCKLAKE_URI`, `DUCKLAKE_DATA_PATH`, R2 creds) and
attaches:

- `r2` (type r2) — data access for `r2://`
- `pg_main` (type postgres) — catalog metadata store (db `lake`)
- `lake` (type ducklake) — `METADATA_PARAMETERS MAP {'TYPE':'postgres','SECRET':'pg_main'}`, `DATA_PATH 'r2://cdsci-lake/'`

Notes:
- The catalog's **stored** `data_path` governs reads/writes on attach;
  the `DATA_PATH` option only matters at first-time init.
- In a fresh worker (no persisted secrets) plain `CREATE OR REPLACE
  SECRET` makes a session secret — fine. On a dev box that *also* has
  persisted `lake`/`pg_main`/`r2` secrets, the two can collide
  (`Ambiguity detected for secret name ...`). For local validation,
  attach via the persisted secret (`ATTACH 'ducklake:lake'`) instead of
  rebuilding them.

## Merge strategy (per entity)

All loaders MERGE a **deduped, typed projection of raw** into the lake by
natural key. There is no intermediate consolidated parquet — raw is the
rebuildable backstop, lake snapshots provide history.

- **Source** must yield **one row per key** (MERGE rejects multiple
  source matches): `QUALIFY row_number() OVER (PARTITION BY <key> ORDER
  BY <recency>) = 1`, null/empty keys filtered.
- **Change gate:** every row carries `_row_hash = md5(to_json({...all
  non-key payload columns...}))`. MERGE updates only when
  `tgt._row_hash <> src._row_hash`, so unchanged rows never rewrite a
  data file (DuckLake is copy-on-write) and a re-run produces no
  snapshot.
- **Shape:** `WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE
  SET ...; WHEN NOT MATCHED THEN INSERT *`.
- **Native nested types are preserved** in the lake (`struct[]`,
  `varchar[]`, `timestamp`, `date`) — do **not** flatten to JSON.

Incremental vs full-snapshot — "merge on incrementals where possible":

| Source shape | Strategy |
|---|---|
| Raw hive-partitioned by date (SRA) | **High-water-mark**: scope source to `date >= <stored watermark>` (inclusive — boundary re-read is a hash-gated no-op), advance to `max(date)` after merge. |
| Flat full dump (bioproject, biosample) | **Full-snapshot** MERGE; hash gate keeps writes incremental. |
| Partitioned NDJSON, no clean date scope (GEO) | **Full-snapshot** from raw NDJSON globs. |
| Flat files, cross-file key revisions (PubMed) | **Full-snapshot** by pmid; deletes (`delete IS TRUE`) removed via a separate labeled `DELETE`. |

High-water marks are stored as semaphore files under namespace
`ducklake/<entity>` (key `latest`) — backfill = clear the watermark and
re-run with `force=True`.

## Commit metadata (self-documenting snapshots)

Stamp every write so `SELECT * FROM lake.snapshots()` is an audit log:

```sql
BEGIN TRANSACTION;
CALL ducklake_set_commit_message('lake', <author>, <message>, extra_info := <json>);
MERGE ... ;            -- or DELETE
COMMIT;
```

- The stamp **must share the DML transaction** — DuckLake clears it on
  commit, so auto-committed statements lose it.
- Conventions: `author = 'prefect:ducklake-load'`,
  `message = 'ducklake-load: <entity> → <schema>'`,
  `extra_info` = JSON `{prefect_run_id, entity, source, ...}`.
- A no-op MERGE writes **no** snapshot, so the stamp simply doesn't land
  when nothing changed.

## Maintenance (retention + compaction)

`DROP TABLE` / rewrites only unlink in the catalog — reclaiming R2 needs
both calls:

```sql
CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 30 DAY);
CALL ducklake_cleanup_old_files('lake', cleanup_all => true);
CALL ducklake_merge_adjacent_files('lake');   -- compaction
```

- Retention: ~30 days for incremental tables; near-zero for
  full-snapshot tables (no useful history between daily snapshots).
- Run as a scheduled (weekly) maintenance flow. Any ad-hoc DROP must be
  followed by expire + cleanup.

## SQL gotchas

- Reserved words: `references`, `rows` must be quoted (`"references"`),
  and cannot be unquoted struct keys — use `{'references': "references"}`.
- `read_ndjson_auto(..., union_by_name = true)` is required for
  hive-partitioned NDJSON globs where early partitions are empty;
  otherwise DuckDB infers a single `json` column.
- Validation that bounds a source with `LIMIT N` must add `ORDER BY
  <key>` — a `LIMIT` view is re-evaluated per MERGE pass and would
  otherwise return different rows, breaking idempotency checks.
- `flow_run.get_id()` returns `None` outside a flow context (valid JSON,
  just `null`).
