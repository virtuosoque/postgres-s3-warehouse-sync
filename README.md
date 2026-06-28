# viamedia-data-pipeline

PostgreSQL → S3 (Apache Iceberg) → DuckDB query gateway → PowerBI.

Low-cost, AWS-light deployment. No ECS, no Glue, no DynamoDB, no KMS.
Single EC2 host running docker-compose; PostgreSQL holds the Iceberg catalog
and the pipeline's operational state; S3 buckets use SSE-S3 encryption.

---

## Architecture

```
                                ┌──────────────────────────────────────┐
                                │              EC2 host                │
                                │ (m7g.xlarge default; optional Spot)  │
                                │ ─────────────────────────────────── │
                                │ docker-compose:                      │
                                │   metadata-postgres  (catalog+state) │
                                │   extractor (Dagster code location)  │
                                │   dagster (UI + scheduler)           │
                                │   gateway (FastAPI + DuckDB)         │
                                │   caddy (TLS termination)            │
                                │                                      │
                                │ EBS gp3 mounted at /data:            │
                                │   - metadata-postgres data           │
                                │   - caddy state                      │
                                └────────┬──────────────────┬──────────┘
                                         │                  │
                ┌────────────────────────┘                  │ HTTPS
                ▼                                           ▼
   ┌──────────────────────┐                        ┌───────────────┐
   │ Source Postgres      │                        │   PowerBI     │
   │ (RDS read replica;   │                        │ (custom .pq   │
   │  external to this    │                        │   connector)  │
   │  stack)              │                        └───────────────┘
   └──────────────────────┘
                                         ▲
                                         │ COPY BINARY
                                         │ + snapshot
                                         ▼
                                 ┌─────────────────────────────────┐
                                 │              S3                  │
                                 │ viamedia-lake-raw       (TTL 7d) │
                                 │ viamedia-lake-curated   (Iceberg)│
                                 │ viamedia-lake-gateway-results    │
                                 │ viamedia-lake-compute-logs       │
                                 │ SSE-S3 (AES256). No KMS.         │
                                 └─────────────────────────────────┘
```

## What was removed (cost-cut version)

| Removed | Replaced by | Why |
|---|---|---|
| ECS / Fargate | Docker on a single EC2 | One m7g.xlarge < $110/mo (~$35/mo on Spot) |
| AWS Glue Data Catalog | PyIceberg **SqlCatalog** in metadata Postgres | Glue catalog cost = a few $; PG catalog = $0 marginal |
| DynamoDB | Same metadata Postgres (`pipeline_watermarks`, `pipeline_chunk_state`) | $0 marginal; one fewer service to monitor |
| KMS CMK | Bucket-default SSE-S3 (AES256) | Drops $1/mo per key + per-request charges. Data still encrypted at rest. |
| ALB (optional) | Caddy in-cluster for TLS, or an ALB if you want one | Save ~$22/mo if you can live with single-host failover |

**Steady-state monthly cost (lake-only, 100M rows replicated, 5-min incremental, light BI):**

| Item | $/mo |
|---|---|
| EC2 m7g.xlarge on-demand | 110 |
| 200 GB gp3 EBS | 16 |
| S3 storage (curated, ~40 GB) | 1 |
| S3 requests + egress (light) | 5–15 |
| Secrets Manager (1 secret) | 0.40 |
| CloudWatch logs | 3–10 |
| **Total** | **~$135–155** |

Spot variant brings the EC2 line to ~$35/mo (~$60/mo total).

## Components

| Path | Purpose |
|---|---|
| `common/settings.py` | Pydantic env config |
| `common/metadata_db.py` | psycopg connection pool for metadata Postgres |
| `common/migrations.py` | Idempotent DDL bootstrap (`viamedia-migrate` console script) |
| `common/pg.py` | Source PG pool + snapshot-capable dedicated connections |
| `common/s3.py` | boto3 client + `put_sse_args()` (AES256) |
| `extract/snapshot.py` | `pg_export_snapshot` lifecycle |
| `extract/chunker.py` | id-range planning for parallel COPY |
| `extract/copy_worker.py` | `COPY BINARY` → PyArrow → parquet → S3 |
| `extract/schema.py` | PG → Arrow → Iceberg type mapping |
| `extract/incremental.py` | (updated_at, id) watermark window |
| `state/watermarks.py` | **Postgres**-backed watermark with forward-only upsert |
| `state/chunks.py` | **Postgres**-backed chunk state for resumable bootstrap |
| `load/iceberg_writer.py` | PyIceberg **SqlCatalog** on Postgres |
| `load/merge.py` | Idempotent `upsert` |
| `load/maintenance.py` | Compaction + snapshot expiry |
| `gateway/app.py` | FastAPI endpoints |
| `gateway/duckdb_pool.py` | Pre-warmed DuckDB connections, iceberg/aws/httpfs loaded |
| `gateway/view_refresher.py` | Polls PG catalog, refreshes DuckDB views every N seconds |
| `gateway/sql_guard.py` | sqlglot SELECT-only / schema-allowlist enforcement |
| `gateway/auth.py` | OIDC/JWT verification via remote JWKS |
| `gateway/query.py` | Async submit → result parquet on S3 → presigned URL |
| `DEPLOY.md` | Manual EC2 deploy: IAM role policy, systemd unit, prod `.env` |

## How catalog reads work in the gateway

1. Pipeline writes parquet + commits to Iceberg via PyIceberg `SqlCatalog`,
   whose metadata lives in `metadata-postgres`.
2. Gateway runs a background `ViewRefresher` thread that every 60 s calls
   `catalog.list_tables(ns)` → `catalog.load_table().metadata_location` for
   each table.
3. For every DuckDB connection in the pool, it issues
   `CREATE OR REPLACE VIEW viamedia_lake.<name> AS SELECT * FROM
   iceberg_scan('s3://.../metadata/<version>.metadata.json')`.
4. PowerBI users query the views — DuckDB pushes predicates / projections
   down to S3 parquet directly.

Stale-read window equals the refresh interval (60 s default). Tune via
`GATEWAY_VIEW_REFRESH_SECONDS`.

## Local development

```bash
make install                       # editable install with dev + gateway extras
make dev-up                        # spins up source-pg + metadata-pg + LocalStack + Dagster + Gateway
# Dagster UI:   http://localhost:3000
# Gateway docs: http://localhost:8080/docs
make test                          # unit tests (watermark tests need pytest-postgresql)
```

Source Postgres is seeded with `public.events` (100k rows) and `public.users`
(10k rows). The `migrate` service runs metadata DDL on first boot.

## Unified web UI + UI-managed config (multi-database)

Configuration now lives in the metadata DB and is managed from a single web UI
(the gateway app), not `.env`. Open **http://localhost:3000** — tabs:

- **Connections** — one row per source database, each with its **own S3 bucket,
  Iceberg namespace, and AWS credentials**. Secrets (source DSN, AWS keys) are
  Fernet-encrypted at rest (key: `CONFIG_ENCRYPTION_KEY`). "Test DSN" validates
  before saving.
- **Sync** — pick a connection, then multi-select the live tables / views /
  materialized views to sync (unchecking = stop syncing; non-destructive).
  Buttons trigger bootstrap / incremental runs (the gateway calls Dagster's API).
- **Query** — DuckDB-over-Iceberg console; tables grouped by connection namespace.
- **Settings** — operational knobs (extractor/parquet/incremental tuning, gateway
  params) stored in `pipeline_settings`; changes apply within ~30s, no restart.
- **Runs** — status of runs launched from the UI.

**Single bucket per database**: each connection uses one `lake_bucket` with fixed
prefixes — `raw/` (extracts), `curated/` (Iceberg warehouse), `gateway-results/`
(query outputs). **Multiple databases** each sync into their own bucket + namespace;
watermarks/chunk-state are keyed by `connection_id`.

Only bootstrap values stay in the environment: `METADATA_PG_DSN`,
`METADATA_PG_SCHEMA`, `CONFIG_ENCRYPTION_KEY`, `LOG_LEVEL` (everything else is in
the UI). On first start, if no connections exist but legacy `.env` values are
present, a `default` connection is seeded so an existing single-DB setup keeps
working.

## Local run against real AWS (RDS source + S3 target)

To test extraction from a real RDS read replica into real S3 buckets while
still running the stack on your laptop:

```bash
cp .env.localaws.example .env.localaws   # fill in RDS DSN, AWS creds, bucket names, SYNC_INCLUDE
make localaws-up                          # metadata-pg (local) + Dagster + Gateway, no LocalStack
make localaws-logs
```

This uses [docker-compose.localaws.yml](docker-compose.localaws.yml): no
LocalStack and no endpoint/credential overrides, so boto3 / PyIceberg / DuckDB
talk to real AWS using the credentials in `.env.localaws`. Buckets must already
exist and your Mac must be able to reach the RDS endpoint.

## Production deployment (EC2, manual)

See [DEPLOY.md](DEPLOY.md) for the full manual deploy: EC2 host setup, the IAM
instance-role policy, S3 buckets, the prod `.env`, and the systemd unit that
runs [compose.prod.yml](compose.prod.yml). Build/push images with `make push`
(set `REGISTRY` + `ENV`) or build them directly on the host. Then bootstrap each
priority table via the Dagster UI and enable the `every_five_minutes_per_table`
schedule.

## Selecting what to sync

The pipeline auto-discovers every table, view, and materialized view in the
source DB via `pg_class`. Use these env vars to narrow it down:

| Env var | Default | Meaning |
|---|---|---|
| `SYNC_INCLUDE` | empty (= all) | Comma-separated globs matched against `schema.name`. Object kept if it matches any glob. |
| `SYNC_EXCLUDE` | empty (= none) | Comma-separated globs. Applied AFTER include. Any match removes the object. |
| `SYNC_OBJECT_TYPES` | `TABLE,VIEW,MATERIALIZED_VIEW` | Subset of these three. `TABLE` covers regular and partitioned tables. |
| `SYNC_SCHEMA_BLACKLIST` | `pg_catalog,information_schema,pg_toast` | Schemas to never scan. |

Examples:

```bash
# Only events + everything in analytics; skip audit log and any _archive_* table
SYNC_INCLUDE=public.events,analytics.*
SYNC_EXCLUDE=public.audit_log,*._archive_*

# Tables only, no views
SYNC_OBJECT_TYPES=TABLE

# Default: sync everything
# (SYNC_INCLUDE empty + SYNC_EXCLUDE empty)
```

Discovery is refreshed every 5 minutes (TTL in `orchestration/tables.py`);
schedule ticks pick up new objects automatically. The
`discover_new_tables_sensor` (in Dagster UI → Sensors) auto-launches a
bootstrap run for any object that's in the discovery set but doesn't yet
have an Iceberg table.

### Object-kind behavior

| Kind | Bootstrap | Incremental |
|---|---|---|
| Numeric-`id` table or MV | Parallel id-range chunks | If `updated_at` exists, every 5 min |
| Non-numeric `id` table | Single COPY (no parallelism) | If `updated_at` exists, every 5 min |
| VIEW with `id` + `updated_at` | Single COPY | Every 5 min via watermark |
| VIEW without `updated_at` | Single COPY | Skipped — bootstrap periodically instead |

### Per-object overrides (optional)

For specific objects you can pin partitioning / PK / watermark by editing
the `OVERRIDES` map in
[orchestration/tables.py](src/viamedia_pipeline/orchestration/tables.py):

```python
OVERRIDES: dict[str, TableOverride] = {
    "public.events": TableOverride(
        partition_by=(("created_at", "day", None), ("id", "bucket", 32)),
    ),
    "audit.snapshots": TableOverride(watermark_column=None),  # force full-only
}
```

Anything not listed gets sensible defaults inferred from its columns.
Rebuild + redeploy the extractor + dagster images to pick up changes here;
include/exclude env vars don't require rebuilds (just restart the
extractor + dagster containers).

## Failure modes & recovery

- **EC2 instance lost** (or Spot reclaim): launch a replacement host and
  re-attach the EBS data volume (see [DEPLOY.md](DEPLOY.md)).
  The EBS data volume persists; on reattach, metadata-postgres comes back
  with the Iceberg catalog and watermarks intact, and the schedule picks up
  where it left off.
- **Metadata Postgres corruption**: take a daily `pg_dump` of the metadata
  DB to S3 via a small cron (todo: bundled). Restoring rebuilds the catalog
  and watermarks; existing parquet in S3 is untouched.
- **Stuck bootstrap**: rerun the job; `pipeline_chunk_state` skips chunks
  already marked `DONE`.
- **Failed incremental**: the watermark is only advanced after a successful
  `upsert`, so a retried tick re-processes the same window safely.

## PowerBI integration

`scripts/viamedia_gateway.pq` — a Power Query M custom connector that posts
SQL to the gateway, polls for completion, and pulls the result parquet via a
signed S3 URL. Paste an OIDC access token at the credential prompt.

## Roadmap

- [ ] `pg_dump` of metadata Postgres to S3 on a cron (DR)
- [ ] Logical replication CDC consumer for hard deletes
- [ ] Multi-host gateway (NLB in front of N gateways; metadata-postgres on RDS)
- [ ] DuckDB Postgres-wire-protocol shim → native PowerBI ODBC


docker exec postgres-s3-warehouse-sync-metadata-postgres-1 \
  psql -U pipeline -d pipeline_metadata -c \
  "INSERT INTO pipeline.pipeline_users (email, role, enabled) VALUES ('prashant.viamedia@gmail.com','admin',true) ON CONFLICT (email) DO UPDATE SET role='admin', enabled=true;" \
  && echo "--- verify ---" \
  && docker exec postgres-s3-warehouse-sync-metadata-postgres-1 psql -U pipeline -d pipeline_metadata -c "SELECT email, role, enabled FROM pipeline.pipeline_users;"