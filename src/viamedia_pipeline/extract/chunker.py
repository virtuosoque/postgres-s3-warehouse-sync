"""Chunk a source object into id-range slices for parallel COPY workers.

For ordinary tables and materialized views with a numeric `id` we use
MIN/MAX + reltuples to estimate chunks. For views or objects without a
numeric monotonic key, we fall back to one single chunk that COPYs the
whole object -- no parallelism, but correct.
"""

from dataclasses import dataclass

import psycopg

from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.settings import get_settings

log = get_logger(__name__)


@dataclass(frozen=True)
class Chunk:
    schema: str
    table: str
    chunk_id: int
    lo: int | None = None     # None = no lower bound (full-object chunk)
    hi: int | None = None     # None = no upper bound
    # Day partitioning: when set, the chunk is scoped to a SINGLE calendar day of
    # `part_col` so every parquet file it produces has one partition value (a hard
    # requirement of Iceberg `add_files`). `day` is an ISO date 'YYYY-MM-DD';
    # `part_tz` True means `part_col` is timestamptz, so day boundaries are UTC.
    # `day_is_null` selects the rows whose `part_col` IS NULL (Iceberg null
    # partition). See plan_partitioned_chunks.
    part_col: str | None = None
    day: str | None = None
    part_tz: bool = False
    day_is_null: bool = False

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def is_full(self) -> bool:
        """No predicate at all: whole object, one COPY (views / no id / no day)."""
        return (
            self.lo is None and self.hi is None
            and self.day is None and not self.day_is_null
        )


def has_leading_index(conn: psycopg.Connection, schema: str, table: str, column: str) -> bool:
    """True if `column` is the LEADING column of some index on the table. Range
    chunking by an unindexed column forces a full scan per chunk, so we only
    parallelize when the key is indexed (otherwise a single streaming COPY)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM   pg_index i
            JOIN   pg_class c     ON c.oid = i.indrelid
            JOIN   pg_namespace n ON n.oid = c.relnamespace
            JOIN   pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
            WHERE  n.nspname = %s AND c.relname = %s AND a.attname = %s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


def plan_range_chunks(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    rows_per_chunk: int | None = None,
) -> list[Chunk]:
    """Numeric id-range chunking. Use only when the object has a numeric `id`."""
    target = rows_per_chunk or get_settings().extract_rows_per_chunk
    with conn.cursor() as cur:
        cur.execute(f'SELECT min(id), max(id) FROM "{schema}"."{table}"')  # noqa: S608
        row = cur.fetchone()
        if isinstance(row, dict):
            lo, hi = row.get("min"), row.get("max")
        else:
            lo, hi = (row[0], row[1]) if row else (None, None)

        cur.execute(
            "SELECT reltuples::bigint AS r FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema, table),
        )
        rt = cur.fetchone()
        approx_rows = (rt["r"] if isinstance(rt, dict) else rt[0]) if rt else 0

    if lo is None or hi is None:
        log.warning("chunker.empty_table", table=f"{schema}.{table}")
        return []

    n_chunks = max(1, approx_rows // target)
    span = hi - lo + 1
    step = max(1, span // n_chunks)
    chunks: list[Chunk] = []
    cur_lo = lo
    cid = 0
    while cur_lo <= hi:
        nxt = cur_lo + step
        chunks.append(
            Chunk(
                schema=schema, table=table, chunk_id=cid,
                lo=cur_lo, hi=nxt if nxt <= hi else hi + 1,
            )
        )
        cur_lo = nxt
        cid += 1
    log.info(
        "chunker.planned.range",
        table=f"{schema}.{table}",
        approx_rows=approx_rows, chunks=len(chunks), step=step,
    )
    return chunks


def _day_expr(part_col: str, part_tz: bool) -> str:
    """SQL expression yielding the Iceberg `day(part_col)` calendar date.

    Iceberg's DayTransform buckets a `timestamptz` by its UTC date and a plain
    `timestamp` by its literal date. We MUST group the source identically or a
    parquet file would straddle two Iceberg partition values and `add_files`
    would reject it. For timestamptz we therefore convert to UTC first.
    """
    col = f'"{part_col}"'
    return f"({col} AT TIME ZONE 'UTC')::date" if part_tz else f"{col}::date"


def plan_partitioned_chunks(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    part_col: str,
    *,
    part_tz: bool,
    has_numeric_id: bool,
    rows_per_chunk: int | None = None,
) -> list[Chunk]:
    """Chunk so that every chunk (and thus every parquet file) belongs to exactly
    ONE Iceberg day partition of `part_col`.

    One GROUP BY pass collects, per UTC/literal day, the row count and (when the
    table has a numeric indexed `id`) the id min/max. Each day is then split into
    id-range sub-chunks sized to `rows_per_chunk` so big days still parallelize;
    days with no numeric id become a single per-day COPY. Rows with a NULL
    partition value get their own chunk (Iceberg's null partition).

    The resulting COPY predicate per chunk is exactly the user's intended shape:
        WHERE created_at >= 'D' AND created_at < 'D'+1 AND id >= lo AND id < hi
    (UTC-anchored literals when `part_col` is timestamptz).
    """
    target = rows_per_chunk or get_settings().extract_rows_per_chunk
    day_sql = _day_expr(part_col, part_tz)
    with conn.cursor() as cur:
        if has_numeric_id:
            cur.execute(  # noqa: S608 - identifiers are quoted; day_sql is built locally
                f'SELECT {day_sql} AS d, count(*) AS n, min(id) AS lo, max(id) AS hi '
                f'FROM "{schema}"."{table}" GROUP BY 1 ORDER BY 1'
            )
        else:
            cur.execute(  # noqa: S608
                f'SELECT {day_sql} AS d, count(*) AS n '
                f'FROM "{schema}"."{table}" GROUP BY 1 ORDER BY 1'
            )
        rows = cur.fetchall()

    def _get(r, key, idx):
        return r[key] if isinstance(r, dict) else r[idx]

    chunks: list[Chunk] = []
    cid = 0
    for r in rows:
        d = _get(r, "d", 0)
        n = _get(r, "n", 1) or 0
        day_iso = d.isoformat() if d is not None else None
        is_null = d is None
        if not has_numeric_id or is_null:
            # One COPY for the whole day (or the null-partition group).
            chunks.append(Chunk(
                schema=schema, table=table, chunk_id=cid,
                part_col=part_col, day=day_iso, part_tz=part_tz, day_is_null=is_null,
            ))
            cid += 1
            continue
        lo = _get(r, "lo", 2)
        hi = _get(r, "hi", 3)
        if lo is None or hi is None:
            continue
        n_sub = max(1, n // target)
        span = hi - lo + 1
        step = max(1, span // n_sub)
        cur_lo = lo
        while cur_lo <= hi:
            nxt = cur_lo + step
            chunks.append(Chunk(
                schema=schema, table=table, chunk_id=cid,
                lo=cur_lo, hi=nxt if nxt <= hi else hi + 1,
                part_col=part_col, day=day_iso, part_tz=part_tz,
            ))
            cur_lo = nxt
            cid += 1
    log.info(
        "chunker.planned.partitioned",
        table=f"{schema}.{table}", part_col=part_col, part_tz=part_tz,
        days=len(rows), chunks=len(chunks),
    )
    return chunks


def plan_full_chunk(schema: str, table: str) -> list[Chunk]:
    """Single-chunk plan: COPY everything in one stream, no WHERE filter.
    Used for views and objects without a numeric monotonic id.
    """
    log.info("chunker.planned.full", table=f"{schema}.{table}")
    return [Chunk(schema=schema, table=table, chunk_id=0, lo=None, hi=None)]


# Backward-compatible alias -- callers that don't yet know about views still
# work; they get range chunking when possible.
def plan_chunks(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    rows_per_chunk: int | None = None,
) -> list[Chunk]:
    return plan_range_chunks(conn, schema, table, rows_per_chunk)
