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

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def is_full(self) -> bool:
        return self.lo is None and self.hi is None


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
