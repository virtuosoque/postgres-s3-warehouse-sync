"""Incremental extract using a (updated_at, id) watermark tuple.

Window:
    lo  = last_watermark - overlap_seconds      # catches commit-lag rows
    hi  = now             - safety_gap_seconds  # avoids in-flight txns

Re-running the same window is safe -- the downstream MERGE is idempotent.
"""

from datetime import datetime, timedelta, timezone

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.common.s3 import put_sse_args, s3_client
from viamedia_pipeline.common.settings import get_settings
from viamedia_pipeline.extract.copy_worker import _coerce_for_arrow
from viamedia_pipeline.state.watermarks import Watermark, get_watermark, set_watermark

log = get_logger(__name__)


def run_incremental(
    conn_cfg: Connection,
    schema: str,
    table: str,
    arrow_schema: pa.Schema,
    s3_prefix: str,
    *,
    pg_oids: list[int] | None = None,
    now: datetime | None = None,
) -> tuple[str | None, Watermark]:
    """Pull rows in the watermark window, write one parquet object, return
    (s3_uri | None, new_watermark). The caller is responsible for MERGE.

    The watermark is NOT advanced here; the orchestrator advances it only
    after a successful MERGE commit -- this is what makes failures safe.
    """
    s = get_settings()
    fqn = f"{schema}.{table}"
    now = now or datetime.now(timezone.utc)

    wm = get_watermark(conn_cfg.id, fqn)
    lo_ts = wm.ts - timedelta(seconds=s.incremental_overlap_seconds)
    hi_ts = now - timedelta(seconds=s.incremental_safety_gap_seconds)
    if hi_ts <= lo_ts:
        log.info("incremental.skip.no_window", table=fqn, lo=lo_ts.isoformat(), hi=hi_ts.isoformat())
        return None, wm

    obj_key = f"{conn_cfg.raw_prefix}/{s3_prefix}/window_{int(now.timestamp())}.parquet"
    s3_uri = f"s3://{conn_cfg.lake_bucket}/{obj_key}"

    copy_stmt = sql.SQL(
        """
        COPY (
            SELECT * FROM {tbl}
            WHERE (updated_at, id) > ({lo_ts}, {lo_id})
              AND updated_at < {hi_ts}
            ORDER BY updated_at, id
        ) TO STDOUT (FORMAT BINARY)
        """
    ).format(
        tbl=sql.Identifier(schema, table),
        lo_ts=sql.Literal(wm.ts),
        lo_id=sql.Literal(wm.last_id),
        hi_ts=sql.Literal(hi_ts),
    )

    rows_seen = 0
    max_ts: datetime = wm.ts
    max_id: int = wm.last_id

    import tempfile

    with tempfile.SpooledTemporaryFile(max_size=64 * 1024**2, mode="w+b") as spool:
        writer = pq.ParquetWriter(
            spool,
            arrow_schema,
            compression=s.parquet_compression,
            compression_level=s.parquet_compression_level,
            use_dictionary=True,
            write_statistics=True,
        )
        conn = dedicated_connection(conn_cfg)
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                with cur.copy(copy_stmt) as copy:
                    if pg_oids:
                        copy.set_types(pg_oids)
                    batch: list[tuple] = []
                    ts_idx = arrow_schema.get_field_index("updated_at")
                    id_idx = arrow_schema.get_field_index("id")
                    for row in copy.rows():
                        batch.append(row)
                        rows_seen += 1
                        # advance running max -- rows already ORDER BY (updated_at, id)
                        rts = row[ts_idx]
                        rid = row[id_idx]
                        if rts is not None and (rts, rid) > (max_ts, max_id):
                            max_ts, max_id = rts, rid
                        if len(batch) >= s.extract_row_group_rows:
                            _flush(writer, arrow_schema, batch)
                            batch.clear()
                    if batch:
                        _flush(writer, arrow_schema, batch)
            conn.rollback()
        finally:
            writer.close()
            conn.close()

        if rows_seen == 0:
            log.info("incremental.empty_window", table=fqn)
            # Still advance the watermark forward to hi_ts to avoid re-scanning
            # the same empty window forever. last_id stays the same.
            return None, Watermark(ts=hi_ts, last_id=wm.last_id)

        spool.flush()
        spool.seek(0)
        s3_client(conn_cfg).upload_fileobj(
            spool, conn_cfg.lake_bucket, obj_key, ExtraArgs=put_sse_args()
        )

    log.info(
        "incremental.window.done",
        table=fqn,
        rows=rows_seen,
        max_ts=max_ts.isoformat(),
        max_id=max_id,
        s3_uri=s3_uri,
    )
    return s3_uri, Watermark(ts=max_ts, last_id=max_id)


def _flush(writer: pq.ParquetWriter, schema: pa.Schema, rows: list[tuple]) -> None:
    columns = list(zip(*rows, strict=True))
    arrays = [
        pa.array(_coerce_for_arrow(col, schema.field(i).type), type=schema.field(i).type)
        for i, col in enumerate(columns)
    ]
    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))


def advance_watermark_after_merge(connection_id: int, table_fqn: str, wm: Watermark) -> None:
    """Call AFTER a successful Iceberg MERGE. Uses a conditional write so
    concurrent runs can't move the watermark backwards."""
    set_watermark(connection_id, table_fqn, wm)
    log.info("watermark.advanced", connection_id=connection_id, table=table_fqn,
             ts=wm.ts.isoformat(), last_id=wm.last_id)
