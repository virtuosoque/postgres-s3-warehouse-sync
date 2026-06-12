"""Incremental extract using a (watermark_column, id) watermark tuple.

The watermark column may be a timestamp/timestamptz, a date, or an integer
(epoch / sequence). The window is built per type:

    timestamp/date : lo = last - overlap, hi = now - safety_gap  (time-bounded)
    integer        : lo = last, no upper bound (opaque monotonic key)

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
from viamedia_pipeline.state.watermarks import (
    Watermark,
    WatermarkKind,
    get_watermark,
    set_watermark,
)

log = get_logger(__name__)


def _as_utc(dt: datetime | None) -> datetime | None:
    """Normalize a datetime to tz-aware UTC so the watermark (aware UTC) and a
    naive `timestamp` column's values are comparable (we treat naive as UTC)."""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc) if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _watermark_kind(arrow_type: pa.DataType) -> WatermarkKind | None:
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_integer(arrow_type):
        return "integer"
    return None


def run_incremental(
    conn_cfg: Connection,
    schema: str,
    table: str,
    arrow_schema: pa.Schema,
    s3_prefix: str,
    *,
    pg_oids: list[int] | None = None,
    now: datetime | None = None,
    watermark_column: str = "updated_at",
    pk: str = "id",
) -> tuple[str | None, Watermark | None, WatermarkKind | None]:
    """Pull rows in the watermark window, write one parquet object, return
    (s3_uri | None, new_watermark | None, kind | None). The caller MERGEs and
    advances the watermark only on success. kind is None when the watermark
    column's type isn't supported for incremental (caller should skip)."""
    s = get_settings()
    fqn = f"{schema}.{table}"
    now = now or datetime.now(timezone.utc)

    field_idx = arrow_schema.get_field_index(watermark_column)
    if field_idx < 0:
        log.warning("incremental.no_watermark_column", table=fqn, column=watermark_column)
        return None, None, None
    kind = _watermark_kind(arrow_schema.field(field_idx).type)
    if kind is None:
        log.warning("incremental.unsupported_watermark_type", table=fqn,
                    column=watermark_column, arrow_type=str(arrow_schema.field(field_idx).type))
        return None, None, None

    wm = get_watermark(conn_cfg.id, fqn, kind)

    # Build the window bounds for this kind.
    hi: object | None = None
    if kind == "timestamp":
        lo = wm.value - timedelta(seconds=s.incremental_overlap_seconds)
        hi = now - timedelta(seconds=s.incremental_safety_gap_seconds)
        if hi <= lo:
            log.info("incremental.skip.no_window", table=fqn, lo=str(lo), hi=str(hi))
            return None, wm, kind
    elif kind == "date":
        overlap_days = max(1, (s.incremental_overlap_seconds + 86399) // 86400)
        lo = wm.value - timedelta(days=overlap_days)
        hi = (now - timedelta(seconds=s.incremental_safety_gap_seconds)).date()
        if hi <= lo:
            log.info("incremental.skip.no_window", table=fqn, lo=str(lo), hi=str(hi))
            return None, wm, kind
    else:  # integer: opaque monotonic key, no time-based bounds
        lo = wm.value

    col = sql.Identifier(watermark_column)
    pkcol = sql.Identifier(pk)
    preds = [
        sql.SQL("({c}, {p}) > ({lo}, {loid})").format(
            c=col, p=pkcol, lo=sql.Literal(lo), loid=sql.Literal(wm.last_id)
        )
    ]
    if hi is not None:
        preds.append(sql.SQL("{c} < {hi}").format(c=col, hi=sql.Literal(hi)))
    copy_stmt = sql.SQL(
        "COPY (SELECT * FROM {tbl} WHERE {where} ORDER BY {c}, {p}) TO STDOUT (FORMAT BINARY)"
    ).format(
        tbl=sql.Identifier(schema, table),
        where=sql.SQL(" AND ").join(preds),
        c=col, p=pkcol,
    )

    obj_key = f"{conn_cfg.raw_prefix}/{s3_prefix}/window_{int(now.timestamp())}.parquet"
    s3_uri = f"s3://{conn_cfg.lake_bucket}/{obj_key}"

    def _norm(v):
        return _as_utc(v) if kind == "timestamp" else v

    rows_seen = 0
    max_val = _norm(wm.value)
    max_id: int = wm.last_id

    import tempfile

    with tempfile.SpooledTemporaryFile(max_size=64 * 1024**2, mode="w+b") as spool:
        writer = pq.ParquetWriter(
            spool, arrow_schema,
            compression=s.parquet_compression,
            compression_level=s.parquet_compression_level,
            use_dictionary=True, write_statistics=True,
        )
        conn = dedicated_connection(conn_cfg)
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                with cur.copy(copy_stmt) as copy:
                    if pg_oids:
                        copy.set_types(pg_oids)
                    batch: list[tuple] = []
                    id_idx = arrow_schema.get_field_index(pk)
                    for row in copy.rows():
                        batch.append(row)
                        rows_seen += 1
                        # advance running max (rows already ORDER BY (col, pk)).
                        rv = _norm(row[field_idx])
                        rid = row[id_idx]
                        if rv is not None and (rv, rid) > (max_val, max_id):
                            max_val, max_id = rv, rid
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
            # For a time-bounded window, advance to hi so we don't re-scan the
            # empty window forever. For an integer key (no hi), leave it.
            if hi is not None:
                return None, Watermark(value=hi, last_id=wm.last_id), kind
            return None, wm, kind

        spool.flush()
        spool.seek(0)
        s3_client(conn_cfg).upload_fileobj(
            spool, conn_cfg.lake_bucket, obj_key, ExtraArgs=put_sse_args()
        )

    log.info("incremental.window.done", table=fqn, rows=rows_seen,
             max_value=str(max_val), max_id=max_id, s3_uri=s3_uri)
    return s3_uri, Watermark(value=max_val, last_id=max_id), kind


def _flush(writer: pq.ParquetWriter, schema: pa.Schema, rows: list[tuple]) -> None:
    columns = list(zip(*rows, strict=True))
    arrays = [
        pa.array(_coerce_for_arrow(col, schema.field(i).type), type=schema.field(i).type)
        for i, col in enumerate(columns)
    ]
    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))


def advance_watermark_after_merge(connection_id: int, table_fqn: str, wm: Watermark,
                                  kind: WatermarkKind = "timestamp") -> None:
    """Call AFTER a successful Iceberg MERGE. Forward-only write so concurrent
    runs can't move the watermark backwards."""
    set_watermark(connection_id, table_fqn, wm, kind)
    log.info("watermark.advanced", connection_id=connection_id, table=table_fqn,
             value=str(wm.value), last_id=wm.last_id)
