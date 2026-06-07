"""Per-chunk COPY BINARY worker.

Streams a server-side binary COPY into batched PyArrow record batches and
writes a single parquet object to S3 via streamed multipart upload.

Optimized for memory: we never materialize the whole chunk in RAM.
"""

import decimal
import json
from dataclasses import dataclass
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.common.s3 import put_sse_args, s3_client
from viamedia_pipeline.common.settings import get_settings
from viamedia_pipeline.extract.chunker import Chunk

log = get_logger(__name__)


@dataclass
class ChunkResult:
    chunk_id: int
    table: str
    s3_uris: list[str]
    rows: int
    bytes_written: int


def _coerce_for_arrow(col: tuple, arrow_type: pa.DataType) -> list:
    """Make a decoded PG column compatible with its Arrow target type.

    psycopg decodes json/jsonb to dict/list and uuid to uuid.UUID, but those
    columns map to Arrow string types -- serialize them back to text so
    pa.array() accepts them. Everything else passes through unchanged.
    """
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        out = []
        for v in col:
            if v is None or isinstance(v, str):
                out.append(v)
            elif isinstance(v, (dict, list)):
                out.append(json.dumps(v, default=str))
            else:
                out.append(str(v))
        return out
    if pa.types.is_decimal(arrow_type):
        # PG `numeric` can carry more scale than the Arrow decimal type allows
        # (esp. unconstrained numeric -> decimal128(38,9)); pyarrow refuses to
        # rescale. Round each value to the target scale so the array builds.
        quant = decimal.Decimal(1).scaleb(-arrow_type.scale)
        out = []
        for v in col:
            if isinstance(v, decimal.Decimal):
                try:
                    out.append(v.quantize(quant, rounding=decimal.ROUND_HALF_UP))
                except (decimal.InvalidOperation, decimal.Overflow):
                    out.append(None)  # value doesn't fit the precision -> null
            else:
                out.append(v)
        return out
    return list(col)


def _flush_batch(writer: pq.ParquetWriter, schema: pa.Schema, rows: list[tuple]) -> None:
    if not rows:
        return
    columns = list(zip(*rows, strict=True))
    arrays = [
        pa.array(_coerce_for_arrow(col, schema.field(i).type), type=schema.field(i).type)
        for i, col in enumerate(columns)
    ]
    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))


def extract_chunk_to_s3(
    conn_cfg: Connection,
    chunk: Chunk,
    snapshot_id: str,
    arrow_schema: pa.Schema,
    s3_prefix: str,
    *,
    pg_oids: list[int] | None = None,
    where_extra: str | None = None,
    where_params: tuple[Any, ...] = (),
) -> ChunkResult:
    """Run COPY BINARY for one id-range; upload one parquet object.

    The S3 object key is deterministic from (s3_prefix, chunk_id) so a re-run
    of a failed chunk overwrites cleanly -- idempotent at the file level.
    Iceberg commit is the atomic boundary across chunks.
    """
    s = get_settings()
    s3 = s3_client(conn_cfg)

    # Build COPY SQL with safely-quoted identifiers. Full-object chunks (views,
    # objects without numeric id) skip the WHERE clause entirely.
    if chunk.is_full:
        where_clause: sql.Composable | None = (
            sql.SQL(where_extra) if where_extra else None  # caller-trusted
        )
    else:
        base_where = sql.SQL("id >= {lo} AND id < {hi}").format(
            lo=sql.Literal(chunk.lo), hi=sql.Literal(chunk.hi)
        )
        where_clause = (
            sql.SQL("({base}) AND ({extra})").format(
                base=base_where, extra=sql.SQL(where_extra)  # caller-trusted
            )
            if where_extra
            else base_where
        )

    if where_clause is None:
        copy_stmt = sql.SQL("COPY (SELECT * FROM {tbl}) TO STDOUT (FORMAT BINARY)").format(
            tbl=sql.Identifier(chunk.schema, chunk.table),
        )
    else:
        copy_stmt = sql.SQL(
            "COPY (SELECT * FROM {tbl} WHERE {where}) TO STDOUT (FORMAT BINARY)"
        ).format(
            tbl=sql.Identifier(chunk.schema, chunk.table),
            where=where_clause,
        )

    import tempfile

    row_group_rows = max(1, s.extract_row_group_rows)
    # Roll over to a new parquet PART every this-many rows so memory stays flat
    # regardless of table size (the parquet footer is kept in RAM until close,
    # so one huge file would grow unboundedly). Each part is uploaded as it
    # finishes, giving incremental progress + resumable-ish behavior.
    part_rows_limit = max(row_group_rows, s.extract_rows_per_chunk)

    s3_uris: list[str] = []
    rows_written = 0
    bytes_written = 0

    def _open_part():
        spool = tempfile.SpooledTemporaryFile(max_size=64 * 1024**2, mode="w+b")
        writer = pq.ParquetWriter(
            spool, arrow_schema,
            compression=s.parquet_compression,
            compression_level=s.parquet_compression_level,
            use_dictionary=True, write_statistics=True, data_page_size=1 << 20,
        )
        return spool, writer

    def _finish_part(spool, writer, part_idx: int) -> int:
        writer.close()
        spool.seek(0, 2)
        size = spool.tell()
        spool.seek(0)
        key = f"{conn_cfg.raw_prefix}/{s3_prefix}/chunk={chunk.chunk_id:06d}-part{part_idx:04d}.parquet"
        s3.upload_fileobj(
            spool, conn_cfg.lake_bucket, key,
            ExtraArgs={**put_sse_args(), "ContentType": "application/octet-stream"},
        )
        spool.close()
        s3_uris.append(f"s3://{conn_cfg.lake_bucket}/{key}")
        return size

    conn = dedicated_connection(conn_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            # Best-effort attach to the exporter's snapshot; fall back to this
            # worker's own REPEATABLE READ view if the exporter txn has closed.
            if snapshot_id:
                try:
                    cur.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot_id)))
                except psycopg.Error as e:
                    log.warning("extract.snapshot.attach_failed", chunk_id=chunk.chunk_id,
                                snapshot_id=snapshot_id, error=str(e))
                    conn.rollback()
                    cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with cur.copy(copy_stmt) as copy:
                if pg_oids:
                    copy.set_types(pg_oids)
                spool, writer = _open_part()
                part_idx = 0
                part_rows = 0
                batch: list[tuple] = []
                for row in copy.rows():
                    batch.append(row)
                    if len(batch) >= row_group_rows:
                        _flush_batch(writer, arrow_schema, batch)
                        n = len(batch); rows_written += n; part_rows += n; batch.clear()
                        if part_rows >= part_rows_limit:
                            bytes_written += _finish_part(spool, writer, part_idx)
                            part_idx += 1; part_rows = 0
                            spool, writer = _open_part()
                if batch:
                    _flush_batch(writer, arrow_schema, batch)
                    n = len(batch); rows_written += n; part_rows += n; batch.clear()
                if part_rows > 0:
                    bytes_written += _finish_part(spool, writer, part_idx)
                else:
                    writer.close(); spool.close()  # empty trailing part
        conn.rollback()
    finally:
        conn.close()

    log.info("extract.chunk.done", table=chunk.fqn, chunk_id=chunk.chunk_id,
             rows=rows_written, bytes=bytes_written, parts=len(s3_uris))
    return ChunkResult(chunk_id=chunk.chunk_id, table=chunk.fqn,
                       s3_uris=s3_uris, rows=rows_written, bytes_written=bytes_written)
