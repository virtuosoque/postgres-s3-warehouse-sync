"""Idempotent MERGE for incremental upserts.

PyIceberg 0.8+ supports `upsert()` which performs MERGE semantics using a
join key. We MERGE on `id` and prefer the new row when `updated_at` is newer.

For tables PyIceberg cannot upsert (e.g. exotic types), fall back to running
the equivalent statement via Spark on EMR Serverless -- see load/maintenance.
"""

import os

import pyarrow as pa
from pyiceberg.table import Table

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger

log = get_logger(__name__)


def merge_arrow_table(
    iceberg_table: Table,
    staged: pa.Table,
    *,
    join_cols: tuple[str, ...] = ("id",),
    when_matched_update_all: bool = True,
    when_not_matched_insert_all: bool = True,
) -> dict:
    """Upsert an Arrow table into an Iceberg table.

    Pre-filter staged rows to keep only the latest (updated_at, id) per id
    before calling upsert -- avoids 'multiple source rows match' errors when
    the watermark window contains multiple updates of the same row.
    """
    if "updated_at" in staged.schema.names and "id" in staged.schema.names:
        staged = _dedupe_latest(staged, key="id", ts="updated_at")

    result = iceberg_table.upsert(
        df=staged,
        join_cols=list(join_cols),
        when_matched_update_all=when_matched_update_all,
        when_not_matched_insert_all=when_not_matched_insert_all,
    )
    summary = {
        "rows_updated": getattr(result, "rows_updated", None),
        "rows_inserted": getattr(result, "rows_inserted", None),
    }
    log.info("iceberg.upsert.done", table=iceberg_table.name(), **summary)
    return summary


def _dedupe_latest(tbl: pa.Table, *, key: str, ts: str) -> pa.Table:
    """Keep only the row with the max ts per key. Stable, single-pass."""
    import pyarrow.compute as pc

    sort_idx = pc.sort_indices(tbl, sort_keys=[(ts, "descending"), (key, "descending")])
    tbl_sorted = tbl.take(sort_idx)
    # Take the first occurrence per key
    keys = tbl_sorted[key]
    seen_mask = pc.is_in(keys, value_set=pa.array([], type=keys.type))
    # Manual de-dup using a Python set on the key column is OK for moderate
    # window sizes (millions of rows). Replace with arrow-native group_by
    # + first() for very large windows.
    seen: set = set()
    keep: list[bool] = []
    for v in keys.to_pylist():
        if v in seen:
            keep.append(False)
        else:
            seen.add(v)
            keep.append(True)
    return tbl_sorted.filter(pa.array(keep))


def load_staging_arrow(conn: Connection, s3_uri: str) -> pa.Table:
    """Read a single staging parquet object into Arrow using the connection's
    AWS credentials (and the LocalStack endpoint when set)."""
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs_kwargs: dict = {"region": conn.aws_region}
    if conn.aws_access_key and conn.aws_secret_key:
        fs_kwargs["access_key"] = conn.aws_access_key
        fs_kwargs["secret_key"] = conn.aws_secret_key
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        fs_kwargs["endpoint_override"] = endpoint
        fs_kwargs["scheme"] = "https" if endpoint.startswith("https") else "http"
    fs = pafs.S3FileSystem(**fs_kwargs)
    return pq.read_table(s3_uri.removeprefix("s3://"), filesystem=fs)
