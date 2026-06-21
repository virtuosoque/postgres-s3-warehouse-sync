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


def align_to_table(staged: pa.Table, iceberg_table: Table) -> pa.Table:
    """Reshape a staged Arrow table to exactly match the Iceberg table's current
    schema: add NULL columns for table columns missing from the source (e.g. a
    column dropped at the source but kept in the lake), drop source columns not
    in the table, and cast types to the table's (possibly promoted) types. This
    keeps `upsert` from failing on a schema mismatch after schema evolution.
    """
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    target = schema_to_pyarrow(iceberg_table.schema())
    have = set(staged.schema.names)
    arrays = []
    for field in target:
        if field.name in have:
            col = staged.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except Exception:  # noqa: BLE001
                    col = col.cast(field.type, safe=False)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(staged.num_rows, type=field.type))
    return pa.table(arrays, schema=target)


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

    # Align to the table's (possibly just-evolved) schema before writing.
    staged = align_to_table(staged, iceberg_table)

    # Upsert via overwrite(filter=In(pk, keys)): atomically delete the rows we're
    # replacing, then append the new versions. Unlike pyiceberg `upsert`, this
    # does NOT reconcile existing-vs-incoming schemas internally, so it survives
    # a column ADDED after the existing data files were written (schema
    # evolution) -- which is exactly what breaks `upsert.get_rows_to_update`.
    from pyiceberg.expressions import In

    pk = join_cols[0]
    keys = [k for k in staged.column(pk).to_pylist() if k is not None]
    if keys:
        iceberg_table.overwrite(df=staged, overwrite_filter=In(pk, keys))
    elif staged.num_rows:
        iceberg_table.append(staged)
    summary = {"rows_written": staged.num_rows, "keys": len(keys)}
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
