"""Primary-key reconciliation: make the lake's row set match the source's.

Incremental sync (the `(updated_at, id)` watermark) only sees inserts/updates
that bump `updated_at`. This pass compares the SET of primary keys in the source
against the keys in the Iceberg table and:

  * DELETES lake rows whose key no longer exists in the source, and
  * ADDS  source rows whose key is missing from the lake (catches inserts with a
    NULL/old `updated_at`, or any key the watermark missed -- regardless of id
    ordering or gaps).

It does NOT detect column-level UPDATES to an existing key (same row, changed
data) -- those still come via incremental or a re-bootstrap. Designed for
periodic (e.g. daily) runs; each change is a copy-on-write rewrite.
"""

import time

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import In

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.load.iceberg_writer import catalog

log = get_logger(__name__)

_DELETE_BATCH = 5000       # ids per Iceberg delete commit
_FETCH_BATCH = 200_000     # source ids fetched per round
_INSERT_BATCH = 10_000     # missing rows fetched + merged per round


def reconcile_table(conn: Connection, schema: str, table: str,
                    iceberg_table_name: str, pk: str = "id") -> dict:
    cat = catalog(conn)
    ident = (conn.iceberg_namespace, iceberg_table_name)
    tbl = cat.load_table(ident)

    # Keys currently in the lake (empty array, with the right type, if no rows).
    lake = tbl.scan(selected_fields=(pk,)).to_arrow()
    lake_ids = lake.column(pk).combine_chunks()

    # Keys currently in the source (streamed via a server-side cursor).
    id_type = lake_ids.type if lake.num_rows else None
    src_chunks: list[pa.Array] = []
    batch: list = []
    with dedicated_connection(conn) as pg_conn:
        with pg_conn.cursor(name="reconcile_ids") as cur:
            cur.itersize = _FETCH_BATCH
            cur.execute(f'SELECT "{pk}" FROM "{schema}"."{table}"')  # noqa: S608
            for (val,) in cur:
                batch.append(val)
                if len(batch) >= _FETCH_BATCH:
                    src_chunks.append(pa.array(batch, type=id_type))
                    batch = []
        pg_conn.rollback()
    if batch:
        src_chunks.append(pa.array(batch, type=id_type))
    source_ids = (pa.chunked_array(src_chunks).combine_chunks()
                  if src_chunks else pa.array([], type=id_type or lake_ids.type))
    if id_type is None and source_ids.type is not None:
        # Lake was empty; adopt the source key type for comparisons.
        lake_ids = lake_ids.cast(source_ids.type) if len(lake_ids) else pa.array([], type=source_ids.type)

    # 1) Lake keys not in the source => deleted at source.
    deleted = 0
    if len(lake_ids):
        deleted_ids = pc.filter(lake_ids, pc.invert(pc.is_in(lake_ids, value_set=source_ids))).to_pylist()
        for i in range(0, len(deleted_ids), _DELETE_BATCH):
            chunk = deleted_ids[i:i + _DELETE_BATCH]
            tbl.delete(In(pk, chunk))
            deleted += len(chunk)
            tbl = cat.load_table(ident)  # refresh snapshot

    # 2) Source keys not in the lake => inserted at source (incl. NULL-updated_at
    #    rows the watermark can't see). Fetch + upsert them.
    added = 0
    if len(source_ids):
        missing_ids = pc.filter(source_ids, pc.invert(pc.is_in(source_ids, value_set=lake_ids))).to_pylist()
        if missing_ids:
            added = _add_missing_rows(conn, schema, table, iceberg_table_name, pk, missing_ids)

    summary = {"lake_rows": len(lake_ids), "source_rows": len(source_ids),
               "deleted": deleted, "added": added}
    log.info("reconcile.done", connection_id=conn.id, table=f"{schema}.{table}", **summary)
    return summary


def _add_missing_rows(conn: Connection, schema: str, table: str,
                      iceberg_table_name: str, pk: str, missing_ids: list) -> int:
    """Fetch source rows for `missing_ids` and upsert them into the lake.
    Reuses the extract->parquet->merge path so types/schema-evolution are
    handled identically to a normal sync."""
    from viamedia_pipeline.extract.chunker import Chunk
    from viamedia_pipeline.extract.copy_worker import extract_chunk_to_s3
    from viamedia_pipeline.extract.schema import (
        arrow_schema_for,
        introspect_columns,
        pg_oids_for,
    )
    from viamedia_pipeline.load.merge import load_staging_arrow, merge_arrow_table

    cat = catalog(conn)
    ident = (conn.iceberg_namespace, iceberg_table_name)
    with dedicated_connection(conn) as pg_conn:
        cols = introspect_columns(pg_conn, schema, table)
    arrow_schema = arrow_schema_for(cols)
    pg_oids = pg_oids_for(cols)
    token = int(time.time())

    added = 0
    for bi in range(0, len(missing_ids), _INSERT_BATCH):
        batch = missing_ids[bi:bi + _INSERT_BATCH]
        # pk is numeric (reconcile only runs on numeric-pk tables) -> safe to inline.
        ids_csv = ",".join(str(int(x)) for x in batch)
        chunk = Chunk(schema=schema, table=table, chunk_id=bi // _INSERT_BATCH)
        res = extract_chunk_to_s3(
            conn_cfg=conn, chunk=chunk, snapshot_id="", arrow_schema=arrow_schema,
            s3_prefix=f"reconcile/{schema}.{table}/insert_{token}",
            pg_oids=pg_oids, where_extra=f'"{pk}" IN ({ids_csv})',
        )
        for uri in res.s3_uris:
            staged = load_staging_arrow(conn, uri)
            tbl = cat.load_table(ident)
            merge_arrow_table(tbl, staged, join_cols=(pk,))
        added += res.rows
    log.info("reconcile.added_missing", connection_id=conn.id,
             table=f"{schema}.{table}", added=added)
    return added
