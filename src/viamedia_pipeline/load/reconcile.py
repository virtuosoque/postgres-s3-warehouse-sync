"""Delete-reconciliation: remove rows from the lake that no longer exist in the
source.

Incremental sync (the `(updated_at, id)` watermark) can only see inserts/updates,
never hard deletes. This pass periodically compares the set of primary keys in
the source table against the keys in the Iceberg table and deletes the ones that
are gone from the source. It's far cheaper than a full re-bootstrap and produces
no duplicates.

Designed for periodic (e.g. daily) runs, not every 5 minutes — each delete is a
copy-on-write rewrite of the affected files.
"""

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import In

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.load.iceberg_writer import catalog

log = get_logger(__name__)

_DELETE_BATCH = 5000      # ids per Iceberg delete commit
_FETCH_BATCH = 200_000    # source ids fetched per round


def reconcile_table(conn: Connection, schema: str, table: str,
                    iceberg_table_name: str, pk: str = "id") -> dict:
    cat = catalog(conn)
    tbl = cat.load_table((conn.iceberg_namespace, iceberg_table_name))

    # Keys currently in the lake.
    lake = tbl.scan(selected_fields=(pk,)).to_arrow()
    if lake.num_rows == 0:
        return {"lake_rows": 0, "source_rows": 0, "deleted": 0, "skipped": "empty_table"}
    lake_ids = lake.column(pk).combine_chunks()

    # Keys currently in the source (streamed via a server-side cursor).
    src_chunks: list[pa.Array] = []
    batch: list = []
    with dedicated_connection(conn) as pg_conn:
        with pg_conn.cursor(name="reconcile_ids") as cur:
            cur.itersize = _FETCH_BATCH
            cur.execute(f'SELECT "{pk}" FROM "{schema}"."{table}"')  # noqa: S608
            for (val,) in cur:
                batch.append(val)
                if len(batch) >= _FETCH_BATCH:
                    src_chunks.append(pa.array(batch, type=lake_ids.type))
                    batch = []
        pg_conn.rollback()
    if batch:
        src_chunks.append(pa.array(batch, type=lake_ids.type))
    source_ids = (pa.chunked_array(src_chunks, type=lake_ids.type).combine_chunks()
                  if src_chunks else pa.array([], type=lake_ids.type))

    # Lake keys not present in the source => deleted at source.
    in_source = pc.is_in(lake_ids, value_set=source_ids)
    deleted_ids = pc.filter(lake_ids, pc.invert(in_source)).to_pylist()

    deleted = 0
    for i in range(0, len(deleted_ids), _DELETE_BATCH):
        chunk = deleted_ids[i:i + _DELETE_BATCH]
        tbl.delete(In(pk, chunk))
        deleted += len(chunk)
        tbl = cat.load_table((conn.iceberg_namespace, iceberg_table_name))  # refresh snapshot

    summary = {"lake_rows": len(lake_ids), "source_rows": len(source_ids), "deleted": deleted}
    log.info("reconcile.done", connection_id=conn.id, table=f"{schema}.{table}", **summary)
    return summary
