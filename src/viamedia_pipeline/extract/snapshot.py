"""Snapshot coordination via pg_export_snapshot.

Workers attach to the exporter's REPEATABLE READ transaction so all parallel
COPY operations see a single consistent point-in-time of the source DB.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.pg import dedicated_connection

log = get_logger(__name__)


@dataclass(frozen=True)
class ExportedSnapshot:
    snapshot_id: str
    txid: int


@contextmanager
def export_snapshot(conn_cfg: Connection) -> Iterator[ExportedSnapshot]:
    """Hold a REPEATABLE READ transaction open and yield its snapshot id.

    The transaction must remain open until every worker has SET TRANSACTION
    SNAPSHOT '<id>'. Closing the transaction invalidates the snapshot for new
    attachers (already-attached workers keep their own).
    """
    conn = dedicated_connection(conn_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute("SELECT pg_export_snapshot(), txid_current()")
            snap_id, txid = cur.fetchone()
        log.info("snapshot.exported", snapshot_id=snap_id, txid=txid)
        yield ExportedSnapshot(snapshot_id=snap_id, txid=int(txid))
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()
            log.info("snapshot.released")
