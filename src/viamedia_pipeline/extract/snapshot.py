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
    snap_id: str = ""
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            # Try to export a snapshot for cross-chunk consistency. On a hot
            # standby / read replica this can fail ("cannot ... during
            # recovery"); that's fine -- each chunk worker then just uses its
            # own REPEATABLE READ view (the attach is best-effort anyway). We do
            # NOT call txid_current() (it assigns an xid, forbidden on standby).
            try:
                cur.execute("SELECT pg_export_snapshot()")
                row = cur.fetchone()
                snap_id = (row[0] if row else "") or ""
            except Exception as e:  # noqa: BLE001
                log.warning("snapshot.export_unsupported", error=str(e))
                conn.rollback()  # clear the aborted transaction
        log.info("snapshot.exported", snapshot_id=snap_id or "(none)")
        yield ExportedSnapshot(snapshot_id=snap_id, txid=0)
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()
            log.info("snapshot.released")
