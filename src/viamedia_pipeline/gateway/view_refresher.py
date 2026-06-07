"""Background view refresher (multi-connection).

For every enabled connection it resolves each Iceberg table's current
metadata.json via that connection's catalog, then on each pooled DuckDB
connection registers a scoped S3 secret and (re)creates one view per table in
the connection's namespace: `<namespace>.<table>`.
"""

import threading

from viamedia_pipeline.common.config_store import Connection, list_connections
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.gateway.duckdb_pool import get_pool, register_connection_secret
from viamedia_pipeline.load.iceberg_writer import catalog

log = get_logger(__name__)


def _resolve_tables(conn: Connection) -> list[tuple[str, str]]:
    """[(table_name, metadata_location), ...] for one connection's namespace."""
    out: list[tuple[str, str]] = []
    try:
        cat = catalog(conn)
        idents = cat.list_tables(conn.iceberg_namespace)
    except Exception as e:
        log.warning("refresher.list_tables_failed", connection_id=conn.id, error=str(e))
        return out
    for ident in idents:
        name = ident[-1] if isinstance(ident, tuple) else str(ident).split(".")[-1]
        try:
            tbl = catalog(conn).load_table(ident)
            out.append((name, tbl.metadata_location))
        except Exception as e:
            log.warning("refresher.load_failed", table=str(ident), error=str(e))
    return out


def refresh_once() -> int:
    conns = list_connections(enabled_only=True)
    per_conn = [(c, _resolve_tables(c)) for c in conns]

    def _apply(dconn) -> None:
        for c, tables in per_conn:
            register_connection_secret(dconn, c)
            dconn.execute(f'CREATE SCHEMA IF NOT EXISTS "{c.iceberg_namespace}";')
            for name, meta_loc in tables:
                try:
                    dconn.execute(
                        f'CREATE OR REPLACE VIEW "{c.iceberg_namespace}"."{name}" AS '
                        f"SELECT * FROM iceberg_scan('{meta_loc}');"
                    )
                except Exception as e:
                    log.warning("refresher.view_create_failed",
                                connection_id=c.id, table=name, error=str(e))

    total = sum(len(t) for _, t in per_conn)
    if total or conns:
        get_pool().for_each(_apply)
    log.info("refresher.applied", connections=len(conns), n_views=total)
    return total


class ViewRefresher:
    def __init__(self, interval_seconds: int):
        self.interval = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, name="view-refresher", daemon=True)
        self._thread.start()
        log.info("refresher.started", interval=self.interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        try:
            refresh_once()
        except Exception as e:
            log.error("refresher.initial_failed", error=str(e))
        while not self._stop.wait(self.interval):
            try:
                refresh_once()
            except Exception as e:
                log.error("refresher.iteration_failed", error=str(e))
