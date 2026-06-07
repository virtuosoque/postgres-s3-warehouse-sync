"""DuckDB connection management for the gateway.

A small pool of pre-warmed in-process DuckDB connections with iceberg/httpfs/aws
loaded. Each connection reads from potentially many lake buckets (one per source
connection), so we register a DuckDB **scoped S3 secret per connection** rather
than a single global S3 config — that lets per-connection credentials coexist.
"""

import os
import threading
from collections import deque
from contextlib import contextmanager
from urllib.parse import urlparse

import duckdb

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger

log = get_logger(__name__)


class DuckDBPool:
    def __init__(self, size: int):
        self.size = size
        self._lock = threading.Lock()
        self._available: deque[duckdb.DuckDBPyConnection] = deque()
        self._cond = threading.Condition(self._lock)

    def init(self) -> None:
        for _ in range(self.size):
            self._available.append(_new_conn())
        log.info("duckdb.pool.init", size=self.size)

    @contextmanager
    def acquire(self, timeout: float = 30.0):
        with self._cond:
            if not self._available:
                if not self._cond.wait_for(lambda: bool(self._available), timeout=timeout):
                    raise TimeoutError("DuckDB pool exhausted")
            conn = self._available.popleft()
        try:
            yield conn
        finally:
            with self._cond:
                self._available.append(conn)
                self._cond.notify()

    def for_each(self, fn) -> None:
        for _ in range(self.size):
            with self.acquire() as conn:
                fn(conn)

    def close(self) -> None:
        with self._cond:
            while self._available:
                try:
                    self._available.popleft().close()
                except Exception:
                    pass


def _new_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL httpfs;  LOAD httpfs;")
    conn.execute("INSTALL aws;     LOAD aws;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    threads = max(2, int(os.cpu_count() or 4))
    conn.execute(f"SET threads = {threads};")
    conn.execute("SET memory_limit = '6GB';")
    conn.execute("SET enable_progress_bar = false;")
    conn.execute("SET enable_http_metadata_cache = true;")
    return conn


def register_connection_secret(dconn: duckdb.DuckDBPyConnection, c: Connection) -> None:
    """Register/replace a DuckDB S3 secret scoped to this connection's bucket."""
    parts = [
        "TYPE S3",
        f"REGION '{c.aws_region}'",
        f"SCOPE 's3://{c.lake_bucket}'",
    ]
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if c.aws_access_key and c.aws_secret_key:
        parts += [f"KEY_ID '{c.aws_access_key}'", f"SECRET '{c.aws_secret_key}'"]
    if endpoint:
        u = urlparse(endpoint)
        parts += [
            f"ENDPOINT '{u.hostname}:{u.port}'",
            "URL_STYLE 'path'",
            f"USE_SSL {'true' if u.scheme == 'https' else 'false'}",
        ]
        if not (c.aws_access_key and c.aws_secret_key):
            parts += [
                f"KEY_ID '{os.environ.get('AWS_ACCESS_KEY_ID', 'test')}'",
                f"SECRET '{os.environ.get('AWS_SECRET_ACCESS_KEY', 'test')}'",
            ]
    else:
        parts.append("URL_STYLE 'vhost'")
    dconn.execute(f"CREATE OR REPLACE SECRET conn_{c.id} ({', '.join(parts)});")


_pool: DuckDBPool | None = None


def get_pool() -> DuckDBPool:
    global _pool
    if _pool is None:
        size = int(os.environ.get("GATEWAY_DUCKDB_POOL_SIZE", "4"))
        _pool = DuckDBPool(size=size)
        _pool.init()
    return _pool
