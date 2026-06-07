"""Source Postgres connections, one pool per connection (database)."""

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool
from tenacity import retry, stop_after_attempt, wait_exponential

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger

log = get_logger(__name__)

# One pool per connection id (each source database has its own DSN).
_POOLS: dict[int, ConnectionPool] = {}


def get_pool(conn: Connection) -> ConnectionPool:
    pool = _POOLS.get(conn.id)
    if pool is None:
        pool = ConnectionPool(
            conninfo=conn.source_dsn,
            min_size=1,
            max_size=8,
            timeout=30,
            kwargs={"autocommit": False, "application_name": "viamedia-extractor"},
            open=True,
        )
        _POOLS[conn.id] = pool
    return pool


@contextmanager
def pooled_connection(conn: Connection) -> Iterator[psycopg.Connection]:
    pool = get_pool(conn)
    with pool.connection() as c:
        yield c


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    reraise=True,
)
def dedicated_connection(conn: Connection) -> psycopg.Connection:
    """A non-pooled connection to a source database. Used for the snapshot
    exporter and per-chunk COPY workers."""
    return psycopg.connect(
        conn.source_dsn,
        autocommit=False,
        application_name="viamedia-extractor-worker",
    )
