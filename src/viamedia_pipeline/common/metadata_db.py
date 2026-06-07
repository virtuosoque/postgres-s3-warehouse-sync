"""Connection pool for the metadata Postgres.

This DB stores:
  - PyIceberg SqlCatalog tables (iceberg_tables, iceberg_namespace_properties)
  - pipeline_watermarks   (incremental sync state)
  - pipeline_chunk_state  (bootstrap chunk resumability)

Schema isolation: everything lives under METADATA_PG_SCHEMA so the DB can be
shared with other tenants/services safely.
"""

import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

# NOTE: this module deliberately reads METADATA_PG_DSN / METADATA_PG_SCHEMA from
# the environment directly rather than via get_settings(). It is the bootstrap
# store that get_settings() itself overlays operational config from, so it must
# not depend on get_settings() (that would be a circular import / recursion).

_POOL: ConnectionPool | None = None


def metadata_dsn() -> str:
    dsn = os.environ.get("METADATA_PG_DSN")
    if not dsn:
        raise RuntimeError("METADATA_PG_DSN is not set (bootstrap env var).")
    return dsn


def metadata_schema() -> str:
    return os.environ.get("METADATA_PG_SCHEMA", "pipeline")


def get_pool() -> ConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = ConnectionPool(
            conninfo=metadata_dsn(),
            min_size=2,
            max_size=10,
            timeout=30,
            kwargs={
                "autocommit": False,
                "application_name": "viamedia-metadata",
                "row_factory": dict_row,
                "options": f"-c search_path={metadata_schema()},public",
            },
            open=True,
        )
    return _POOL


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.close()
        _POOL = None
