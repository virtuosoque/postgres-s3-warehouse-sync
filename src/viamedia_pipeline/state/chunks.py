"""Postgres-backed per-chunk run state for resumable initial loads.

Schema (created by viamedia_pipeline.common.migrations):
    pipeline_chunk_state (
        run_id, chunk_key, status, s3_uri, rows, error, updated_at
    )
"""

from dataclasses import dataclass
from typing import Literal

from viamedia_pipeline.common.metadata_db import connection

Status = Literal["PENDING", "RUNNING", "DONE", "FAILED"]


@dataclass(frozen=True)
class ChunkRecord:
    run_id: str
    chunk_key: str
    status: Status
    connection_id: int = 1
    s3_uri: str | None = None
    rows: int | None = None
    error: str | None = None


def chunk_key(table_fqn: str, chunk_id: int) -> str:
    return f"{table_fqn}#{chunk_id:06d}"


def put(record: ChunkRecord) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_chunk_state
                (run_id, chunk_key, connection_id, status, s3_uri, rows, error, updated_at)
            VALUES (%(run_id)s, %(chunk_key)s, %(conn)s, %(status)s, %(s3_uri)s, %(rows)s, %(error)s, now())
            ON CONFLICT (run_id, chunk_key) DO UPDATE
            SET status     = EXCLUDED.status,
                s3_uri     = COALESCE(EXCLUDED.s3_uri, pipeline_chunk_state.s3_uri),
                rows       = COALESCE(EXCLUDED.rows,   pipeline_chunk_state.rows),
                error      = EXCLUDED.error,
                updated_at = now()
            """,
            {
                "run_id":    record.run_id,
                "chunk_key": record.chunk_key,
                "conn":      record.connection_id,
                "status":    record.status,
                "s3_uri":    record.s3_uri,
                "rows":      record.rows,
                "error":     (record.error or "")[:2000] or None,
            },
        )
        conn.commit()


def get(run_id: str, key: str) -> ChunkRecord | None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT run_id, chunk_key, status, s3_uri, rows, error "
            "FROM pipeline_chunk_state WHERE run_id = %s AND chunk_key = %s",
            (run_id, key),
        )
        row = cur.fetchone()
    if not row:
        return None
    return ChunkRecord(
        run_id=row["run_id"],
        chunk_key=row["chunk_key"],
        status=row["status"],
        s3_uri=row["s3_uri"],
        rows=row["rows"],
        error=row["error"],
    )


def list_done(run_id: str) -> set[str]:
    """Return chunk_keys already marked DONE -- used for resumability."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT chunk_key FROM pipeline_chunk_state "
            "WHERE run_id = %s AND status = 'DONE'",
            (run_id,),
        )
        return {r["chunk_key"] for r in cur.fetchall()}
