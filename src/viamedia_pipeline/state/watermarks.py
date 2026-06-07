"""Postgres-backed (updated_at, id) watermark per table.

Schema (created by viamedia_pipeline.common.migrations):
    pipeline_watermarks (
        table_fqn  TEXT PRIMARY KEY,
        last_ts    TIMESTAMPTZ,
        last_id    BIGINT,
        updated_at TIMESTAMPTZ
    )
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.metadata_db import connection

log = get_logger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Watermark:
    ts: datetime
    last_id: int


def get_watermark(connection_id: int, table_fqn: str) -> Watermark:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_ts, last_id FROM pipeline_watermarks "
            "WHERE connection_id = %s AND table_fqn = %s",
            (connection_id, table_fqn),
        )
        row = cur.fetchone()
    if not row:
        return Watermark(ts=_EPOCH, last_id=0)
    return Watermark(ts=row["last_ts"], last_id=int(row["last_id"]))


def set_watermark(connection_id: int, table_fqn: str, wm: Watermark) -> None:
    """Upsert with a forward-only condition: only advance if (last_ts, last_id)
    strictly increases. Prevents out-of-order concurrent runs from rewinding.
    """
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_watermarks (connection_id, table_fqn, last_ts, last_id, updated_at)
            VALUES (%(c)s, %(t)s, %(ts)s, %(id)s, now())
            ON CONFLICT (connection_id, table_fqn) DO UPDATE
            SET last_ts    = EXCLUDED.last_ts,
                last_id    = EXCLUDED.last_id,
                updated_at = now()
            WHERE (pipeline_watermarks.last_ts, pipeline_watermarks.last_id)
                < (EXCLUDED.last_ts, EXCLUDED.last_id)
            """,
            {"c": connection_id, "t": table_fqn, "ts": wm.ts, "id": wm.last_id},
        )
        affected = cur.rowcount
        conn.commit()
    if affected == 0:
        log.warning(
            "watermark.skip.older_than_current",
            connection_id=connection_id,
            table=table_fqn,
            attempted_ts=wm.ts.isoformat(),
            attempted_id=wm.last_id,
        )
