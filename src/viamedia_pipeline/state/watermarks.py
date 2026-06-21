"""Postgres-backed (watermark_value, id) watermark per (connection, table).

The watermark column can be a timestamp/timestamptz, a date, or an integer
(e.g. an epoch or sequence `updated_at`). The value is stored as text in
`last_value` and parsed back according to `kind`. `last_ts` is retained for
backward compatibility (populated only for timestamp watermarks).
"""

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Literal

from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.metadata_db import connection

log = get_logger(__name__)

# timestamp_tz   = `timestamp with time zone`    -> tz-aware UTC instants
# timestamp_naive = `timestamp without time zone` -> naive wall-clock, compared
#                   in its own frame (NEVER coerced to UTC, or the window breaks
#                   when the DB's zone isn't UTC).
WatermarkKind = Literal["timestamp_tz", "timestamp_naive", "date", "integer"]

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EPOCH_NAIVE = datetime(1970, 1, 1)


@dataclass(frozen=True)
class Watermark:
    value: object   # datetime (tz-aware or naive) | date | int -- per the column's kind
    last_id: int


def default_value(kind: WatermarkKind):
    if kind == "integer":
        return 0
    if kind == "date":
        return date(1970, 1, 1)
    if kind == "timestamp_naive":
        return _EPOCH_NAIVE
    return _EPOCH  # timestamp_tz


def _serialize(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(int(value))  # integer


def _parse(text: str | None, kind: WatermarkKind):
    if text is None:
        return default_value(kind)
    if kind == "integer":
        return int(text)
    if kind == "date":
        return date.fromisoformat(text)
    dt = datetime.fromisoformat(text)
    if kind == "timestamp_naive":
        return dt.replace(tzinfo=None)  # always naive, even if an offset was stored
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)  # timestamp_tz


def get_watermark(connection_id: int, table_fqn: str, kind: WatermarkKind = "timestamp_tz") -> Watermark:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_value, last_ts, last_id FROM pipeline_watermarks "
            "WHERE connection_id = %s AND table_fqn = %s",
            (connection_id, table_fqn),
        )
        row = cur.fetchone()
    if not row:
        return Watermark(value=default_value(kind), last_id=0)
    if row["last_value"] is not None:
        return Watermark(value=_parse(row["last_value"], kind), last_id=int(row["last_id"]))
    # Legacy row predating last_value: fall back to last_ts for timestamp kinds.
    if kind in ("timestamp_tz", "timestamp_naive") and row["last_ts"] is not None:
        val = row["last_ts"]
        if kind == "timestamp_naive" and val.tzinfo is not None:
            val = val.replace(tzinfo=None)
        return Watermark(value=val, last_id=int(row["last_id"]))
    return Watermark(value=default_value(kind), last_id=int(row["last_id"] or 0))


def set_watermark(connection_id: int, table_fqn: str, wm: Watermark,
                  kind: WatermarkKind = "timestamp_tz") -> None:
    """Forward-only upsert: advance only if (value, last_id) strictly increases.
    Comparison is done in Python under a row lock -- a single text column can't
    correctly compare timestamps, dates and integers in SQL."""
    new_text = _serialize(wm.value)
    # last_ts is legacy/secondary (TIMESTAMPTZ); only populate it for tz-aware
    # timestamps -- a naive value there would be reinterpreted in the DB's zone.
    last_ts = wm.value if (kind == "timestamp_tz" and isinstance(wm.value, datetime)) else None
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_value, last_id FROM pipeline_watermarks "
            "WHERE connection_id = %s AND table_fqn = %s FOR UPDATE",
            (connection_id, table_fqn),
        )
        row = cur.fetchone()
        if row is not None:
            cur_val = (_parse(row["last_value"], kind)
                       if row["last_value"] is not None else default_value(kind))
            cur_id = int(row["last_id"] or 0)
            if (wm.value, wm.last_id) <= (cur_val, cur_id):
                conn.commit()
                log.warning("watermark.skip.older_than_current", connection_id=connection_id,
                            table=table_fqn, attempted=new_text, attempted_id=wm.last_id)
                return
            cur.execute(
                "UPDATE pipeline_watermarks "
                "SET last_value=%s, last_ts=%s, last_id=%s, updated_at=now() "
                "WHERE connection_id=%s AND table_fqn=%s",
                (new_text, last_ts, wm.last_id, connection_id, table_fqn),
            )
        else:
            cur.execute(
                "INSERT INTO pipeline_watermarks "
                "(connection_id, table_fqn, last_value, last_ts, last_id, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, now())",
                (connection_id, table_fqn, new_text, last_ts, wm.last_id),
            )
        conn.commit()
