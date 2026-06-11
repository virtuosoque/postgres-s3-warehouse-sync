"""Per-connection object configuration, derived from live source discovery.

For each connection we walk its source `pg_class` (filtered by the connection's
sync globs / object types / schema blacklist), then keep only the objects the UI
has selected (`pipeline_sync_selection`), falling back to the glob result when a
connection has no explicit selection rows. Results are cached per connection.
"""

import dataclasses
import time
from dataclasses import dataclass, field

from viamedia_pipeline.common.config_store import Connection, enabled_fqns
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.extract.discovery import DiscoveredObject, Kind, discover

log = get_logger(__name__)

_DISCOVERY_TTL_SECONDS = 300


@dataclass(frozen=True)
class TableConfig:
    schema: str
    table: str
    kind: Kind
    partition_by: tuple[tuple[str, str, int | None], ...] = ()
    watermark_column: str | None = "updated_at"
    pk: str = "id"
    supports_parallel_chunking: bool = True

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def iceberg_table_name(self) -> str:
        return f"{self.schema}__{self.table}"

    @property
    def supports_incremental(self) -> bool:
        return self.watermark_column is not None


@dataclass(frozen=True)
class TableOverride:
    partition_by: tuple[tuple[str, str, int | None], ...] | None = None
    pk: str | None = None
    watermark_column: str | None = field(default="__keep__")


# Optional hand-tuned overrides keyed by fqn. NOTE: add_files only supports
# identity + time transforms — do not use bucket/truncate here.
OVERRIDES: dict[str, TableOverride] = {}


def _default_partition_for(obj: DiscoveredObject) -> tuple[tuple[str, str, int | None], ...]:
    """Partition by `day(created_at)` when the object has a timestamp
    `created_at`; otherwise leave it unpartitioned.

    We load via PyIceberg `add_files` (zero-copy), which requires every file to
    belong to a SINGLE partition value. The bootstrap planner
    (`plan_partitioned_chunks`) therefore slices each object into one-day
    (and id-range) chunks so each parquet file carries exactly one day -> the
    partitioned table accepts them. Day partitioning lets Athena/DuckDB/PowerBI
    prune by date cheaply. Objects without a timestamp `created_at` (views,
    lookup tables) stay unpartitioned and rely on per-file min/max stats.
    """
    col = next((c for c in obj.columns if c.name == "created_at"), None)
    # format_type() yields "timestamp without time zone" / "timestamp with time
    # zone" (optionally with a precision, e.g. "timestamp(3) with time zone").
    # A prefix check covers all timestamp variants while excluding plain `date`.
    if col is not None and col.pg_type.lower().startswith("timestamp"):
        return (("created_at", "day", None),)
    return ()


def _config_from_object(obj: DiscoveredObject) -> TableConfig:
    override = OVERRIDES.get(obj.fqn)
    partition_by = (
        override.partition_by if override and override.partition_by is not None
        else _default_partition_for(obj)
    )
    pk = (override.pk if override and override.pk is not None else "id")
    if override and override.watermark_column != "__keep__":
        wm_col = override.watermark_column
    else:
        wm_col = "updated_at" if obj.supports_incremental else None
    return TableConfig(
        schema=obj.schema, table=obj.name, kind=obj.kind,
        partition_by=partition_by, watermark_column=wm_col, pk=pk,
        supports_parallel_chunking=obj.supports_parallel_chunking,
    )


# cache keyed by connection id -> (timestamp, configs)
_cache: dict[int, tuple[float, tuple[TableConfig, ...]]] = {}


def get_tables(conn: Connection, force: bool = False) -> tuple[TableConfig, ...]:
    now = time.monotonic()
    cached = _cache.get(conn.id)
    if not force and cached and (now - cached[0]) < _DISCOVERY_TTL_SECONDS:
        return cached[1]

    # When an explicit UI selection exists it is authoritative: discover ALL
    # objects (ignore the connection's include/exclude globs) so any selected
    # object is found, then keep only the enabled ones. With no selection yet,
    # fall back to the connection's globs.
    selected = enabled_fqns(conn.id)
    disc_cfg = (
        dataclasses.replace(conn, sync_include="", sync_exclude="")
        if selected is not None else conn
    )
    # When a selection exists, introspect ONLY the selected objects (avoids
    # introspecting every table in a large source on each planning tick).
    with dedicated_connection(conn) as pg_conn:
        objects = discover(pg_conn, disc_cfg, only_fqns=selected)

    configs = tuple(_config_from_object(o) for o in objects)
    _cache[conn.id] = (now, configs)
    log.info(
        "tables.refreshed",
        connection_id=conn.id,
        n=len(configs),
        incremental_eligible=sum(1 for c in configs if c.supports_incremental),
        selection_applied=selected is not None,
    )
    return configs


def get_table(conn: Connection, fqn: str) -> TableConfig | None:
    for cfg in get_tables(conn):
        if cfg.fqn == fqn:
            return cfg
    for cfg in get_tables(conn, force=True):
        if cfg.fqn == fqn:
            return cfg
    # Explicitly requested object that isn't in the cached/selected set -- e.g. a
    # materialized view bootstrapped on demand, or an object not (yet) checked in
    # the sync selection. Discover JUST this fqn directly; only_fqns is
    # authoritative, so the connection's object-type/glob filters don't exclude
    # it. This makes "bootstrap this object" work for any real source object.
    try:
        with dedicated_connection(conn) as pg_conn:
            for o in discover(pg_conn, conn, only_fqns={fqn}):
                if o.fqn == fqn:
                    return _config_from_object(o)
    except Exception as e:  # noqa: BLE001
        log.warning("get_table.targeted_discovery_failed", connection_id=conn.id, fqn=fqn, error=str(e))
    return None
