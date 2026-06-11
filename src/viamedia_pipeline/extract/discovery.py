"""Discover source objects (tables / views / materialized views) and filter.

Filter precedence:
    1. SYNC_SCHEMA_BLACKLIST removes system schemas first (always).
    2. SYNC_OBJECT_TYPES limits to selected kinds.
    3. SYNC_INCLUDE globs: empty means "include all"; otherwise object must match
       at least one glob (against "schema.name").
    4. SYNC_EXCLUDE globs: applied last; any match removes the object.

For each kept object we introspect columns and determine capabilities:
    - supports_parallel_chunking: has a numeric monotonic `id` column
    - supports_incremental:       has both `id` and `updated_at`
"""

from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Literal

import psycopg

from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.extract.schema import Column, introspect_columns

log = get_logger(__name__)

Kind = Literal["TABLE", "VIEW", "MATERIALIZED_VIEW"]

# Map pg_class.relkind -> our friendly kind
_RELKIND_TO_KIND: dict[str, Kind] = {
    "r": "TABLE",              # ordinary table
    "p": "TABLE",              # partitioned table
    "v": "VIEW",
    "m": "MATERIALIZED_VIEW",
}

_NUMERIC_TYPES = {"smallint", "integer", "bigint"}


@dataclass(frozen=True)
class DiscoveredObject:
    schema: str
    name: str
    kind: Kind
    columns: tuple[Column, ...]

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def column_names(self) -> frozenset[str]:
        return frozenset(c.name for c in self.columns)

    @property
    def has_id(self) -> bool:
        return "id" in self.column_names

    @property
    def has_updated_at(self) -> bool:
        return "updated_at" in self.column_names

    @property
    def supports_parallel_chunking(self) -> bool:
        """True if `id` is a plain integer (smallint/integer/bigint).
        Numeric ids let us do MIN/MAX + range chunks; everything else falls
        back to a single COPY without WHERE clauses.
        """
        if not self.has_id:
            return False
        id_col = next(c for c in self.columns if c.name == "id")
        return id_col.pg_type.lower() in _NUMERIC_TYPES

    @property
    def supports_incremental(self) -> bool:
        return self.has_id and self.has_updated_at


def _list_objects_raw(conn: psycopg.Connection, *, schema_blacklist: set[str]) -> list[tuple[str, str, Kind]]:
    sql = """
        SELECT n.nspname  AS schema,
               c.relname  AS name,
               c.relkind  AS relkind
        FROM   pg_class     c
        JOIN   pg_namespace n ON n.oid = c.relnamespace
        WHERE  c.relkind IN ('r','p','v','m')
          AND  n.nspname NOT LIKE 'pg_temp_%%'
          AND  n.nspname NOT LIKE 'pg_toast_temp_%%'
          AND  NOT (n.nspname = ANY(%s))
        ORDER  BY n.nspname, c.relname
    """
    with conn.cursor() as cur:
        cur.execute(sql, (sorted(schema_blacklist),))
        rows = cur.fetchall()

    out: list[tuple[str, str, Kind]] = []
    for r in rows:
        # cursor row_factory may be dict_row or tuple -- handle both defensively
        if isinstance(r, dict):
            schema, name, relkind = r["schema"], r["name"], r["relkind"]
        else:
            schema, name, relkind = r[0], r[1], r[2]
        kind = _RELKIND_TO_KIND.get(relkind)
        if kind:
            out.append((schema, name, kind))
    return out


def _glob_match_any(fqn: str, globs: list[str]) -> bool:
    return any(fnmatch(fqn, g) for g in globs)


def discover(pg_conn: psycopg.Connection, cfg, only_fqns: set[str] | None = None) -> list[DiscoveredObject]:
    """Return the filtered list of objects to sync for a connection, with columns
    introspected. `cfg` is a config_store.Connection providing the sync globs /
    object-type / schema-blacklist filters. If `only_fqns` is given, only those
    objects are introspected (used so a bootstrap doesn't introspect every table)."""
    include = cfg.sync_include_globs
    exclude = cfg.sync_exclude_globs
    kinds = cfg.sync_object_types_set
    blacklist = cfg.sync_schema_blacklist_set

    raw = _list_objects_raw(pg_conn, schema_blacklist=blacklist)

    discovered: list[DiscoveredObject] = []
    skipped_by_kind: list[str] = []
    skipped_by_include: list[str] = []
    skipped_by_exclude: list[str] = []

    for schema, name, kind in raw:
        fqn = f"{schema}.{name}"
        if only_fqns is not None:
            # An explicit UI selection is AUTHORITATIVE: if the user picked this
            # object, sync it regardless of the connection's object-type toggles
            # or include/exclude globs (e.g. a selected materialized view whose
            # kind isn't in sync_object_types). Only the selection membership and
            # the schema blacklist (applied in the raw query) gate it.
            if fqn not in only_fqns:
                continue
        else:
            if kind not in kinds:
                skipped_by_kind.append(fqn)
                continue
            if include and not _glob_match_any(fqn, include):
                skipped_by_include.append(fqn)
                continue
            if exclude and _glob_match_any(fqn, exclude):
                skipped_by_exclude.append(fqn)
                continue
        cols = introspect_columns(pg_conn, schema, name)
        discovered.append(
            DiscoveredObject(schema=schema, name=name, kind=kind, columns=tuple(cols))
        )

    log.info(
        "discovery.summary",
        total_in_db=len(raw),
        kept=len(discovered),
        skipped_kind=len(skipped_by_kind),
        skipped_include=len(skipped_by_include),
        skipped_exclude=len(skipped_by_exclude),
        include_globs=include,
        exclude_globs=exclude,
        kinds=sorted(kinds),
    )
    if skipped_by_exclude:
        log.debug("discovery.excluded", objects=skipped_by_exclude)
    return discovered
