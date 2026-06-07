"""Database-backed configuration store (UI-managed).

Replaces most of the old .env config. Lives in the metadata Postgres:
  - pipeline_connections   : one source database each (own bucket + namespace),
                             with Fernet-encrypted secrets
  - pipeline_sync_selection: explicit per-object sync selection (the UI checklist)
  - pipeline_settings      : global operational settings (tuning, gateway params)

Only bootstrap values (METADATA_PG_DSN, METADATA_PG_SCHEMA, CONFIG_ENCRYPTION_KEY,
LOG_LEVEL) stay in the environment — they're needed to reach and decrypt this store.

Secrets are decrypted in `Connection` for internal use (pg/s3/iceberg). Use
`Connection.public_dict()` when returning to the UI so secrets are masked.
"""

import threading
import time
from dataclasses import dataclass, field

from viamedia_pipeline.common import crypto
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.metadata_db import connection

log = get_logger(__name__)

_CACHE_TTL_SECONDS = 30


@dataclass(frozen=True)
class Connection:
    id: int
    name: str
    enabled: bool
    source_dsn: str                      # decrypted
    aws_access_key: str | None           # decrypted
    aws_secret_key: str | None           # decrypted
    aws_region: str
    lake_bucket: str
    iceberg_namespace: str
    sync_include: str = ""
    sync_exclude: str = ""
    sync_object_types: str = "TABLE,VIEW,MATERIALIZED_VIEW"
    sync_schema_blacklist: str = "pg_catalog,information_schema,pg_toast"

    # --- single-bucket layout: one bucket, fixed prefixes -----------------
    @property
    def raw_prefix(self) -> str:
        return "raw"

    @property
    def results_prefix(self) -> str:
        return "gateway-results"

    @property
    def warehouse_s3(self) -> str:
        """Iceberg warehouse root for this connection."""
        return f"s3://{self.lake_bucket}/curated/"

    # --- discovery glob/type helpers (mirror old Settings properties) -----
    @property
    def sync_include_globs(self) -> list[str]:
        return [g.strip() for g in self.sync_include.split(",") if g.strip()]

    @property
    def sync_exclude_globs(self) -> list[str]:
        return [g.strip() for g in self.sync_exclude.split(",") if g.strip()]

    @property
    def sync_object_types_set(self) -> set[str]:
        return {t.strip().upper() for t in self.sync_object_types.split(",") if t.strip()}

    @property
    def sync_schema_blacklist_set(self) -> set[str]:
        return {s.strip() for s in self.sync_schema_blacklist.split(",") if s.strip()}

    @property
    def dsn_password(self) -> str:
        return parse_dsn(self.source_dsn).get("db_password", "")

    def public_dict(self) -> dict:
        """Serializable form with secrets masked, for API responses. Includes the
        parsed DB connection components (no password) for the edit form."""
        parts = parse_dsn(self.source_dsn)
        return {
            "id": self.id,
            "name": self.name,
            "enabled": self.enabled,
            "db_host": parts["db_host"],
            "db_port": parts["db_port"],
            "db_name": parts["db_name"],
            "db_user": parts["db_user"],
            "db_sslmode": parts["db_sslmode"],
            "aws_access_key": _mask(self.aws_access_key),
            "aws_secret_key": "********" if self.aws_secret_key else None,
            "aws_region": self.aws_region,
            "lake_bucket": self.lake_bucket,
            "iceberg_namespace": self.iceberg_namespace,
        }


def build_dsn(*, db_host: str, db_port: int, db_name: str, db_user: str,
              db_password: str, db_sslmode: str = "require") -> str:
    from urllib.parse import quote
    user = quote(db_user or "", safe="")
    pw = quote(db_password or "", safe="")
    return f"postgresql://{user}:{pw}@{db_host}:{db_port}/{db_name}?sslmode={db_sslmode}"


def parse_dsn(dsn: str | None) -> dict:
    """Parse a libpq DSN/URL into components. Uses libpq's own parser (via
    psycopg) so it's robust to special characters (#, &, !) in passwords that
    would break a naive urlparse."""
    out = {"db_host": "", "db_port": 5432, "db_name": "", "db_user": "",
           "db_password": "", "db_sslmode": "require"}
    if not dsn:
        return out
    try:
        from psycopg.conninfo import conninfo_to_dict
        d = conninfo_to_dict(dsn)
        out.update({
            "db_host": d.get("host", "") or "",
            "db_port": int(d.get("port") or 5432),
            "db_name": d.get("dbname", "") or "",
            "db_user": d.get("user", "") or "",
            "db_password": d.get("password", "") or "",
            "db_sslmode": d.get("sslmode", "require") or "require",
        })
    except Exception:
        pass
    return out


def normalize_dsn(dsn: str | None) -> str | None:
    """Re-encode a (possibly special-char-laden) DSN into a clean URL form that
    round-trips through parse_dsn/build_dsn."""
    if not dsn:
        return dsn
    p = parse_dsn(dsn)
    if not p["db_host"]:
        return dsn
    return build_dsn(db_host=p["db_host"], db_port=p["db_port"], db_name=p["db_name"],
                     db_user=p["db_user"], db_password=p["db_password"], db_sslmode=p["db_sslmode"])


def _mask(secret: str | None) -> str | None:
    if not secret:
        return secret
    # show only a short suffix so the user can recognize which value it is
    tail = secret[-4:] if len(secret) > 4 else ""
    return f"********{tail}"


# --- connections cache (short TTL; cleared on writes) ----------------------
_lock = threading.Lock()
_cache: tuple[float, list[Connection]] | None = None

_CONN_COLS = (
    "id, name, enabled, source_dsn_enc, aws_access_key_enc, aws_secret_key_enc, "
    "aws_region, lake_bucket, iceberg_namespace, sync_include, sync_exclude, "
    "sync_object_types, sync_schema_blacklist"
)


def _row_to_connection(r: dict) -> Connection:
    return Connection(
        id=r["id"],
        name=r["name"],
        enabled=r["enabled"],
        source_dsn=crypto.decrypt(r["source_dsn_enc"]),
        aws_access_key=crypto.decrypt(r["aws_access_key_enc"]),
        aws_secret_key=crypto.decrypt(r["aws_secret_key_enc"]),
        aws_region=r["aws_region"],
        lake_bucket=r["lake_bucket"],
        iceberg_namespace=r["iceberg_namespace"],
        sync_include=r["sync_include"],
        sync_exclude=r["sync_exclude"],
        sync_object_types=r["sync_object_types"],
        sync_schema_blacklist=r["sync_schema_blacklist"],
    )


def _load_all() -> list[Connection]:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_CONN_COLS} FROM pipeline_connections ORDER BY id")
        return [_row_to_connection(r) for r in cur.fetchall()]


def invalidate_cache() -> None:
    global _cache
    with _lock:
        _cache = None


def list_connections(*, enabled_only: bool = False, force: bool = False) -> list[Connection]:
    global _cache
    now = time.monotonic()
    with _lock:
        if force or _cache is None or (now - _cache[0]) >= _CACHE_TTL_SECONDS:
            _cache = (now, _load_all())
        conns = _cache[1]
    return [c for c in conns if c.enabled] if enabled_only else list(conns)


def get_connection(connection_id: int) -> Connection | None:
    for c in list_connections():
        if c.id == connection_id:
            return c
    for c in list_connections(force=True):
        if c.id == connection_id:
            return c
    return None


def get_connection_by_name(name: str) -> Connection | None:
    for c in list_connections():
        if c.name == name:
            return c
    return None


def upsert_connection(
    *,
    connection_id: int | None,
    name: str,
    enabled: bool,
    source_dsn: str | None,
    aws_access_key: str | None,
    aws_secret_key: str | None,
    aws_region: str,
    lake_bucket: str,
    iceberg_namespace: str,
    sync_include: str = "",
    sync_exclude: str = "",
    sync_object_types: str = "TABLE,VIEW,MATERIALIZED_VIEW",
    sync_schema_blacklist: str = "pg_catalog,information_schema,pg_toast",
) -> int:
    """Insert or update a connection. On update, secret fields left as None keep
    their existing stored value (so the UI never has to re-send secrets)."""
    with connection() as conn, conn.cursor() as cur:
        if connection_id is None:
            cur.execute(
                """
                INSERT INTO pipeline_connections
                    (name, enabled, source_dsn_enc, aws_access_key_enc, aws_secret_key_enc,
                     aws_region, lake_bucket, iceberg_namespace,
                     sync_include, sync_exclude, sync_object_types, sync_schema_blacklist)
                VALUES (%(name)s, %(enabled)s, %(dsn)s, %(ak)s, %(sk)s,
                        %(region)s, %(bucket)s, %(ns)s,
                        %(inc)s, %(exc)s, %(types)s, %(black)s)
                RETURNING id
                """,
                {
                    "name": name, "enabled": enabled,
                    "dsn": crypto.encrypt(source_dsn),
                    "ak": crypto.encrypt(aws_access_key),
                    "sk": crypto.encrypt(aws_secret_key),
                    "region": aws_region, "bucket": lake_bucket, "ns": iceberg_namespace,
                    "inc": sync_include, "exc": sync_exclude,
                    "types": sync_object_types, "black": sync_schema_blacklist,
                },
            )
            new_id = cur.fetchone()["id"]
        else:
            # COALESCE keeps existing encrypted secret when the new value is NULL
            cur.execute(
                """
                UPDATE pipeline_connections SET
                    name = %(name)s, enabled = %(enabled)s,
                    source_dsn_enc      = COALESCE(%(dsn)s, source_dsn_enc),
                    aws_access_key_enc  = COALESCE(%(ak)s, aws_access_key_enc),
                    aws_secret_key_enc  = COALESCE(%(sk)s, aws_secret_key_enc),
                    aws_region = %(region)s, lake_bucket = %(bucket)s,
                    iceberg_namespace = %(ns)s,
                    sync_include = %(inc)s, sync_exclude = %(exc)s,
                    sync_object_types = %(types)s, sync_schema_blacklist = %(black)s,
                    updated_at = now()
                WHERE id = %(id)s
                """,
                {
                    "id": connection_id, "name": name, "enabled": enabled,
                    "dsn": crypto.encrypt(source_dsn),
                    "ak": crypto.encrypt(aws_access_key),
                    "sk": crypto.encrypt(aws_secret_key),
                    "region": aws_region, "bucket": lake_bucket, "ns": iceberg_namespace,
                    "inc": sync_include, "exc": sync_exclude,
                    "types": sync_object_types, "black": sync_schema_blacklist,
                },
            )
            new_id = connection_id
        conn.commit()
    invalidate_cache()
    log.info("config.connection.upsert", id=new_id, name=name)
    return new_id


def delete_connection(connection_id: int) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline_connections WHERE id = %s", (connection_id,))
        conn.commit()
    invalidate_cache()
    log.info("config.connection.delete", id=connection_id)


# --- per-object sync selection --------------------------------------------
@dataclass(frozen=True)
class SelectionRow:
    schema: str
    name: str
    kind: str
    enabled: bool


def get_selection(connection_id: int) -> list[SelectionRow]:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT schema_name, object_name, kind, enabled FROM pipeline_sync_selection "
            "WHERE connection_id = %s ORDER BY schema_name, object_name",
            (connection_id,),
        )
        return [
            SelectionRow(schema=r["schema_name"], name=r["object_name"],
                         kind=r["kind"], enabled=r["enabled"])
            for r in cur.fetchall()
        ]


def enabled_fqns(connection_id: int) -> set[str] | None:
    """Set of `schema.name` explicitly enabled for this connection, or None if
    the connection has no selection rows at all (caller falls back to globs)."""
    rows = get_selection(connection_id)
    if not rows:
        return None
    return {f"{r.schema}.{r.name}" for r in rows if r.enabled}


def set_selection(connection_id: int, items: list[dict]) -> None:
    """Replace the selection for a connection. Each item: {schema, name, kind, enabled}."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM pipeline_sync_selection WHERE connection_id = %s", (connection_id,))
        for it in items:
            cur.execute(
                """
                INSERT INTO pipeline_sync_selection
                    (connection_id, schema_name, object_name, kind, enabled)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (connection_id, it["schema"], it["name"], it.get("kind", "TABLE"),
                 bool(it.get("enabled", True))),
            )
        conn.commit()
    log.info("config.selection.set", connection_id=connection_id, n=len(items))


# --- global operational settings (key/value) -------------------------------
_settings_cache: tuple[float, dict[str, str]] | None = None


def all_settings(force: bool = False) -> dict[str, str]:
    global _settings_cache
    now = time.monotonic()
    with _lock:
        if force or _settings_cache is None or (now - _settings_cache[0]) >= _CACHE_TTL_SECONDS:
            with connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT key, value FROM pipeline_settings")
                _settings_cache = (now, {r["key"]: r["value"] for r in cur.fetchall()})
        return dict(_settings_cache[1])


def get_setting(key: str, default: str | None = None) -> str | None:
    return all_settings().get(key, default)


def ensure_seed_connection() -> int | None:
    """One-time migration: if no connections exist yet but legacy .env values are
    present, seed a single connection so an existing single-DB deployment keeps
    working after the .env -> UI move. The legacy curated bucket becomes the new
    single lake bucket (raw/curated/gateway-results live under it as prefixes)."""
    if list_connections(force=True):
        return None
    from viamedia_pipeline.common.settings import get_settings
    s = get_settings()
    bucket = s.curated_bucket_legacy or s.raw_bucket_legacy
    if not s.pg_replica_dsn_legacy or not bucket:
        return None
    cid = upsert_connection(
        connection_id=None,
        name="default",
        enabled=True,
        source_dsn=normalize_dsn(s.pg_replica_dsn_legacy),
        aws_access_key=s.aws_access_key_legacy or None,
        aws_secret_key=s.aws_secret_key_legacy or None,
        aws_region=s.aws_region,
        lake_bucket=bucket,
        iceberg_namespace=s.iceberg_namespace_legacy or "viamedia_lake",
        sync_include=s.sync_include_legacy,
        sync_exclude=s.sync_exclude_legacy,
        sync_object_types=s.sync_object_types_legacy,
        sync_schema_blacklist=s.sync_schema_blacklist_legacy,
    )
    log.info("config.seed_connection.created", id=cid, name="default", bucket=bucket)
    return cid


def set_setting(key: str, value: str | None) -> None:
    global _settings_cache
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_settings (key, value, updated_at) VALUES (%s, %s, now())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            """,
            (key, value),
        )
        conn.commit()
    with _lock:
        _settings_cache = None
