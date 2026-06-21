"""Global settings.

Two tiers (see config_store for the per-connection tier):

  * Bootstrap + global-operational values live here. Bootstrap (metadata DSN,
    log level, encryption key) come from the environment. Global-operational
    values (extractor/parquet/incremental tuning, gateway params) default from
    the environment but are OVERLAID at runtime from the DB-backed
    `pipeline_settings` table, so the UI can change them without a restart.

  * Per-connection values (source DSN, AWS creds, bucket, namespace, sync
    selection) are NOT here — they live on `config_store.Connection`.

The `*_legacy` fields exist only to seed "connection 1" from a pre-existing
.env during the transition; runtime code uses Connection instead.
"""

import time

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

def _as_bool(v: object) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# pipeline_settings key -> (Settings attribute, caster)
_OVERLAY: dict[str, tuple[str, type]] = {
    "extract_rows_per_chunk": ("extract_rows_per_chunk", int),
    "extract_row_group_rows": ("extract_row_group_rows", int),
    "parquet_compression": ("parquet_compression", str),
    "parquet_compression_level": ("parquet_compression_level", int),
    "incremental_overlap_seconds": ("incremental_overlap_seconds", int),
    "incremental_safety_gap_seconds": ("incremental_safety_gap_seconds", int),
    "iceberg_catalog_name": ("iceberg_catalog_name", str),
    "aws_region": ("aws_region", str),
    "gateway_jwt_audience": ("gateway_jwt_audience", str),
    "gateway_jwt_issuer": ("gateway_jwt_issuer", str),
    "gateway_jwks_url": ("gateway_jwks_url", str),
    "gateway_max_bytes_scanned": ("gateway_max_bytes_scanned", int),
    "gateway_query_timeout_seconds": ("gateway_query_timeout_seconds", int),
    "gateway_allowed_schemas": ("gateway_allowed_schemas", str),
    "gateway_view_refresh_seconds": ("gateway_view_refresh_seconds", int),
    "gateway_default_row_limit": ("gateway_default_row_limit", int),
    "gateway_max_row_limit": ("gateway_max_row_limit", int),
    "gateway_admin_sql_unrestricted": ("gateway_admin_sql_unrestricted", _as_bool),
    "sync_drop_removed_columns": ("sync_drop_removed_columns", _as_bool),
    "log_level": ("log_level", str),
}

# keys that the Settings UI is allowed to edit (exposed by the gateway)
EDITABLE_SETTING_KEYS = tuple(_OVERLAY.keys())


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Bootstrap (env)
    metadata_pg_dsn: str = Field("", alias="METADATA_PG_DSN")
    metadata_pg_schema: str = Field("pipeline", alias="METADATA_PG_SCHEMA")
    aws_region: str = Field("us-east-1", alias="AWS_REGION")
    iceberg_catalog_name: str = Field("pg", alias="ICEBERG_CATALOG_NAME")

    # Global operational (env defaults, overlaid from pipeline_settings)
    extract_rows_per_chunk: int = Field(1_000_000, alias="EXTRACT_ROWS_PER_CHUNK")
    extract_row_group_rows: int = Field(100_000, alias="EXTRACT_ROW_GROUP_ROWS")
    parquet_compression: str = Field("zstd", alias="PARQUET_COMPRESSION")
    parquet_compression_level: int = Field(3, alias="PARQUET_COMPRESSION_LEVEL")
    incremental_overlap_seconds: int = Field(300, alias="INCREMENTAL_OVERLAP_SECONDS")
    incremental_safety_gap_seconds: int = Field(30, alias="INCREMENTAL_SAFETY_GAP_SECONDS")

    gateway_jwt_audience: str = Field("viamedia-gateway", alias="GATEWAY_JWT_AUDIENCE")
    gateway_jwt_issuer: str = Field("", alias="GATEWAY_JWT_ISSUER")
    gateway_jwks_url: str = Field("", alias="GATEWAY_JWKS_URL")
    gateway_max_bytes_scanned: int = Field(10 * 1024**3, alias="GATEWAY_MAX_BYTES_SCANNED")
    gateway_query_timeout_seconds: int = Field(120, alias="GATEWAY_QUERY_TIMEOUT_SECONDS")
    gateway_allowed_schemas: str = Field("viamedia_lake", alias="GATEWAY_ALLOWED_SCHEMAS")
    gateway_view_refresh_seconds: int = Field(60, alias="GATEWAY_VIEW_REFRESH_SECONDS")
    # Query result-row governance (editable from the UI). -1 = unlimited.
    #   default_row_limit: LIMIT auto-applied when a query has none.
    #   max_row_limit:     largest explicit LIMIT allowed.
    gateway_default_row_limit: int = Field(100_000, alias="GATEWAY_DEFAULT_ROW_LIMIT")
    gateway_max_row_limit: int = Field(1_000_000, alias="GATEWAY_MAX_ROW_LIMIT")
    # When true, ADMIN login users can run unrestricted SQL in the Query tab
    # (guard bypassed). Off by default -- even admins stay read-only until enabled.
    gateway_admin_sql_unrestricted: bool = Field(False, alias="GATEWAY_ADMIN_SQL_UNRESTRICTED")
    # When true, a column dropped at the source is also DROPPED from the lake
    # table (destructive). Off by default = removed columns are kept as NULL.
    # Never drops the pk / watermark / partition columns.
    sync_drop_removed_columns: bool = Field(False, alias="SYNC_DROP_REMOVED_COLUMNS")

    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # Legacy env, used ONLY to seed connection 1 during the .env -> UI transition.
    pg_replica_dsn_legacy: str = Field("", alias="PG_REPLICA_DSN")
    aws_access_key_legacy: str = Field("", alias="AWS_ACCESS_KEY_ID")
    aws_secret_key_legacy: str = Field("", alias="AWS_SECRET_ACCESS_KEY")
    raw_bucket_legacy: str = Field("", alias="RAW_BUCKET")
    curated_bucket_legacy: str = Field("", alias="CURATED_BUCKET")
    iceberg_namespace_legacy: str = Field("viamedia_lake", alias="ICEBERG_NAMESPACE")
    sync_include_legacy: str = Field("", alias="SYNC_INCLUDE")
    sync_exclude_legacy: str = Field("", alias="SYNC_EXCLUDE")
    sync_object_types_legacy: str = Field("TABLE,VIEW,MATERIALIZED_VIEW", alias="SYNC_OBJECT_TYPES")
    sync_schema_blacklist_legacy: str = Field(
        "pg_catalog,information_schema,pg_toast", alias="SYNC_SCHEMA_BLACKLIST"
    )

    @property
    def allowed_schemas(self) -> set[str]:
        return {s.strip() for s in self.gateway_allowed_schemas.split(",") if s.strip()}


_cache: tuple[float, Settings] | None = None
_TTL = 30.0


def get_settings() -> Settings:
    """Settings with the DB-backed operational overlay applied. Rebuilt every
    _TTL seconds so UI edits to pipeline_settings take effect without a restart.
    Falls back to env-only if the config store is unavailable (e.g. pre-migrate)."""
    global _cache
    now = time.monotonic()
    if _cache is not None and (now - _cache[0]) < _TTL:
        return _cache[1]

    s = Settings()  # type: ignore[call-arg]
    try:
        from viamedia_pipeline.common import config_store  # lazy: avoids import cycle
        overlay = config_store.all_settings()
        for key, value in overlay.items():
            if value is None or key not in _OVERLAY:
                continue
            attr, caster = _OVERLAY[key]
            try:
                setattr(s, attr, caster(value))
            except (ValueError, TypeError):
                pass
    except Exception:
        pass  # config store not ready yet — env defaults are fine

    _cache = (now, s)
    return s
