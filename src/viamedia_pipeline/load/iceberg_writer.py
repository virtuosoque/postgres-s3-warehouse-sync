"""Iceberg catalog + table operations via PyIceberg's SqlCatalog (Postgres).

The catalog tables live in the metadata Postgres. Each *connection* targets its
own warehouse (``s3://<lake_bucket>/curated/``) and Iceberg namespace, with its
own AWS credentials, so we build/cache one catalog per connection.
"""

import os

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.metadata_db import connection as metadata_connection
from viamedia_pipeline.common.metadata_db import metadata_dsn
from viamedia_pipeline.common.settings import get_settings

log = get_logger(__name__)

_CATALOGS: dict[int, Catalog] = {}


def catalog(conn: Connection) -> Catalog:
    cat = _CATALOGS.get(conn.id)
    if cat is not None:
        return cat

    s = get_settings()
    props = {
        "type": "sql",
        "uri": _to_sqlalchemy_url(metadata_dsn()),
        "warehouse": conn.warehouse_s3,            # s3://<bucket>/curated/
        "s3.region": conn.aws_region,
        "init_catalog_tables": "true",
    }
    # Per-connection AWS credentials for PyIceberg's FileIO.
    if conn.aws_access_key and conn.aws_secret_key:
        props["s3.access-key-id"] = conn.aws_access_key
        props["s3.secret-access-key"] = conn.aws_secret_key
    # LocalStack endpoint (FileIO does not read AWS_ENDPOINT_URL on its own).
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        props["s3.endpoint"] = endpoint
        props["s3.path-style-access"] = "true"
        props.setdefault("s3.access-key-id", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        props.setdefault("s3.secret-access-key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))

    cat = load_catalog(s.iceberg_catalog_name, **props)
    _CATALOGS[conn.id] = cat
    return cat


def _to_sqlalchemy_url(dsn: str) -> str:
    if dsn.startswith("postgresql+"):
        return dsn
    for prefix in ("postgresql://", "postgres://"):
        if dsn.startswith(prefix):
            return "postgresql+psycopg://" + dsn[len(prefix):]
    return dsn


def ensure_namespace(conn: Connection) -> None:
    cat = catalog(conn)
    try:
        cat.create_namespace(conn.iceberg_namespace)
        log.info("iceberg.namespace.created", ns=conn.iceberg_namespace)
    except Exception as e:
        if "AlreadyExistsError" not in repr(e) and "already exists" not in repr(e).lower():
            raise


def create_or_get_table(
    conn: Connection,
    table: str,
    iceberg_schema: IcebergSchema,
    *,
    partition_by: list[tuple[str, str, int | None]] | None = None,
) -> Table:
    s = get_settings()
    cat = catalog(conn)
    ident = (conn.iceberg_namespace, table)

    try:
        return cat.load_table(ident)
    except NoSuchTableError:
        pass  # genuinely new -> create below
    except Exception as e:  # noqa: BLE001
        # Registered in the catalog but its metadata couldn't be loaded. If the
        # metadata.json is *confirmed missing* in S3 (e.g. the bucket folder was
        # deleted), the catalog row is orphaned -> drop it and recreate. If the
        # object still exists, this is a transient error -> re-raise (never
        # destroy a live table on a blip).
        loc = _catalog_metadata_location(conn, table)
        if loc and not _s3_object_exists(conn, loc):
            log.warning("iceberg.table.orphaned_recreating",
                        table=f"{conn.iceberg_namespace}.{table}", metadata=loc)
            _drop_catalog_entry(conn, cat, table)
        else:
            raise

    spec = _build_partition_spec(iceberg_schema, partition_by or [])
    props = {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": s.parquet_compression,
        "write.target-file-size-bytes": str(512 * 1024**2),
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "20",
        "format-version": "2",
    }
    try:
        tbl = cat.create_table(identifier=ident, schema=iceberg_schema,
                               partition_spec=spec, properties=props)
    except Exception as e:  # noqa: BLE001
        # Lost a race / stale row -> drop the catalog entry and create once more.
        if "already exists" not in repr(e).lower():
            raise
        _drop_catalog_entry(conn, cat, table)
        tbl = cat.create_table(identifier=ident, schema=iceberg_schema,
                               partition_spec=spec, properties=props)
    log.info("iceberg.table.created", table=f"{conn.iceberg_namespace}.{table}")
    return tbl


def _catalog_metadata_location(conn: Connection, table: str) -> str | None:
    """The metadata_location the SqlCatalog has registered for this table."""
    try:
        with metadata_connection() as c, c.cursor() as cur:
            cur.execute(
                "SELECT metadata_location FROM iceberg_tables "
                "WHERE catalog_name = %s AND table_namespace = %s AND table_name = %s",
                (get_settings().iceberg_catalog_name, conn.iceberg_namespace, table),
            )
            row = cur.fetchone()
        return row["metadata_location"] if row else None
    except Exception:  # noqa: BLE001
        return None


def _s3_object_exists(conn: Connection, s3_uri: str) -> bool:
    from viamedia_pipeline.common.s3 import s3_client
    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    try:
        s3_client(conn).head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001
        return False


def _drop_catalog_entry(conn: Connection, cat: Catalog, table: str) -> None:
    """Remove a stale catalog row (does NOT touch S3 data)."""
    ident = (conn.iceberg_namespace, table)
    try:
        cat.drop_table(ident)
        return
    except Exception:  # noqa: BLE001
        pass  # drop_table may itself try to read missing metadata -> SQL fallback
    try:
        with metadata_connection() as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM iceberg_tables "
                "WHERE catalog_name = %s AND table_namespace = %s AND table_name = %s",
                (get_settings().iceberg_catalog_name, conn.iceberg_namespace, table),
            )
            c.commit()
    except Exception as e:  # noqa: BLE001
        log.warning("iceberg.catalog_entry.delete_failed", table=table, error=str(e))


def _build_partition_spec(
    schema: IcebergSchema,
    partition_by: list[tuple[str, str, int | None]],
) -> PartitionSpec:
    fields: list[PartitionField] = []
    for idx, (col, transform_name, n_buckets) in enumerate(partition_by, start=1000):
        src = schema.find_field(col)
        if transform_name == "identity":
            transform = IdentityTransform()
            partition_name = col
        elif transform_name == "day":
            transform = DayTransform()
            partition_name = f"{col}_day"
        elif transform_name == "bucket":
            if not n_buckets:
                raise ValueError("bucket transform requires n_buckets")
            transform = BucketTransform(num_buckets=n_buckets)
            partition_name = f"{col}_bucket"
        else:
            raise ValueError(f"unknown transform: {transform_name}")
        fields.append(
            PartitionField(
                source_id=src.field_id, field_id=idx,
                transform=transform, name=partition_name,
            )
        )
    return PartitionSpec(*fields) if fields else PartitionSpec()


def commit_parquet_files(table: Table, s3_uris: list[str]) -> None:
    if not s3_uris:
        log.warning("iceberg.commit.empty")
        return
    table.add_files(file_paths=s3_uris)
    log.info("iceberg.commit.success", n_files=len(s3_uris))


def evolve_table_schema(conn: Connection, table: str, source_schema: IcebergSchema) -> Table:
    """Reconcile the Iceberg table's schema with the current source schema, then
    return the reloaded table.

    Uses PyIceberg `union_by_name`, which (matching by column name):
      - ADDS columns present in the source but not the table (old rows read NULL),
      - applies SAFE type promotions (int->long, float->double, decimal precision
        increase),
      - never drops columns (a column removed at the source stays in the lake and
        is simply NULL for new rows -- standard lakehouse behavior).

    Incompatible changes (e.g. string<->int, decimal SCALE changes) are not valid
    Iceberg promotions; we log and skip them rather than fail the sync (such a
    column would need a manual re-bootstrap).
    """
    cat = catalog(conn)
    ident = (conn.iceberg_namespace, table)
    tbl = cat.load_table(ident)
    try:
        with tbl.update_schema() as upd:
            upd.union_by_name(source_schema)
        log.info("iceberg.schema.evolved", table=f"{conn.iceberg_namespace}.{table}")
    except Exception as e:  # noqa: BLE001
        log.warning("iceberg.schema.evolve_skipped",
                    table=f"{conn.iceberg_namespace}.{table}", error=str(e))
    return cat.load_table(ident)
