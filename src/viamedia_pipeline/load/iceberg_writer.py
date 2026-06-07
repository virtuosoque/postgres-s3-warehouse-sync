"""Iceberg catalog + table operations via PyIceberg's SqlCatalog (Postgres).

The catalog tables live in the metadata Postgres. Each *connection* targets its
own warehouse (``s3://<lake_bucket>/curated/``) and Iceberg namespace, with its
own AWS credentials, so we build/cache one catalog per connection.
"""

import os

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform

from viamedia_pipeline.common.config_store import Connection
from viamedia_pipeline.common.logging import get_logger
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
    except Exception:
        pass

    spec = _build_partition_spec(iceberg_schema, partition_by or [])
    tbl = cat.create_table(
        identifier=ident,
        schema=iceberg_schema,
        partition_spec=spec,
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": s.parquet_compression,
            "write.target-file-size-bytes": str(512 * 1024**2),
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "20",
            "format-version": "2",
        },
    )
    log.info("iceberg.table.created", table=f"{conn.iceberg_namespace}.{table}")
    return tbl


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
