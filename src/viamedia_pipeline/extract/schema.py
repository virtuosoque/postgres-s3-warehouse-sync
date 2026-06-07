"""PostgreSQL -> PyArrow -> Iceberg schema mapping.

We rely on psycopg's binary COPY type-oid resolution, so the PyArrow schema
must match column order exactly. Iceberg schema is derived from the same.
"""

from dataclasses import dataclass

import psycopg
import pyarrow as pa
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

from viamedia_pipeline.common.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str            # e.g. "bigint", "timestamp with time zone"
    nullable: bool
    ordinal: int
    oid: int = 0            # pg_attribute.atttypid -- needed for binary COPY decoding


def introspect_columns(conn: psycopg.Connection, schema: str, table: str) -> list[Column]:
    sql = """
        SELECT a.attname AS column_name,
               format_type(a.atttypid, a.atttypmod) AS data_type,
               NOT a.attnotnull AS nullable,
               a.attnum AS ordinal,
               a.atttypid AS type_oid
        FROM   pg_attribute a
        JOIN   pg_class     c ON c.oid = a.attrelid
        JOIN   pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = %s
          AND  c.relname = %s
          AND  a.attnum > 0
          AND  NOT a.attisdropped
        ORDER  BY a.attnum
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [
            Column(name=r[0], pg_type=r[1], nullable=r[2], ordinal=r[3], oid=r[4])
            for r in cur.fetchall()
        ]


def pg_oids_for(columns: list[Column]) -> list[int]:
    """Ordered list of PostgreSQL type OIDs, for psycopg `copy.set_types()`.

    Binary COPY does not carry type info in its stream, so the reader must
    declare the column types up front or every value comes back as raw bytes.
    """
    return [c.oid for c in sorted(columns, key=lambda c: c.ordinal)]


_PG_TO_ARROW = {
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "boolean": pa.bool_(),
    "real": pa.float32(),
    "double precision": pa.float64(),
    "text": pa.large_string(),
    "uuid": pa.string(),
    "date": pa.date32(),
    "timestamp without time zone": pa.timestamp("us"),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
    "bytea": pa.large_binary(),
    "json": pa.large_string(),
    "jsonb": pa.large_string(),
}


def pg_to_arrow_type(pg_type: str) -> pa.DataType:
    base = pg_type.lower()
    if base in _PG_TO_ARROW:
        return _PG_TO_ARROW[base]
    if base.startswith("character varying") or base.startswith("varchar") or base.startswith("character"):
        return pa.large_string()
    if base.startswith("numeric") or base.startswith("decimal"):
        # decimal(p,s) — parse precision/scale; default if unspecified
        if "(" in base:
            inside = base[base.index("(") + 1 : base.index(")")]
            parts = [int(x.strip()) for x in inside.split(",")]
            precision = parts[0]
            scale = parts[1] if len(parts) > 1 else 0
            return pa.decimal128(precision, scale)
        return pa.decimal128(38, 9)
    if base.endswith("[]"):
        inner = pg_to_arrow_type(base[:-2])
        return pa.large_list(inner)
    log.warning("schema.unknown_pg_type", pg_type=pg_type, fallback="large_string")
    return pa.large_string()


def arrow_schema_for(columns: list[Column]) -> pa.Schema:
    fields = [
        pa.field(c.name, pg_to_arrow_type(c.pg_type), nullable=c.nullable)
        for c in columns
    ]
    return pa.schema(fields)


def _arrow_to_iceberg_type(t: pa.DataType):
    if pa.types.is_int16(t) or pa.types.is_int32(t):
        return IntegerType()
    if pa.types.is_int64(t):
        return LongType()
    if pa.types.is_boolean(t):
        return BooleanType()
    if pa.types.is_float32(t):
        return FloatType()
    if pa.types.is_float64(t):
        return DoubleType()
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return StringType()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return BinaryType()
    if pa.types.is_date(t):
        return DateType()
    if pa.types.is_timestamp(t):
        return TimestamptzType() if t.tz else TimestampType()
    if pa.types.is_decimal(t):
        return DecimalType(t.precision, t.scale)
    raise ValueError(f"unsupported arrow type for iceberg: {t}")


def iceberg_schema_for(columns: list[Column]) -> IcebergSchema:
    fields: list[NestedField] = []
    for idx, c in enumerate(columns, start=1):
        # uuid columns get UUIDType in Iceberg even though Arrow is string
        if c.pg_type.lower() == "uuid":
            ftype = UUIDType()
        else:
            ftype = _arrow_to_iceberg_type(pg_to_arrow_type(c.pg_type))
        fields.append(
            NestedField(field_id=idx, name=c.name, field_type=ftype, required=not c.nullable)
        )
    return IcebergSchema(*fields)
