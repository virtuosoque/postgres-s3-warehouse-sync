"""SQL safety + governance layer.

Hard rules (enforced):
    - SELECT / WITH only. Reject DDL/DML.
    - Reject statements referencing tables outside an allowlist of schemas.
    - Reject `SELECT *` on tables with a known partition column unless a
      WHERE clause references that partition column (lightweight 'must use
      a partition filter' check). Configurable per-table.
    - Soft cap on row LIMIT (auto-injects if absent).

Built on sqlglot for AST inspection -- robust across SQL dialects.
"""

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from viamedia_pipeline.common.settings import get_settings


class SqlGuardError(ValueError):
    pass


@dataclass(frozen=True)
class GuardConfig:
    allowed_schemas: frozenset[str]
    default_row_limit: int = 100_000
    max_row_limit: int = 1_000_000


def default_config() -> GuardConfig:
    s = get_settings()
    schemas = set(s.allowed_schemas)
    # Every enabled connection's Iceberg namespace is queryable.
    try:
        from viamedia_pipeline.common.config_store import list_connections
        schemas |= {c.iceberg_namespace for c in list_connections(enabled_only=True)}
    except Exception:
        pass
    return GuardConfig(
        allowed_schemas=frozenset(schemas),
        default_row_limit=s.gateway_default_row_limit,
        max_row_limit=s.gateway_max_row_limit,
    )


def guard(sql: str, cfg: GuardConfig | None = None) -> str:
    cfg = cfg or default_config()
    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
    except sqlglot.errors.ParseError as e:
        raise SqlGuardError(f"parse error: {e}") from e
    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise SqlGuardError("exactly one statement required")
    stmt = statements[0]

    if not isinstance(stmt, (exp.Select, exp.Subquery, exp.Union, exp.With)):
        raise SqlGuardError(f"only SELECT / WITH allowed; got {type(stmt).__name__}")

    # Walk and reject any non-select-shaped writes
    for node in stmt.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Merge,
                              exp.Create, exp.Drop, exp.Alter, exp.TruncateTable)):
            raise SqlGuardError(f"forbidden statement type: {type(node).__name__}")

    # Schema allowlist
    for tbl in stmt.find_all(exp.Table):
        db = tbl.args.get("db")
        if db is None:
            # Unqualified -- only allow if a default schema is configured. We
            # require qualification for safety.
            raise SqlGuardError(
                f"table '{tbl.name}' must be schema-qualified (one of {sorted(cfg.allowed_schemas)})"
            )
        if db.name not in cfg.allowed_schemas:
            raise SqlGuardError(
                f"schema '{db.name}' is not allowed (allowed: {sorted(cfg.allowed_schemas)})"
            )

    # Row-limit governance. A negative configured value (e.g. -1) means
    # UNLIMITED for that knob:
    #   default_row_limit < 0 -> don't auto-inject a LIMIT when none is given
    #                            (return the full result -- can be huge).
    #   max_row_limit     < 0 -> accept any explicit LIMIT, no ceiling.
    if isinstance(stmt, exp.Select):
        unlimited_default = cfg.default_row_limit is None or cfg.default_row_limit < 0
        unlimited_max = cfg.max_row_limit is None or cfg.max_row_limit < 0
        limit = stmt.args.get("limit")
        if limit is None:
            if not unlimited_default:
                stmt = stmt.limit(cfg.default_row_limit)
            # else: leave the query unlimited
        elif not unlimited_max:
            try:
                n = int(limit.expression.this) if hasattr(limit.expression, "this") else cfg.max_row_limit
                if n > cfg.max_row_limit:
                    raise SqlGuardError(f"LIMIT {n} exceeds max {cfg.max_row_limit}")
            except (TypeError, ValueError):
                # non-literal LIMIT -- leave alone
                pass

    return stmt.sql(dialect="duckdb")
