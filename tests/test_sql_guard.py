import pytest

from viamedia_pipeline.gateway.sql_guard import GuardConfig, SqlGuardError, guard


CFG = GuardConfig(allowed_schemas=frozenset({"viamedia_lake"}))


def test_allows_qualified_select():
    sql = "SELECT id, created_at FROM viamedia_lake.public__events LIMIT 10"
    out = guard(sql, CFG)
    assert "viamedia_lake" in out
    assert "LIMIT" in out.upper()


def test_rejects_unqualified():
    with pytest.raises(SqlGuardError, match="schema-qualified"):
        guard("SELECT * FROM events", CFG)


def test_rejects_disallowed_schema():
    with pytest.raises(SqlGuardError, match="not allowed"):
        guard("SELECT * FROM secrets.api_keys", CFG)


def test_rejects_dml():
    for stmt in [
        "DELETE FROM viamedia_lake.public__events",
        "INSERT INTO viamedia_lake.public__events VALUES (1)",
        "UPDATE viamedia_lake.public__events SET id = 1",
        "DROP TABLE viamedia_lake.public__events",
    ]:
        with pytest.raises(SqlGuardError):
            guard(stmt, CFG)


def test_injects_default_limit():
    out = guard("SELECT id FROM viamedia_lake.public__events", CFG)
    assert "LIMIT" in out.upper()


def test_rejects_oversized_limit():
    with pytest.raises(SqlGuardError, match="exceeds max"):
        guard(
            f"SELECT id FROM viamedia_lake.public__events LIMIT {CFG.max_row_limit + 1}",
            CFG,
        )
