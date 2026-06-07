"""Discovery filter tests.

The pg_class introspection itself is exercised by integration; here we unit-
test the glob filtering logic in isolation.
"""

from unittest.mock import MagicMock, patch

from viamedia_pipeline.extract.discovery import discover
from viamedia_pipeline.extract.schema import Column


def _fake_cols() -> list[Column]:
    return [
        Column(name="id",         pg_type="bigint",                       nullable=False, ordinal=1),
        Column(name="created_at", pg_type="timestamp with time zone",     nullable=False, ordinal=2),
        Column(name="updated_at", pg_type="timestamp with time zone",     nullable=False, ordinal=3),
    ]


def _patch_env(monkeypatch, **overrides) -> None:
    monkeypatch.setenv("SYNC_INCLUDE", overrides.get("include", ""))
    monkeypatch.setenv("SYNC_EXCLUDE", overrides.get("exclude", ""))
    monkeypatch.setenv("SYNC_OBJECT_TYPES", overrides.get("types", "TABLE,VIEW,MATERIALIZED_VIEW"))
    from viamedia_pipeline.common.settings import get_settings
    get_settings.cache_clear()


def _mock_conn(rows: list[tuple[str, str, str]]) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchall.return_value = rows
    return conn


def test_include_empty_means_all(monkeypatch):
    _patch_env(monkeypatch)
    conn = _mock_conn([("public", "events", "r"), ("public", "users", "r")])
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=_fake_cols()):
        result = discover(conn)
    assert {o.fqn for o in result} == {"public.events", "public.users"}


def test_include_globs(monkeypatch):
    _patch_env(monkeypatch, include="public.events,analytics.*")
    conn = _mock_conn([
        ("public", "events", "r"),
        ("public", "users", "r"),
        ("analytics", "daily_summary", "m"),
    ])
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=_fake_cols()):
        result = discover(conn)
    assert {o.fqn for o in result} == {"public.events", "analytics.daily_summary"}


def test_exclude_after_include(monkeypatch):
    _patch_env(monkeypatch, include="public.*", exclude="public.audit_log,public._archive_*")
    conn = _mock_conn([
        ("public", "events", "r"),
        ("public", "audit_log", "r"),
        ("public", "_archive_2024", "r"),
        ("public", "users", "r"),
    ])
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=_fake_cols()):
        result = discover(conn)
    assert {o.fqn for o in result} == {"public.events", "public.users"}


def test_filter_by_object_type(monkeypatch):
    _patch_env(monkeypatch, types="TABLE")
    conn = _mock_conn([
        ("public", "events", "r"),
        ("public", "events_view", "v"),
        ("public", "events_mv", "m"),
    ])
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=_fake_cols()):
        result = discover(conn)
    assert {o.fqn for o in result} == {"public.events"}


def test_capability_flags(monkeypatch):
    _patch_env(monkeypatch)
    conn = _mock_conn([("public", "events", "r")])
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=_fake_cols()):
        result = discover(conn)
    obj = result[0]
    assert obj.has_id is True
    assert obj.has_updated_at is True
    assert obj.supports_parallel_chunking is True
    assert obj.supports_incremental is True


def test_view_without_updated_at_is_full_refresh(monkeypatch):
    _patch_env(monkeypatch)
    conn = _mock_conn([("analytics", "summary_view", "v")])
    cols = [
        Column(name="id",     pg_type="bigint", nullable=False, ordinal=1),
        Column(name="amount", pg_type="numeric", nullable=False, ordinal=2),
    ]
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=cols):
        result = discover(conn)
    obj = result[0]
    assert obj.kind == "VIEW"
    assert obj.supports_incremental is False
    assert obj.supports_parallel_chunking is True  # id is bigint


def test_uuid_id_falls_back_to_full_chunk(monkeypatch):
    _patch_env(monkeypatch)
    conn = _mock_conn([("public", "sessions", "r")])
    cols = [
        Column(name="id",         pg_type="uuid",                     nullable=False, ordinal=1),
        Column(name="updated_at", pg_type="timestamp with time zone", nullable=False, ordinal=2),
    ]
    with patch("viamedia_pipeline.extract.discovery.introspect_columns", return_value=cols):
        result = discover(conn)
    obj = result[0]
    assert obj.supports_parallel_chunking is False
    assert obj.supports_incremental is True  # incremental still works via watermark
