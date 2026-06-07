"""Watermark tests against a real Postgres via pytest-postgresql.

Run only when a Postgres is available; otherwise skipped.
Use `pytest -k watermark` to run; CI should spin up a postgres service.
"""

from datetime import datetime, timezone

import pytest

pytest_postgresql = pytest.importorskip("pytest_postgresql")
from pytest_postgresql import factories  # noqa: E402

postgresql_proc = factories.postgresql_proc(port=None, unixsocketdir="/tmp")
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
def metadata_pg(postgresql, monkeypatch):
    """Point the metadata pool at the ephemeral postgres + run migrations."""
    info = postgresql.info
    dsn = f"postgresql://{info.user}@{info.host}:{info.port}/{info.dbname}"
    monkeypatch.setenv("METADATA_PG_DSN", dsn)

    from viamedia_pipeline.common import metadata_db
    from viamedia_pipeline.common.migrations import apply
    from viamedia_pipeline.common.settings import get_settings

    get_settings.cache_clear()
    metadata_db.close_pool()
    apply()
    yield dsn
    metadata_db.close_pool()


def test_watermark_roundtrip(metadata_pg):
    from viamedia_pipeline.state.watermarks import Watermark, get_watermark, set_watermark

    wm = get_watermark("public.events")
    assert wm.last_id == 0

    new = Watermark(ts=datetime(2026, 5, 26, 10, 0, tzinfo=timezone.utc), last_id=12345)
    set_watermark("public.events", new)

    fetched = get_watermark("public.events")
    assert fetched.ts == new.ts
    assert fetched.last_id == 12345


def test_watermark_does_not_go_backward(metadata_pg):
    from viamedia_pipeline.state.watermarks import Watermark, get_watermark, set_watermark

    later   = Watermark(ts=datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc), last_id=999)
    earlier = Watermark(ts=datetime(2026, 5, 26, 10, 0, tzinfo=timezone.utc), last_id=1)

    set_watermark("public.events", later)
    set_watermark("public.events", earlier)

    assert get_watermark("public.events").last_id == 999
