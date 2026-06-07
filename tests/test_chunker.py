from unittest.mock import MagicMock

from viamedia_pipeline.extract.chunker import plan_chunks


def test_plan_chunks_basic():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.side_effect = [
        (1, 10_000_000),       # min, max id
        (10_000_000,),          # reltuples
    ]
    chunks = plan_chunks(conn, "public", "events", rows_per_chunk=2_500_000)
    assert len(chunks) == 4
    assert chunks[0].lo == 1
    assert chunks[-1].hi == 10_000_001  # exclusive upper


def test_plan_chunks_empty_table():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.side_effect = [(None, None), (0,)]
    assert plan_chunks(conn, "public", "empty") == []
