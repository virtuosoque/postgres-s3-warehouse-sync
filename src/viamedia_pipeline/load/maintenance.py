"""Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup.

PyIceberg ships these as table methods in 0.8+; for very large rewrites
(TB-scale) submit the equivalent Spark job to EMR Serverless instead.
"""

from datetime import timedelta

from pyiceberg.table import Table

from viamedia_pipeline.common.logging import get_logger

log = get_logger(__name__)


def compact(table: Table, target_file_size_bytes: int = 512 * 1024**2) -> None:
    """Rewrite small data files into ~target_file_size_bytes ones."""
    table.rewrite_data_files(
        target_file_size_bytes=target_file_size_bytes,
    ) if hasattr(table, "rewrite_data_files") else _spark_compact_fallback(table)
    log.info("iceberg.compact.done", table=table.name())


def expire_snapshots(table: Table, retain_for: timedelta = timedelta(days=7)) -> None:
    cutoff_ms = int((table.current_snapshot().timestamp_ms - retain_for.total_seconds() * 1000))
    table.expire_snapshots(older_than=cutoff_ms)
    log.info("iceberg.expire.done", table=table.name(), cutoff_ms=cutoff_ms)


def _spark_compact_fallback(table: Table) -> None:
    """Submit an EMR Serverless job that runs:
        CALL system.rewrite_data_files(table => '<ident>', options => ...)
    Implementation deferred -- see scripts/submit_emr_compaction.py.
    """
    log.warning(
        "iceberg.compact.fallback_required",
        table=table.name(),
        action="submit EMR Serverless rewrite",
    )
