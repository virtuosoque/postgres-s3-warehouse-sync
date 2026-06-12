"""Delete-reconciliation job: one job parameterized by (connection_id, table_fqn),
plus a daily schedule that fans out across all enabled connections' selected
objects that have a primary key. Removes lake rows that no longer exist in source.
"""

from datetime import datetime, timezone

from dagster import (
    Backoff,
    DefaultScheduleStatus,
    OpExecutionContext,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    job,
    op,
    schedule,
)

from viamedia_pipeline.common.config_store import get_connection, list_connections
from viamedia_pipeline.load.reconcile import reconcile_table
from viamedia_pipeline.orchestration.tables import get_table, get_tables


@op(retry_policy=RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL))
def reconcile_one_table(context: OpExecutionContext) -> dict:
    connection_id: int = context.op_config["connection_id"]
    fqn: str = context.op_config["table_fqn"]
    conn = get_connection(connection_id)
    if conn is None:
        raise RuntimeError(f"connection_id {connection_id} not found")
    cfg = get_table(conn, fqn)
    if cfg is None or not cfg.supports_parallel_chunking:
        # Needs a numeric primary key to compare keys; skip otherwise.
        context.log.info(f"reconcile.skip conn={connection_id} table={fqn} reason=no_numeric_pk")
        return {"skipped": True}
    summary = reconcile_table(conn, cfg.schema, cfg.table, cfg.iceberg_table_name, pk=cfg.pk)
    from viamedia_pipeline.common import config_store
    config_store.record_object_sync(connection_id, fqn, "reconcile", run_id=context.run_id)
    context.log.info(f"reconcile.done conn={connection_id} table={fqn} {summary}")
    return summary


@job(config={"ops": {"reconcile_one_table": {"config": {"connection_id": 1, "table_fqn": "public.events"}}}})
def reconcile_job():
    reconcile_one_table()


@schedule(
    job=reconcile_job,
    cron_schedule="0 6 * * *",  # daily 06:00 UTC (off-peak)
    default_status=DefaultScheduleStatus.STOPPED,
)
def daily_reconcile(context: ScheduleEvaluationContext):
    """One RunRequest per (enabled connection, selected object with a numeric PK)."""
    now_iso = (
        context.scheduled_execution_time.isoformat()
        if context.scheduled_execution_time
        else datetime.now(timezone.utc).isoformat()
    )
    for conn in list_connections(enabled_only=True):
        for cfg in get_tables(conn):
            if not cfg.supports_parallel_chunking:
                continue
            yield RunRequest(
                run_key=f"reconcile:{conn.id}:{cfg.fqn}@{now_iso}",
                run_config={"ops": {"reconcile_one_table": {"config": {
                    "connection_id": conn.id, "table_fqn": cfg.fqn}}}},
                tags={"connection": str(conn.id), "table": cfg.fqn, "kind": "reconcile"},
            )
