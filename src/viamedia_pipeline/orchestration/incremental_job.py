"""Incremental sync: one job, parameterized by (connection_id, table_fqn).

The schedule fans out one RunRequest per (connection, incremental-eligible object)
across all enabled connections.
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
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.extract.incremental import (
    advance_watermark_after_merge,
    run_incremental,
)
from viamedia_pipeline.extract.schema import (
    arrow_schema_for,
    iceberg_schema_for,
    introspect_columns,
    pg_oids_for,
)
from viamedia_pipeline.load.iceberg_writer import create_or_get_table, ensure_namespace
from viamedia_pipeline.load.merge import load_staging_arrow, merge_arrow_table
from viamedia_pipeline.orchestration.tables import get_table, get_tables


@op(retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL))
def incremental_one_table(context: OpExecutionContext) -> dict:
    connection_id: int = context.op_config["connection_id"]
    fqn: str = context.op_config["table_fqn"]
    conn = get_connection(connection_id)
    if conn is None:
        raise RuntimeError(f"connection_id {connection_id} not found")
    cfg = get_table(conn, fqn)
    if cfg is None or not cfg.supports_incremental:
        context.log.info(f"incremental.skip conn={connection_id} table={fqn} reason=not_eligible")
        return {"rows": 0, "skipped": True}

    ensure_namespace(conn)

    with dedicated_connection(conn) as pg_conn:
        cols = introspect_columns(pg_conn, cfg.schema, cfg.table)
    arrow_schema = arrow_schema_for(cols)
    pg_oids = pg_oids_for(cols)
    iceberg_schema = iceberg_schema_for(cols)
    tbl = create_or_get_table(conn, cfg.iceberg_table_name, iceberg_schema,
                              partition_by=list(cfg.partition_by))

    now = datetime.now(timezone.utc)
    s3_prefix = f"incremental/{fqn}"
    s3_uri, new_wm = run_incremental(
        conn, cfg.schema, cfg.table, arrow_schema, s3_prefix, pg_oids=pg_oids, now=now,
    )
    if s3_uri is None:
        advance_watermark_after_merge(conn.id, fqn, new_wm)
        return {"rows": 0}

    staged = load_staging_arrow(conn, s3_uri)
    merge_summary = merge_arrow_table(tbl, staged, join_cols=(cfg.pk,))
    advance_watermark_after_merge(conn.id, fqn, new_wm)

    context.log.info(
        f"incremental.done conn={connection_id} table={fqn} rows={staged.num_rows} "
        f"merge={merge_summary} watermark_ts={new_wm.ts.isoformat()} watermark_id={new_wm.last_id}"
    )
    return {"rows": staged.num_rows, **merge_summary}


@job(config={"ops": {"incremental_one_table": {"config": {"connection_id": 1, "table_fqn": "public.events"}}}})
def incremental_job():
    incremental_one_table()


@schedule(
    job=incremental_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)
def every_five_minutes_per_table(context: ScheduleEvaluationContext):
    """Fan out one RunRequest per (enabled connection, incremental-eligible object)."""
    now_iso = (
        context.scheduled_execution_time.isoformat()
        if context.scheduled_execution_time
        else datetime.now(timezone.utc).isoformat()
    )
    for conn in list_connections(enabled_only=True):
        for cfg in get_tables(conn):
            if not cfg.supports_incremental:
                continue
            yield RunRequest(
                run_key=f"{conn.id}:{cfg.fqn}@{now_iso}",
                run_config={
                    "ops": {"incremental_one_table": {"config": {
                        "connection_id": conn.id, "table_fqn": cfg.fqn,
                    }}},
                },
                tags={"connection": str(conn.id), "table": cfg.fqn, "kind": "incremental"},
            )
