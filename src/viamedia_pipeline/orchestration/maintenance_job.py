"""Weekly Iceberg maintenance + an on-demand sensor that bootstraps newly-
selected objects (across all enabled connections).
"""

from datetime import timedelta

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    OpExecutionContext,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    job,
    op,
    schedule,
    sensor,
)

from viamedia_pipeline.common.config_store import list_connections
from viamedia_pipeline.load.iceberg_writer import catalog
from viamedia_pipeline.load.maintenance import compact, expire_snapshots
from viamedia_pipeline.orchestration.bootstrap_job import bootstrap_table_job
from viamedia_pipeline.orchestration.tables import get_tables


@op
def maintain_all_tables(context: OpExecutionContext) -> dict:
    summary: dict[str, str] = {}
    for conn in list_connections(enabled_only=True):
        cat = catalog(conn)
        for cfg in get_tables(conn):
            label = f"{conn.id}:{cfg.fqn}"
            try:
                tbl = cat.load_table((conn.iceberg_namespace, cfg.iceberg_table_name))
                compact(tbl)
                expire_snapshots(tbl, retain_for=timedelta(days=7))
                summary[label] = "ok"
            except Exception as e:
                context.log.error(f"maintenance.failed table={label} error={e}")
                summary[label] = f"error: {e}"
    return summary


@job
def maintenance_job():
    maintain_all_tables()


@schedule(
    job=maintenance_job,
    cron_schedule="0 5 * * 0",  # Sunday 05:00 UTC
    default_status=DefaultScheduleStatus.STOPPED,
)
def weekly_maintenance(_):
    yield RunRequest(run_key=None, run_config={})


# --- Sensor: auto-bootstrap newly selected objects ------------------------
@sensor(
    job=bootstrap_table_job,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.STOPPED,
)
def discover_new_tables_sensor(context: SensorEvaluationContext):
    """Bootstrap any selected object (any connection) whose Iceberg table does
    not yet exist. Idempotent -- a successful bootstrap creates the table, so
    the sensor stops emitting for that (connection, fqn)."""
    requests = []
    for conn in list_connections(enabled_only=True):
        cat = catalog(conn)
        for cfg in get_tables(conn, force=True):
            try:
                cat.load_table((conn.iceberg_namespace, cfg.iceberg_table_name))
                continue  # already bootstrapped
            except Exception:
                pass
            requests.append(RunRequest(
                run_key=f"bootstrap:{conn.id}:{cfg.fqn}",
                run_config={
                    "ops": {"plan_bootstrap": {"config": {
                        "connection_id": conn.id, "table_fqn": cfg.fqn,
                    }}},
                },
                tags={"connection": str(conn.id), "table": cfg.fqn, "kind": "bootstrap"},
            ))

    if not requests:
        return SkipReason("all selected objects already bootstrapped")
    return requests
