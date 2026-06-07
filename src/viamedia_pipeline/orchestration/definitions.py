"""Dagster repository entrypoint.

Run with:
    dagster dev -m viamedia_pipeline.orchestration.definitions
"""

from dagster import Definitions

from viamedia_pipeline.common.logging import configure_logging
from viamedia_pipeline.orchestration.bootstrap_job import (
    bootstrap_all_tables_job,
    bootstrap_table_job,
)
from viamedia_pipeline.orchestration.incremental_job import (
    every_five_minutes_per_table,
    incremental_job,
)
from viamedia_pipeline.orchestration.maintenance_job import (
    discover_new_tables_sensor,
    maintenance_job,
    weekly_maintenance,
)
from viamedia_pipeline.orchestration.reconcile_job import daily_reconcile, reconcile_job
from viamedia_pipeline.orchestration.resources import SettingsResource

configure_logging()

# One-time: if no connections exist yet but legacy .env values are present,
# seed a "default" connection so an existing single-DB deployment keeps working
# after the move to UI-managed config. Best-effort; never blocks code loading.
try:
    from viamedia_pipeline.common.config_store import ensure_seed_connection
    ensure_seed_connection()
except Exception:  # noqa: BLE001
    pass

defs = Definitions(
    jobs=[
        bootstrap_table_job,
        bootstrap_all_tables_job,
        incremental_job,
        maintenance_job,
        reconcile_job,
    ],
    schedules=[every_five_minutes_per_table, weekly_maintenance, daily_reconcile],
    sensors=[discover_new_tables_sensor],
    resources={"settings": SettingsResource()},
)
