"""Dagster bootstrap jobs (per connection).

    bootstrap_table_job      : bootstrap ONE object (connection_id + table_fqn)
    bootstrap_all_tables_job : bootstrap every selected object of ONE connection

Op graph: plan_bootstrap -> [extract_chunk_op]*N -> commit_chunks (Iceberg commit).
"""

import os
from datetime import datetime, timezone

from dagster import (
    Backoff,
    DynamicOut,
    DynamicOutput,
    OpExecutionContext,
    Out,
    RetryPolicy,
    job,
    multiprocess_executor,
    op,
)

# Cap how many chunks extract concurrently. Each worker holds a row batch in
# memory, so size this to the host: ~roughly (RAM_GB - 2) / 0.8 workers.
# Tune via BOOTSTRAP_MAX_CONCURRENT without code changes.
_BOOTSTRAP_MAX_CONCURRENT = int(os.environ.get("BOOTSTRAP_MAX_CONCURRENT", "4"))
_BOOTSTRAP_EXECUTOR = multiprocess_executor.configured(
    {"max_concurrent": _BOOTSTRAP_MAX_CONCURRENT}
)

from viamedia_pipeline.common.config_store import get_connection
from viamedia_pipeline.common.pg import dedicated_connection
from viamedia_pipeline.extract.chunker import (
    Chunk,
    has_leading_index,
    plan_full_chunk,
    plan_partitioned_chunks,
    plan_range_chunks,
)
from viamedia_pipeline.extract.copy_worker import extract_chunk_to_s3
from viamedia_pipeline.extract.schema import (
    arrow_schema_for,
    iceberg_schema_for,
    introspect_columns,
    pg_oids_for,
)
from viamedia_pipeline.extract.snapshot import export_snapshot
from viamedia_pipeline.load.iceberg_writer import (
    commit_files_with_retry,
    create_or_get_table,
    ensure_namespace,
    evolve_table_schema,
)
from viamedia_pipeline.orchestration.tables import get_table, get_tables
from viamedia_pipeline.state import chunks as chunk_state


def _require_connection(connection_id: int):
    conn = get_connection(connection_id)
    if conn is None:
        raise RuntimeError(f"connection_id {connection_id} not found in config store")
    return conn


@op(out=DynamicOut())
def plan_bootstrap(context: OpExecutionContext):
    connection_id: int = context.op_config["connection_id"]
    fqn: str = context.op_config["table_fqn"]
    conn = _require_connection(connection_id)
    cfg = get_table(conn, fqn)
    if cfg is None:
        raise RuntimeError(
            f"table_fqn '{fqn}' is not in connection {connection_id}'s sync set."
        )

    ensure_namespace(conn)

    with dedicated_connection(conn) as pg_conn, export_snapshot(conn) as snap:
        cols = introspect_columns(pg_conn, cfg.schema, cfg.table)
        arrow_schema = arrow_schema_for(cols)
        pg_oids = pg_oids_for(cols)
        iceberg_schema = iceberg_schema_for(cols)
        create_or_get_table(conn, cfg.iceberg_table_name, iceberg_schema,
                            partition_by=list(cfg.partition_by))
        # If re-bootstrapping after a source DDL change, evolve the existing
        # table so it's a superset of the current source columns before add_files.
        evolve_table_schema(conn, cfg.iceberg_table_name, iceberg_schema)
        # Parallel id-range chunking only pays off when the key is indexed;
        # otherwise each chunk would full-scan the table, so stream one COPY.
        numeric_id = cfg.supports_parallel_chunking and has_leading_index(
            pg_conn, cfg.schema, cfg.table, cfg.pk
        )
        # A day-partitioned table MUST be chunked one-day-per-file (add_files
        # rejects multi-day files), so use the partition-aware planner whenever
        # the table has a day() partition -- regardless of numeric id (days with
        # no numeric id become a single per-day COPY).
        day_part = next((p for p in cfg.partition_by if p[1] == "day"), None)
        if day_part is not None:
            part_col = day_part[0]
            pcol = next((c for c in cols if c.name == part_col), None)
            part_tz = pcol is not None and "with time zone" in pcol.pg_type.lower()
            context.log.info(
                f"bootstrap.partitioned table={fqn} part_col={part_col} "
                f"tz={part_tz} numeric_id={numeric_id}"
            )
            chunks = plan_partitioned_chunks(
                pg_conn, cfg.schema, cfg.table, part_col,
                part_tz=part_tz, has_numeric_id=numeric_id,
            )
        elif numeric_id:
            chunks = plan_range_chunks(pg_conn, cfg.schema, cfg.table)
        else:
            context.log.info(f"bootstrap.single_copy table={fqn} reason=pk_not_indexed_or_non_numeric")
            chunks = plan_full_chunk(cfg.schema, cfg.table)

    run_id = context.run_id
    already_done = chunk_state.list_done(run_id)
    context.log.info(
        f"bootstrap.plan conn={connection_id} table={fqn} kind={cfg.kind} "
        f"chunks={len(chunks)} resumed={len(already_done)}"
    )

    for c in chunks:
        key = chunk_state.chunk_key(fqn, c.chunk_id)
        if key in already_done:
            continue
        yield DynamicOutput(
            value={
                "connection_id": connection_id,
                "chunk": c,
                "snapshot_id": snap.snapshot_id,
                "arrow_schema_serialized": arrow_schema.serialize().to_pybytes(),
                "pg_oids": pg_oids,
                "iceberg_table_name": cfg.iceberg_table_name,
                "run_id": run_id,
            },
            mapping_key=f"c{c.chunk_id:06d}",
        )


@op(retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL), out=Out(dict))
def extract_chunk_op(context: OpExecutionContext, payload: dict) -> dict:
    import pyarrow as pa

    connection_id: int = payload["connection_id"]
    conn = _require_connection(connection_id)
    c: Chunk = payload["chunk"]
    snapshot_id: str = payload["snapshot_id"]
    arrow_schema = pa.ipc.read_schema(pa.BufferReader(payload["arrow_schema_serialized"]))
    run_id: str = payload["run_id"]
    iceberg_table_name: str = payload["iceberg_table_name"]
    key = chunk_state.chunk_key(c.fqn, c.chunk_id)

    # Idempotency under op retries: if a previous attempt already extracted AND
    # committed this chunk, do not extract/commit again (add_files does not dedup
    # -- re-committing would duplicate rows).
    if key in chunk_state.list_done(run_id):
        context.log.info(f"chunk.skip_already_committed key={key}")
        return {"connection_id": connection_id, "s3_uris": [], "rows": 0,
                "iceberg_table_name": iceberg_table_name, "table_fqn": c.fqn}

    chunk_state.put(chunk_state.ChunkRecord(
        run_id=run_id, chunk_key=key, connection_id=connection_id, status="RUNNING"))
    try:
        s3_prefix = f"bootstrap/{c.fqn}/run_id={run_id}"
        res = extract_chunk_to_s3(
            conn_cfg=conn, chunk=c, snapshot_id=snapshot_id,
            arrow_schema=arrow_schema, s3_prefix=s3_prefix,
            pg_oids=payload.get("pg_oids"),
        )
        # Commit THIS chunk's files now (per-chunk), so the table fills up live
        # and a single bad chunk fails only itself. Retries on Iceberg commit
        # conflicts (many workers commit concurrently). Mark DONE only after the
        # commit succeeds so the idempotency guard above is accurate.
        commit_files_with_retry(conn, iceberg_table_name, res.s3_uris)
        chunk_state.put(chunk_state.ChunkRecord(
            run_id=run_id, chunk_key=key, connection_id=connection_id, status="DONE",
            s3_uri=(res.s3_uris[0] if res.s3_uris else None), rows=res.rows,
        ))
        return {
            "connection_id": connection_id,
            "s3_uris": res.s3_uris,
            "rows": res.rows,
            "iceberg_table_name": iceberg_table_name,
            "table_fqn": c.fqn,
        }
    except Exception as e:
        chunk_state.put(chunk_state.ChunkRecord(
            run_id=run_id, chunk_key=key, connection_id=connection_id, status="FAILED",
            error=str(e),
        ))
        raise


@op
def commit_chunks(context: OpExecutionContext, results: list[dict]) -> dict:
    """Finalizer/summary. Files are committed per-chunk in extract_chunk_op
    (so the table is queryable progressively); here we just total up the run."""
    results = [r for r in results if r]
    if not results:
        context.log.warning("bootstrap.no_chunks")
        return {"rows": 0, "files": 0}

    connection_id = results[0]["connection_id"]
    iceberg_table_name = results[0]["iceberg_table_name"]
    table_fqn = results[0].get("table_fqn")
    total_files = sum(len(r.get("s3_uris", [])) for r in results)
    total_rows = sum(r["rows"] for r in results)
    if table_fqn:
        from viamedia_pipeline.common import config_store
        config_store.record_object_sync(connection_id, table_fqn, "bootstrap",
                                         run_id=context.run_id, rows=total_rows)
    context.log.info(
        f"bootstrap.done conn={connection_id} table={iceberg_table_name} "
        f"files={total_files} rows={total_rows} ts={datetime.now(timezone.utc).isoformat()}"
    )
    return {"rows": total_rows, "files": total_files}


@job(
    config={"ops": {"plan_bootstrap": {"config": {"connection_id": 1, "table_fqn": "public.events"}}}},
    executor_def=_BOOTSTRAP_EXECUTOR,
)
def bootstrap_table_job():
    """Bootstrap ONE object. Override connection_id + table_fqn in launch config."""
    payloads = plan_bootstrap()
    results = payloads.map(extract_chunk_op).collect()
    commit_chunks(results)


# --- Bulk variant: bootstrap every selected object of one connection -------
@op(out=DynamicOut())
def fan_out_all_tables(context: OpExecutionContext):
    connection_id: int = context.op_config["connection_id"]
    conn = _require_connection(connection_id)
    for cfg in get_tables(conn, force=True):
        yield DynamicOutput(
            value={"connection_id": connection_id, "fqn": cfg.fqn},
            mapping_key=cfg.fqn.replace(".", "__").replace("-", "_"),
        )


@op
def bootstrap_one_in_bulk(context: OpExecutionContext, item: dict) -> str:
    context.log.info(f"bulk.queue conn={item['connection_id']} table={item['fqn']}")
    return item["fqn"]


@job(config={"ops": {"fan_out_all_tables": {"config": {"connection_id": 1}}}})
def bootstrap_all_tables_job():
    """Bulk bootstrap of one connection's selected objects. For real parallelism
    use the sensor in maintenance_job.py (one bootstrap_table_job run per object)."""
    fqns = fan_out_all_tables()
    fqns.map(bootstrap_one_in_bulk)
