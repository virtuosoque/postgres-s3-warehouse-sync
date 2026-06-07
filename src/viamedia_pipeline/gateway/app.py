"""Unified gateway: the single web UI + API.

Pages/endpoints cover connections (CRUD), live source-object selection,
operational settings, query console, and triggering/monitoring Dagster runs.
Dagster is driven behind the scenes via GraphQL (see dagster_client).
"""

from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal

import psycopg
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from viamedia_pipeline.common import config_store
from viamedia_pipeline.common.logging import configure_logging, get_logger
from viamedia_pipeline.common.settings import EDITABLE_SETTING_KEYS, get_settings
from viamedia_pipeline.extract.discovery import discover
from viamedia_pipeline.gateway import dagster_client
from viamedia_pipeline.gateway import query as q
from viamedia_pipeline.gateway.auth import Principal, current_principal
from viamedia_pipeline.gateway.duckdb_pool import get_pool
from viamedia_pipeline.gateway.sql_guard import SqlGuardError, guard
from viamedia_pipeline.gateway.view_refresher import ViewRefresher

configure_logging()
log = get_logger(__name__)

limiter = Limiter(key_func=lambda r: getattr(r.state, "principal", None).sub
                  if hasattr(r.state, "principal") and r.state.principal
                  else get_remote_address(r))

_refresher: ViewRefresher | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _refresher
    try:
        config_store.ensure_seed_connection()
    except Exception as e:  # noqa: BLE001
        log.warning("gateway.seed_failed", error=str(e))
    s = get_settings()
    get_pool()
    _refresher = ViewRefresher(interval_seconds=s.gateway_view_refresh_seconds)
    _refresher.start()
    yield
    if _refresher:
        _refresher.stop()


app = FastAPI(title="Viamedia Data Lake", version="0.2.0", lifespan=lifespan)
app.state.limiter = limiter


# --- models ---------------------------------------------------------------
class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=200_000)


class RunRequestBody(BaseModel):
    sql: str = Field(..., min_length=1, max_length=200_000)
    limit: int = Field(1000, ge=1, le=10_000)


class ConnectionIn(BaseModel):
    name: str
    enabled: bool = True
    # Source DB connection components (password blank on update = keep existing)
    db_host: str | None = None
    db_port: int = 5432
    db_name: str | None = None
    db_user: str | None = None
    db_password: str | None = None
    db_sslmode: str = "require"
    # Target
    aws_access_key: str | None = None
    aws_secret_key: str | None = None
    aws_region: str = "us-east-1"
    lake_bucket: str
    iceberg_namespace: str


class SelectionItem(BaseModel):
    schema_: str = Field(..., alias="schema")
    name: str
    kind: str = "TABLE"
    enabled: bool = True

    model_config = {"populate_by_name": True}


class RunLaunch(BaseModel):
    connection_id: int
    table_fqn: str
    mode: str = Field("bootstrap", pattern="^(bootstrap|incremental|reconcile)$")


@app.exception_handler(RateLimitExceeded)
def _rate_limit_handler(request, exc):
    return JSONResponse(status_code=429, content={"detail": "rate limit exceeded"})


# --- health / UI ----------------------------------------------------------
@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict:
    get_settings()
    get_pool()
    return {"status": "ready"}


@app.get("/", response_class=HTMLResponse)
def ui() -> str:
    return _UI_HTML


# --- connections ----------------------------------------------------------
@app.get("/connections")
def list_connections(principal: Principal = Depends(current_principal)) -> dict:
    return {"connections": [c.public_dict() for c in config_store.list_connections(force=True)]}


def _resolve_secrets(body: ConnectionIn, existing) -> tuple[str, str | None, str | None]:
    """Build the source DSN + resolve AWS creds, keeping existing secrets when a
    field is left blank on update."""
    pw = body.db_password or (existing.dsn_password if existing else "")
    dsn = config_store.build_dsn(
        db_host=body.db_host or "", db_port=body.db_port, db_name=body.db_name or "",
        db_user=body.db_user or "", db_password=pw, db_sslmode=body.db_sslmode,
    )
    ak = body.aws_access_key or (existing.aws_access_key if existing else None)
    sk = body.aws_secret_key or (existing.aws_secret_key if existing else None)
    return dsn, ak, sk


def _test_db(dsn: str) -> str | None:
    try:
        with psycopg.connect(dsn, connect_timeout=8) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception as e:  # noqa: BLE001
        return str(e)
    return None


def _test_aws(ak: str | None, sk: str | None, region: str, bucket: str) -> str | None:
    import os

    import boto3
    from botocore.config import Config as BotoConfig
    try:
        kw: dict = {"region_name": region}
        if ak and sk:
            kw["aws_access_key_id"] = ak
            kw["aws_secret_access_key"] = sk
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:
            kw["endpoint_url"] = endpoint
            kw["config"] = BotoConfig(s3={"addressing_style": "path"})
        boto3.client("s3", **kw).head_bucket(Bucket=bucket)
    except Exception as e:  # noqa: BLE001
        return str(e)
    return None


def _validate_or_400(body: ConnectionIn, existing) -> tuple[str, str | None, str | None]:
    dsn, ak, sk = _resolve_secrets(body, existing)
    db_err = _test_db(dsn)
    if db_err:
        raise HTTPException(400, f"database test failed: {db_err}")
    if not body.lake_bucket:
        raise HTTPException(400, "lake_bucket is required")
    aws_err = _test_aws(ak, sk, body.aws_region, body.lake_bucket)
    if aws_err:
        raise HTTPException(400, f"AWS/bucket test failed: {aws_err}")
    return dsn, ak, sk


@app.post("/connections")
def create_connection(body: ConnectionIn, principal: Principal = Depends(current_principal)) -> dict:
    if not (body.db_host and body.db_name and body.db_user and body.db_password):
        raise HTTPException(400, "db_host, db_name, db_user and db_password are required")
    if not (body.aws_access_key and body.aws_secret_key):
        raise HTTPException(400, "AWS access key and secret are required")
    dsn, ak, sk = _validate_or_400(body, None)
    cid = config_store.upsert_connection(
        connection_id=None, name=body.name, enabled=body.enabled, source_dsn=dsn,
        aws_access_key=ak, aws_secret_key=sk, aws_region=body.aws_region,
        lake_bucket=body.lake_bucket, iceberg_namespace=body.iceberg_namespace,
    )
    return {"id": cid}


@app.put("/connections/{connection_id}")
def update_connection(connection_id: int, body: ConnectionIn,
                      principal: Principal = Depends(current_principal)) -> dict:
    existing = config_store.get_connection(connection_id)
    if existing is None:
        raise HTTPException(404, "connection not found")
    dsn, ak, sk = _validate_or_400(body, existing)
    config_store.upsert_connection(
        connection_id=connection_id, name=body.name, enabled=body.enabled, source_dsn=dsn,
        aws_access_key=ak, aws_secret_key=sk, aws_region=body.aws_region,
        lake_bucket=body.lake_bucket, iceberg_namespace=body.iceberg_namespace,
    )
    return {"id": connection_id}


@app.delete("/connections/{connection_id}")
def delete_connection(connection_id: int, principal: Principal = Depends(current_principal)) -> dict:
    config_store.delete_connection(connection_id)
    return {"deleted": connection_id}


@app.post("/connections/test")
def test_connection(body: ConnectionIn, principal: Principal = Depends(current_principal)) -> dict:
    """Test DB + AWS without saving. Returns each result so the UI can show both."""
    existing = config_store.get_connection_by_name(body.name) if body.name else None
    dsn, ak, sk = _resolve_secrets(body, existing)
    db_err = _test_db(dsn)
    aws_err = _test_aws(ak, sk, body.aws_region, body.lake_bucket) if body.lake_bucket else "lake_bucket is required"
    return {
        "db": {"ok": db_err is None, "error": db_err},
        "aws": {"ok": aws_err is None, "error": aws_err},
    }


@app.get("/connections/{connection_id}/objects")
def connection_objects(connection_id: int,
                       principal: Principal = Depends(current_principal)) -> dict:
    import dataclasses

    conn = config_store.get_connection(connection_id)
    if conn is None:
        raise HTTPException(404, "connection not found")
    from viamedia_pipeline.common.pg import dedicated_connection
    # Show ALL discoverable objects in the picker (ignore include/exclude globs);
    # the explicit checkbox selection is what actually drives syncing.
    picker_cfg = dataclasses.replace(conn, sync_include="", sync_exclude="")
    try:
        with dedicated_connection(conn) as pg_conn:
            objs = discover(pg_conn, picker_cfg)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(400, f"discovery failed: {e}") from e
    selected = config_store.enabled_fqns(connection_id)
    return {
        "objects": [
            {
                "schema": o.schema, "name": o.name, "kind": o.kind,
                "selected": (selected is None) or (o.fqn in selected),
                "incremental": o.supports_incremental,
            }
            for o in objs
        ]
    }


@app.get("/connections/{connection_id}/selection")
def get_selection(connection_id: int, principal: Principal = Depends(current_principal)) -> dict:
    rows = config_store.get_selection(connection_id)
    return {"selection": [{"schema": r.schema, "name": r.name, "kind": r.kind,
                           "enabled": r.enabled} for r in rows]}


@app.post("/connections/{connection_id}/selection")
def set_selection(connection_id: int, items: list[SelectionItem],
                  principal: Principal = Depends(current_principal)) -> dict:
    payload = [{"schema": it.schema_, "name": it.name, "kind": it.kind,
                "enabled": it.enabled} for it in items]
    config_store.set_selection(connection_id, payload)
    return {"saved": len(payload)}


# --- settings -------------------------------------------------------------
@app.get("/settings")
def read_settings(principal: Principal = Depends(current_principal)) -> dict:
    s = get_settings()
    return {k: getattr(s, k) for k in EDITABLE_SETTING_KEYS}


@app.post("/settings")
def write_settings(values: dict[str, str], principal: Principal = Depends(current_principal)) -> dict:
    saved = {}
    for k, v in values.items():
        if k in EDITABLE_SETTING_KEYS:
            config_store.set_setting(k, str(v))
            saved[k] = v
    return {"saved": saved}


# --- runs (trigger + status via Dagster) ----------------------------------
@app.post("/runs")
def launch_run(body: RunLaunch, principal: Principal = Depends(current_principal)) -> dict:
    try:
        if body.mode == "incremental":
            run = dagster_client.launch_incremental(body.connection_id, body.table_fqn)
        elif body.mode == "reconcile":
            run = dagster_client.launch_reconcile(body.connection_id, body.table_fqn)
        else:
            run = dagster_client.launch_bootstrap(body.connection_id, body.table_fqn)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"failed to launch run: {e}") from e
    return run


@app.get("/runs")
def list_runs(connection_id: int, principal: Principal = Depends(current_principal)) -> dict:
    try:
        return {"runs": dagster_client.recent_runs(connection_id)}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"failed to list runs: {e}") from e


@app.get("/runs/{run_id}")
def get_run(run_id: str, principal: Principal = Depends(current_principal)) -> dict:
    try:
        return dagster_client.run_status(run_id)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"failed to read run: {e}") from e


# --- tables (grouped by connection namespace) -----------------------------
@app.get("/tables")
def list_tables(principal: Principal = Depends(current_principal)) -> dict:
    conns = config_store.list_connections(enabled_only=True)
    ns_to_conn = {c.iceberg_namespace: c for c in conns}
    with get_pool().acquire() as conn:
        rows = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_type = 'VIEW' ORDER BY table_schema, table_name"
        ).fetchall()
    grouped: dict[str, list[str]] = {}
    for schema, name in rows:
        if schema in ns_to_conn:
            grouped.setdefault(schema, []).append(name)
    return {
        "connections": [
            {"id": c.id, "name": c.name, "namespace": c.iceberg_namespace,
             "tables": grouped.get(c.iceberg_namespace, [])}
            for c in conns
        ]
    }


# --- query (sync for UI; async kept for PowerBI) --------------------------
def _json_safe(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    return str(v)


@app.post("/query")
def run_query_sync(body: RunRequestBody, principal: Principal = Depends(current_principal)) -> dict:
    try:
        safe_sql = guard(body.sql)
    except SqlGuardError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e)) from e
    try:
        with get_pool().acquire() as conn:
            cur = conn.execute(safe_sql)
            desc = cur.description or []
            columns = [d[0] for d in desc]
            column_types = [str(d[1]) for d in desc]  # DuckDB type per column
            rows = cur.fetchmany(body.limit)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status.HTTP_400_BAD_REQUEST, repr(e)[:1000]) from e
    return {
        "columns": columns,
        "column_types": column_types,
        "rows": [[_json_safe(v) for v in r] for r in rows],
        "row_count": len(rows),
        "truncated": len(rows) >= body.limit,
    }


class QuerySubmitted(BaseModel):
    job_id: str
    status: str


class QueryStatus(BaseModel):
    job_id: str
    status: str
    rows: int | None = None
    bytes_scanned: int | None = None
    error: str | None = None
    result_signed_url: str | None = None


@app.post("/queries", response_model=QuerySubmitted)
@limiter.limit("10/minute")
async def submit_query(request: QueryRequest,
                       principal: Principal = Depends(current_principal)) -> QuerySubmitted:
    try:
        safe_sql = guard(request.sql)
    except SqlGuardError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e)) from e
    job = q.submit(safe_sql, principal_sub=principal.sub)
    return QuerySubmitted(job_id=job.job_id, status=job.status)


@app.get("/queries/{job_id}", response_model=QueryStatus)
def query_status(job_id: str, principal: Principal = Depends(current_principal)) -> QueryStatus:
    job = q.get(job_id)
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    if job.principal_sub != principal.sub:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "not your job")
    return QueryStatus(
        job_id=job.job_id, status=job.status, rows=job.rows,
        bytes_scanned=job.bytes_scanned, error=job.error,
        result_signed_url=q.result_signed_url(job) if job.status == "SUCCEEDED" else None,
    )


def run() -> None:
    import uvicorn
    uvicorn.run(
        "viamedia_pipeline.gateway.app:app",
        host="0.0.0.0",  # noqa: S104
        port=8080,
        workers=1,
        log_level=get_settings().log_level.lower(),
    )


if __name__ == "__main__":
    run()


from viamedia_pipeline.gateway.ui import UI_HTML as _UI_HTML  # noqa: E402
