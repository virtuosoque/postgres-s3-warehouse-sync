"""Async query execution backed by an in-memory job store + DuckDB pool.

For multi-instance gateways replace _JOBS with DynamoDB or Redis and stream
result parquet files via signed S3 URLs.
"""

import asyncio
import os
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Literal

import pyarrow.parquet as pq

from viamedia_pipeline.common.config_store import Connection, list_connections
from viamedia_pipeline.common.logging import get_logger
from viamedia_pipeline.common.s3 import put_sse_args, s3_client
from viamedia_pipeline.gateway.duckdb_pool import get_pool

log = get_logger(__name__)

JobStatus = Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"]


@dataclass
class Job:
    job_id: str
    sql: str
    principal_sub: str
    status: JobStatus = "PENDING"
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    rows: int | None = None
    bytes_scanned: int | None = None
    result_s3_uri: str | None = None
    error: str | None = None


_JOBS: dict[str, Job] = {}
_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="duckdb-query")


def submit(sql: str, principal_sub: str) -> Job:
    job = Job(job_id=str(uuid.uuid4()), sql=sql, principal_sub=principal_sub)
    _JOBS[job.job_id] = job
    asyncio.get_running_loop().run_in_executor(_EXECUTOR, _run, job)
    return job


def get(job_id: str) -> Job | None:
    return _JOBS.get(job_id)


def _results_connection() -> Connection:
    conns = list_connections(enabled_only=True)
    if not conns:
        raise RuntimeError("no enabled connection available to store query results")
    return conns[0]


def _run(job: Job) -> None:
    job.status = "RUNNING"
    started = time.time()
    tmp_path: str | None = None
    try:
        rc = _results_connection()
        # Stream the result STRAIGHT to a parquet file via DuckDB COPY, instead of
        # materializing the whole Arrow table in RAM. DuckDB streams and spills to
        # disk, so results far larger than memory (hundreds of millions of rows)
        # succeed -- which is what makes an unlimited (-1) row limit usable.
        # GATEWAY_RESULT_TMP_DIR can point this at a large/fast volume.
        tmp_dir = os.environ.get("GATEWAY_RESULT_TMP_DIR") or None
        fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=tmp_dir)
        os.close(fd)
        with get_pool().acquire() as conn:
            # NB: DuckDB has no statement_timeout; the SQL guard governs result
            # size. job.sql is already guard-validated (single read-only SELECT/
            # WITH), so wrapping it in COPY (...) is safe.
            conn.execute(
                f"COPY ({job.sql}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION 'zstd')"
            )

        key = f"{rc.results_prefix}/{job.job_id}.parquet"
        s3_client(rc).upload_file(
            tmp_path, rc.lake_bucket, key, ExtraArgs=put_sse_args()
        )

        job.rows = pq.read_metadata(tmp_path).num_rows
        job.bytes_scanned = os.path.getsize(tmp_path)
        job.result_s3_uri = f"s3://{rc.lake_bucket}/{key}"
        job.status = "SUCCEEDED"
        log.info(
            "gateway.query.success",
            job_id=job.job_id,
            rows=job.rows,
            bytes=job.bytes_scanned,
            duration_ms=int((time.time() - started) * 1000),
        )
    except Exception as e:
        job.status = "FAILED"
        job.error = repr(e)[:2000]
        log.error("gateway.query.failed", job_id=job.job_id, error=str(e))
    finally:
        job.finished_at = time.time()
        if tmp_path:
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def result_signed_url(job: Job, expires_seconds: int = 3600) -> str | None:
    if not job.result_s3_uri:
        return None
    bucket, _, key = job.result_s3_uri.removeprefix("s3://").partition("/")
    return s3_client(_results_connection()).generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )
