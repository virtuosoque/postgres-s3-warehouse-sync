"""Async query execution backed by an in-memory job store + DuckDB pool.

For multi-instance gateways replace _JOBS with DynamoDB or Redis and stream
result parquet files via signed S3 URLs.
"""

import asyncio
import io
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa
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
    try:
        with get_pool().acquire() as conn:
            # NB: DuckDB has no `statement_timeout`; rely on the SQL guard's
            # LIMIT cap to bound result size. Add a watchdog if you need a hard
            # wall-clock limit.
            result = conn.execute(job.sql)
            arrow_tbl: pa.Table = result.fetch_arrow_table()

        # Write result parquet to a connection's bucket under gateway-results/.
        rc = _results_connection()
        buf = io.BytesIO()
        pq.write_table(arrow_tbl, buf, compression="zstd")
        buf.seek(0)
        key = f"{rc.results_prefix}/{job.job_id}.parquet"
        s3_client(rc).upload_fileobj(buf, rc.lake_bucket, key, ExtraArgs=put_sse_args())

        job.rows = arrow_tbl.num_rows
        job.bytes_scanned = buf.getbuffer().nbytes
        job.result_s3_uri = f"s3://{rc.lake_bucket}/{key}"
        job.status = "SUCCEEDED"
        log.info(
            "gateway.query.success",
            job_id=job.job_id,
            rows=job.rows,
            duration_ms=int((time.time() - started) * 1000),
        )
    except Exception as e:
        job.status = "FAILED"
        job.error = repr(e)[:2000]
        log.error("gateway.query.failed", job_id=job.job_id, error=str(e))
    finally:
        job.finished_at = time.time()


def result_signed_url(job: Job, expires_seconds: int = 3600) -> str | None:
    if not job.result_s3_uri:
        return None
    bucket, _, key = job.result_s3_uri.removeprefix("s3://").partition("/")
    return s3_client(_results_connection()).generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )
