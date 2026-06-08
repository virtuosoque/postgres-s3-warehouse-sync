"""Per-connection S3 clients.

Each connection carries its own AWS credentials + region + bucket. We build (and
cache) one boto3 client per connection. When AWS_ENDPOINT_URL is set (LocalStack)
we use path-style addressing; otherwise virtual-hosted style for real AWS.
"""

import os

import boto3
from botocore.config import Config

from viamedia_pipeline.common.config_store import Connection

_CLIENTS: dict[int, object] = {}


def s3_client(conn: Connection):
    client = _CLIENTS.get(conn.id)
    if client is not None:
        return client

    addressing_style = "path" if os.environ.get("AWS_ENDPOINT_URL") else "virtual"
    kwargs = {
        "region_name": conn.aws_region,
        "config": Config(
            retries={"max_attempts": 10, "mode": "adaptive"},
            max_pool_connections=64,
            s3={"addressing_style": addressing_style},
        ),
    }
    # Per-connection credentials (fall back to the default chain / instance role
    # when a connection doesn't carry explicit keys).
    if conn.aws_access_key and conn.aws_secret_key:
        kwargs["aws_access_key_id"] = conn.aws_access_key
        kwargs["aws_secret_access_key"] = conn.aws_secret_key

    client = boto3.client("s3", **kwargs)
    _CLIENTS[conn.id] = client
    return client


def delete_prefix(conn: Connection, prefix: str) -> int:
    """Delete every object under `prefix` in the connection's lake bucket.
    Returns the number of objects deleted."""
    cli = s3_client(conn)
    bucket = conn.lake_bucket
    paginator = cli.get_paginator("list_objects_v2")
    total = 0
    batch: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) >= 1000:
                cli.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                total += len(batch)
                batch = []
    if batch:
        cli.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        total += len(batch)
    return total


def put_sse_args() -> dict:
    """ExtraArgs for SSE-S3 (AES256). Bucket-default encryption also applies,
    but passing this explicitly fails fast if a request would land unencrypted."""
    return {"ServerSideEncryption": "AES256"}
