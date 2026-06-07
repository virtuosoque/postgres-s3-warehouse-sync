"""Trigger Dagster jobs + read run status from the gateway via GraphQL.

The gateway is the single UI; it drives Dagster (the engine) behind the scenes.
Targets the in-cluster webserver at DAGSTER_GRAPHQL_URL (default the compose
service `dagster:3000`).
"""

import json
import os

import httpx

from viamedia_pipeline.common.logging import get_logger

log = get_logger(__name__)

_LOCATION = "viamedia_pipeline"
_REPO = "__repository__"

_LAUNCH = """
mutation($p: ExecutionParams!) {
  launchRun(executionParams: $p) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
    ... on RunConfigValidationInvalid { errors { message } }
    ... on PipelineNotFoundError { message }
  }
}
"""

_STATUS = """
query($id: ID!) {
  runOrError(runId: $id) {
    __typename
    ... on Run { runId status startTime endTime }
    ... on PythonError { message }
  }
}
"""


def _graphql_url() -> str:
    return os.environ.get("DAGSTER_GRAPHQL_URL", "http://dagster:3000/graphql")


def _launch(job_name: str, run_config: dict) -> dict:
    params = {
        "selector": {
            "repositoryLocationName": _LOCATION,
            "repositoryName": _REPO,
            "jobName": job_name,
        },
        "runConfigData": json.dumps(run_config),
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(_graphql_url(), json={"query": _LAUNCH, "variables": {"p": params}})
        resp.raise_for_status()
        body = resp.json()
    data = body.get("data", {}).get("launchRun")
    if not data or data.get("__typename") != "LaunchRunSuccess":
        msg = (data or {}).get("message") or json.dumps(data)
        raise RuntimeError(f"launch failed: {msg}")
    log.info("dagster.launch", job=job_name, run_id=data["run"]["runId"])
    return data["run"]


def launch_bootstrap(connection_id: int, table_fqn: str) -> dict:
    return _launch("bootstrap_table_job", {
        "ops": {"plan_bootstrap": {"config": {
            "connection_id": connection_id, "table_fqn": table_fqn}}}
    })


def launch_incremental(connection_id: int, table_fqn: str) -> dict:
    return _launch("incremental_job", {
        "ops": {"incremental_one_table": {"config": {
            "connection_id": connection_id, "table_fqn": table_fqn}}}
    })


def run_status(run_id: str) -> dict:
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(_graphql_url(), json={"query": _STATUS, "variables": {"id": run_id}})
        resp.raise_for_status()
        return resp.json().get("data", {}).get("runOrError", {})


_RUNS = """
query($f: RunsFilter!, $limit: Int!) {
  runsOrError(filter: $f, limit: $limit) {
    __typename
    ... on Runs { results { runId status jobName startTime tags { key value } } }
    ... on PythonError { message }
  }
}
"""


def recent_runs(connection_id: int, limit: int = 25) -> list[dict]:
    """Recent runs for a connection (runs are tagged connection=<id>)."""
    variables = {"f": {"tags": [{"key": "connection", "value": str(connection_id)}]}, "limit": limit}
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(_graphql_url(), json={"query": _RUNS, "variables": variables})
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("runsOrError", {})
    if data.get("__typename") != "Runs":
        return []
    out = []
    for r in data.get("results", []):
        tags = {t["key"]: t["value"] for t in r.get("tags", [])}
        out.append({"runId": r["runId"], "status": r["status"], "jobName": r["jobName"],
                    "startTime": r.get("startTime"), "table": tags.get("table", ""),
                    "kind": tags.get("kind", "")})
    return out
