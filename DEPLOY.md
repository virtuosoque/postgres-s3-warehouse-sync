# Manual EC2 deployment

Distilled from the (now-removed) Terraform so you have the essentials for a
hand-rolled deploy. The app runs as a single `docker compose` stack managed by
systemd, same shape as [compose.prod.yml](compose.prod.yml).

## 1. EC2 host
- Instance: `m7g.xlarge` (Graviton) is the reference size; anything with ~4 vCPU / 16 GB works for a start.
- OS: Amazon Linux 2023 (has `dnf`, `awscli`, SSM agent available).
- Attach a separate **EBS gp3 volume** (e.g. 200 GB) for `/data` — it holds the metadata-postgres data and Caddy state, so the catalog + watermarks survive instance replacement.
- Install Docker + the **compose v2 plugin**: `dnf install -y docker && systemctl enable --now docker`, then install the compose plugin (or use the `docker-compose` binary and adjust commands).

## 2. IAM instance role (so no AWS keys live on the box)
Attach an instance profile whose role has this policy. Replace the bucket names.
This is the minimum for the pipeline to read/write the lake:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LakeRW",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-RAW-BUCKET",            "arn:aws:s3:::YOUR-RAW-BUCKET/*",
        "arn:aws:s3:::YOUR-CURATED-BUCKET",        "arn:aws:s3:::YOUR-CURATED-BUCKET/*",
        "arn:aws:s3:::YOUR-GATEWAY-RESULTS-BUCKET","arn:aws:s3:::YOUR-GATEWAY-RESULTS-BUCKET/*"
      ]
    }
  ]
}
```

Optional add-ons (attach AWS-managed policies / statements as needed):
- **SSM Session Manager** (shell without SSH/open ports): managed policy `AmazonSSMManagedInstanceCore`.
- **CloudWatch logs**: `logs:CreateLogGroup/CreateLogStream/PutLogEvents/DescribeLogStreams`.
- **ECR pull** (only if you push images to ECR instead of building on the box): `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:GetDownloadUrlForLayer`, `ecr:BatchGetImage`.
- **Secrets Manager** (only if you store the source DSN there instead of in `.env`): `secretsmanager:GetSecretValue` on that secret ARN.

With the role attached, boto3 / PyIceberg / DuckDB get S3 credentials automatically — **leave `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` OUT of the prod `.env`.**

## 3. Buckets
Create them yourself (the pipeline does not): raw, curated, gateway-results
(+ compute-logs if you keep S3 compute logs). Default **SSE-S3 (AES256)** is fine — no KMS.

> ⚠️ The loader uses PyIceberg `add_files`, which registers the bootstrap
> parquet **in place** under `RAW_BUCKET`. Do **not** put a short TTL/expiry
> lifecycle on the raw bucket or committed table data will be deleted. Either
> drop the TTL, or run compaction to rewrite into curated before expiry.

## 4. `/opt/viamedia/.env` on the host — bootstrap only

Source databases, buckets, namespaces, sync selection, secrets, and operational
tuning are now **managed in the web UI** (stored in the metadata DB). The host
`.env` only needs the bootstrap values:

```
METADATA_PG_DSN=postgresql://pipeline:<STRONG_PW>@metadata-postgres:5432/pipeline_metadata
METADATA_PG_SCHEMA=pipeline
# Fernet key used to encrypt connection secrets (DSNs, AWS keys) at rest.
# Generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
CONFIG_ENCRYPTION_KEY=<fernet-key>
LOG_LEVEL=INFO
# Gateway -> Dagster (compose service name):
DAGSTER_GRAPHQL_URL=http://dagster:3000/graphql
# Optional prod JWT for the gateway (empty = open/local):
GATEWAY_JWT_ISSUER=https://<your-idp>/
GATEWAY_JWKS_URL=https://<your-idp>/.well-known/jwks.json
```

After the stack is up, open the gateway UI and add each source database under
**Connections** (its own bucket + namespace + AWS creds), then pick objects under
**Sync**. The IAM role's `LakeRW` S3 permissions (section 2) must cover every
connection's bucket. (For a one-time migration from an old single-DB `.env`, the
legacy vars — `PG_REPLICA_DSN`, `CURATED_BUCKET`, `ICEBERG_NAMESPACE`,
`SYNC_INCLUDE`, `AWS_ACCESS_KEY_ID`/`SECRET` — if present, seed a `default`
connection automatically on first boot.)

Use [compose.prod.yml](compose.prod.yml) as the stack definition (it mounts
`/data/metadata-postgres`, runs `migrate` then `extractor`/`dagster`/`gateway`,
and fronts the gateway with Caddy). You'll need a `daemon` service too — copy the
`daemon` block from [docker-compose.localaws.yml](docker-compose.localaws.yml)
(QueuedRunCoordinator needs the daemon to dequeue runs and to tick the
schedule/sensor). For prod compute logs to S3, set `COMPUTE_LOGS_BUCKET`; for
local-disk logs reuse [docker/dagster.dev.yaml](docker/dagster.dev.yaml).

## 5. systemd unit (`/etc/systemd/system/viamedia.service`)
```ini
[Unit]
Description=Viamedia data pipeline (docker compose)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/viamedia
ExecStart=/usr/bin/docker compose -f /opt/viamedia/compose.prod.yml --env-file /opt/viamedia/.env up -d
ExecStop=/usr/bin/docker compose -f /opt/viamedia/compose.prod.yml down
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
```
```
systemctl daemon-reload && systemctl enable --now viamedia.service
```
(If you build images on the box, `docker compose build` first; if you pull from
ECR, add an `ExecStartPre` that runs `aws ecr get-login-password | docker login`.)

## 6. First sync
Open the Dagster UI (`http://<host>:3000`, or via Caddy/SSM tunnel), launch
`bootstrap_table_job` for your table, then enable the
`every_five_minutes_per_table` schedule for incrementals.
