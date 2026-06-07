import os

import pytest

# Stub env so settings can parse without a real .env
os.environ.setdefault("PG_REPLICA_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("METADATA_PG_DSN", "postgresql://test:test@localhost:5432/metadata")
os.environ.setdefault("AWS_ACCOUNT_ID", "000000000000")
os.environ.setdefault("RAW_BUCKET", "test-raw")
os.environ.setdefault("CURATED_BUCKET", "test-curated")
os.environ.setdefault("GATEWAY_RESULTS_BUCKET", "test-results")
os.environ.setdefault("ICEBERG_WAREHOUSE_S3", "s3://test-curated/")


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    from viamedia_pipeline.common.settings import get_settings
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
