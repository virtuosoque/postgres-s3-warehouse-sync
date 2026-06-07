"""Dagster resources -- thin wrappers so ops/assets don't import lazily."""

from dagster import ConfigurableResource

from viamedia_pipeline.common.settings import Settings, get_settings


class SettingsResource(ConfigurableResource):
    """Exposes parsed env settings to ops/assets via Dagster's resource system."""

    def get(self) -> Settings:
        return get_settings()
