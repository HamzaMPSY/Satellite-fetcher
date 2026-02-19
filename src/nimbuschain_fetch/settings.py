from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    nimbus_db_backend: str = Field(default="mongodb", alias="NIMBUS_DB_BACKEND")
    nimbus_db_path: Path = Field(default=Path("./data/nimbus.db"), alias="NIMBUS_DB_PATH")
    nimbus_mongodb_uri: str = Field(
        default="mongodb://127.0.0.1:27017",
        alias="NIMBUS_MONGODB_URI",
    )
    nimbus_mongodb_db: str = Field(default="nimbuschain_fetch", alias="NIMBUS_MONGODB_DB")

    nimbus_data_dir: Path = Field(default=Path("./data/downloads"), alias="NIMBUS_DATA_DIR")
    nimbus_runtime_role: str = Field(default="all", alias="NIMBUS_RUNTIME_ROLE")
    nimbus_max_jobs: int = Field(default=4, alias="NIMBUS_MAX_JOBS", ge=1, le=128)
    nimbus_queue_poll_seconds: float = Field(
        default=1.0,
        alias="NIMBUS_QUEUE_POLL_SECONDS",
        ge=0.1,
        le=30.0,
    )
    nimbus_stale_job_seconds: int = Field(
        default=900,
        alias="NIMBUS_STALE_JOB_SECONDS",
        ge=30,
        le=86400,
    )
    nimbus_log_level: str = Field(default="INFO", alias="NIMBUS_LOG_LEVEL")
    nimbus_log_json: bool = Field(default=False, alias="NIMBUS_LOG_JSON")
    nimbus_enable_metrics: bool = Field(default=True, alias="NIMBUS_ENABLE_METRICS")

    nimbus_api_key: str | None = Field(default=None, alias="NIMBUS_API_KEY")
    nimbus_cors_origins: str = Field(default="", alias="NIMBUS_CORS_ORIGINS")
    nimbus_max_request_mb: int = Field(default=10, alias="NIMBUS_MAX_REQUEST_MB", ge=1, le=200)
    nimbus_provider_limits: str = Field(
        default="copernicus=2,usgs=4", alias="NIMBUS_PROVIDER_LIMITS"
    )

    nimbus_copernicus_base_url: str = Field(
        default="https://catalogue.dataspace.copernicus.eu", alias="NIMBUS_COPERNICUS_BASE_URL"
    )
    nimbus_copernicus_token_url: str = Field(
        default=(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        ),
        alias="NIMBUS_COPERNICUS_TOKEN_URL",
    )
    nimbus_copernicus_download_url: str = Field(
        default="https://zipper.dataspace.copernicus.eu", alias="NIMBUS_COPERNICUS_DOWNLOAD_URL"
    )
    nimbus_copernicus_username: str | None = Field(
        default=None, alias="NIMBUS_COPERNICUS_USERNAME"
    )
    nimbus_copernicus_password: str | None = Field(
        default=None, alias="NIMBUS_COPERNICUS_PASSWORD"
    )

    nimbus_usgs_service_url: str = Field(
        default="https://m2m.cr.usgs.gov/api/api/json/stable/",
        alias="NIMBUS_USGS_SERVICE_URL",
    )
    nimbus_usgs_username: str | None = Field(default=None, alias="NIMBUS_USGS_USERNAME")
    nimbus_usgs_token: str | None = Field(default=None, alias="NIMBUS_USGS_TOKEN")

    @property
    def cors_origins(self) -> list[str]:
        if not self.nimbus_cors_origins.strip():
            return []
        return [item.strip() for item in self.nimbus_cors_origins.split(",") if item.strip()]

    @property
    def provider_limits_map(self) -> dict[str, int]:
        parsed: dict[str, int] = {"copernicus": 2, "usgs": 4}
        raw = (self.nimbus_provider_limits or "").strip()
        if not raw:
            return parsed

        for chunk in raw.split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            name, value = item.split("=", 1)
            key = name.strip().lower()
            if not key:
                continue
            try:
                val = int(value.strip())
            except ValueError:
                continue
            parsed[key] = max(1, val)
        return parsed

    @property
    def runtime_role(self) -> str:
        value = self.nimbus_runtime_role.strip().lower()
        if value in {"all", "api", "worker"}:
            return value
        return "all"

    def ensure_runtime_dirs(self) -> None:
        self.nimbus_data_dir.mkdir(parents=True, exist_ok=True)
        if self.nimbus_db_backend.strip().lower() == "sqlite":
            self.nimbus_db_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
