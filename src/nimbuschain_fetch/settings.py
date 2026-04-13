from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
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
    nimbus_mask_service_url: str | None = Field(default=None, alias="NIMBUS_MASK_SERVICE_URL")
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
    nimbus_worker_heartbeat_seconds: float = Field(
        default=5.0,
        alias="NIMBUS_WORKER_HEARTBEAT_SECONDS",
        ge=1.0,
        le=300.0,
    )
    nimbus_worker_stale_seconds: int = Field(
        default=20,
        alias="NIMBUS_WORKER_STALE_SECONDS",
        ge=5,
        le=3600,
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
    nimbus_copernicus_account_pool_json: str | None = Field(
        default=None,
        alias="NIMBUS_COPERNICUS_ACCOUNT_POOL_JSON",
    )
    nimbus_copernicus_account_pool_file: Path | None = Field(
        default=None,
        alias="NIMBUS_COPERNICUS_ACCOUNT_POOL_FILE",
    )
    nimbus_copernicus_account_pool_concurrency: int = Field(
        default=4,
        alias="NIMBUS_COPERNICUS_ACCOUNT_POOL_CONCURRENCY",
        ge=1,
        le=8,
    )

    nimbus_usgs_service_url: str = Field(
        default="https://m2m.cr.usgs.gov/api/api/json/stable/",
        alias="NIMBUS_USGS_SERVICE_URL",
    )
    nimbus_usgs_username: str | None = Field(default=None, alias="NIMBUS_USGS_USERNAME")
    nimbus_usgs_token: str | None = Field(default=None, alias="NIMBUS_USGS_TOKEN")

    @field_validator(
        "nimbus_copernicus_base_url",
        "nimbus_copernicus_token_url",
        "nimbus_copernicus_download_url",
        "nimbus_usgs_service_url",
        "nimbus_mask_service_url",
        mode="before",
    )
    @classmethod
    def _strip_required_strings(cls, value: str | None) -> str | None:
        if value is None:
            return value
        return str(value).strip()

    @field_validator(
        "nimbus_api_key",
        "nimbus_copernicus_username",
        "nimbus_copernicus_password",
        "nimbus_copernicus_account_pool_json",
        "nimbus_usgs_username",
        "nimbus_usgs_token",
        mode="before",
    )
    @classmethod
    def _strip_optional_strings(cls, value: str | None) -> str | None:
        if value is None:
            return value
        cleaned = str(value).strip()
        return cleaned or None

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

    @staticmethod
    def _normalize_copernicus_account_entry(
        item: Any,
        *,
        default_label: str,
    ) -> dict[str, str] | None:
        if not isinstance(item, dict):
            return None
        username = str(item.get("username") or "").strip()
        password = str(item.get("password") or "").strip()
        if not username or not password:
            return None
        label = str(item.get("label") or default_label).strip() or default_label
        return {
            "username": username,
            "password": password,
            "label": label,
        }

    def _load_copernicus_account_pool_entries(self) -> list[dict[str, str]]:
        raw_payload = str(self.nimbus_copernicus_account_pool_json or "").strip()
        if self.nimbus_copernicus_account_pool_file:
            try:
                if self.nimbus_copernicus_account_pool_file.exists():
                    raw_payload = self.nimbus_copernicus_account_pool_file.read_text(encoding="utf-8").strip()
            except Exception:
                raw_payload = ""
        if not raw_payload:
            return []
        try:
            parsed = json.loads(raw_payload)
        except Exception:
            return []
        if isinstance(parsed, dict):
            items = parsed.get("accounts")
            if not isinstance(items, list):
                return []
        elif isinstance(parsed, list):
            items = parsed
        else:
            return []

        entries: list[dict[str, str]] = []
        for index, item in enumerate(items, start=1):
            normalized = self._normalize_copernicus_account_entry(
                item,
                default_label=f"pool-{index}",
            )
            if normalized is not None:
                entries.append(normalized)
        return entries

    @property
    def copernicus_account_pool_accounts(self) -> list[dict[str, str]]:
        accounts: list[dict[str, str]] = []
        seen_usernames: set[str] = set()

        primary = self._normalize_copernicus_account_entry(
            {
                "username": self.nimbus_copernicus_username,
                "password": self.nimbus_copernicus_password,
                "label": "primary",
            },
            default_label="primary",
        )
        if primary is not None:
            accounts.append(primary)
            seen_usernames.add(primary["username"].lower())

        for item in self._load_copernicus_account_pool_entries():
            key = item["username"].lower()
            if key in seen_usernames:
                continue
            accounts.append(item)
            seen_usernames.add(key)
        return accounts

    @property
    def copernicus_account_pool_size(self) -> int:
        return len(self.copernicus_account_pool_accounts)

    @property
    def copernicus_account_pool_available(self) -> bool:
        return self.copernicus_account_pool_size > 1

    def ensure_runtime_dirs(self) -> None:
        self.nimbus_data_dir.mkdir(parents=True, exist_ok=True)
        if self.nimbus_db_backend.strip().lower() == "sqlite":
            self.nimbus_db_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
