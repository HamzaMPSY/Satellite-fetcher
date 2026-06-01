from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, PrivateAttr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from nimbuschain_fetch.launch_modes import (
    PipelineLaunchMode,
    default_host_mps_mask_url,
    normalize_pipeline_launch_mode,
    service_defaults,
)


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    _explicit_setting_names: set[str] = PrivateAttr(default_factory=set)

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
    nimbus_pipeline_launch_mode: str = Field(default="mps", alias="NIMBUS_PIPELINE_LAUNCH_MODE")
    nimbus_host_mps_mask_url: str = Field(
        default_factory=default_host_mps_mask_url,
        alias="NIMBUS_HOST_MPS_MASK_URL",
    )
    nimbus_mask_service_url: str | None = Field(default=None, alias="NIMBUS_MASK_SERVICE_URL")
    nimbus_integrated_mask_water_backend: str = Field(
        default="omniwatermask",
        alias="NIMBUS_INTEGRATED_MASK_WATER_BACKEND",
    )
    nimbus_integrated_mask_fail_on_error: bool = Field(
        default=True,
        alias="NIMBUS_INTEGRATED_MASK_FAIL_ON_ERROR",
    )
    nimbus_zarr_service_url: str | None = Field(default=None, alias="NIMBUS_ZARR_SERVICE_URL")
    nimbus_sen2like_service_url: str | None = Field(
        default=None,
        alias="NIMBUS_SEN2LIKE_SERVICE_URL",
    )
    nimbus_sen2like_work_dir: str | None = Field(
        default=None,
        alias="NIMBUS_SEN2LIKE_WORK_DIR",
    )
    nimbus_sen2like_workers: int = Field(
        default=4,
        alias="NIMBUS_SEN2LIKE_WORKERS",
        ge=1,
        le=128,
    )
    nimbus_sen2like_timeout_seconds: float | None = Field(
        default=3600.0,
        alias="NIMBUS_SEN2LIKE_TIMEOUT_SECONDS",
        gt=0,
    )
    nimbus_sen2like_raw_fallback: bool = Field(
        default=False,
        alias="NIMBUS_SEN2LIKE_RAW_FALLBACK",
    )
    nimbus_sen2like_nested_band_parallelism: bool = Field(
        default=True,
        alias="NIMBUS_SEN2LIKE_NESTED_BAND_PARALLELISM",
    )
    nimbus_sen2like_band_workers: int = Field(
        default=4,
        alias="NIMBUS_SEN2LIKE_BAND_WORKERS",
        ge=1,
        le=32,
    )
    nimbus_sen2like_safe_retry: bool = Field(
        default=True,
        alias="NIMBUS_SEN2LIKE_SAFE_RETRY",
    )
    nimbus_sen2like_preprocess_target_shape: str | None = Field(
        default="native",
        alias="NIMBUS_SEN2LIKE_PREPROCESS_TARGET_SHAPE",
    )
    nimbus_sen2like_direct_zarr: bool = Field(
        default=False,
        alias="NIMBUS_SEN2LIKE_DIRECT_ZARR",
    )
    nimbus_sen2like_zarr_dir: str | None = Field(
        default=None,
        alias="NIMBUS_SEN2LIKE_ZARR_DIR",
    )
    nimbus_runtime_role: str = Field(default="all", alias="NIMBUS_RUNTIME_ROLE")
    nimbus_executor_backend: str = Field(default="inprocess", alias="NIMBUS_EXECUTOR_BACKEND")
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
    nimbus_provider_job_limits: str | None = Field(
        default=None,
        alias="NIMBUS_PROVIDER_JOB_LIMITS",
    )
    nimbus_provider_control_plane_limits: str | None = Field(
        default=None,
        alias="NIMBUS_PROVIDER_CONTROL_PLANE_LIMITS",
    )
    nimbus_provider_data_plane_limits: str | None = Field(
        default=None,
        alias="NIMBUS_PROVIDER_DATA_PLANE_LIMITS",
    )
    nimbus_download_global_limit: int = Field(
        default=8,
        alias="NIMBUS_DOWNLOAD_GLOBAL_LIMIT",
        ge=1,
        le=256,
    )
    nimbus_download_min_free_bytes: int = Field(
        default=0,
        alias="NIMBUS_DOWNLOAD_MIN_FREE_BYTES",
        ge=0,
    )
    nimbus_download_global_max_bps: int | None = Field(
        default=None,
        alias="NIMBUS_DOWNLOAD_GLOBAL_MAX_BPS",
        ge=1,
    )
    nimbus_download_coordinator_db_path: Path | None = Field(
        default=None,
        alias="NIMBUS_DOWNLOAD_COORDINATOR_DB_PATH",
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

    def __init__(self, **values: Any):
        explicit_names: set[str] = set()
        for field_name, field_info in type(self).model_fields.items():
            alias = field_info.alias or field_name
            if field_name in values or alias in values:
                explicit_names.add(field_name)
        super().__init__(**values)
        self._explicit_setting_names = explicit_names

    @field_validator(
        "nimbus_pipeline_launch_mode",
        mode="before",
    )
    @classmethod
    def _normalize_pipeline_launch_mode(cls, value: str | None) -> str:
        return normalize_pipeline_launch_mode(value).value

    @field_validator(
        "nimbus_copernicus_base_url",
        "nimbus_copernicus_token_url",
        "nimbus_copernicus_download_url",
        "nimbus_usgs_service_url",
        "nimbus_host_mps_mask_url",
        "nimbus_mask_service_url",
        "nimbus_zarr_service_url",
        "nimbus_sen2like_service_url",
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
        "nimbus_sen2like_work_dir",
        "nimbus_sen2like_preprocess_target_shape",
        "nimbus_sen2like_zarr_dir",
        "nimbus_integrated_mask_water_backend",
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

    @staticmethod
    def _parse_provider_limit_string(
        raw: str | None,
        *,
        defaults: dict[str, int],
    ) -> dict[str, int]:
        parsed: dict[str, int] = {
            str(name).strip().lower(): max(1, int(value))
            for name, value in defaults.items()
            if str(name).strip()
        }
        value = str(raw or "").strip()
        if not value:
            return parsed

        for chunk in value.split(","):
            item = chunk.strip()
            if not item or "=" not in item:
                continue
            name, limit = item.split("=", 1)
            key = name.strip().lower()
            if not key:
                continue
            try:
                parsed[key] = max(1, int(limit.strip()))
            except ValueError:
                continue
        return parsed

    def _setting_is_explicit(self, field_name: str) -> bool:
        if field_name in self._explicit_setting_names:
            return True
        field_info = type(self).model_fields.get(field_name)
        alias = field_info.alias if field_info is not None else None
        return bool(alias and alias in os.environ)

    @property
    def provider_limits_map(self) -> dict[str, int]:
        return self.provider_job_limits_map

    @property
    def provider_job_limits_map(self) -> dict[str, int]:
        raw: str | None = None
        if self._setting_is_explicit("nimbus_provider_job_limits"):
            raw = self.nimbus_provider_job_limits
        elif self._setting_is_explicit("nimbus_provider_limits"):
            raw = self.nimbus_provider_limits
        return self._parse_provider_limit_string(
            raw,
            defaults={"copernicus": 2, "usgs": 4},
        )

    @property
    def provider_control_plane_limits_map(self) -> dict[str, int]:
        raw: str | None = None
        if self._setting_is_explicit("nimbus_provider_control_plane_limits"):
            raw = self.nimbus_provider_control_plane_limits
        elif self._setting_is_explicit("nimbus_provider_limits"):
            raw = self.nimbus_provider_limits
        return self._parse_provider_limit_string(
            raw,
            defaults={"copernicus": 2, "usgs": 1},
        )

    @property
    def provider_data_plane_limits_map(self) -> dict[str, int]:
        raw = self.nimbus_provider_data_plane_limits if self._setting_is_explicit("nimbus_provider_data_plane_limits") else None
        return self._parse_provider_limit_string(
            raw,
            defaults={"copernicus": 32, "usgs": 6},
        )

    @property
    def download_coordinator_db_path(self) -> Path:
        configured = self.nimbus_download_coordinator_db_path
        if configured is not None:
            return configured
        return self.nimbus_data_dir / "download_coordinator.db"

    @property
    def runtime_role(self) -> str:
        value = self.nimbus_runtime_role.strip().lower()
        if value in {"all", "api", "worker"}:
            return value
        return "all"

    @property
    def effective_zarr_service_url(self) -> str:
        defaults = service_defaults(self.nimbus_pipeline_launch_mode)
        return str(self.nimbus_zarr_service_url or defaults.zarr_service_url).strip()

    @property
    def effective_mask_service_url(self) -> str:
        launch_mode = normalize_pipeline_launch_mode(self.nimbus_pipeline_launch_mode)
        defaults = service_defaults(launch_mode)
        if launch_mode is PipelineLaunchMode.mps:
            return str(self.nimbus_host_mps_mask_url or defaults.mask_service_url).strip()
        return str(self.nimbus_mask_service_url or defaults.mask_service_url).strip()

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
        self.download_coordinator_db_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
