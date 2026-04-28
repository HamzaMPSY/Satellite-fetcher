from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _as_str_list(value: Any) -> list[str]:
    return [str(item) for item in list(value or []) if str(item).strip()]


def _as_int_list(value: Any) -> list[int]:
    items: list[int] = []
    for item in list(value or []):
        try:
            items.append(int(item))
        except (TypeError, ValueError):
            continue
    return items


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


@dataclass(slots=True)
class JobRowRecord:
    job_id: str
    job_type: str | None = None
    provider: str | None = None
    collection: str | None = None
    product_type: str | None = None
    tile_id: str | None = None
    worker_id: str | None = None
    state: str | None = None
    pipeline_state: str | None = None
    pipeline_step: str | None = None
    pipeline_progress: float | None = None
    request: dict[str, Any] = field(default_factory=dict)
    pipeline_metadata: dict[str, Any] = field(default_factory=dict)
    conversion_metadata: dict[str, Any] = field(default_factory=dict)
    raw_outputs: list[str] = field(default_factory=list)
    zarr_outputs: list[str] = field(default_factory=list)
    cube_outputs: list[str] = field(default_factory=list)
    masked_zarr_outputs: list[str] = field(default_factory=list)
    watermask_outputs: list[str] = field(default_factory=list)
    cloudmask_outputs: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    progress: float = 0.0
    bytes_downloaded: int = 0
    bytes_total: int = 0
    retry_count: int = 0
    last_retry_at: datetime | None = None
    source_job_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any] | None) -> "JobRowRecord":
        payload = _as_dict(row)
        return cls(
            job_id=str(payload.get("job_id") or "").strip(),
            job_type=str(payload.get("job_type") or "").strip() or None,
            provider=str(payload.get("provider") or "").strip() or None,
            collection=str(payload.get("collection") or "").strip() or None,
            product_type=str(payload.get("product_type") or "").strip() or None,
            tile_id=str(payload.get("tile_id") or "").strip() or None,
            worker_id=str(payload.get("worker_id") or "").strip() or None,
            state=str(payload.get("state") or "").strip() or None,
            pipeline_state=str(payload.get("pipeline_state") or "").strip() or None,
            pipeline_step=str(payload.get("pipeline_step") or "").strip() or None,
            pipeline_progress=float(payload.get("pipeline_progress")) if payload.get("pipeline_progress") is not None else None,
            request=_as_dict(payload.get("request")),
            pipeline_metadata=_as_dict(payload.get("pipeline_metadata")),
            conversion_metadata=_as_dict(payload.get("conversion_metadata")),
            raw_outputs=_as_str_list(payload.get("raw_outputs")),
            zarr_outputs=_as_str_list(payload.get("zarr_outputs")),
            cube_outputs=_as_str_list(payload.get("cube_outputs")),
            masked_zarr_outputs=_as_str_list(payload.get("masked_zarr_outputs")),
            watermask_outputs=_as_str_list(payload.get("watermask_outputs")),
            cloudmask_outputs=_as_str_list(payload.get("cloudmask_outputs")),
            errors=_as_str_list(payload.get("errors")),
            progress=float(payload.get("progress") or 0.0),
            bytes_downloaded=int(payload.get("bytes_downloaded") or 0),
            bytes_total=int(payload.get("bytes_total") or 0),
            retry_count=int(payload.get("retry_count") or 0),
            last_retry_at=_as_datetime(payload.get("last_retry_at")),
            source_job_id=str(payload.get("source_job_id") or "").strip() or None,
            created_at=_as_datetime(payload.get("created_at")),
            updated_at=_as_datetime(payload.get("updated_at")),
            started_at=_as_datetime(payload.get("started_at")),
            finished_at=_as_datetime(payload.get("finished_at")),
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "provider": self.provider,
            "collection": self.collection,
            "product_type": self.product_type,
            "tile_id": self.tile_id,
            "worker_id": self.worker_id,
            "state": self.state,
            "pipeline_state": self.pipeline_state,
            "pipeline_step": self.pipeline_step,
            "pipeline_progress": self.pipeline_progress,
            "request": dict(self.request),
            "pipeline_metadata": dict(self.pipeline_metadata),
            "conversion_metadata": dict(self.conversion_metadata),
            "raw_outputs": list(self.raw_outputs),
            "zarr_outputs": list(self.zarr_outputs),
            "cube_outputs": list(self.cube_outputs),
            "masked_zarr_outputs": list(self.masked_zarr_outputs),
            "watermask_outputs": list(self.watermask_outputs),
            "cloudmask_outputs": list(self.cloudmask_outputs),
            "errors": list(self.errors),
            "progress": self.progress,
            "bytes_downloaded": self.bytes_downloaded,
            "bytes_total": self.bytes_total,
            "retry_count": self.retry_count,
            "last_retry_at": self.last_retry_at.isoformat() if self.last_retry_at else None,
            "source_job_id": self.source_job_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


@dataclass(slots=True)
class JobResultRecord:
    job_id: str
    paths: list[str] = field(default_factory=list)
    raw_outputs: list[str] = field(default_factory=list)
    zarr_outputs: list[str] = field(default_factory=list)
    cube_outputs: list[str] = field(default_factory=list)
    masked_zarr_outputs: list[str] = field(default_factory=list)
    watermask_outputs: list[str] = field(default_factory=list)
    cloudmask_outputs: list[str] = field(default_factory=list)
    checksums: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    manifest_entry: dict[str, Any] = field(default_factory=dict)
    pipeline_metadata: dict[str, Any] = field(default_factory=dict)
    conversion_metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, job_id: str, row: Mapping[str, Any] | None) -> "JobResultRecord":
        payload = _as_dict(row)
        return cls(
            job_id=str(job_id).strip(),
            paths=_as_str_list(payload.get("paths")),
            raw_outputs=_as_str_list(payload.get("raw_outputs")),
            zarr_outputs=_as_str_list(payload.get("zarr_outputs")),
            cube_outputs=_as_str_list(payload.get("cube_outputs")),
            masked_zarr_outputs=_as_str_list(payload.get("masked_zarr_outputs")),
            watermask_outputs=_as_str_list(payload.get("watermask_outputs")),
            cloudmask_outputs=_as_str_list(payload.get("cloudmask_outputs")),
            checksums=_as_dict(payload.get("checksums")),
            metadata=_as_dict(payload.get("metadata")),
            manifest_entry=_as_dict(payload.get("manifest_entry")),
            pipeline_metadata=_as_dict(payload.get("pipeline_metadata")),
            conversion_metadata=_as_dict(payload.get("conversion_metadata")),
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "paths": list(self.paths),
            "raw_outputs": list(self.raw_outputs),
            "zarr_outputs": list(self.zarr_outputs),
            "cube_outputs": list(self.cube_outputs),
            "masked_zarr_outputs": list(self.masked_zarr_outputs),
            "watermask_outputs": list(self.watermask_outputs),
            "cloudmask_outputs": list(self.cloudmask_outputs),
            "checksums": dict(self.checksums),
            "metadata": dict(self.metadata),
            "manifest_entry": dict(self.manifest_entry),
            "pipeline_metadata": dict(self.pipeline_metadata),
            "conversion_metadata": dict(self.conversion_metadata),
        }


@dataclass(slots=True)
class ArtifactRowRecord:
    artifact_id: str
    artifact_type: str
    artifact_uri: str
    provider: str | None = None
    collection: str | None = None
    scene_id: str | None = None
    source_uri: str | None = None
    created_by_job_id: str | None = None
    source_job_id: str | None = None
    data_family: str | None = None
    band_names: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    shape: list[int] = field(default_factory=list)
    size_bytes: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Mapping[str, Any] | None) -> "ArtifactRowRecord":
        payload = _as_dict(row)
        size_bytes = payload.get("size_bytes")
        try:
            normalized_size = int(size_bytes) if size_bytes is not None else None
        except (TypeError, ValueError):
            normalized_size = None
        return cls(
            artifact_id=str(payload.get("artifact_id") or "").strip(),
            artifact_type=str(payload.get("artifact_type") or "").strip(),
            artifact_uri=str(payload.get("artifact_uri") or "").strip(),
            provider=str(payload.get("provider") or "").strip() or None,
            collection=str(payload.get("collection") or "").strip() or None,
            scene_id=str(payload.get("scene_id") or "").strip() or None,
            source_uri=str(payload.get("source_uri") or "").strip() or None,
            created_by_job_id=str(payload.get("created_by_job_id") or "").strip() or None,
            source_job_id=str(payload.get("source_job_id") or "").strip() or None,
            data_family=str(payload.get("data_family") or "").strip() or None,
            band_names=_as_str_list(payload.get("band_names")),
            dimensions=_as_str_list(payload.get("dimensions")),
            shape=_as_int_list(payload.get("shape")),
            size_bytes=normalized_size,
            metadata=_as_dict(payload.get("metadata")),
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "artifact_uri": self.artifact_uri,
            "provider": self.provider,
            "collection": self.collection,
            "scene_id": self.scene_id,
            "source_uri": self.source_uri,
            "created_by_job_id": self.created_by_job_id,
            "source_job_id": self.source_job_id,
            "data_family": self.data_family,
            "band_names": list(self.band_names),
            "dimensions": list(self.dimensions),
            "shape": list(self.shape),
            "size_bytes": self.size_bytes,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class JobEventRecord:
    id: int | None
    job_id: str
    type: str
    timestamp: datetime | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Mapping[str, Any] | None) -> "JobEventRecord":
        payload = _as_dict(row)
        event_id = payload.get("id")
        try:
            normalized_id = int(event_id) if event_id is not None else None
        except (TypeError, ValueError):
            normalized_id = None
        return cls(
            id=normalized_id,
            job_id=str(payload.get("job_id") or "").strip(),
            type=str(payload.get("type") or "").strip(),
            timestamp=_as_datetime(payload.get("timestamp")),
            payload=_as_dict(payload.get("payload")),
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_id": self.job_id,
            "type": self.type,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "payload": dict(self.payload),
        }


@dataclass(slots=True)
class WorkerHeartbeatRecord:
    worker_id: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Mapping[str, Any] | None) -> "WorkerHeartbeatRecord":
        payload = _as_dict(row)
        return cls(
            worker_id=str(payload.get("worker_id") or "").strip(),
            payload=payload,
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            **dict(self.payload),
        }
