from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import requests


class Sen2LikeServiceClient:
    """Thin HTTP client for the standalone Sen2Like normalization service."""

    def __init__(self, *, service_url: str):
        normalized = str(service_url or "").strip().rstrip("/")
        if not normalized:
            raise ValueError("service_url is required for Sen2LikeServiceClient.")
        self.service_url = normalized
        self._session = requests.Session()

    def close(self) -> None:
        self._session.close()

    def health(self) -> tuple[int, dict[str, Any]]:
        return self._get_json("/health")

    def readiness(self) -> tuple[int, dict[str, Any]]:
        return self._get_json("/readiness")

    def schema(self) -> tuple[int, dict[str, Any]]:
        return self._get_json("/schema")

    def normalize(
        self,
        *,
        products: Sequence[str],
        job_id: str | None = None,
        pipeline_id: str | None = None,
        trace_id: str | None = None,
        working_dir: str | None = None,
        workers: int = 4,
        steps: Sequence[str] | None = None,
        s2_path: str | None = None,
        no_resume: bool = False,
        no_routing: bool = False,
        router_fallback_ok: bool = False,
        exclude_water: bool = False,
        cleanup_mode: str = "none",
        cleanup_dry_run: bool = False,
        timeout_seconds: float | None = None,
        spark_master: str | None = None,
    ) -> dict[str, Any]:
        response = self._session.post(
            f"{self.service_url}/normalize",
            json={
                "job_id": job_id,
                "pipeline_id": pipeline_id,
                "trace_id": trace_id,
                "products": list(products),
                "working_dir": working_dir,
                "workers": int(workers),
                "steps": list(steps) if steps else None,
                "s2_path": s2_path,
                "no_resume": bool(no_resume),
                "no_routing": bool(no_routing),
                "router_fallback_ok": bool(router_fallback_ok),
                "exclude_water": bool(exclude_water),
                "cleanup_mode": cleanup_mode,
                "cleanup_dry_run": bool(cleanup_dry_run),
                "timeout_seconds": timeout_seconds,
                "spark_master": spark_master,
            },
            timeout=(30, None),
        )
        payload = self._response_payload(response)
        if response.status_code >= 500:
            raise RuntimeError(self._error_detail(payload, response, prefix="Sen2Like service failed"))
        if response.status_code >= 400:
            raise ValueError(self._error_detail(payload, response, prefix="Sen2Like request was rejected"))
        return payload

    def _get_json(self, path: str) -> tuple[int, dict[str, Any]]:
        response = self._session.get(f"{self.service_url}{path}", timeout=30)
        payload = self._response_payload(response)
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"Sen2Like service returned a non-object payload for {path}.")
        return int(response.status_code), dict(payload)

    @staticmethod
    def _response_payload(response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, Mapping):
            return {}
        return dict(payload)

    @staticmethod
    def _error_detail(payload: Mapping[str, Any], response: requests.Response, *, prefix: str) -> str:
        detail = payload.get("detail")
        if detail:
            if isinstance(detail, Mapping):
                return f"{prefix}: {Sen2LikeServiceClient._summarize_structured_error(detail)}"
            return f"{prefix}: {detail}"
        text = str(getattr(response, "text", "") or "").strip()
        if text:
            return f"{prefix}: HTTP {response.status_code}: {text[:500]}"
        return f"{prefix}: HTTP {response.status_code}"

    @staticmethod
    def _summarize_structured_error(detail: Mapping[str, Any]) -> str:
        stderr_tail = str(detail.get("stderr_tail") or "").strip()
        lowered = stderr_tail.lower()
        if "failed to create a temp directory" in lowered:
            spark_dir = str(
                dict(detail.get("metadata") or {}).get("spark_local_dirs")
                or ""
            ).strip()
            suffix = f" ({spark_dir})" if spark_dir else ""
            return f"Spark could not create its temporary directory{suffix}."
        if "permission denied" in lowered:
            return "Sen2Like could not write to one of its working directories."
        if "no such file or directory" in lowered:
            return "Sen2Like could not find one of the input files or runtime paths."

        metadata = dict(detail.get("metadata") or {})
        return_code = detail.get("return_code")
        try:
            numeric_return_code = int(return_code)
        except (TypeError, ValueError):
            numeric_return_code = None
        if numeric_return_code in {-9, 137}:
            return (
                "Sen2Like was killed during processing, most likely because the "
                "Podman VM or Sen2Like container does not have enough memory."
            )

        input_issues = list(metadata.get("input_issues") or [])
        if input_issues:
            first_issue = input_issues[0]
            if isinstance(first_issue, Mapping):
                message = str(first_issue.get("message") or "").strip()
                if message:
                    return message
            return "Sen2Like input is missing or invalid."

        output_issues = list(metadata.get("output_issues") or [])
        if output_issues:
            first_issue = output_issues[0]
            if isinstance(first_issue, Mapping):
                message = str(first_issue.get("message") or "").strip()
                if message:
                    return message
            return "Sen2Like did not produce a valid Sentinel-like SAFE output."

        duration = detail.get("duration_seconds")
        bits = ["Sen2Like subprocess failed"]
        if return_code is not None:
            bits.append(f"exit code {return_code}")
        if duration is not None:
            try:
                bits.append(f"{float(duration):.1f}s")
            except (TypeError, ValueError):
                pass
        if len(bits) == 1:
            return f"{bits[0]}."
        return f"{bits[0]} after {', '.join(bits[1:])}."
