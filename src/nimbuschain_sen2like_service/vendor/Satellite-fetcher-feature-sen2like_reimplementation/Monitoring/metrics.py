
from __future__ import annotations
import logging
import os
import time

log = logging.getLogger("sen2like.metrics")

# ---------------------------------------------------------------------------
# Pushgateway URL — override with env var in production
# ---------------------------------------------------------------------------
_GATEWAY = os.environ.get("PROMETHEUS_PUSHGATEWAY", "http://localhost:9091")
_ENABLED = os.environ.get("METRICS_ENABLED", "1") != "0"


class MetricsCollector:

    def __init__(self, product_id: str, gateway: str = _GATEWAY):
        self.product_id = product_id
        self.gateway    = gateway
        self._enabled   = _ENABLED

        if not self._enabled:
            log.debug("[metrics] Disabled via METRICS_ENABLED=0")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push_step(
        self,
        step_name: str,
        elapsed: float,
        status: str,          # "success" | "failed" | "skipped"
        outputs: int = 0,
    ) -> None:
        """Push metrics for a single completed step."""
        if not self._enabled:
            return

        status_code = {"success": 1, "skipped": -1}.get(status, 0)

        labels = {"step": step_name, "product": self._short_id()}

        self._push({
            f"sen2like_step_duration_seconds": (elapsed, {**labels, "status": status}),
            f"sen2like_step_status":           (status_code, labels),
            f"sen2like_step_outputs_total":    (outputs, labels),
        })

        log.debug("[metrics] Pushed step=%s status=%s elapsed=%.2fs", step_name, status, elapsed)

    def push_pipeline_complete(
        self,
        total_elapsed: float,
        n_success: int,
        n_failed: int,
    ) -> None:
        if not self._enabled:
            return

        labels = {"product": self._short_id()}

        self._push({
            "sen2like_pipeline_duration_seconds": (total_elapsed, labels),
            "sen2like_pipeline_success":          (1 if n_failed == 0 else 0, labels),
            "sen2like_pipeline_steps_succeeded":  (n_success, labels),
            "sen2like_pipeline_steps_failed":     (n_failed, labels),
        })

        log.debug(
            "[metrics] Pushed pipeline complete — elapsed=%.1fs success=%d failed=%d",
            total_elapsed, n_success, n_failed,
        )

    def push_valid_pixel_fraction(self, valid_fraction: float) -> None:
        if not self._enabled:
            return

        self._push({
            "sen2like_valid_pixel_fraction": (
                valid_fraction,
                {"product": self._short_id()},
            )
        })

    def push_brdf_deltas(self, band_deltas: dict[str, float]) -> None:

        if not self._enabled:
            return

        metrics = {
            "sen2like_brdf_delta": [
                (delta, {"product": self._short_id(), "band": band})
                for band, delta in band_deltas.items()
            ]
        }
        self._push_multi(metrics)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _short_id(self) -> str:
        return self.product_id[-64:].replace("/", "_").replace(" ", "_")

    def _push(self, metrics: dict) -> None:

        self._push_multi({k: [v] for k, v in metrics.items()})

    def _push_multi(self, metrics: dict) -> None:

        try:
            import urllib.request

            lines: list[str] = []
            for metric_name, entries in metrics.items():
                lines.append(f"# TYPE {metric_name} gauge")
                for value, labels in entries:
                    label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
                    lines.append(f"{metric_name}{{{label_str}}} {value}")
            lines.append("")  # trailing newline required by text format

            body    = "\n".join(lines).encode("utf-8")
            short   = self._short_id()
            url     = f"{self.gateway}/metrics/job/sen2like/instance/{short}"
            request = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={"Content-Type": "text/plain; version=0.0.4"},
            )
            with urllib.request.urlopen(request, timeout=3) as resp:
                if resp.status not in (200, 202):
                    log.warning("[metrics] Pushgateway returned HTTP %d", resp.status)

        except Exception as exc:
            # Never crash the pipeline over monitoring
            log.debug("[metrics] Push failed (non-fatal): %s", exc)