from __future__ import annotations

from nimbuschain_fetch_ui.runtime_status import (
    _coordinator_status_rows,
    refresh_api_runtime_statuses,
    status_card_payload,
)


def test_refresh_api_runtime_statuses_fetches_download_coordinator_snapshot(monkeypatch) -> None:
    requested_paths: list[str] = []

    def _fake_fetch_status_json(*, base_url: str, path: str, api_key: str = "", timeout: int = 15):
        _ = base_url, api_key, timeout
        requested_paths.append(path)
        return {"status": "ok"}

    monkeypatch.setattr(
        "nimbuschain_fetch_ui.runtime_status.fetch_status_json",
        _fake_fetch_status_json,
    )

    payload = refresh_api_runtime_statuses(api_url="http://localhost:8000", api_key="secret")

    assert "download_coordinator_snapshot" in payload
    assert "/v1/worker/download-coordinator" in requested_paths


def test_status_card_payload_reports_active_download_coordinator() -> None:
    snapshot = {
        "status": "active",
        "workers_reporting": 1,
        "summary": {
            "status": "active",
            "machine": {"active_downloads": 3},
            "jobs": {"pending_tasks_total": 7},
        },
    }

    state, detail, color = status_card_payload(snapshot, kind="coordinator")

    assert state == "active"
    assert "3 active" in detail
    assert "7 pending" in detail
    assert color == "#22c55e"


def test_coordinator_status_rows_include_provider_breakdown_and_total() -> None:
    summary = {
        "providers": {
            "copernicus": {
                "counts": {
                    "queued": 2,
                    "preparing": 1,
                    "ready": 3,
                    "downloading": 4,
                    "done": 5,
                    "failed": 1,
                    "cancelled": 0,
                }
            },
            "usgs": {
                "counts": {
                    "queued": 0,
                    "preparing": 2,
                    "ready": 1,
                    "downloading": 1,
                    "done": 6,
                    "failed": 0,
                    "cancelled": 1,
                }
            },
        }
    }

    rows = _coordinator_status_rows(summary)

    assert rows[0]["provider"] == "COPERNICUS"
    assert rows[0]["downloading"] == 4
    assert rows[1]["provider"] == "USGS"
    assert rows[1]["done"] == 6
    assert rows[2]["provider"] == "TOTAL"
    assert rows[2]["queued"] == 2
    assert rows[2]["preparing"] == 3
    assert rows[2]["done"] == 11
    assert rows[2]["total"] == 27
