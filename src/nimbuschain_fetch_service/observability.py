from __future__ import annotations

from typing import Any

from prometheus_client import Counter, Gauge, Histogram, generate_latest

from nimbuschain_fetch.engine.nimbus_fetcher import NimbusFetcher
from nimbuschain_fetch.models import JobState


HTTP_REQUESTS_TOTAL = Counter(
    "nimbus_http_requests_total",
    "Total HTTP requests handled by Nimbus API.",
    labelnames=("method", "path", "status"),
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "nimbus_http_request_duration_seconds",
    "HTTP request duration in seconds.",
    labelnames=("method", "path"),
    buckets=(0.01, 0.05, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0),
)

JOB_SUBMISSIONS_TOTAL = Counter(
    "nimbus_job_submissions_total",
    "Total submitted jobs grouped by type/provider.",
    labelnames=("job_type", "provider"),
)

JOB_CANCELLATIONS_TOTAL = Counter(
    "nimbus_job_cancellations_total",
    "Total cancellation requests grouped by provider.",
    labelnames=("provider",),
)

JOB_STATE_GAUGE = Gauge(
    "nimbus_jobs_state_total",
    "Current number of jobs per state.",
    labelnames=("state",),
)


def record_http_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    m = method.upper().strip()
    p = path.strip() or "_unknown"
    s = str(int(status_code))
    HTTP_REQUESTS_TOTAL.labels(method=m, path=p, status=s).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=m, path=p).observe(max(0.0, duration_seconds))


def record_job_submission(job_type: str, provider: str) -> None:
    jt = job_type.strip()
    pr = provider.strip().lower()
    JOB_SUBMISSIONS_TOTAL.labels(job_type=jt, provider=pr).inc()


def record_job_cancellation(provider: str) -> None:
    pr = provider.strip().lower()
    JOB_CANCELLATIONS_TOTAL.labels(provider=pr).inc()


def update_job_state_gauges(fetcher: NimbusFetcher) -> None:
    for state in JobState:
        response = fetcher.list_jobs(
            state=state.value,
            provider=None,
            date_from=None,
            date_to=None,
            page=1,
            page_size=1,
        )
        JOB_STATE_GAUGE.labels(state=state.value).set(float(response.total))


def render_metrics(fetcher: NimbusFetcher) -> bytes:
    update_job_state_gauges(fetcher)
    return generate_latest()

