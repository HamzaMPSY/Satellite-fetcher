#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_URL="${API_URL:-http://127.0.0.1:8000}"
API_KEY="${NIMBUS_API_KEY:-}"
PROVIDER="${PROVIDER:-copernicus}"
COLLECTION="${COLLECTION:-SENTINEL-2}"
PRODUCT_TYPE="${PRODUCT_TYPE:-S2MSI2A}" # Sentinel-2 Bottom Of Atmosphere
START_DATE="${START_DATE:-2025-01-01}" # this script runs one job over 3 days (start..start+2)
AOI_WKT="${AOI_WKT:-POLYGON((-13.20 27.50,-13.20 36.20,-0.90 36.20,-0.90 27.50,-13.20 27.50))}"
TILE_ID="${TILE_ID:-}" # Optional: e.g. 29SPT
POLL_SECONDS="${POLL_SECONDS:-8}"
OUT_PREFIX="${OUT_PREFIX:-benchmarks/morocco_3days}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

RESULT_DIR="${ROOT_DIR}/benchmark_results/${RUN_ID}_3d_single"
FINAL_STATUS_FILE="${RESULT_DIR}/final_status.json"
REPORT_JSON="${RESULT_DIR}/report.json"
SUMMARY_TXT="${RESULT_DIR}/summary.txt"

mkdir -p "${RESULT_DIR}"

log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

api_call() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local url="${API_URL}${path}"

  local args=(-sS -f -X "$method" "$url")
  if [[ -n "${API_KEY}" ]]; then
    args+=(-H "X-API-Key: ${API_KEY}")
  fi
  if [[ -n "${body}" ]]; then
    args+=(-H "Content-Type: application/json" -d "${body}")
  fi
  curl "${args[@]}"
}

compute_end_date() {
  python3 - "$START_DATE" <<'PY'
import sys
from datetime import date, timedelta

start = date.fromisoformat(sys.argv[1])
end = start + timedelta(days=2)
print(end.isoformat())
PY
}

build_payload() {
  local end_date="$1"
  python3 - "$START_DATE" "$end_date" "$PROVIDER" "$COLLECTION" "$PRODUCT_TYPE" "$AOI_WKT" "$TILE_ID" "$OUT_PREFIX" "$RUN_ID" <<'PY'
import json
import sys

start_date = sys.argv[1]
end_date = sys.argv[2]
provider = sys.argv[3]
collection = sys.argv[4]
product_type = sys.argv[5]
aoi_wkt = sys.argv[6]
tile_id = sys.argv[7].strip()
out_prefix = sys.argv[8]
run_id = sys.argv[9]

payload = {
    "job_type": "search_download",
    "provider": provider,
    "collection": collection,
    "product_type": product_type,
    "start_date": start_date,
    "end_date": end_date,
    "aoi": {"wkt": aoi_wkt},
    "output_dir": f"{out_prefix}/{run_id}/single_job",
}
if tile_id:
    payload["tile_id"] = tile_id
print(json.dumps(payload))
PY
}

parse_job_id() {
  local response="$1"
  python3 - "$response" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
job_id = payload.get("job_id")
if not job_id:
    raise SystemExit(f"Missing job_id in response: {payload}")
print(job_id)
PY
}

status_brief() {
  local response="$1"
  python3 - "$response" <<'PY'
import json
import sys

try:
    j = json.loads(sys.argv[1])
except Exception:
    raise SystemExit(2)

print(
    f"{j.get('state','')}|"
    f"{int(j.get('bytes_downloaded') or 0)}|"
    f"{int(j.get('bytes_total') or 0)}|"
    f"{float(j.get('progress') or 0.0):.2f}|"
    f"{j.get('duration_seconds')}"
)
PY
}

wait_for_job() {
  local job_id="$1"
  while true; do
    local response
    if ! response="$(api_call GET "/v1/jobs/${job_id}" 2>/dev/null)"; then
      log "job=${job_id} status poll failed, retrying..."
      sleep "${POLL_SECONDS}"
      continue
    fi
    local parsed
    if ! parsed="$(status_brief "${response}" 2>/dev/null)"; then
      log "job=${job_id} invalid status payload, retrying..."
      sleep "${POLL_SECONDS}"
      continue
    fi
    IFS='|' read -r state bytes_downloaded bytes_total progress duration <<<"${parsed}"
    log "job=${job_id} state=${state} progress=${progress}% bytes=${bytes_downloaded}/${bytes_total} duration=${duration}"
    if [[ "${state}" == "succeeded" || "${state}" == "failed" || "${state}" == "cancelled" ]]; then
      printf '%s\n' "${response}"
      return 0
    fi
    sleep "${POLL_SECONDS}"
  done
}

build_summary() {
  local elapsed_seconds="$1"
  python3 - "$FINAL_STATUS_FILE" "$elapsed_seconds" "$REPORT_JSON" "$SUMMARY_TXT" <<'PY'
import json
import sys
from pathlib import Path

status_file = Path(sys.argv[1])
elapsed_seconds = float(sys.argv[2])
report_path = Path(sys.argv[3])
summary_path = Path(sys.argv[4])

job = json.loads(status_file.read_text(encoding="utf-8").strip())
report = {
    "mode": "single_job_3days",
    "elapsed_seconds": elapsed_seconds,
    "elapsed_minutes": elapsed_seconds / 60.0,
    "jobs": 1,
    "succeeded": 1 if job.get("state") == "succeeded" else 0,
    "failed": 1 if job.get("state") == "failed" else 0,
    "cancelled": 1 if job.get("state") == "cancelled" else 0,
    "bytes_downloaded": int(job.get("bytes_downloaded") or 0),
    "bytes_total": int(job.get("bytes_total") or 0),
    "job_ids": [job.get("job_id")],
}
report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

summary = "\n".join(
    [
        "Morocco 3-day single-job benchmark completed",
        f"- Elapsed: {elapsed_seconds:.1f} seconds",
        f"- Elapsed: {elapsed_seconds / 60.0:.2f} minutes",
        f"- State: {job.get('state')}",
        f"- Bytes: {report['bytes_downloaded']}/{report['bytes_total']}",
        f"- Report: {report_path}",
    ]
)
summary_path.write_text(summary + "\n", encoding="utf-8")
print(summary)
PY
}

log "Checking API health at ${API_URL}"
api_call GET "/v1/health" >/dev/null
log "API reachable."

END_DATE="$(compute_end_date)"
log "Single job range: ${START_DATE} -> ${END_DATE}"
if [[ -n "${TILE_ID}" ]]; then
  log "Tile filter enabled: ${TILE_ID}"
else
  log "Tile filter disabled (full Morocco AOI)."
fi

payload="$(build_payload "${END_DATE}")"
create_response="$(api_call POST "/v1/jobs" "${payload}")"
job_id="$(parse_job_id "${create_response}")"
log "Created single job: ${job_id}"

START_EPOCH="$(date +%s)"
wait_for_job "${job_id}" > "${FINAL_STATUS_FILE}"
END_EPOCH="$(date +%s)"

ELAPSED_SECONDS=$((END_EPOCH - START_EPOCH))
build_summary "${ELAPSED_SECONDS}"

log "Done."
log "Summary: ${SUMMARY_TXT}"
