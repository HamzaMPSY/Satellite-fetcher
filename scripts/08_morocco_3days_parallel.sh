#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_URL="${API_URL:-http://127.0.0.1:8000}"
API_KEY="${NIMBUS_API_KEY:-}"
PROVIDER="${PROVIDER:-copernicus}"
COLLECTION="${COLLECTION:-SENTINEL-2}"
PRODUCT_TYPE="${PRODUCT_TYPE:-S2MSI2A}" # Sentinel-2 Bottom Of Atmosphere
START_DATE="${START_DATE:-2025-01-01}" # day 1, script runs day1/day2/day3
AOI_WKT="${AOI_WKT:-POLYGON((-13.20 27.50,-13.20 36.20,-0.90 36.20,-0.90 27.50,-13.20 27.50))}"
TILE_ID="${TILE_ID:-}" # Optional: e.g. 29RQT
POLL_SECONDS="${POLL_SECONDS:-8}"
OUT_PREFIX="${OUT_PREFIX:-benchmarks/morocco_3days}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

RESULT_DIR="${ROOT_DIR}/benchmark_results/${RUN_ID}_3d_parallel"
DAYS_FILE="${RESULT_DIR}/days.txt"
JOB_IDS_FILE="${RESULT_DIR}/job_ids.txt"
FINAL_STATUS_FILE="${RESULT_DIR}/final_status.jsonl"
REPORT_JSON="${RESULT_DIR}/report.json"
SUMMARY_TXT="${RESULT_DIR}/summary.txt"

mkdir -p "${RESULT_DIR}"

log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
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

generate_days() {
  python3 - "$START_DATE" > "${DAYS_FILE}" <<'PY'
import sys
from datetime import date, timedelta

start = date.fromisoformat(sys.argv[1])
for i in range(3):
    d = start + timedelta(days=i)
    print(f"{d.isoformat()}|day{i+1}")
PY
}

build_batch_payload() {
  python3 - "$DAYS_FILE" "$PROVIDER" "$COLLECTION" "$PRODUCT_TYPE" "$AOI_WKT" "$TILE_ID" "$OUT_PREFIX" "$RUN_ID" <<'PY'
import json
import sys
from pathlib import Path

days_file = Path(sys.argv[1])
provider = sys.argv[2]
collection = sys.argv[3]
product_type = sys.argv[4]
aoi_wkt = sys.argv[5]
tile_id = sys.argv[6].strip()
out_prefix = sys.argv[7]
run_id = sys.argv[8]

jobs = []
for line in days_file.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line:
        continue
    day, label = line.split("|")
    payload = {
        "job_type": "search_download",
        "provider": provider,
        "collection": collection,
        "product_type": product_type,
        "start_date": day,
        "end_date": day,
        "aoi": {"wkt": aoi_wkt},
        "output_dir": f"{out_prefix}/{run_id}/parallel/{label}",
    }
    if tile_id:
        payload["tile_id"] = tile_id
    jobs.append(payload)

print(json.dumps({"jobs": jobs}))
PY
}

parse_job_ids() {
  local response="$1"
  python3 - "$response" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
job_ids = payload.get("job_ids")
if not isinstance(job_ids, list) or not job_ids:
    raise SystemExit(f"Missing job_ids in response: {payload}")
for jid in job_ids:
    print(jid)
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

wait_for_many() {
  : > "${FINAL_STATUS_FILE}"

  local job_ids=()
  while IFS= read -r jid; do
    [[ -z "${jid}" ]] && continue
    job_ids+=("${jid}")
  done < "${JOB_IDS_FILE}"

  if [[ "${#job_ids[@]}" -eq 0 ]]; then
    echo "ERROR: no job ids found in ${JOB_IDS_FILE}" >&2
    exit 1
  fi

  while true; do
    local pending=0
    local queued=0
    local running=0
    local cancel_requested=0
    local succeeded=0
    local failed=0
    local cancelled=0
    local bytes_downloaded_sum=0
    local bytes_total_sum=0

    for jid in "${job_ids[@]}"; do
      local response
      if ! response="$(api_call GET "/v1/jobs/${jid}" 2>/dev/null)"; then
        pending=$((pending + 1))
        continue
      fi
      local parsed
      if ! parsed="$(status_brief "${response}" 2>/dev/null)"; then
        pending=$((pending + 1))
        continue
      fi
      IFS='|' read -r state bytes_downloaded bytes_total progress duration <<<"${parsed}"

      bytes_downloaded_sum=$((bytes_downloaded_sum + bytes_downloaded))
      bytes_total_sum=$((bytes_total_sum + bytes_total))

      case "${state}" in
        queued)
          queued=$((queued + 1))
          pending=$((pending + 1))
          ;;
        running)
          running=$((running + 1))
          pending=$((pending + 1))
          ;;
        cancel_requested)
          cancel_requested=$((cancel_requested + 1))
          pending=$((pending + 1))
          ;;
        succeeded)
          succeeded=$((succeeded + 1))
          ;;
        failed)
          failed=$((failed + 1))
          ;;
        cancelled)
          cancelled=$((cancelled + 1))
          ;;
        *)
          pending=$((pending + 1))
          ;;
      esac
    done

    log "parallel: pending=${pending} queued=${queued} running=${running} cancel_requested=${cancel_requested} succeeded=${succeeded} failed=${failed} cancelled=${cancelled} bytes=${bytes_downloaded_sum}/${bytes_total_sum}"
    if [[ "${pending}" -eq 0 ]]; then
      break
    fi
    sleep "${POLL_SECONDS}"
  done

  for jid in "${job_ids[@]}"; do
    api_call GET "/v1/jobs/${jid}" >> "${FINAL_STATUS_FILE}"
    printf '\n' >> "${FINAL_STATUS_FILE}"
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

items = []
for line in status_file.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line:
        continue
    items.append(json.loads(line))

succeeded = sum(1 for x in items if x.get("state") == "succeeded")
failed = sum(1 for x in items if x.get("state") == "failed")
cancelled = sum(1 for x in items if x.get("state") == "cancelled")
bytes_downloaded = sum(int(x.get("bytes_downloaded") or 0) for x in items)
bytes_total = sum(int(x.get("bytes_total") or 0) for x in items)

report = {
    "mode": "parallel_3jobs",
    "elapsed_seconds": elapsed_seconds,
    "elapsed_minutes": elapsed_seconds / 60.0,
    "jobs": len(items),
    "succeeded": succeeded,
    "failed": failed,
    "cancelled": cancelled,
    "bytes_downloaded": bytes_downloaded,
    "bytes_total": bytes_total,
    "job_ids": [x.get("job_id") for x in items],
}
report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

summary = "\n".join(
    [
        "Morocco 3-day parallel benchmark completed",
        f"- Elapsed: {elapsed_seconds:.1f} seconds",
        f"- Elapsed: {elapsed_seconds / 60.0:.2f} minutes",
        f"- Jobs: {len(items)} (succeeded={succeeded}, failed={failed}, cancelled={cancelled})",
        f"- Bytes: {bytes_downloaded}/{bytes_total}",
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

generate_days
log "Using START_DATE=${START_DATE} -> three days."
if [[ -n "${TILE_ID}" ]]; then
  log "Tile filter enabled: ${TILE_ID}"
else
  log "Tile filter disabled (full Morocco AOI)."
fi

batch_payload="$(build_batch_payload)"
create_response="$(api_call POST "/v1/jobs/batch" "${batch_payload}")"
parse_job_ids "${create_response}" > "${JOB_IDS_FILE}"

START_EPOCH="$(date +%s)"
wait_for_many
END_EPOCH="$(date +%s)"

ELAPSED_SECONDS=$((END_EPOCH - START_EPOCH))
build_summary "${ELAPSED_SECONDS}"

log "Done."
log "Summary: ${SUMMARY_TXT}"
