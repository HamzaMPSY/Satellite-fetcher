#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

API_URL="${API_URL:-http://127.0.0.1:8000}"
API_KEY="${NIMBUS_API_KEY:-}"
PROVIDER="${PROVIDER:-copernicus}"
COLLECTION="${COLLECTION:-SENTINEL-2}"
PRODUCT_TYPE="${PRODUCT_TYPE:-S2MSI2A}" # Sentinel-2 L2A (Bottom Of Atmosphere)
START_DATE="${START_DATE:-2025-01-01}"
END_DATE="${END_DATE:-2026-01-31}"
AOI_WKT="${AOI_WKT:-POLYGON((-13.20 27.50,-13.20 36.20,-0.90 36.20,-0.90 27.50,-13.20 27.50))}"
TILE_ID="${TILE_ID:-}" # Optional: set to reduce volume (example: 29RQT)
POLL_SECONDS="${POLL_SECONDS:-8}"
MAX_MONTHS="${MAX_MONTHS:-0}" # 0 = all months in range
OUT_PREFIX="${OUT_PREFIX:-benchmarks/morocco_s2_boa}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
DRY_RUN="${DRY_RUN:-0}"

RESULT_DIR="${ROOT_DIR}/benchmark_results/${RUN_ID}"
MONTHS_FILE="${RESULT_DIR}/months.txt"
PARALLEL_IDS_FILE="${RESULT_DIR}/parallel_job_ids.txt"
SEQUENTIAL_IDS_FILE="${RESULT_DIR}/sequential_job_ids.txt"
PARALLEL_STATUS_FILE="${RESULT_DIR}/parallel_final_status.jsonl"
SEQUENTIAL_STATUS_FILE="${RESULT_DIR}/sequential_final_status.jsonl"
REPORT_JSON="${RESULT_DIR}/report.json"
SUMMARY_TXT="${RESULT_DIR}/summary.txt"

mkdir -p "${RESULT_DIR}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: missing required command '$1'" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd python3

log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

api_call() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local url="${API_URL}${path}"

  local args=(
    -sS
    -f
    -X "$method"
    "$url"
  )
  if [[ -n "${API_KEY}" ]]; then
    args+=(-H "X-API-Key: ${API_KEY}")
  fi
  if [[ -n "${body}" ]]; then
    args+=(-H "Content-Type: application/json" -d "${body}")
  fi

  curl "${args[@]}"
}

generate_months() {
  python3 - "$START_DATE" "$END_DATE" "$MAX_MONTHS" > "${MONTHS_FILE}" <<'PY'
import calendar
import sys
from datetime import date, timedelta

start = date.fromisoformat(sys.argv[1])
end = date.fromisoformat(sys.argv[2])
max_months = int(sys.argv[3])
count = 0

cursor = date(start.year, start.month, 1)
while cursor <= end:
    month_last = calendar.monthrange(cursor.year, cursor.month)[1]
    month_start = max(start, date(cursor.year, cursor.month, 1))
    month_end = min(end, date(cursor.year, cursor.month, month_last))
    label = f"{cursor.year:04d}-{cursor.month:02d}"
    print(f"{month_start.isoformat()}|{month_end.isoformat()}|{label}")
    count += 1
    if max_months > 0 and count >= max_months:
        break
    cursor = month_end + timedelta(days=1)
PY
}

build_single_payload() {
  local month_start="$1"
  local month_end="$2"
  local month_label="$3"
  local mode="$4"

  python3 - "$month_start" "$month_end" "$month_label" "$mode" "$PROVIDER" "$COLLECTION" "$PRODUCT_TYPE" "$AOI_WKT" "$TILE_ID" "$OUT_PREFIX" "$RUN_ID" <<'PY'
import json
import sys

month_start = sys.argv[1]
month_end = sys.argv[2]
month_label = sys.argv[3]
mode = sys.argv[4]
provider = sys.argv[5]
collection = sys.argv[6]
product_type = sys.argv[7]
aoi_wkt = sys.argv[8]
tile_id = sys.argv[9].strip()
out_prefix = sys.argv[10]
run_id = sys.argv[11]

payload = {
    "job_type": "search_download",
    "provider": provider,
    "collection": collection,
    "product_type": product_type,
    "start_date": month_start,
    "end_date": month_end,
    "aoi": {"wkt": aoi_wkt},
    "output_dir": f"{out_prefix}/{run_id}/{mode}/{month_label}",
}
if tile_id:
    payload["tile_id"] = tile_id
print(json.dumps(payload))
PY
}

build_batch_payload() {
  local mode="$1"

  python3 - "$MONTHS_FILE" "$mode" "$PROVIDER" "$COLLECTION" "$PRODUCT_TYPE" "$AOI_WKT" "$TILE_ID" "$OUT_PREFIX" "$RUN_ID" <<'PY'
import json
import sys
from pathlib import Path

months_file = Path(sys.argv[1])
mode = sys.argv[2]
provider = sys.argv[3]
collection = sys.argv[4]
product_type = sys.argv[5]
aoi_wkt = sys.argv[6]
tile_id = sys.argv[7].strip()
out_prefix = sys.argv[8]
run_id = sys.argv[9]

jobs = []
for line in months_file.read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    month_start, month_end, month_label = line.split("|")
    payload = {
        "job_type": "search_download",
        "provider": provider,
        "collection": collection,
        "product_type": product_type,
        "start_date": month_start,
        "end_date": month_end,
        "aoi": {"wkt": aoi_wkt},
        "output_dir": f"{out_prefix}/{run_id}/{mode}/{month_label}",
    }
    if tile_id:
        payload["tile_id"] = tile_id
    jobs.append(payload)

print(json.dumps({"jobs": jobs}))
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
  local status_json="$1"
  python3 - "$status_json" <<'PY'
import json
import sys

s = json.loads(sys.argv[1])
state = s.get("state", "")
bytes_downloaded = int(s.get("bytes_downloaded") or 0)
bytes_total = int(s.get("bytes_total") or 0)
progress = float(s.get("progress") or 0.0)
duration = s.get("duration_seconds")
print(f"{state}|{bytes_downloaded}|{bytes_total}|{progress:.2f}|{duration}")
PY
}

wait_for_single_job() {
  local job_id="$1"
  while true; do
    local response
    response="$(api_call GET "/v1/jobs/${job_id}")"
    local parsed
    parsed="$(status_brief "${response}")"
    IFS='|' read -r state bytes_downloaded bytes_total progress duration <<<"${parsed}"
    log "job=${job_id} state=${state} progress=${progress}% bytes=${bytes_downloaded}/${bytes_total} duration=${duration}"
    if [[ "${state}" == "succeeded" || "${state}" == "failed" || "${state}" == "cancelled" ]]; then
      printf '%s\n' "${response}"
      return 0
    fi
    sleep "${POLL_SECONDS}"
  done
}

wait_for_many_jobs() {
  local ids_file="$1"
  local mode_label="$2"
  local final_status_file="$3"
  : > "${final_status_file}"

  local job_ids=()
  while IFS= read -r jid; do
    [[ -z "${jid}" ]] && continue
    job_ids+=("${jid}")
  done < "${ids_file}"
  if [[ "${#job_ids[@]}" -eq 0 ]]; then
    echo "ERROR: no job_ids in ${ids_file}" >&2
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
      response="$(api_call GET "/v1/jobs/${jid}")"
      local parsed
      parsed="$(status_brief "${response}")"
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
        succeeded) succeeded=$((succeeded + 1)) ;;
        failed) failed=$((failed + 1)) ;;
        cancelled) cancelled=$((cancelled + 1)) ;;
        *) pending=$((pending + 1)) ;;
      esac
    done

    log "${mode_label}: pending=${pending} queued=${queued} running=${running} cancel_requested=${cancel_requested} succeeded=${succeeded} failed=${failed} cancelled=${cancelled} bytes=${bytes_downloaded_sum}/${bytes_total_sum}"
    if [[ "${pending}" -eq 0 ]]; then
      break
    fi
    sleep "${POLL_SECONDS}"
  done

  for jid in "${job_ids[@]}"; do
    api_call GET "/v1/jobs/${jid}" >> "${final_status_file}"
    printf '\n' >> "${final_status_file}"
  done
}

build_summary() {
  local parallel_seconds="$1"
  local sequential_seconds="$2"

  python3 - "$PARALLEL_STATUS_FILE" "$SEQUENTIAL_STATUS_FILE" "$parallel_seconds" "$sequential_seconds" "$REPORT_JSON" "$SUMMARY_TXT" <<'PY'
import json
import sys
from pathlib import Path

parallel_file = Path(sys.argv[1])
sequential_file = Path(sys.argv[2])
parallel_seconds = float(sys.argv[3])
sequential_seconds = float(sys.argv[4])
report_path = Path(sys.argv[5])
summary_path = Path(sys.argv[6])

def load_jsonl(path: Path):
    items = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        items.append(json.loads(line))
    return items

def summarize(items):
    out = {
        "jobs": len(items),
        "succeeded": 0,
        "failed": 0,
        "cancelled": 0,
        "bytes_downloaded": 0,
        "bytes_total": 0,
        "job_ids": [],
    }
    for item in items:
        state = item.get("state")
        out["job_ids"].append(item.get("job_id"))
        out["bytes_downloaded"] += int(item.get("bytes_downloaded") or 0)
        out["bytes_total"] += int(item.get("bytes_total") or 0)
        if state == "succeeded":
            out["succeeded"] += 1
        elif state == "failed":
            out["failed"] += 1
        elif state == "cancelled":
            out["cancelled"] += 1
    return out

parallel_items = load_jsonl(parallel_file)
sequential_items = load_jsonl(sequential_file)
parallel_summary = summarize(parallel_items)
sequential_summary = summarize(sequential_items)

speedup = None
if parallel_seconds > 0:
    speedup = sequential_seconds / parallel_seconds

report = {
    "parallel": {
        "elapsed_seconds": parallel_seconds,
        **parallel_summary,
    },
    "sequential": {
        "elapsed_seconds": sequential_seconds,
        **sequential_summary,
    },
    "comparison": {
        "saved_seconds": sequential_seconds - parallel_seconds,
        "speedup_ratio": speedup,
    },
}

report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

lines = [
    "Benchmark completed",
    f"- Parallel elapsed:   {parallel_seconds:.1f}s",
    f"- Sequential elapsed: {sequential_seconds:.1f}s",
]
if speedup is not None:
    lines.append(f"- Speedup ratio (seq/par): {speedup:.2f}x")
lines.extend([
    f"- Parallel jobs: succeeded={parallel_summary['succeeded']} failed={parallel_summary['failed']} cancelled={parallel_summary['cancelled']}",
    f"- Sequential jobs: succeeded={sequential_summary['succeeded']} failed={sequential_summary['failed']} cancelled={sequential_summary['cancelled']}",
    f"- Report JSON: {report_path}",
])
summary_text = "\n".join(lines)
summary_path.write_text(summary_text + "\n", encoding="utf-8")
print(summary_text)
PY
}

generate_months
MONTH_COUNT="$(wc -l < "${MONTHS_FILE}" | tr -d ' ')"
if [[ "${MONTH_COUNT}" -le 0 ]]; then
  echo "ERROR: no monthly windows generated for ${START_DATE}..${END_DATE}" >&2
  exit 1
fi

log "Generated ${MONTH_COUNT} monthly windows (${START_DATE} -> ${END_DATE})."
log "Output prefix in service data dir: ${OUT_PREFIX}/${RUN_ID}"
log "Warning: full-country + multi-month Sentinel-2 BOA can generate very large volumes."
if [[ -n "${TILE_ID}" ]]; then
  log "Tile filter enabled: ${TILE_ID}"
else
  log "Tile filter disabled (full AOI)."
fi

if [[ "${DRY_RUN}" == "1" ]]; then
  log "DRY_RUN=1: generating sample payloads only."
  build_batch_payload "parallel" > "${RESULT_DIR}/parallel_payload.json"
  head -n 3 "${MONTHS_FILE}" || true
  log "Saved: ${RESULT_DIR}/parallel_payload.json"
  exit 0
fi

log "Checking API health at ${API_URL}"
api_call GET "/v1/health" >/dev/null
log "API is reachable."

log "Submitting PARALLEL batch jobs (optimized simultaneous run)..."
PARALLEL_PAYLOAD="$(build_batch_payload "parallel")"
PARALLEL_CREATE_RESP="$(api_call POST "/v1/jobs/batch" "${PARALLEL_PAYLOAD}")"
parse_job_ids "${PARALLEL_CREATE_RESP}" > "${PARALLEL_IDS_FILE}"

PARALLEL_START_EPOCH="$(date +%s)"
wait_for_many_jobs "${PARALLEL_IDS_FILE}" "parallel" "${PARALLEL_STATUS_FILE}"
PARALLEL_END_EPOCH="$(date +%s)"
PARALLEL_SECONDS=$((PARALLEL_END_EPOCH - PARALLEL_START_EPOCH))

log "Starting SEQUENTIAL run (same workload, one job after another)..."
: > "${SEQUENTIAL_IDS_FILE}"
: > "${SEQUENTIAL_STATUS_FILE}"

SEQUENTIAL_START_EPOCH="$(date +%s)"
while IFS='|' read -r month_start month_end month_label; do
  [[ -z "${month_start}" ]] && continue
  log "Sequential submit: ${month_label} (${month_start} -> ${month_end})"
  SINGLE_PAYLOAD="$(build_single_payload "${month_start}" "${month_end}" "${month_label}" "sequential")"
  SINGLE_CREATE_RESP="$(api_call POST "/v1/jobs" "${SINGLE_PAYLOAD}")"
  JOB_ID="$(parse_job_id "${SINGLE_CREATE_RESP}")"
  printf '%s\n' "${JOB_ID}" >> "${SEQUENTIAL_IDS_FILE}"
  wait_for_single_job "${JOB_ID}" >> "${SEQUENTIAL_STATUS_FILE}"
  printf '\n' >> "${SEQUENTIAL_STATUS_FILE}"
done < "${MONTHS_FILE}"
SEQUENTIAL_END_EPOCH="$(date +%s)"
SEQUENTIAL_SECONDS=$((SEQUENTIAL_END_EPOCH - SEQUENTIAL_START_EPOCH))

build_summary "${PARALLEL_SECONDS}" "${SEQUENTIAL_SECONDS}"

log "Done."
log "Artifacts:"
log "- ${SUMMARY_TXT}"
log "- ${REPORT_JSON}"
log "- ${PARALLEL_STATUS_FILE}"
log "- ${SEQUENTIAL_STATUS_FILE}"
