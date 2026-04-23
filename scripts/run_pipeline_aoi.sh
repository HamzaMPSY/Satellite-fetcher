#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_pipeline_aoi.sh [options]

Optional:
  --start-date DATE          Inclusive start date
                             default: 2026-04-19
  --end-date DATE            Inclusive end date
                             default: 2026-04-20
  --aoi-file PATH            AOI file path (.wkt, .json, .geojson)
                             default: docs/aoi_saudi_arabia_northwest.wkt
  --provider NAME            default: copernicus
  --collection NAME          default: SENTINEL-2
  --product-type NAME        default: S2MSI2A
  --tile-id ID               Optional Sentinel tile filter
  --mask-types TYPES         Comma-separated list: water,cloud
                             default: water,cloud
  --cube-mode MODE           one of: none, before_mask, after_mask
                             default: after_mask
  --cube-start-date DATE     Optional cube start date
  --cube-end-date DATE       Optional cube end date
  --mode MODE                direct or service, default: direct
  --service-url URL          default: http://127.0.0.1:8000
  --download-only            Stop after raw download (no Zarr, masks, or cubes)
  --no-wait                  Return immediately after job submission
  -h, --help                 Show this help

Examples:
  scripts/run_pipeline_aoi.sh

  scripts/run_pipeline_aoi.sh \
    --tile-id 37RDP \
    --mode direct
EOF
}

START_DATE="2026-04-19"
END_DATE="2026-04-20"
AOI_FILE="${ROOT_DIR}/docs/aoi_saudi_arabia_northwest.wkt"
PROVIDER="copernicus"
COLLECTION="SENTINEL-2"
PRODUCT_TYPE="S2MSI2A"
TILE_ID=""
MASK_TYPES="water,cloud"
CUBE_MODE="after_mask"
CUBE_START_DATE=""
CUBE_END_DATE=""
MODE="direct"
SERVICE_URL="http://127.0.0.1:8000"
NO_WAIT=0
DOWNLOAD_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date)
      START_DATE="${2:-}"
      shift 2
      ;;
    --end-date)
      END_DATE="${2:-}"
      shift 2
      ;;
    --aoi-file)
      AOI_FILE="${2:-}"
      shift 2
      ;;
    --provider)
      PROVIDER="${2:-}"
      shift 2
      ;;
    --collection)
      COLLECTION="${2:-}"
      shift 2
      ;;
    --product-type)
      PRODUCT_TYPE="${2:-}"
      shift 2
      ;;
    --tile-id)
      TILE_ID="${2:-}"
      shift 2
      ;;
    --mask-types)
      MASK_TYPES="${2:-}"
      shift 2
      ;;
    --cube-mode)
      CUBE_MODE="${2:-}"
      shift 2
      ;;
    --cube-start-date)
      CUBE_START_DATE="${2:-}"
      shift 2
      ;;
    --cube-end-date)
      CUBE_END_DATE="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --service-url)
      SERVICE_URL="${2:-}"
      shift 2
      ;;
    --no-wait)
      NO_WAIT=1
      shift
      ;;
    --download-only)
      DOWNLOAD_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "${AOI_FILE}" ]]; then
  echo "AOI file not found: ${AOI_FILE}" >&2
  exit 1
fi

export NIMBUS_DB_BACKEND="${NIMBUS_DB_BACKEND:-sqlite}"
export NIMBUS_DB_PATH="${NIMBUS_DB_PATH:-${ROOT_DIR}/data/nimbus.db}"
export NIMBUS_DATA_DIR="${NIMBUS_DATA_DIR:-${ROOT_DIR}/data/downloads}"

CMD=(
  nimbuschain-fetch
  --mode "${MODE}"
  --service-url "${SERVICE_URL}"
  --provider "${PROVIDER}"
  --collection "${COLLECTION}"
  --product-type "${PRODUCT_TYPE}"
  --start-date "${START_DATE}"
  --end-date "${END_DATE}"
  --aoi_file "${AOI_FILE}"
)

if [[ "${DOWNLOAD_ONLY}" -eq 1 ]]; then
  CMD+=(--cube-mode "none")
else
  CMD+=(--cube-mode "${CUBE_MODE}")
fi

if [[ -n "${TILE_ID}" ]]; then
  CMD+=(--tile-id "${TILE_ID}")
fi

if [[ "${DOWNLOAD_ONLY}" -ne 1 && -n "${MASK_TYPES}" ]]; then
  CMD+=(--mask-types "${MASK_TYPES}")
fi

if [[ "${DOWNLOAD_ONLY}" -ne 1 && -n "${CUBE_START_DATE}" ]]; then
  CMD+=(--cube-start-date "${CUBE_START_DATE}")
fi

if [[ "${DOWNLOAD_ONLY}" -ne 1 && -n "${CUBE_END_DATE}" ]]; then
  CMD+=(--cube-end-date "${CUBE_END_DATE}")
fi

if [[ "${NO_WAIT}" -eq 1 ]]; then
  CMD+=(--no-wait)
fi

if [[ "${DOWNLOAD_ONLY}" -eq 1 ]]; then
  CMD+=(--download-only)
fi

echo "Running pipeline command:"
printf ' %q' "${CMD[@]}"
echo

cd "${ROOT_DIR}"
"${CMD[@]}"
