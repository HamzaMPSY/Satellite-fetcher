
from __future__ import annotations
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import numpy as np
import dask.array as da
import rasterio
from scipy.ndimage import binary_dilation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output mask bit definitions
# ---------------------------------------------------------------------------
class MaskBits:
    FILL          = 0   # bit 0 → value 1
    CLOUD         = 1   # bit 1 → value 2
    CLOUD_SHADOW  = 2   # bit 2 → value 4
    SNOW_ICE      = 3   # bit 3 → value 8
    WATER         = 4   # bit 4 → value 16
    SATURATED     = 5   # bit 5 → value 32
    HIGH_AEROSOL  = 6   # bit 6 → value 64
    CLEAR         = 7   # bit 7 → value 128


# ---------------------------------------------------------------------------
# QA_PIXEL bit positions (Landsat Collection 2)
# ---------------------------------------------------------------------------
class QAPixelBits:
    FILL           = 0
    DILATED_CLOUD  = 1
    CIRRUS         = 2
    CLOUD          = 3
    CLOUD_SHADOW   = 4
    SNOW           = 5
    CLEAR          = 6
    WATER          = 7


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class MaskConfig:
    # Morphological dilation radius (pixels) applied to cloud + cloud-shadow masks
    cloud_dilation_radius: int = 3
    shadow_dilation_radius: int = 3
    # High-aerosol confidence threshold bit in SR_QA_AEROSOL
    high_aerosol_bit: int = 6
    # Include dilated-cloud pixels as cloud (slightly more conservative)
    use_dilated_cloud: bool = True
    # Include cirrus pixels as cloud
    use_cirrus: bool = True
    # Treat water pixels as disqualifying for the clear bit.
    # False (default): clear water surfaces are flagged as valid.
    # True: water pixels are never marked clear regardless of QA_PIXEL.
    exclude_water: bool = False
    # Chunk size for Dask arrays (pixels)
    chunk_size: int = 1024


# ---------------------------------------------------------------------------
# Helper – read a single-band GeoTIFF as a Dask array
# ---------------------------------------------------------------------------
def _read_band_dask(path: str | Path, chunk: int = 1024) -> tuple[da.Array, dict]:
    with rasterio.open(path) as src:
        profile = src.profile.copy()
        data = src.read(1)
    arr = da.from_array(data, chunks=(chunk, chunk))
    return arr, profile


# ---------------------------------------------------------------------------
# Core: extract flags from QA_PIXEL
# ---------------------------------------------------------------------------
def _decode_qa_pixel(
    qa: da.Array,
    cfg: MaskConfig,
) -> dict[str, da.Array]:

    def _bit(arr: da.Array, bit: int) -> da.Array:
        return (arr & np.uint16(1 << bit)).astype(bool)

    flags: dict[str, da.Array] = {}
    flags["fill"]         = _bit(qa, QAPixelBits.FILL)
    flags["cloud"]        = _bit(qa, QAPixelBits.CLOUD)
    flags["cloud_shadow"] = _bit(qa, QAPixelBits.CLOUD_SHADOW)
    flags["snow"]         = _bit(qa, QAPixelBits.SNOW)
    flags["water"]        = _bit(qa, QAPixelBits.WATER)
    flags["clear"]        = _bit(qa, QAPixelBits.CLEAR)   # authoritative clear signal

    if cfg.use_dilated_cloud:
        flags["cloud"] = flags["cloud"] | _bit(qa, QAPixelBits.DILATED_CLOUD)

    if cfg.use_cirrus:
        flags["cloud"] = flags["cloud"] | _bit(qa, QAPixelBits.CIRRUS)

    return flags


# ---------------------------------------------------------------------------
# Morphological dilation (operates on computed numpy arrays)
# ---------------------------------------------------------------------------
def _dilate(mask_np: np.ndarray, radius: int) -> np.ndarray:
    if radius <= 0:
        return mask_np
    diameter = 2 * radius + 1
    y, x = np.ogrid[-radius:radius + 1, -radius:radius + 1]
    struct = (x ** 2 + y ** 2) <= radius ** 2
    return binary_dilation(mask_np, structure=struct).astype(mask_np.dtype)


def _dilate_dask(mask: da.Array, radius: int, chunk: int) -> da.Array:
    if radius <= 0:
        return mask
    mask_np = mask.compute()
    dilated = _dilate(mask_np, radius)
    return da.from_array(dilated, chunks=(chunk, chunk))


# ---------------------------------------------------------------------------
# Core: decode SR_QA_AEROSOL
# ---------------------------------------------------------------------------
def _decode_aerosol(aerosol: da.Array, high_aerosol_bit: int) -> da.Array:
    return (aerosol & np.uint8(1 << high_aerosol_bit)).astype(bool)


# ---------------------------------------------------------------------------
# Core: decode QA_RADSAT
# ---------------------------------------------------------------------------
def _decode_radsat(radsat: da.Array) -> da.Array:
    sat_mask = np.uint16(0x00FF)   # bits 0-7
    return (radsat & sat_mask).astype(bool)


# ---------------------------------------------------------------------------
# Main mask builder
# ---------------------------------------------------------------------------
def build_valid_pixel_mask(
    qa_pixel_path: str | Path,
    qa_radsat_path: str | Path,
    sr_qa_aerosol_path: str | Path | None,
    cfg: Optional[MaskConfig] = None,
) -> tuple[np.ndarray, dict]:
    if cfg is None:
        cfg = MaskConfig()

    logger.info("Reading QA layers …")
    qa,     prof_qa = _read_band_dask(qa_pixel_path,  cfg.chunk_size)
    radsat, _       = _read_band_dask(qa_radsat_path, cfg.chunk_size)

    qa     = qa.astype(np.uint16)
    radsat = radsat.astype(np.uint16)

    # ---- Decode QA_PIXEL ----
    logger.info("Decoding QA_PIXEL …")
    flags = _decode_qa_pixel(qa, cfg)

    # ---- Apply morphological dilation (cloud + shadow) ----
    logger.info(
        "Applying dilation: cloud radius=%d, shadow radius=%d …",
        cfg.cloud_dilation_radius, cfg.shadow_dilation_radius,
    )
    cloud_dilated  = _dilate_dask(flags["cloud"],        cfg.cloud_dilation_radius,  cfg.chunk_size)
    shadow_dilated = _dilate_dask(flags["cloud_shadow"], cfg.shadow_dilation_radius, cfg.chunk_size)

    # ---- Decode saturation & aerosol ----
    logger.info("Decoding QA_RADSAT & SR_QA_AEROSOL …")
    sat_flag = _decode_radsat(radsat)

    if sr_qa_aerosol_path is not None:
        aerosol, _ = _read_band_dask(sr_qa_aerosol_path, cfg.chunk_size)
        aerosol      = aerosol.astype(np.uint8)
        aerosol_flag = _decode_aerosol(aerosol, cfg.high_aerosol_bit)
        logger.info("SR_QA_AEROSOL loaded from %s", sr_qa_aerosol_path)
    else:
        logger.info("SR_QA_AEROSOL absent (L1TP) — high-aerosol flag set to all-False")
        aerosol_flag = da.zeros(qa.shape, dtype=bool, chunks=(cfg.chunk_size, cfg.chunk_size))

    # ---- Assemble output mask (uint8 bit-packed) ----
    logger.info("Assembling output mask …")

    def _pack(*flag_arrays_and_bits):
        result = da.zeros(qa.shape, dtype=np.uint8, chunks=(cfg.chunk_size, cfg.chunk_size))
        for arr, bit in flag_arrays_and_bits:
            result = result | (arr.astype(np.uint8) << np.uint8(bit))
        return result

    mask = _pack(
        (flags["fill"],    MaskBits.FILL),
        (cloud_dilated,    MaskBits.CLOUD),
        (shadow_dilated,   MaskBits.CLOUD_SHADOW),
        (flags["snow"],    MaskBits.SNOW_ICE),
        (flags["water"],   MaskBits.WATER),
        (sat_flag,         MaskBits.SATURATED),
        (aerosol_flag,     MaskBits.HIGH_AEROSOL),
    )

    disqualified = (
        flags["fill"]
        | cloud_dilated
        | shadow_dilated
        | flags["snow"]
        | sat_flag
        | aerosol_flag
    )
    if cfg.exclude_water:
        disqualified = disqualified | flags["water"]

    # A pixel is clear when QA_PIXEL says so AND it has no disqualifying flag.
    actually_clear = flags["clear"] & ~disqualified
    clear_flag     = actually_clear.astype(np.uint8) * np.uint8(1 << MaskBits.CLEAR)
    mask           = mask | clear_flag

    # ---- Compute ----
    logger.info("Computing mask (Dask) …")
    mask_np: np.ndarray = mask.compute().astype(np.uint8)

    profile = prof_qa.copy()
    profile.update(
        dtype="uint8",
        count=1,
        nodata=None,
        compress="deflate",
        predictor=2,
        tiled=True,
        blockxsize=512,
        blockysize=512,
    )

    logger.info(
        "Valid pixel mask built. Shape=%s, unique values=%s",
        mask_np.shape, np.unique(mask_np),
    )
    return mask_np, profile


# ---------------------------------------------------------------------------
# Convenience: write the mask to disk
# ---------------------------------------------------------------------------
def write_mask(mask_np: np.ndarray, profile: dict, out_path: str | Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(mask_np[np.newaxis, ...])
    logger.info("Mask written → %s", out_path)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------
def validate_mask(mask_np: np.ndarray, profile: dict) -> dict:

    total = mask_np.size
    stats: dict = {"total_pixels": total, "flags": {}}

    flag_defs = {
        "fill":         MaskBits.FILL,
        "cloud":        MaskBits.CLOUD,
        "cloud_shadow": MaskBits.CLOUD_SHADOW,
        "snow_ice":     MaskBits.SNOW_ICE,
        "water":        MaskBits.WATER,
        "saturated":    MaskBits.SATURATED,
        "high_aerosol": MaskBits.HIGH_AEROSOL,
        "clear":        MaskBits.CLEAR,
    }

    for name, bit in flag_defs.items():
        count = int(np.sum((mask_np & (1 << bit)).astype(bool)))
        stats["flags"][name] = {
            "count": count,
            "pct": round(100.0 * count / total, 2),
        }

    clear_count = stats["flags"]["clear"]["count"]
    stats["valid_fraction"] = round(clear_count / total, 4)

    fill_count = stats["flags"]["fill"]["count"]
    stats["warnings"] = []
    if fill_count > 0.5 * total:
        stats["warnings"].append(
            f"More than 50 % fill pixels ({fill_count / total:.1%}) — "
            "product may be mostly outside swath."
        )
    if stats["valid_fraction"] < 0.05:
        stats["warnings"].append(
            "Fewer than 5 % valid (clear) pixels — heavy cloud/shadow cover."
        )

    return stats


def print_validation_report(stats: dict) -> None:
    total = stats["total_pixels"]
    print("=" * 60)
    print("Valid Pixel Mask – Validation Report")
    print("=" * 60)
    print(f"Total pixels  : {total:,}")
    print(f"Valid fraction: {stats['valid_fraction']:.2%}")
    print()
    print(f"{'Flag':<20}  {'Count':>10}  {'%':>7}")
    print("-" * 42)
    for name, info in stats["flags"].items():
        print(f"  {name:<18}  {info['count']:>10,}  {info['pct']:>6.2f} %")
    if stats["warnings"]:
        print()
        print("Warnings:")
        for w in stats["warnings"]:
            print(f"  ⚠  {w}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Utility: find QA files automatically from a Landsat product directory
# ---------------------------------------------------------------------------
def find_landsat_qa_files(product_dir: str | Path) -> dict[str, str | None]:

    product_dir = Path(product_dir)

    required_patterns = {
        "qa_pixel":  "*_QA_PIXEL.TIF",
        "qa_radsat": "*_QA_RADSAT.TIF",
    }
    optional_patterns = {
        "sr_qa_aerosol": "*_SR_QA_AEROSOL.TIF",
    }

    result: dict[str, str | None] = {}

    for key, pat in required_patterns.items():
        matches = list(product_dir.glob(pat))
        if not matches:
            raise FileNotFoundError(f"Could not find {pat} in {product_dir}")
        result[key] = str(matches[0])
        logger.info("Found %s → %s", key, result[key])

    for key, pat in optional_patterns.items():
        matches = list(product_dir.glob(pat))
        if matches:
            result[key] = str(matches[0])
            logger.info("Found %s → %s", key, result[key])
        else:
            result[key] = None
            logger.info(
                "%s not found in %s (L1TP product — aerosol flag will be skipped)",
                pat, product_dir,
            )

    return result


# ---------------------------------------------------------------------------
# Main – entry point for standalone testing
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Sen2Like Valid Pixel Mask – standalone test"
    )
    parser.add_argument(
        "product_dir",
        help="Path to a Landsat Collection-2 L2SP product directory, "
             "e.g. LC08_L2SP_198027_20260105_20260114_02_T1",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output GeoTIFF path (default: <product_dir>/<product>_VALID_PIXEL_MASK.TIF)",
    )
    parser.add_argument("--cloud-dilation",   type=int,            default=3)
    parser.add_argument("--shadow-dilation",  type=int,            default=3)
    parser.add_argument("--no-cirrus",        action="store_true")
    parser.add_argument("--no-dilated-cloud", action="store_true")
    parser.add_argument("--exclude-water",    action="store_true",
                        help="Treat water pixels as not clear (excluded from valid fraction)")
    parser.add_argument("--chunks",           type=int,            default=1024)

    args = parser.parse_args()

    product_dir = Path(args.product_dir).resolve()
    if not product_dir.is_dir():
        print(f"ERROR: {product_dir} is not a directory.", file=sys.stderr)
        sys.exit(1)

    try:
        qa_files = find_landsat_qa_files(product_dir)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        out_path = Path(args.output)
    else:
        prod_name = product_dir.name
        out_path  = product_dir / f"{prod_name}_VALID_PIXEL_MASK.TIF"

    cfg = MaskConfig(
        cloud_dilation_radius  = args.cloud_dilation,
        shadow_dilation_radius = args.shadow_dilation,
        use_cirrus             = not args.no_cirrus,
        use_dilated_cloud      = not args.no_dilated_cloud,
        exclude_water          = args.exclude_water,
        chunk_size             = args.chunks,
    )

    print(f"\nProduct      : {product_dir.name}")
    print(f"Output       : {out_path}")
    print(f"Cloud dil    : {cfg.cloud_dilation_radius} px")
    print(f"Shadow dil   : {cfg.shadow_dilation_radius} px")
    print(f"Cirrus       : {cfg.use_cirrus}")
    print(f"Dil.cloud    : {cfg.use_dilated_cloud}")
    print(f"Exclude water: {cfg.exclude_water}")
    print()

    mask_np, profile = build_valid_pixel_mask(
        qa_pixel_path      = qa_files["qa_pixel"],
        qa_radsat_path     = qa_files["qa_radsat"],
        sr_qa_aerosol_path = qa_files["sr_qa_aerosol"],
        cfg                = cfg,
    )

    write_mask(mask_np, profile, out_path)

    stats = validate_mask(mask_np, profile)
    print_validation_report(stats)

    print(f"\nOutput written → {out_path}")


if __name__ == "__main__":
    main()