
import argparse
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import rasterio
from rasterio.enums import Resampling as RIOResampling
from skimage.registration import phase_cross_correlation


# ---------------------------------------------------------------------------
# Cloud masking
# ---------------------------------------------------------------------------

def load_landsat_cloud_mask(qa_path: str | Path, target_shape: tuple) -> np.ndarray:

    with rasterio.open(qa_path) as ds:
        qa = ds.read(1, out_shape=target_shape, resampling=RIOResampling.nearest)
    qa = qa.astype(np.uint16)
    cloud        = (qa >> 3) & 1   # bit 3
    cloud_shadow = (qa >> 4) & 1   # bit 4
    mask = (cloud | cloud_shadow).astype(bool)
    pct = 100 * mask.sum() / mask.size
    print(f"  Landsat cloud mask: {pct:.1f}% pixels masked")
    return mask


def load_s2_cloud_mask(scl_path: str | Path, target_shape: tuple) -> np.ndarray:

    CLOUDY_SCL = {3, 8, 9, 10, 11}
    with rasterio.open(scl_path) as ds:
        scl = ds.read(1, out_shape=target_shape, resampling=RIOResampling.nearest)
    mask = np.isin(scl, list(CLOUDY_SCL))
    pct = 100 * mask.sum() / mask.size
    print(f"  S2 SCL cloud mask : {pct:.1f}% pixels masked")
    return mask


def build_combined_mask(
    s2: np.ndarray,
    ls: np.ndarray,
    qa_path: str | Path | None,
    scl_path: str | Path | None,
) -> np.ndarray:

    h, w = s2.shape
    mask = np.isnan(s2) | np.isnan(ls)   # nodata

    if qa_path is not None:
        mask |= load_landsat_cloud_mask(qa_path, (h, w))

    if scl_path is not None:
        mask |= load_s2_cloud_mask(scl_path, (h, w))

    clear_pct = 100 * (~mask).sum() / mask.size
    print(f"  Clear pixels      : {clear_pct:.1f}%")

    if clear_pct < 5:
        print("  ⚠  Less than 5% clear pixels — scene is too cloudy for reliable validation.")

    return mask


# ---------------------------------------------------------------------------
# I/O & normalisation
# ---------------------------------------------------------------------------

def read_band(path: str | Path, band_idx: int = 1, overview: int = 4) -> np.ndarray:
    with rasterio.open(path) as ds:
        arr = ds.read(
            band_idx,
            out_shape=(ds.height // overview, ds.width // overview),
            resampling=RIOResampling.average,
        )
    arr = arr.astype(np.float32)
    arr[arr == 0] = np.nan
    return arr


def normalize(arr: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Percentile stretch using clear pixels only."""
    valid = arr[~mask]
    if valid.size == 0:
        return np.zeros_like(arr)
    lo = np.nanpercentile(valid, 2)
    hi = np.nanpercentile(valid, 98)
    out = np.clip((arr - lo) / (hi - lo + 1e-6), 0, 1)
    out[mask] = np.nan
    return out


# ---------------------------------------------------------------------------
# Shift estimation
# ---------------------------------------------------------------------------

def estimate_shifts(s2: np.ndarray, ls: np.ndarray, n_crops: int = 4) -> list:
    nr, nc = s2.shape
    results = []
    for i in range(n_crops):
        y0 = nr // (n_crops + 1) * (i + 1) - nr // 8
        x0 = nc // (n_crops + 1) * (i + 1) - nc // 8
        sy = slice(max(0, y0), min(nr, y0 + nr // 4))
        sx = slice(max(0, x0), min(nc, x0 + nc // 4))

        rc = np.nan_to_num(s2[sy, sx])
        tc = np.nan_to_num(ls[sy, sx])

        # Skip crop if mostly masked (< 30% valid)
        valid_frac = np.sum(rc != 0) / max(rc.size, 1)
        if valid_frac < 0.30:
            print(f"  Crop {i} skipped (only {valid_frac*100:.0f}% clear pixels)")
            continue

        shift, _, _ = phase_cross_correlation(rc, tc, upsample_factor=8)
        y_c = (sy.start + sy.stop) // 2
        x_c = (sx.start + sx.stop) // 2
        results.append((y_c, x_c, float(shift[0]), float(shift[1])))

    return results


# ---------------------------------------------------------------------------
# Plot 1 — Shift vector map
# ---------------------------------------------------------------------------

def plot_shift_vectors(s2_norm: np.ndarray, shifts: list, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.imshow(s2_norm, cmap="gray", interpolation="nearest", vmin=0, vmax=1)

    mags = []
    for (y, x, dr, dc) in shifts:
        mag = float(np.hypot(dr, dc))
        mags.append(mag)
        color = "lime" if mag < 1.0 else "orange" if mag < 3.0 else "red"
        ax.annotate(
            "", xy=(x + dc * 20, y + dr * 20), xytext=(x, y),
            arrowprops=dict(arrowstyle="->", color=color, lw=2),
        )
        ax.plot(x, y, "o", color=color, ms=6)
        ax.text(x + 5, y - 5, f"{mag:.2f}px", color=color, fontsize=8)

    ax.set_title(
        "Co-registration shift vectors (cloud-masked)\n"
        "green <1px | orange 1–3px | red >3px",
        fontsize=11,
    )
    ax.axis("off")

    if mags:
        ax.text(
            0.02, 0.04,
            f"Crops: {len(mags)}   Mean: {np.mean(mags):.2f}px   Max: {np.max(mags):.2f}px",
            transform=ax.transAxes, fontsize=9, color="white",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="black", alpha=0.6),
        )
    else:
        ax.text(0.5, 0.5, "No valid crops\n(scene too cloudy)",
                transform=ax.transAxes, ha="center", va="center",
                fontsize=14, color="red")

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path.name}")


# ---------------------------------------------------------------------------
# Plot 2 — Scatter (clear pixels only)
# ---------------------------------------------------------------------------

def plot_scatter(s2_norm: np.ndarray, ls_norm: np.ndarray, out_path: Path) -> float:
    mask = np.isnan(s2_norm) | np.isnan(ls_norm)
    idx  = np.flatnonzero(~mask)

    if len(idx) < 100:
        print("  ⚠  Not enough clear pixels for scatter plot.")
        r = float("nan")
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.text(0.5, 0.5, "Not enough clear pixels", transform=ax.transAxes,
                ha="center", va="center", fontsize=14, color="red")
        plt.savefig(out_path, dpi=150)
        plt.close()
        return r

    if len(idx) > 5000:
        idx = np.random.choice(idx, 5000, replace=False)

    x = s2_norm.ravel()[idx]
    y = ls_norm.ravel()[idx]
    r = float(np.corrcoef(x, y)[0, 1])

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.hexbin(x, y, gridsize=60, cmap="Blues", mincnt=1, extent=[0, 1, 0, 1])
    ax.plot([0, 1], [0, 1], "r--", lw=1.5, label="1:1 line")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Sentinel-2 B04 (normalized, clear pixels)", fontsize=11)
    ax.set_ylabel("Landsat B4 (normalized, clear pixels)", fontsize=11)
    ax.set_title(f"S2 vs Landsat — cloud-masked & normalized\nPearson r = {r:.4f}", fontsize=12)
    ax.legend(fontsize=9)

    quality = "Excellent" if r > 0.90 else "Good" if r > 0.75 else "Poor"
    color   = "green"     if r > 0.90 else "orange" if r > 0.75 else "red"
    ax.text(0.05, 0.92, f"Alignment: {quality}", transform=ax.transAxes,
            fontsize=10, color=color, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path.name}")
    return r


# ---------------------------------------------------------------------------
# Plot 3 — Difference heatmap (clear pixels only)
# ---------------------------------------------------------------------------

def plot_difference(s2_norm: np.ndarray, ls_norm: np.ndarray, out_path: Path) -> tuple:
    diff = s2_norm - ls_norm                        # NaN where cloudy
    mae  = float(np.nanmean(np.abs(diff)))
    rmse = float(np.sqrt(np.nanmean(diff ** 2)))
    vmax = float(np.nanpercentile(np.abs(diff[~np.isnan(diff)]), 95))

    fig, ax = plt.subplots(figsize=(7, 7))
    im = ax.imshow(diff, cmap="RdBu_r", vmin=-vmax, vmax=vmax, interpolation="nearest")
    plt.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="S2 − LS (normalized)")
    ax.set_title(
        f"Difference map — cloud-masked & normalized\nMAE = {mae:.4f}   RMSE = {rmse:.4f}",
        fontsize=11,
    )
    ax.axis("off")

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path.name}")
    return mae, rmse


# ---------------------------------------------------------------------------
# Cloud coverage summary plot
# ---------------------------------------------------------------------------

def plot_cloud_mask(mask: np.ndarray, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.imshow(mask, cmap="RdYlGn_r", vmin=0, vmax=1, interpolation="nearest")
    clear_pct = 100 * (~mask).sum() / mask.size
    ax.set_title(
        f"Combined cloud mask\nGreen = clear ({clear_pct:.1f}%)   Red = masked",
        fontsize=11,
    )
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out_path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Validate geometric processing with cloud masking")
    p.add_argument("--s2",          required=True,  help="S2 output GeoTIFF")
    p.add_argument("--ls",          required=True,  help="Landsat output GeoTIFF")
    p.add_argument("--qa",          default=None,   help="Landsat QA_PIXEL.TIF (cloud mask)")
    p.add_argument("--scl",         default=None,   help="S2 SCL band JP2 (scene classification)")
    p.add_argument("--outdir",      default="./validation")
    p.add_argument("--s2-red-band", type=int, default=3, help="1-based red band in S2 file (default 3 = B04)")
    p.add_argument("--ls-red-band", type=int, default=3, help="1-based red band in LS file (default 3 = B4)")
    p.add_argument("--overview",    type=int, default=4, help="Downscale factor (default 4)")
    args = p.parse_args()

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Reading bands ...")
    s2_raw = read_band(args.s2, band_idx=args.s2_red_band, overview=args.overview)
    ls_raw = read_band(args.ls, band_idx=args.ls_red_band, overview=args.overview)

    # Crop to same shape
    h = min(s2_raw.shape[0], ls_raw.shape[0])
    w = min(s2_raw.shape[1], ls_raw.shape[1])
    s2_raw = s2_raw[:h, :w]
    ls_raw = ls_raw[:h, :w]

    print("\nBuilding cloud mask ...")
    cloud_mask = build_combined_mask(s2_raw, ls_raw, args.qa, args.scl)

    print("\nNormalizing clear pixels ...")
    s2_norm = normalize(s2_raw, cloud_mask)
    ls_norm = normalize(ls_raw, cloud_mask)

    print("\nGenerating plots ...")
    plot_cloud_mask(cloud_mask, out_dir / "0_cloud_mask.png")

    shifts    = estimate_shifts(s2_norm, ls_norm)
    plot_shift_vectors(s2_norm, shifts, out_dir / "1_shift_vectors.png")

    r         = plot_scatter(s2_norm, ls_norm, out_dir / "2_scatter_s2_vs_ls.png")
    mae, rmse = plot_difference(s2_norm, ls_norm, out_dir / "3_difference_heatmap.png")

    mags = [float(np.hypot(dr, dc)) for (_, _, dr, dc) in shifts]
    clear_pct = 100 * (~cloud_mask).sum() / cloud_mask.size

    print("\n" + "=" * 45)
    print("VALIDATION SUMMARY")
    print("=" * 45)
    print(f"  Clear pixels          : {clear_pct:.1f}%")
    print(f"  Crops evaluated       : {len(shifts)}")
    if mags:
        print(f"  Mean shift            : {np.mean(mags):.3f} px")
        print(f"  Max  shift            : {np.max(mags):.3f} px")
    if not np.isnan(r):
        print(f"  Pearson r (normalized): {r:.4f}  {'✓ Good' if r > 0.75 else '✗ Poor'}")
    print(f"  MAE  (normalized)     : {mae:.4f}  (ideal → 0)")
    print(f"  RMSE (normalized)     : {rmse:.4f}  (ideal → 0)")
    print("=" * 45)

    if clear_pct < 10:
        print("\n⚠  Scene is too cloudy for reliable validation.")
        print("   Download a clear-sky acquisition for the same area.")
        print("   Netherlands: best months are June–August.")
    elif not np.isnan(r) and r > 0.85:
        print("\n✓  Geometric alignment looks good!")
    elif not np.isnan(r) and r < 0.5:
        print("\n⚠  Poor alignment even on clear pixels.")
        print("   Check that input scenes cover the same geographic area.")

    print(f"\nOutputs saved to: {out_dir}/")


if __name__ == "__main__":
    main()