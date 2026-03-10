"""TheConverter CLI."""

from pathlib import Path

import click
import numpy as np
from PIL import Image
from shapely import wkt
from shapely.geometry import mapping


@click.group()
@click.version_option(version="0.0.1")
def main():
    """Satellite data converter CLI."""
    pass


@main.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option(
    "--output",
    "-o",
    default=None,
    help="Output Zarr path (default: same name with .zarr)",
)
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Config YAML file",
)
@click.option(
    "--chunks", default="512,512", help="Chunk size as 'y,x' (default: 512,512)"
)
def convert(input_path, output, config, chunks):
    """Convert SAFE or Landsat folder to Zarr."""
    from converter.readers import LandsatReader, Sentinel2Reader
    from converter.writers import ZarrWriter

    input_path = Path(input_path)
    chunk_y, chunk_x = map(int, chunks.split(","))

    if output is None:
        base_name = input_path.name
        for ext in [".SAFE", ".safe", "_SR", "_ST"]:
            base_name = base_name.replace(ext, "")
        output = str(input_path.parent / f"{base_name}.zarr")

    if "MSIL" in input_path.name:
        click.echo(f"Detected Sentinel-2 product")
        reader = Sentinel2Reader(config)
    elif "L1TP" in input_path.name or "L2SP" in input_path.name:
        click.echo(f"Detected Landsat product")
        reader = LandsatReader(config)
    else:
        raise click.ClickException(
            "Unknown product type. Expected Sentinel-2 or Landsat."
        )

    click.echo(f"Reading: {input_path}")
    ds = reader.read(str(input_path))

    click.echo(f"Shape: {dict(ds.sizes)}")

    writer = ZarrWriter(chunks={"time": 1, "y": chunk_y, "x": chunk_x})
    writer.write(ds, output)

    click.echo(f"Written: {output}")


@main.command("batch-convert")
@click.option(
    "--input-dir",
    required=True,
    help="Directory with SAFE/Landsat products (local path or OCI path like 'safes/')",
)
@click.option(
    "--output-dir",
    default=None,
    help="Output directory for Zarr files (local path or OCI path; default: same as input)",
)
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Config YAML file",
)
@click.option(
    "--chunks", default="512,512", help="Chunk size as 'y,x' (default: 512,512)"
)
@click.option("--bucket", "-b", default=None, help="OCI bucket name")
@click.option("--namespace", "-n", default=None, help="OCI namespace")
@click.option("--profile", default="prof", help="OCI config profile")
@click.option("--skip-existing", is_flag=True, help="Skip if .zarr already exists")
def batch_convert(
    input_dir, output_dir, config, chunks, bucket, namespace, profile, skip_existing
):
    """Batch-convert all SAFE/Landsat products in a directory to Zarr.

    Supports both local and OCI sources. For OCI, provide --bucket and --namespace.

    Examples:

      # Local SAFEs -> local Zarrs
      converter batch-convert --input-dir ./data --config config/config.yaml

      # OCI SAFEs -> OCI Zarrs (streaming, no download)
      converter batch-convert --input-dir safes/ --output-dir zarrs/ \\
        --config config/config.yaml \\
        --bucket my-bucket --namespace my-ns --skip-existing
    """
    import sys

    from loguru import logger

    from converter.readers import LandsatReader, Sentinel2Reader
    from converter.writers import ZarrWriter

    # Configure logger to stderr
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    chunk_y, chunk_x = map(int, chunks.split(","))
    is_oci = bucket is not None and namespace is not None

    if output_dir is None:
        output_dir = input_dir

    # --- Discover products ---
    products = []

    if is_oci:
        try:
            from converter.utilities import OCIStore
        except ImportError:
            raise click.ClickException(
                "Missing OCI dependencies. Install with: pip install ocifs oci"
            )

        store = OCIStore(bucket=bucket, namespace=namespace, profile=profile)
        fs = store.fs
        full_input = store._build_path(input_dir)
        click.echo(f"Scanning OCI: {store.get_url(input_dir)}")

        try:
            entries = fs.ls(full_input, detail=False)
        except FileNotFoundError:
            raise click.ClickException(f"OCI path not found: {full_input}")

        prefix = store._build_path("").rstrip("/") + "/"
        for entry in entries:
            name = entry.rstrip("/").split("/")[-1]
            rel = entry[len(prefix) :] if entry.startswith(prefix) else entry
            if "MSIL" in name or "L1TP" in name or "L2SP" in name:
                products.append(rel)
    else:
        input_path = Path(input_dir)
        if not input_path.exists():
            raise click.ClickException(f"Directory not found: {input_dir}")

        for item in sorted(input_path.iterdir()):
            if item.is_dir() and (
                "MSIL" in item.name or "L1TP" in item.name or "L2SP" in item.name
            ):
                products.append(str(item))

    if not products:
        raise click.ClickException("No SAFE/Landsat products found in the directory")

    click.echo(f"Found {len(products)} product(s) to convert")

    # --- Convert each product ---
    writer = ZarrWriter(chunks={"time": 1, "y": chunk_y, "x": chunk_x})
    converted = 0
    skipped = 0
    failed = 0

    for i, product_path in enumerate(products, 1):
        product_name = product_path.rstrip("/").split("/")[-1]

        # Derive output zarr name
        base_name = product_name
        for ext in [".SAFE", ".safe", "_SR", "_ST"]:
            base_name = base_name.replace(ext, "")

        if is_oci:
            zarr_path = f"{output_dir.rstrip('/')}/{base_name}.zarr"
        else:
            zarr_path = str(Path(output_dir) / f"{base_name}.zarr")

        click.echo(f"\n[{i}/{len(products)}] {product_name}")

        # Skip-existing check
        if skip_existing:
            if is_oci:
                remote_zarr = store._build_path(zarr_path)
                if fs.exists(remote_zarr):
                    click.echo(f"  SKIP (zarr exists in OCI)")
                    skipped += 1
                    continue
            else:
                if Path(zarr_path).exists():
                    click.echo(f"  SKIP (zarr exists locally)")
                    skipped += 1
                    continue

        # Detect product type and create reader
        try:
            if "MSIL" in product_name:
                reader = Sentinel2Reader(config, oci_store=store if is_oci else None)
            elif "L1TP" in product_name or "L2SP" in product_name:
                reader = LandsatReader(config, oci_store=store if is_oci else None)
            else:
                click.echo(f"  ERROR: Unknown product type, skipping")
                failed += 1
                continue

            click.echo(f"  Reading...")
            ds = reader.read(product_path)
            click.echo(f"  Shape: {dict(ds.sizes)}")

            # Write zarr: to OCI mapper or local path
            if is_oci:
                output_mapper = store.get_mapper(zarr_path, create=True)
                writer.write(ds, output_mapper)
                click.echo(f"  Written: {store.get_url(zarr_path)}")
            else:
                writer.write(ds, zarr_path)
                click.echo(f"  Written: {zarr_path}")

            converted += 1

        except Exception as e:
            click.echo(f"  ERROR: {e}")
            failed += 1

    # --- Summary ---
    click.echo(f"\n{'='*50}")
    click.echo(f"Batch conversion complete:")
    click.echo(f"  Converted: {converted}")
    click.echo(f"  Skipped:   {skipped}")
    click.echo(f"  Failed:    {failed}")
    click.echo(f"  Total:     {len(products)}")


@main.command()
@click.argument("local_path", type=click.Path(exists=True))
@click.option("--bucket", "-b", required=True, help="OCI bucket name")
@click.option("--namespace", "-n", required=True, help="OCI namespace")
@click.option("--path", "-p", required=True, help="Remote path in bucket")
@click.option("--profile", default="prof", help="OCI config profile")
@click.option("--skip-existing", is_flag=True, help="Skip if already uploaded")
def upload(local_path, bucket, namespace, path, profile, skip_existing):
    """Upload local Zarr to OCI bucket."""
    from converter.utilities import OCIStore

    store = OCIStore(bucket=bucket, namespace=namespace, profile=profile)

    clean_path = path.strip("/")
    if clean_path == "." or clean_path == "":
        base_remote = f"{bucket}@{namespace}"
        display_path = "/"
    else:
        base_remote = f"{bucket}@{namespace}/{clean_path}"
        display_path = clean_path

    # Check if already exists
    if skip_existing:
        try:
            if store.fs.exists(base_remote):
                click.echo(f"Already exists: {store.get_url(display_path)} - skipping")
                return
        except Exception:
            pass
    click.echo(f"Uploading: {local_path} -> oci://{base_remote}")

    local_path = Path(local_path)
    fs = store.fs

    files = [f for f in local_path.rglob("*") if f.is_file()]
    click.echo(f"Files: {len(files)}")

    with click.progressbar(
        files, label="Uploading files", item_show_func=lambda i: i.name if i else None
    ) as bar:
        for file in bar:
            # We want to preserve the folder name being uploaded (e.g. "S2MSI1C")
            # so we take relative_to its parent instead of the directory itself
            rel_path = file.relative_to(local_path.parent)
            remote_path = f"{base_remote}/{rel_path}"

            # Stream the file safely to avoid reading whole files into RAM and failing OCI multipart
            fs.put_file(str(file), remote_path)

    click.echo(f"Uploaded: {store.get_url(display_path)}")


@main.command()
@click.argument("zarr_paths", type=str, nargs=-1, required=True)
@click.option("--wkt", "wkt_str", required=True, help="WKT polygon string")
@click.option("--output", "-o", required=True, help="Output path/directory")
@click.option("--preview", "-p", default=None, help="Optional RGB preview image path")
@click.option("--start", default=None, help="Start date (YYYY-MM-DD)")
@click.option("--end", default=None, help="End date (YYYY-MM-DD)")
@click.option("--bucket", "-b", default=None, help="OCI bucket (for remote zarr)")
@click.option("--namespace", "-n", default=None, help="OCI namespace (for remote zarr)")
@click.option("--bands", default=None, help="Bands to extract (default: all)")
# Use zero-padded defaults to match typical Sentinel-2 naming
@click.option("--rgb", default="B04,B03,B02", help="RGB bands for preview")
@click.option("--crs", default="EPSG:32629", help="Dataset CRS")
@click.option("--profile", default="prof", help="OCI config profile")
@click.option(
    "--format",
    "-f",
    type=click.Choice(["zarr", "parquet", "tessera"], case_sensitive=False),
    default="zarr",
    help="Output format (default: zarr)",
)
@click.option(
    "--tile-size", default=500, help="Tile size for Tessera format (default: 500)"
)
def query(
    zarr_paths,
    wkt_str,
    output,
    preview,
    start,
    end,
    bucket,
    namespace,
    bands,
    rgb,
    crs,
    profile,
    format,
    tile_size,
):
    """Query multiple Zarrs with WKT polygon and time range, stack on time axis."""
    from datetime import datetime

    try:
        import rioxarray  # noqa: F401 - ensures .rio accessor is registered
    except ImportError:
        raise click.ClickException(
            "Missing rioxarray dependency. Install with: pip install rioxarray"
        )

    import xarray as xr

    from converter.writers import ParquetWriter, TesseraWriter, ZarrWriter

    is_oci = bucket is not None and namespace is not None
    store = None
    if is_oci:
        try:
            from converter.utilities import OCIStore
        except ImportError:
            raise click.ClickException(
                "Missing OCI dependencies. Install with: pip install ocifs oci"
            )

        store = OCIStore(bucket=bucket, namespace=namespace, profile=profile)

    start_dt = datetime.strptime(start, "%Y-%m-%d") if start else None
    end_dt = datetime.strptime(end, "%Y-%m-%d") if end else None

    polygon = wkt.loads(wkt_str)
    click.echo(f"Clipping to: {polygon.bounds}")
    if start_dt or end_dt:
        click.echo(f"Time range: {start or '*'} to {end or '*'}")

    expanded_paths = []
    if is_oci:
        for path in zarr_paths:
            if "*" in path:
                prefix = path.split("*")[0]
                if not prefix.endswith("/"):
                    pass

                click.echo(f"Expanding glob in OCI: {path}")
                try:
                    full_glob = f"{bucket}@{namespace}/{path}"
                    matched = store.fs.glob(full_glob)
                    prefix_to_strip = f"{bucket}@{namespace}/"
                    for m in matched:
                        if m.startswith(prefix_to_strip):
                            expanded_paths.append(m[len(prefix_to_strip) :])
                        else:
                            expanded_paths.append(m)
                except Exception as e:
                    click.echo(f"Glob failed: {e}")
            else:
                expanded_paths.append(path)
    else:
        for path in zarr_paths:
            import glob

            if "*" in path:
                expanded_paths.extend(glob.glob(path))
            else:
                expanded_paths.append(path)

    zarr_paths = sorted(list(set(expanded_paths)))
    click.echo(f"Found {len(zarr_paths)} datasets to process")

    clipped_datasets = []

    for zarr_path in zarr_paths:
        if is_oci:
            click.echo(f"Opening: {store.get_url(zarr_path)}")
            source = store.get_mapper(zarr_path)
        else:
            click.echo(f"Opening: {zarr_path}")
            source = zarr_path

        try:
            ds = xr.open_zarr(source, consolidated=True)
        except Exception:
            ds = xr.open_zarr(source, consolidated=False)

        if crs:
            ds = ds.rio.write_crs(crs)
        else:
            if ds.rio.crs is None:
                click.echo(
                    "WARNING: No CRS found in dataset and none provided with --crs. Defaulting to EPSG:32629"
                )
                ds = ds.rio.write_crs("EPSG:32629")

        if start_dt or end_dt:
            times = ds.coords["time"].values
            mask = np.ones(len(times), dtype=bool)
            if start_dt:
                mask &= times >= np.datetime64(start_dt)
            if end_dt:
                mask &= times <= np.datetime64(end_dt)
            if not mask.any():
                click.echo(f"  Skipped: no data in time range")
                continue
            ds = ds.isel(time=mask)

        try:
            clipped = ds.rio.clip([mapping(polygon)], crs="EPSG:4326", all_touched=True)
            if bands:
                band_list = [b.strip() for b in bands.split(",")]
                clipped = clipped.sel(band=band_list)

            click.echo(f"  Shape: {dict(clipped.sizes)}")
            clipped_datasets.append(clipped)
        except Exception as e:
            if "NoDataInBounds" in str(e):
                click.echo(f"  WARNING: No data in bounds for {zarr_path}")
            else:
                click.echo(f"  Error clipping {zarr_path}: {e}")

    if not clipped_datasets:
        raise click.ClickException("No datasets matched the query criteria")

    if len(clipped_datasets) > 1:
        result = xr.concat(clipped_datasets, dim="time")
        result = result.sortby("time")
        click.echo(f"Stacked {len(clipped_datasets)} datasets")
    else:
        result = clipped_datasets[0]

    click.echo(f"Result shape: {dict(result.sizes)}")

    if format == "zarr":
        writer = ZarrWriter()
        writer.write(result, output)
    elif format == "parquet":
        writer = ParquetWriter()
        writer.write(result, output)
    elif format == "tessera":
        writer = TesseraWriter(tile_size=tile_size)
        writer.write(result, output)

    click.echo(f"Saved: {output} (Format: {format})")

    if preview:
        band_names = [str(b) for b in list(result.coords["band"].values)]
        rgb_bands = [b.strip() for b in rgb.split(",")]

        # Normalize user-provided band names to match dataset (handle missing zero padding)
        normalized_rgb = []
        for b in rgb_bands:
            if b in band_names:
                normalized_rgb.append(b)
                continue
            # Try zero-padding if needed (e.g., B4 -> B04)
            if len(b) == 2 and b.startswith("B"):
                padded = f"B0{b[1]}"
                if padded in band_names:
                    normalized_rgb.append(padded)
                    continue
            normalized_rgb.append(b)

        missing = [b for b in normalized_rgb if b not in band_names]
        if missing:
            click.echo(
                f"WARNING: Preview bands not found in dataset: {missing}. Available: {band_names}"
            )
        else:
            bands_data = result["bands"].isel(time=0).values
            indices = [band_names.index(b) for b in normalized_rgb]
            rgb_data = np.stack([_normalize(bands_data[i]) for i in indices], axis=-1)
            rgb_uint8 = (rgb_data * 255).astype(np.uint8)

            img = Image.fromarray(rgb_uint8)
            img.save(preview)
            click.echo(f"Preview: {preview} ({img.width}x{img.height} pixels)")


@main.command("download-s1")
@click.option(
    "--wkt", "wkt_str", required=True, help="WKT polygon of the area of interest"
)
@click.option("--start", required=True, help="Start date YYYY-MM-DD")
@click.option("--end", required=True, help="End date YYYY-MM-DD")
@click.option("--output", "-o", required=True, help="Output Zarr path")
@click.option(
    "--orbit",
    type=click.Choice(["ascending", "descending"]),
    default=None,
    help="Orbit direction filter",
)
@click.option("--crs", default="EPSG:4326", help="Target CRS (default: EPSG:4326)")
@click.option(
    "--resolution",
    default=10.0,
    type=float,
    help="Target resolution in metres (default: 10)",
)
@click.option("--max-items", default=200, type=int, help="Max STAC items to fetch")
@click.option("--bucket", "-b", default=None, help="OCI bucket (to upload result)")
@click.option(
    "--namespace", "-n", default=None, help="OCI namespace (to upload result)"
)
@click.option("--profile", default="prof", help="OCI config profile")
def download_s1(
    wkt_str,
    start,
    end,
    output,
    orbit,
    crs,
    resolution,
    max_items,
    bucket,
    namespace,
    profile,
):
    """Download Sentinel-1 RTC data from Planetary Computer STAC and save as Zarr.

    Downloads VV + VH backscatter for the given WKT polygon and date range.
    Requires: pip install planetary-computer pystac-client

    Examples:

      # Save locally
      satkit download-s1 --wkt "POLYGON(...)" --start 2025-03-01 --end 2025-07-31 -o s1_tile.zarr

      # Filter ascending orbit only
      satkit download-s1 --wkt "POLYGON(...)" --start 2025-03-01 --end 2025-07-31 \\
        --orbit ascending -o s1_asc.zarr

      # Upload to OCI after download
      satkit download-s1 --wkt "POLYGON(...)" --start 2025-03-01 --end 2025-07-31 \\
        -o s1_tile.zarr --bucket my-bucket --namespace my-ns
    """
    from converter.readers.sar import SARDownloader
    from converter.writers import ZarrWriter

    downloader = SARDownloader(target_crs=crs, target_resolution=resolution)

    click.echo(f"Searching Planetary Computer (sentinel-1-rtc)...")
    click.echo(f"  Period : {start} → {end}")
    click.echo(f"  Orbit  : {orbit or 'any'}")
    click.echo(f"  CRS    : {crs}  |  Resolution: {resolution}m")

    try:
        items = downloader.search_by_polygon(
            wkt_str, start, end, orbit=orbit, max_items=max_items
        )
    except ImportError:
        raise click.ClickException(
            "Missing dependencies. Install with:\n"
            "  pip install planetary-computer pystac-client"
        )

    if not items:
        raise click.ClickException(
            f"No S1 scenes found for the given area and date range."
        )

    click.echo(f"Found {len(items)} scene(s). Downloading...")

    ds = downloader.download_for_polygon(wkt_str, start, end, orbit=orbit)

    click.echo(f"Downloaded: {dict(ds.sizes)}")
    click.echo(f"Time steps : {len(ds.coords['time'].values)}")
    click.echo(f"Bands      : {list(ds.coords['band'].values)}")

    is_oci = bucket is not None and namespace is not None

    if is_oci:
        from converter.utilities import OCIStore

        store = OCIStore(bucket=bucket, namespace=namespace, profile=profile)
        mapper = store.get_mapper(output, create=True)
        ZarrWriter().write(ds, mapper)
        click.echo(f"Saved: {store.get_url(output)}")
    else:
        ZarrWriter().write(ds, output)
        click.echo(f"Saved: {output}")


@main.command()
@click.option(
    "--modality",
    type=click.Choice(["s1", "s2", "both"]),
    default="both",
    show_default=True,
    help="Which modality to download",
)
@click.option(
    "--wkt", "wkt_str", required=True, help="WKT polygon of the area of interest"
)
@click.option("--start", required=True, help="Start date YYYY-MM-DD")
@click.option("--end", required=True, help="End date YYYY-MM-DD")
@click.option(
    "--output",
    "-o",
    required=True,
    help="Output directory (one subfolder per modality)",
)
@click.option("--crs", default="EPSG:32629", show_default=True, help="Target CRS")
@click.option(
    "--resolution",
    default=10.0,
    type=float,
    show_default=True,
    help="Target resolution (metres)",
)
@click.option(
    "--cloud-cover",
    default=30.0,
    type=float,
    show_default=True,
    help="S2 max cloud cover %",
)
@click.option(
    "--orbit",
    type=click.Choice(["ascending", "descending"]),
    default=None,
    help="S1 orbit filter",
)
@click.option(
    "--max-items",
    default=200,
    type=int,
    show_default=True,
    help="Max STAC items per modality",
)
@click.option(
    "--format",
    "-f",
    "out_format",
    type=click.Choice(["zarr", "tessera"]),
    default="tessera",
    show_default=True,
    help="Output format",
)
@click.option(
    "--tile-size",
    default=500,
    type=int,
    show_default=True,
    help="Tile size for tessera format",
)
def download(
    modality,
    wkt_str,
    start,
    end,
    output,
    crs,
    resolution,
    cloud_cover,
    orbit,
    max_items,
    out_format,
    tile_size,
):
    """Download S1 and/or S2 from STAC in the format Tessera expects.

    Saves each modality to its own sub-folder:

    \b
      <output>/s2/   ← Sentinel-2 L2A (B02 B03 B04 B05 B06 B07 B08 B8A B11 B12)
      <output>/s1/   ← Sentinel-1 RTC (VV VH)

    Format tessera writes retiled chip directories ready for Tessera inference.
    Format zarr writes a single Zarr store per modality.

    Requires:  pip install planetary-computer pystac-client

    Examples:

    \b
      # Download both modalities as tessera tiles
      satkit download --modality both \\
        --wkt "POLYGON(...)" --start 2025-03-01 --end 2025-07-31 \\
        --output /data/field_A/

    \b
      # S2 only, zarr format, stricter cloud filter
      satkit download --modality s2 --cloud-cover 10 \\
        --wkt "POLYGON(...)" --start 2025-03-01 --end 2025-07-31 \\
        --output /data/field_A/ --format zarr
    """
    import sys
    from pathlib import Path

    from converter.writers import TesseraWriter, ZarrWriter

    out_path = Path(output)
    out_path.mkdir(parents=True, exist_ok=True)

    do_s2 = modality in ("s2", "both")
    do_s1 = modality in ("s1", "both")

    try:
        import planetary_computer  # noqa: F401
        from pystac_client import Client  # noqa: F401
    except ImportError:
        raise click.ClickException(
            "Missing STAC dependencies. Install with:\n"
            "  pip install planetary-computer pystac-client"
        )

    # ------------------------------------------------------------------ S2
    if do_s2:
        from converter.readers.stac_s2 import Sentinel2STACDownloader

        click.echo(
            f"\n[S2] Searching sentinel-2-l2a  {start} → {end}  cloud≤{cloud_cover}%"
        )
        downloader_s2 = Sentinel2STACDownloader(
            target_crs=crs, target_resolution=resolution
        )

        try:
            ds_s2 = downloader_s2.download_for_polygon(
                wkt_str,
                start,
                end,
                max_cloud_cover=cloud_cover,
                max_items=max_items,
            )
        except ValueError as e:
            click.echo(f"[S2] WARNING: {e}")
            ds_s2 = None

        if ds_s2 is not None:
            click.echo(f"[S2] Downloaded: {dict(ds_s2.sizes)}")
            s2_out = str(out_path / "s2")
            if out_format == "tessera":
                TesseraWriter(tile_size=tile_size).write(ds_s2, s2_out)
            else:
                ZarrWriter().write(ds_s2, s2_out + ".zarr")
            click.echo(f"[S2] Saved → {s2_out}")

    # ------------------------------------------------------------------ S1
    if do_s1:
        from converter.readers.sar import SARDownloader

        click.echo(
            f"\n[S1] Searching sentinel-1-rtc  {start} → {end}  orbit={orbit or 'any'}"
        )
        downloader_s1 = SARDownloader(target_crs=crs, target_resolution=resolution)

        try:
            ds_s1 = downloader_s1.download_for_polygon(wkt_str, start, end, orbit=orbit)
        except ValueError as e:
            click.echo(f"[S1] WARNING: {e}")
            ds_s1 = None

        if ds_s1 is not None:
            click.echo(f"[S1] Downloaded: {dict(ds_s1.sizes)}")
            s1_out = str(out_path / "s1")
            if out_format == "tessera":
                # TesseraWriter expects the S1 passed as sar_ascending/descending kwarg
                sar_kwarg = (
                    "sar_ascending" if orbit != "descending" else "sar_descending"
                )
                # Write a stub S2-shaped tessera dir with S1 attached, or just Zarr
                ZarrWriter().write(ds_s1, s1_out + ".zarr")
                click.echo(
                    f"[S1] Note: S1 saved as zarr (use satkit-crop --with-sar to merge at inference)"
                )
            else:
                ZarrWriter().write(ds_s1, s1_out + ".zarr")
            click.echo(f"[S1] Saved → {s1_out}.zarr")

    click.echo(f"\nDone. Output: {output}")


def _normalize(band, low_pct=2, high_pct=98):

    band = band.astype(np.float32)
    valid = band[~np.isnan(band)]
    if len(valid) == 0:
        return np.zeros_like(band)
    low = np.nanpercentile(valid, low_pct)
    high = np.nanpercentile(valid, high_pct)
    if high == low:
        return np.zeros_like(band)
    band = np.clip((band - low) / (high - low), 0, 1)
    return np.nan_to_num(band, nan=0)


if __name__ == "__main__":
    main()
