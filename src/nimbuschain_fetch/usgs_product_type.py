from __future__ import annotations

import re


def canonicalize_usgs_product_type(product_type: str | None) -> str:
    """
    Convert strict UI/backend Landsat keys such as:

      8L2SP
      9L1TP

    back to the canonical USGS product type expected by downstream Zarr logic:

      L2SP
      L1TP
    """
    value = str(product_type or "").strip().upper()
    if re.fullmatch(r"[0-9]L[0-9A-Z]{3}", value):
        return value[1:]
    return value


def normalize_usgs_product_type_from_display_id(display_id: str) -> str:
    """
    Convert a USGS display ID such as:

      LC08_L2SP_207024_20250101_...
      LC09_L1TP_199032_20250101_...

    into the UI/backend product key form:

      8L2SP
      9L1TP

    Returns an empty string when the display ID does not follow the expected
    Landsat pattern.
    """
    parts = [part.strip().upper() for part in str(display_id or "").split("_") if part.strip()]
    if len(parts) < 2:
        return ""

    platform = parts[0]
    product_code = parts[1]

    digits = re.findall(r"\d", platform)
    if not digits:
        return ""

    satellite_digit = digits[-1]
    if not product_code.startswith("L"):
        return ""

    return f"{satellite_digit}{product_code}"


def landsat_path_row_from_display_id(display_id: str) -> str:
    """
    Extract the six-digit WRS-2 path/row from a Landsat display or bundle ID.

    Supports both USGS display IDs with underscores, for example
    `LC09_L1TP_199026_20260101_...`, and compact bundle names observed in
    downloaded files, for example `LC92010262026114LGN00`.
    """
    value = str(display_id or "").strip().upper()
    if not value:
        return ""
    value = value.rsplit("/", 1)[-1]
    value = re.sub(r"\.(TAR|ZIP|TGZ|GZ)$", "", value)

    parts = [part.strip().upper() for part in value.split("_") if part.strip()]
    if len(parts) >= 3 and re.fullmatch(r"\d{6}", parts[2]):
        return parts[2]

    compact = re.match(r"^L[A-Z]\d(?P<path>\d{3})(?P<row>\d{3})\d{7}[A-Z0-9]*$", value)
    if compact:
        return f"{compact.group('path')}{compact.group('row')}"
    return ""


def usgs_display_id_matches_tile(display_id: str, tile_id: str | None) -> bool:
    requested = str(tile_id or "").strip()
    if not requested:
        return True
    requested_digits = "".join(re.findall(r"\d", requested))
    if len(requested_digits) != 6:
        return True
    path_row = landsat_path_row_from_display_id(display_id)
    return not path_row or path_row == requested_digits


def usgs_product_type_matches(display_id: str, product_type: str) -> bool:
    """
    Match a requested product type against a USGS display ID.

    Supports both:
    - strict UI/backend keys: `8L2SP`, `9L1TP`
    - legacy partial keys: `L2SP`, `L1TP`
    """
    requested = str(product_type or "").strip().upper()
    if not requested:
        return True

    display = str(display_id or "").strip().upper()
    if not display:
        return False

    if requested in display:
        return True

    normalized = normalize_usgs_product_type_from_display_id(display)
    if requested == normalized:
        return True

    return False
