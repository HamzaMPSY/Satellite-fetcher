from __future__ import annotations

import re


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
