from __future__ import annotations

from datetime import datetime, timezone
import re


def parse_timestamp_from_product_id(product_id: str, *, prefix: str | None = None) -> str | None:
    token = str(product_id)
    if prefix:
        token = token.upper().replace(prefix.upper(), "")
    match = re.search(r"(\d{8}T\d{6})", token)
    if not match:
        return None
    value = match.group(1)
    try:
        parsed = datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return parsed.isoformat()


def ensure_time(value: str | None) -> str:
    if value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()
