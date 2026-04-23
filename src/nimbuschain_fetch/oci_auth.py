from __future__ import annotations

import os


def resolve_oci_auth_mode(explicit: str | None = None) -> str:
    candidate = (
        explicit
        or os.getenv("NIMBUS_OCI_AUTH")
        or os.getenv("OCI_CLI_AUTH")
        or "config"
    )
    normalized = str(candidate or "").strip().lower().replace("-", "_")
    if normalized in {"instance_principal", "instanceprincipal"}:
        return "instance_principal"
    return "config"


def default_oci_config_path(explicit: str | None = None) -> str:
    return os.path.expanduser(
        explicit
        or os.getenv("NIMBUS_OCI_CONFIG_FILE")
        or os.getenv("OCI_CONFIG_FILE")
        or "~/.oci/config"
    )


def default_oci_profile(explicit: str | None = None) -> str:
    return (
        str(
            explicit
            or os.getenv("NIMBUS_OCI_PROFILE")
            or os.getenv("OCI_PROFILE")
            or "DEFAULT"
        ).strip()
        or "DEFAULT"
    )


def default_oci_region() -> str | None:
    region = str(
        os.getenv("NIMBUS_OCI_REGION")
        or os.getenv("OCI_REGION")
        or ""
    ).strip()
    return region or None


__all__ = [
    "default_oci_config_path",
    "default_oci_profile",
    "default_oci_region",
    "resolve_oci_auth_mode",
]
