from __future__ import annotations

from types import SimpleNamespace

import pytest

from nimbuschain_fetch.download.oci_object_storage import OCIObjectStorageUploader
from nimbuschain_fetch.oci_auth import resolve_oci_auth_mode
from nimbuschain_zarr_service import oci_storage


def test_resolve_oci_auth_mode_defaults_to_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NIMBUS_OCI_AUTH", raising=False)
    monkeypatch.delenv("OCI_CLI_AUTH", raising=False)

    assert resolve_oci_auth_mode() == "config"


def test_resolve_oci_auth_mode_accepts_instance_principal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OCI_CLI_AUTH", "instance_principal")

    assert resolve_oci_auth_mode() == "instance_principal"


def test_oci_store_uses_instance_principal_signer(monkeypatch: pytest.MonkeyPatch) -> None:
    signer = SimpleNamespace(region="me-jeddah-1")
    fake_oci = SimpleNamespace(
        config=SimpleNamespace(
            from_file=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("config.from_file should not be used in instance principal mode")
            )
        ),
        auth=SimpleNamespace(
            signers=SimpleNamespace(
                InstancePrincipalsSecurityTokenSigner=lambda: signer,
                SecurityTokenSigner=lambda *_args, **_kwargs: None,
            )
        ),
        signer=SimpleNamespace(load_private_key_from_file=lambda _path: None),
        object_storage=SimpleNamespace(ObjectStorageClient=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(oci_storage, "OCI_SUPPORT_AVAILABLE", True)
    monkeypatch.setattr(oci_storage, "oci", fake_oci)

    store = oci_storage.OCIStore(bucket="bucket-a", auth_mode="instance_principal")

    config, resolved_signer = store._get_auth()

    assert resolved_signer is signer
    assert config["region"] == "me-jeddah-1"
    assert store.auth_mode == "instance_principal"


def test_oci_support_status_reports_instance_principal_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NIMBUS_OCI_AUTH", "instance_principal")
    monkeypatch.setenv("NIMBUS_OCI_REGION", "me-jeddah-1")

    status = oci_storage.oci_support_status()

    assert status["auth_mode"] == "instance_principal"
    assert status["region"] == "me-jeddah-1"


def test_oci_object_storage_uploader_uses_instance_principal_signer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signer = SimpleNamespace(region="eu-frankfurt-1")
    client_calls: list[tuple[dict[str, str], object]] = []

    class FakeClient:
        def __init__(self, *, config, signer=None):
            client_calls.append((dict(config), signer))

        def get_bucket(self, *, namespace_name: str, bucket_name: str) -> None:
            assert namespace_name == "namespace-a"
            assert bucket_name == "bucket-a"

        def get_namespace(self, compartment_id=None):
            return SimpleNamespace(data="namespace-a")

    fake_oci = SimpleNamespace(
        config=SimpleNamespace(
            from_file=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("config.from_file should not be used in instance principal mode")
            )
        ),
        auth=SimpleNamespace(
            signers=SimpleNamespace(
                InstancePrincipalsSecurityTokenSigner=lambda: signer,
                SecurityTokenSigner=lambda *_args, **_kwargs: None,
            )
        ),
        object_storage=SimpleNamespace(ObjectStorageClient=FakeClient),
        signer=SimpleNamespace(load_private_key_from_file=lambda _path: None),
    )
    monkeypatch.setattr(
        OCIObjectStorageUploader,
        "_load_oci_module",
        staticmethod(lambda: fake_oci),
    )
    monkeypatch.setenv("NIMBUS_OCI_REGION", "eu-frankfurt-1")

    uploader = OCIObjectStorageUploader(
        bucket="bucket-a",
        namespace="namespace-a",
        verify_bucket_access=True,
        auth_mode="instance_principal",
    )

    assert uploader.auth_mode == "instance_principal"
    assert uploader.namespace == "namespace-a"
    assert client_calls == [({"region": "eu-frankfurt-1"}, signer)]
