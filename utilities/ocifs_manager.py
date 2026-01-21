import os
import sys
from typing import IO

import fsspec
import oci
from loguru import logger


class OCIFSManager(object):
    """
    Provides a unified interface for opening files either locally or in OCI Object Storage via OCIFS.
    """

    def __init__(
        self, bucket: str, config_path: str = "~/.oci/config", profile: str = "DEFAULT"
    ):
        """
        Initialize OCIFS manager with an OCI config and profile.
        """
        logger.info(f"Initializing OCIFS with config={config_path}, profile={profile}")
        self.bucket = bucket
        # Initialize the OCIFileSystem
        oci_config = oci.config.from_file(
            file_location=config_path, profile_name=profile
        )
        security_token_file_path = os.path.expanduser(oci_config["security_token_file"])
        with open(security_token_file_path, "r") as fh:
            token = fh.read()
        stc = oci.auth.security_token_container.SecurityTokenContainer(None, token)
        if not stc.valid():
            logger.exception("CLI token has expired, please re-authenticate!")
            sys.exit(1)
        priv_key = oci.signer.load_private_key_from_file(oci_config["key_file"])
        self.signer = oci.auth.signers.SecurityTokenSigner(token, priv_key)
        self.config = oci_config
        self.object_storage_client = oci.object_storage.ObjectStorageClient(
            config=self.config, signer=self.signer
        )
        compartment_ocid = os.environ.get("COMPARTMENT_OCID")
        if not compartment_ocid:
            logger.exception("COMPARTMENT_OCID environment variable is not set.")
            sys.exit(1)
        self.namespace = self.object_storage_client.get_namespace(
            compartment_id=compartment_ocid
        ).data
        self.fs = fsspec.filesystem("oci", config=oci_config, signer=self.signer)
        logger.info(f"OCIFS initialized successfully with namespace={self.namespace}")

    def open(self, filename: str, mode: str = "wb") -> IO:
        """
        Open a file for reading/writing, transparently supporting both local and OCI paths.

        Args:
            bucket (str): The OCI bucket name.
            prefix (str): The OCI prefix (namespace/path).
            filename (str): The name of the file to open.
            mode (str): File mode ("wb", "ab", "rb", etc.).

        Returns:
            A file-like object (usable in `with ... as f:`).
        """
        path = f"oci://{self.bucket}@{self.namespace}/{filename}"
        logger.debug(f"Opening path: {path} with mode={mode}")
        return self.fs.open(path, mode)
