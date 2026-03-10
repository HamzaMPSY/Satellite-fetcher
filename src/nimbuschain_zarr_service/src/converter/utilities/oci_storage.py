import os
from typing import List, Optional

try:
    import oci
    import ocifs

    OCIFS_AVAILABLE = True
except ImportError:
    OCIFS_AVAILABLE = False


class OCIStore:
    """OCI Object Storage interface for Zarr data."""

    def __init__(
        self,
        bucket: str,
        namespace: Optional[str] = None,
        compartment_id: Optional[str] = None,
        config: str = "~/.oci/config",
        profile: str = "DEFAULT",
    ):
        if not OCIFS_AVAILABLE:
            raise ImportError("ocifs not installed. pip install ocifs oci")

        self.bucket = bucket
        self.config = os.path.expanduser(config)
        self.profile = profile
        self.compartment_id = compartment_id
        self._fs = None
        self._namespace = namespace
        self._signer = None
        self._oci_config = None

    def _get_auth(self):
        """
        Returns the OCI config and signer objects.

        If the OCI config has not been loaded before, it will load it from the
        file specified in the constructor. If the security token file is specified
        in the config, it will load the token and private key from the file and
        create a SecurityTokenSigner object.

        Returns:
            tuple: (oci.config, oci.auth.signers.SecurityTokenSigner)
        """
        if self._oci_config is None:
            self._oci_config = oci.config.from_file(self.config, self.profile)
            if "security_token_file" in self._oci_config:
                token_file = self._oci_config["security_token_file"]
                with open(token_file, "r") as f:
                    token = f.read()
                private_key = oci.signer.load_private_key_from_file(
                    self._oci_config["key_file"]
                )
                self._signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
        return self._oci_config, self._signer

    @property
    def fs(self) -> "ocifs.OCIFileSystem":
        """
        Returns an OCIFileSystem object for the bucket.

        The OCIFileSystem object provides a file system interface to the
        OCI Object Storage service. It can be used to list files, open files
        for reading and writing, and delete files.

        The object is lazily initialized when this property is first accessed.
        After initialization, it is reused for subsequent accesses.

        Returns:
            ocifs.OCIFileSystem: The OCIFileSystem object for the bucket.
        """
        if self._fs is None:
            config, signer = self._get_auth()
            # Pass namespace if we have it to avoid network calls/permission errors
            kwargs = {}
            if self._namespace:
                kwargs["namespace"] = self._namespace

            if signer:
                self._fs = ocifs.OCIFileSystem(config=config, signer=signer, **kwargs)
            else:
                self._fs = ocifs.OCIFileSystem(
                    config=self.config, profile=self.profile, **kwargs
                )
        return self._fs

    @property
    def namespace(self) -> str:
        """
        Returns the namespace of the OCI Object Storage bucket.

        The namespace is lazily initialized when this property is first accessed.
        After initialization, it is reused for subsequent accesses.

        Returns:
            str: The namespace of the OCI Object Storage bucket.
        """
        if self._namespace is None:
            config, signer = self._get_auth()
            if signer:
                os_client = oci.object_storage.ObjectStorageClient(
                    config, signer=signer
                )
            else:
                os_client = oci.object_storage.ObjectStorageClient(config)
            if self.compartment_id:
                self._namespace = os_client.get_namespace(
                    compartment_id=self.compartment_id
                ).data
            else:
                self._namespace = os_client.get_namespace().data
        return self._namespace

    def _build_path(self, path: str) -> str:
        """
        Builds the full OCI path: bucket@namespace/path.

        Args:
            path: str, The path to build into a full OCI path.

        Returns:
            str, The full OCI path.
        """
        return f"{self.bucket}@{self.namespace}/{path.lstrip('/')}"

    def get_mapper(self, path: str, create: bool = False):
        """Get a Zarr MutableMapping for the path.

        Args:
            path: Relative path inside the bucket.
            create: Whether to allow creation of the mapping (passed to FSMap).
        """

        from fsspec.mapping import FSMap

        full_path = self._build_path(path)
        return FSMap(full_path, self.fs, check=False, create=create)

    def list_zarrs(self, prefix: str = "") -> List[str]:
        """
        List all Zarr files under a remote path.

        Args:
            prefix: str, The path to list Zarr files under.

        Returns:
            List[str], A list of relative paths (stripped of bucket@namespace prefix) of Zarr files.
        """
        try:
            items = self.fs.ls(self._build_path(prefix))
        except FileNotFoundError:
            return []
        return [i.split("/")[-1].rstrip("/") for i in items if ".zarr" in i]

    def exists(self, path: str) -> bool:
        """
        Check if a file exists in the OCI Object Storage bucket.

        Args:
            path: str, The path to check for existence.

        Returns:
            bool, True if the file exists, False otherwise.
        """
        return self.fs.exists(self._build_path(path))

    def delete(self, path: str, recursive: bool = True) -> None:
        """
        Delete a file or directory from the OCI Object Storage bucket.

        Args:
            path: str, The path to delete.
            recursive: bool, If True, delete all files and directories recursively under the given path.
        """
        self.fs.rm(self._build_path(path), recursive=recursive)

    def get_url(self, path: str) -> str:
        """
        Return a URL for the given path in the OCI Object Storage bucket.

        Args:
            path: str, The path to get a URL for.

        Returns:
            str, The URL of the given path in the OCI Object Storage bucket.
        """
        return f"oci://{self._build_path(path)}"


# https://objectstorage.uk-london-1.oraclecloud.com/p/7GVjVQCxKZnB2U1c225W3baySjRLPr6n2svIdQzCdALq9A240oDNRuPzMJn140kt/n/lrdwfp6kyp5x/b/El-Mohammed/o/rwanda_ndvi_mar2025.tif
