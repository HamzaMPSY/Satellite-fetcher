import yaml
import os
import json
from loguru import logger


class ConfigLoader:
    """
    Loads configuration files in various formats (JSON, YAML).

    This class provides methods to load a configuration file and to retrieve deeply nested configuration
    variables using dot-separated keys. All major actions and errors are logged.

    Attributes:
        config_file_path (str): Path to the configuration file.
        config (dict): Parsed configuration content.
    """

    _MISSING = object()

    def __init__(self, config_file_path: str):
        """
        Initialize the ConfigLoader and load the configuration file.

        Args:
            config_file_path (str): The path to the configuration file (JSON or YAML).

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            ValueError: If the file format is unsupported or parsing fails.
        """
        self.config_file_path = config_file_path
        logger.info(f"Initializing ConfigLoader with file: {config_file_path}")
        self.config = {}
        self._load_config()

    def _load_config(self):
        """
        Load a configuration file based on its extension and content.

        Sets the config attribute as a dictionary parsed from JSON or YAML.
        Raises on error so callers (cli.py) can catch and report to the user.

        Returns:
            None

        Raises:
            FileNotFoundError: If the config file is missing.
            ValueError: If the file extension is unsupported or content is invalid.
        """
        _, ext = os.path.splitext(self.config_file_path)
        logger.info(f"Attempting to load config (detected extension: {ext})")

        if ext == ".json":
            try:
                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self.config = json.load(f) or {}
            except FileNotFoundError:
                logger.error(f"Config file not found: {self.config_file_path}")
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {self.config_file_path}: {e}")
                raise ValueError(f"Invalid JSON: {e}") from e

        elif ext in (".yaml", ".yml"):
            try:
                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self.config = yaml.safe_load(f) or {}
            except FileNotFoundError:
                logger.error(f"Config file not found: {self.config_file_path}")
                raise
            except yaml.YAMLError as e:
                logger.error(f"Invalid YAML in {self.config_file_path}: {e}")
                raise ValueError(f"Invalid YAML: {e}") from e
        else:
            msg = f"Unsupported config file extension: '{ext}' (expected .json, .yaml, or .yml)"
            logger.error(msg)
            raise ValueError(msg)

        # FIX: Validate that the loaded config is actually a dict
        # (a YAML file containing just a string or list would pass yaml.safe_load
        # but break all downstream get_var calls)
        if not isinstance(self.config, dict):
            logger.error(
                f"Config file {self.config_file_path} did not parse to a dict "
                f"(got {type(self.config).__name__}). Using empty config."
            )
            self.config = {}
            return

        logger.info(
            f"Loaded config from {self.config_file_path} "
            f"({len(self.config)} top-level keys)"
        )

    def get_var(self, var_name: str, default=_MISSING):
        """
        Retrieve a variable from the loaded configuration using dot notation key.

        Args:
            var_name (str): Dot-separated key for the variable to retrieve
                            (e.g. "section.key.field")
            default: Optional default value returned when the key is missing
                     or when the stored value is None.  If not provided and
                     the key is missing, returns None (backward-compatible).

        Returns:
            The value of the variable if present and not None, else ``default``
            if provided, otherwise None.

        Example:
            loader.get_var("providers.usgs.credentials.username")
            loader.get_var("download_manager.max_retries", 5)
        """
        if not isinstance(self.config, dict) or not var_name:
            return default if default is not self._MISSING else None

        keys = var_name.split(".")
        value = self.config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                # FIX: Only log at DEBUG level to avoid flooding logs when
                # optional keys (like download_manager.*) are intentionally
                # absent from config.yaml.  The previous WARNING level caused
                # dozens of noisy log lines on every startup.
                logger.debug(
                    f"Config key '{var_name}' not found (missing segment: '{key}'). "
                    f"Using default={default if default is not self._MISSING else 'None'}."
                )
                return default if default is not self._MISSING else None

        # If a key exists but the value is explicitly None, honor default if provided
        if value is None and default is not self._MISSING:
            return default

        return value

    def get_section(self, section_name: str) -> dict:
        """
        Retrieve an entire configuration section as a dict.

        Useful for getting all settings under a namespace at once, e.g.
        ``loader.get_section("download_manager")`` returns the full dict
        of download manager settings.

        Args:
            section_name: Dot-separated path to the section.

        Returns:
            The section dict, or an empty dict if not found.
        """
        result = self.get_var(section_name, default={})
        if not isinstance(result, dict):
            logger.warning(
                f"Config section '{section_name}' is not a dict "
                f"(got {type(result).__name__}). Returning empty dict."
            )
            return {}
        return result

    def __repr__(self) -> str:
        return (
            f"ConfigLoader(file='{self.config_file_path}', "
            f"keys={list(self.config.keys()) if self.config else '[]'})"
        )