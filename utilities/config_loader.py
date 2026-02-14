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
        """
        self.config_file_path = config_file_path
        logger.info(f"Initializing ConfigLoader with file: {config_file_path}")
        self.config = {}
        self._load_config()

    def _load_config(self):
        """
        Load a configuration file based on its extension and content.

        Sets the config attribute as a dictionary parsed from JSON or YAML.
        Errors are logged if unsupported file type is found.

        Returns:
            None
        """
        _, ext = os.path.splitext(self.config_file_path)
        logger.info(f"Attempting to load config (detected extension: {ext})")

        try:
            if ext == ".json":
                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self.config = json.load(f) or {}
            elif ext in [".yaml", ".yml"]:
                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self.config = yaml.safe_load(f) or {}
            else:
                logger.error(f"Unsupported file extension: {ext}")
                self.config = {}
                return
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.config_file_path}")
            self.config = {}
            return
        except Exception as e:
            logger.exception(f"Failed to load config from {self.config_file_path}: {e}")
            self.config = {}
            return

        logger.info(f"Loaded config from {self.config_file_path}")

    def get_var(self, var_name: str, default=_MISSING):
        """
        Retrieve a variable from the loaded configuration using dot notation key.

        Args:
            var_name (str): Dot-separated key for the variable to retrieve (e.g. "section.key.field")
            default: Optional default value returned when the key is missing. If not provided,
                     returns None (keeps previous behavior).

        Returns:
            The value of the variable if present, else `default` if provided, otherwise None.

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
                logger.warning(
                    f"Variable '{var_name}' not found in configuration (missing key: '{key}')."
                )
                return default if default is not self._MISSING else None

        # If a key exists but the value is explicitly None, still honor default if provided
        if value is None and default is not self._MISSING:
            return default

        return value
