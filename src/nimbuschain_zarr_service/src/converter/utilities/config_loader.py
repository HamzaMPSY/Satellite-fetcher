import json
import os

import yaml
from loguru import logger


class ConfigLoader:
    """Loads JSON/YAML config files with dot-notation access."""

    def __init__(self, config_file_path: str):

        self.config_file_path = config_file_path
        logger.info(f"Initializing ConfigLoader with file: {config_file_path}")
        self._load_config()

    def _load_config(self):
        _, ext = os.path.splitext(self.config_file_path)
        if ext == ".json":
            with open(self.config_file_path, "r") as f:
                self.config = json.load(f)
        elif ext in [".yaml", ".yml"]:
            with open(self.config_file_path, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            logger.error(f"Unsupported file extension: {ext}")
            self.config = None

    def get_var(self, var_name: str):
        keys = var_name.split(".")
        value = self.config
        for key in keys:
            if key in value:
                value = value[key]
            else:
                logger.warning(
                    f"Variable '{var_name}' not found (missing key: '{key}')."
                )
                return None
        return value
