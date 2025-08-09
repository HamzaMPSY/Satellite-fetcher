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
    
    def __init__(self, config_file_path: str):
        """
        Initialize the ConfigLoader and load the configuration file.

        Args:
            config_file_path (str): The path to the configuration file (JSON or YAML).
        """
        self.config_file_path = config_file_path
        logger.info(f"Initializing ConfigLoader with file: {config_file_path}")
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
        if ext == '.json':
            with open(self.config_file_path, 'r') as f:
                self.config = json.load(f)
        elif ext in ['.yaml', '.yml']:
            with open(self.config_file_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            logger.error(f"Unsupported file extension: {ext}")
            self.config = None
        logger.info(f"Loaded config from {self.config_file_path}")
    
    def get_var(self, var_name: str):
        """
        Retrieve a variable from the loaded configuration using dot notation key.

        Args:
            var_name (str): Dot-separated key for the variable to retrieve (e.g. "section.key.field")
        
        Returns:
            The value of the variable if present, else None.

        Example:
            loader.get_var("providers.usgs.credentials.username")

        Raises:
            None
        """
        keys = var_name.split('.')
        value = self.config
        # Traverse the nested config dictionary using the dot-separated path
        for key in keys:
            if key in value:
                value = value[key]
            else:
                logger.warning(f"Variable '{var_name}' not found in configuration (missing key: '{key}').")
                return None
        logger.debug(f"Retrieved config value for '{var_name}': {value}")
        return value
