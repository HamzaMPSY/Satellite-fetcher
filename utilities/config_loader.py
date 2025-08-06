import yaml
import os
import json
from loguru import logger

class ConfigLoader:
    """
        Loads configuration files in various formats (JSON, YAML).
        Provides methods to load configurations and retrieve variables.
    """
    
    def __init__(self, config_file_path: str):
        self.config_file_path = config_file_path
        self._load_config()
    
    def _load_config(self):
        """
        Load a configuration file based on its extension.
        Args:
            file_path (str): The path to the configuration file.
        Returns:
            dict: The loaded configuration as a dictionary.
        """
        _, ext = os.path.splitext(self.config_file_path)
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
        Get a variable from the loaded configuration.
        Args:
            var_name (str): The name of the variable to retrieve.
        Returns:
            The value of the variable, or None if not found.
        """
        
        keys = var_name.split('.')
        value = self.config
        for key in keys:
            if key in value:
                value = value[key]
            else:
                logger.warning(f"Variable '{var_name}' not found in configuration.")
                return None

        return value