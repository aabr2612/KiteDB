import yaml
import os
import logging
from typing import Dict, Any
from src.logging.logger import Logger

temp_logger = logging.getLogger("temp_config")
temp_logger.setLevel(logging.INFO)
temp_handler = logging.StreamHandler()
temp_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
temp_logger.addHandler(temp_handler)


class Config:
    DEFAULT_CONFIG = {
        "storage": {
            "data_root": os.path.join(os.getcwd(), "db"),
            "chunk_size": 1000,
            "encryption_key": os.environ.get(
                "KITEDB_ENCRYPTION_KEY", "thisisasecretkey"
            ),
        },
        "logging": {"level": "INFO", "directory": os.path.join(os.getcwd(), "logs")},
        "server": {"host": "localhost", "port": 5432},
    }

    def __init__(self, config_file: str = "config.yaml"):
        """Initialize the Config class with default settings and load from a YAML file."""
        self.config = self.DEFAULT_CONFIG
        self.load_config(config_file)

    def load_config(self, config_file: str) -> None:
        """Load and merge configuration from a YAML file into the default config."""
        try:
            if os.path.exists(config_file):
                with open(config_file, "r") as f:
                    user_config = yaml.safe_load(f) or {}
                self._merge_config(self.config, user_config)
                temp_logger.info(f"Loaded configuration from {config_file}")
            else:
                temp_logger.warning(
                    f"Config file {config_file} not found, using defaults"
                )
        except Exception as e:
            temp_logger.error(f"Failed to load config: {e}")
            raise ValueError(f"Invalid configuration: {e}")

    def _merge_config(self, default: Dict, user: Dict) -> None:
        """Recursively merge user configuration into the default configuration."""
        for key, value in user.items():
            if (
                key in default
                and isinstance(default[key], dict)
                and isinstance(value, dict)
            ):
                self._merge_config(default[key], value)
            else:
                default[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a configuration value by key, supporting nested keys with dot notation."""
        keys = key.split(".")
        value = self.config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default


config = Config()
logger = Logger(config.get("logging.directory"), config.get("logging.level", "INFO"))