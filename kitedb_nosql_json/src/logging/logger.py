import logging
import os
from datetime import datetime


class Logger:
    _instance = None

    def __new__(cls, log_dir: str, log_level: str):
        """Create a singleton Logger instance, initializing the logging system with a file handler."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"{datetime.now():%Y%m%d_%H%M%S}.log")
            level = getattr(logging, log_level.upper(), logging.INFO)
            logging.basicConfig(
                filename=log_file,
                level=level,
                format="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            cls._instance.info("Logger initialized")
        return cls._instance

    def __init__(self, log_dir: str, log_level: str):
        """Initialize the Logger with a log directory and log level."""
        self.log_dir = log_dir
        self.log_level = log_level

    def debug(self, msg):
        """Log a message at the DEBUG level."""
        logging.debug(msg)

    def info(self, msg):
        """Log a message at the INFO level."""
        logging.info(msg)

    def warning(self, msg):
        """Log a message at the WARNING level."""
        logging.warning(msg)

    def error(self, msg):
        """Log a message at the ERROR level."""
        logging.error(msg)