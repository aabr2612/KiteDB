import logging
import os
from datetime import datetime

class Logger:
    _instance = None

    def __init__(self, log_dir: str, log_level: str):
        self.log_dir = log_dir
        self.log_level = log_level

    def __new__(cls, log_dir: str, log_level: str):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"{datetime.now():%Y%m%d_%H%M%S}.log")
            level = getattr(logging, log_level.upper(), logging.INFO)
            logging.basicConfig(
                filename=log_file,
                level=level,
                format="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            cls._instance.info("Logger initialized")
        return cls._instance

    def debug(self, msg): logging.debug(msg)
    def info(self, msg): logging.info(msg)
    def warning(self, msg): logging.warning(msg)
    def error(self, msg): logging.error(msg)