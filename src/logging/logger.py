import logging
import os
from datetime import datetime
from src.config import LOG_ROOT

class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            os.makedirs(LOG_ROOT, exist_ok=True)
            log_file = os.path.join(LOG_ROOT, f"{datetime.now():%Y%m%d_%H%M%S}.log")
            logging.basicConfig(
                filename=log_file,
                level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            cls._instance = super().__new__(cls)
            cls._instance.info("Logger initialized")
        return cls._instance

    def debug(self, msg): logging.debug(msg)
    def info(self, msg): logging.info(msg)
    def warning(self, msg): logging.warning(msg)
    def error(self, msg): logging.error(msg)

logger = Logger()