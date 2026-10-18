"""
Centralized logging configuration for RetailPulse pipeline.
Configures synchronized console logging and file rotation in logs/pipeline.log.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config.settings import LOGS_DIR, LOG_LEVEL

_initialized = False

def setup_logger(name: str = "RetailPulse") -> logging.Logger:
    """
    Initializes and returns a production-ready logger.
    Streams formatted output to stderr/stdout and rotating log files.
    """
    global _initialized
    logger = logging.getLogger(name)

    if not _initialized:
        logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        logger.addHandler(console_handler)

        # Rotating File Handler (Max 10MB per file, keep 5 backups)
        log_file_path = LOGS_DIR / "pipeline.log"
        file_handler = RotatingFileHandler(
            filename=str(log_file_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        logger.propagate = False
        _initialized = True

    return logger
