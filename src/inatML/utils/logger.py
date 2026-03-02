import logging
import sys
from pathlib import Path


def init_logger(log_file: Path, level: int = logging.DEBUG) -> logging.Logger:
    """Configure application-wide logging."""

    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_formatter = logging.Formatter("%(levelname)s: %(message)s")

    # File handler
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Root logger
    rootlogger = logging.getLogger("nukekit")
    rootlogger.setLevel(logging.DEBUG)
    rootlogger.addHandler(file_handler)
    rootlogger.addHandler(console_handler)

    return rootlogger
