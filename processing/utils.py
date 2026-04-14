"""
utils.py - Shared utilities for processing scripts.
"""

import logging #for log info. It sorts messages according to their importance (Error or information?).

def get_logger(name: str) -> logging.Logger:
    """Return a logger with a consistent format for all scripts."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger