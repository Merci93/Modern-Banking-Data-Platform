"""Module containing helper functions."""
import json

from utils.log_handler import get_logger


logger = get_logger(__name__)


def read_reference_data(file_path: str, key: str = None):
    """A helper function to read reference data from JSON files."""
    logger.info("Loading %s from %s", key, file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info("%s data load completed.", key)
    return data.get(key) if key else data
