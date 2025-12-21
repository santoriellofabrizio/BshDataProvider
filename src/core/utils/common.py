# --- YAML config ---
import logging
import os

from ruamel.yaml import YAML

logger = logging.getLogger(__name__)

def load_yaml(config_path: str) -> dict:
    if config_path and os.path.exists(config_path):
        try:
            yaml = YAML(typ="safe")
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.load(f)

            if not cfg or not isinstance(cfg, dict):
                raise ValueError(f"Invalid or empty YAML: {config_path}")
            return cfg
        except Exception as e:
            logger.exception(f"Failed loading Timescale config from {config_path}: {e}")


def normalize_list(value, n: int):
    """Rende qualsiasi input una lista lunga n."""
    if value is None:
        return [None] * n
    if isinstance(value, list):
        if len(value) == 1 and n > 1:
            return value * n
        if len(value) != n:
            raise ValueError("Lista parametri incoerente con numero strumenti.")
        return value
    return [value] * n
