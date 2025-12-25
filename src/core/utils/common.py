# --- YAML config ---
import logging
import os
from typing import Optional

from ruamel.yaml import YAML

logger = logging.getLogger(__name__)

def load_yaml(config_path: Optional[str]) -> dict:
    """
    Load YAML configuration file.
    
    This function is maintained for backward compatibility.
    It now uses ConfigManager internally for caching and consistency.
    
    Args:
        config_path: Path to YAML config file
        
    Returns:
        Dictionary with config data, or empty dict on error
    """
    # Use ConfigManager for consistency and caching
    try:
        from core.utils.config_manager import ConfigManager
        config_manager = ConfigManager.load(config_path)
        return config_manager.get_raw_config()
    except Exception:
        # Fallback to original implementation if ConfigManager fails
        if config_path and os.path.exists(config_path):
            try:
                yaml = YAML(typ="safe")
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg = yaml.load(f)

                if not cfg or not isinstance(cfg, dict):
                    raise ValueError(f"Invalid or empty YAML: {config_path}")
                return cfg
            except Exception as e:
                logger.exception(f"Failed loading config from {config_path}: {e}")
    return {}


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


def normalize_param(value, instruments, default=None):
    """
    Normalizes parameter input to aligned list matching instruments.

    Supports three modes:
    1. Single value (str/scalar): Replicate for all instruments
    2. List: Must match instrument count (or length 1 to replicate)
    3. Dict: Map instrument IDs to values, use default for missing entries

    Args:
        value: Input parameter (str, list, or dict)
        instruments: List of instrument objects with .id attribute
        default: Default value for missing dict entries (default: None)

    Returns:
        List aligned with instruments

    Raises:
        ValueError: If list length doesn't match instrument count
        TypeError: If value type is not supported

    Examples:
        >>> instruments = [inst1, inst2, inst3]  # inst1.id="AAPL", inst2.id="MSFT", inst3.id="GOOGL"

        # Single value - replicate to all
        >>> normalize_param("USD", instruments)
        ["USD", "USD", "USD"]

        # List - must match count
        >>> normalize_param(["USD", "EUR", "GBP"], instruments)
        ["USD", "EUR", "GBP"]

        # Dict - map by ID, use default for missing
        >>> normalize_param({"AAPL": "USD", "GOOGL": "EUR"}, instruments, default="USD")
        ["USD", "USD", "EUR"]
    """
    n = len(instruments)

    # None → list of default values
    if value is None:
        return [default] * n

    # Dict mode: map by instrument ID
    if isinstance(value, dict):
        result = []
        for inst in instruments:
            inst_id = inst.id if hasattr(inst, 'id') else str(inst)
            result.append(value.get(inst_id, default))
        return result

    # List mode: validate length
    if isinstance(value, list):
        if len(value) == 1 and n > 1:
            return value * n
        if len(value) != n:
            raise ValueError(
                f"Lista parametri incoerente con numero strumenti. "
                f"Atteso {n}, ricevuto {len(value)}."
            )
        return value

    # Single value mode: replicate to all
    return [value] * n
