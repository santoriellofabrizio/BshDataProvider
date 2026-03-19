import logging
from enum import Enum
from typing import Optional
from ruamel.yaml import YAML
import os

logger = logging.getLogger(__name__)
_config_path = os.path.join(os.path.dirname(__file__), "markets.yaml")


class Market(Enum):
    UNKNOWN = "UNKNOWN"

    _config_path = _config_path

    @staticmethod
    def _load_config(config_path: Optional[str] = None) -> dict[str, dict[str, str]]:
        """
        Carica e normalizza la configurazione YAML provider->mercato.
        Non usa attributi di classe e cache: è una funzione pura.
        """
        yaml = YAML(typ="safe")
        path = config_path or os.path.join(os.path.dirname(__file__), _config_path)

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.load(f)

            if not isinstance(data, dict):
                raise TypeError(f"Invalid YAML structure in {path}: expected mapping at root")

            # Normalizza provider e chiavi in uppercase
            normalized = {
                provider.lower(): {
                    str(k).strip().upper(): str(v).strip() if v else ""
                    for k, v in mapping.items()
                }
                for provider, mapping in data.items()
            }

            return normalized

        except Exception as e:
            logger.error(f"[Market] Error loading {path}: {e}")
            return {}

    @classmethod
    def from_str(cls, value: str, provider: Optional[str] = None) -> str:
        """Converte il mercato in codice provider-specifico."""
        if not value:
            return cls.UNKNOWN.value

        val = str(value).strip().upper()
        if not provider:
            return val

        provider = provider.lower()
        config = cls._load_config()
        mapping = config.get(provider, {}) if isinstance(config, dict) else {}
        result = mapping.get(val)

        if result is None:
            if provider == "timescale":
                raise ValueError(
                    f"Market {value} not found for Timescale. Please check {cls._config_path}"
                )
            logger.info(
                f"[Market] alias '{val}' not found for provider '{provider}', returning unchanged"
            )
            return val

        return result

    @staticmethod
    def get_timescale_segments(config_path: Optional[str] = _config_path) -> dict[str, str]:
        """
        Restituisce la mappa dei segmenti Timescale (gruppi logici di mercati).
        """
        cfg = Market._load_config(config_path)
        return cfg.get("timescale_segments", {})


from sfm_data_provider.core.enums.datasources import DataSource


def normalize_market(market: str | None, source: DataSource | str | None) -> str | None:
    if not market:
        return None
    market = market.upper()
    return market


