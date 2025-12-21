import os
import warnings
import logging
from typing import Union, Optional

from ruamel.yaml import YAML
from core.enums.datasources import DataSource

logger = logging.getLogger(__name__)


# ============================================================
# CONFIG LOADER
# ============================================================

def _load_field_config(path: Optional[str] = None) -> dict:
    """
    Carica e normalizza la configurazione da fields.yaml.

    Struttura attesa:

        market:
          - MID
          - BID
          - ...

        info:
          reference: [ ... ]
          bulk: [ ... ]
          historical: [ ... ]

        aliases:
          PX_LAST: LAST
          FX_COMP: FX_COMPOSITION
          ...

    Tutto viene normalizzato in UPPERCASE per coerenza.
    """
    yaml = YAML(typ="safe")
    file_path = path or os.path.join(os.path.dirname(__file__), "fields.yaml")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.load(f)

        if not isinstance(data, dict):
            raise TypeError(f"Invalid YAML structure in {file_path}")

        def norm_list(xs):
            return [str(x).strip().upper() for x in xs]

        def norm_dict(d):
            return {str(k).strip().upper(): str(v).strip().upper() for k, v in d.items()}

        cfg: dict = {}

        for key, val in data.items():
            skey = str(key).lower()  # Le chiavi di sezione restano lowercase
            if isinstance(val, list):
                cfg[skey] = norm_list(val)
            elif isinstance(val, dict):
                sub = {}
                for k2, v2 in val.items():
                    if isinstance(v2, list):
                        sub[str(k2).lower()] = norm_list(v2)
                    elif isinstance(v2, dict):
                        sub[str(k2).lower()] = norm_dict(v2)
                    else:
                        sub[str(k2).lower()] = str(v2).strip().upper()
                cfg[skey] = sub
            else:
                cfg[skey] = str(val).strip().upper()

        # Ensure expected keys
        cfg.setdefault("market", [])
        cfg.setdefault("info", {})
        cfg["info"].setdefault("reference", [])
        cfg["info"].setdefault("bulk", [])
        cfg["info"].setdefault("historical", [])
        cfg.setdefault("aliases", {})

        return cfg

    except Exception as e:
        logger.error(f"[FieldConfigLoader] Failed to load {file_path}: {e}")
        return {
            "market": [],
            "info": {"reference": [], "bulk": [], "historical": []},
            "aliases": {},
        }


_CONFIG = _load_field_config()

_MARKET_FIELDS = frozenset(_CONFIG.get("market", []))
_INFO_REF = frozenset(_CONFIG.get("info", {}).get("reference", []))
_INFO_BULK = frozenset(_CONFIG.get("info", {}).get("bulk", []))
_INFO_HIST = frozenset(_CONFIG.get("info", {}).get("historical", []))
_INFO_GENERAL = frozenset(_CONFIG.get("general", []))
_INFO_ALL = frozenset().union(_INFO_REF, _INFO_BULK, _INFO_HIST, _INFO_GENERAL)
_ALIASES = _CONFIG.get("aliases", {})


# ============================================================
# MarketField (helper, non per forza Enum)
# ============================================================

class MarketField:
    """
    Helper per i campi di mercato.

    - Fonte unica: fields.yaml (sezione `market` + `aliases`)
    - `from_str` restituisce il nome canonico in UPPERCASE (string)
    - `exists` verifica se un campo/alias è valido
    - attributi UPPERCASE creati dinamicamente per autocomplete:
        es. MarketField.MID == "MID"
    """

    @classmethod
    def from_str(cls, value: str) -> str:
        if not value:
            raise ValueError("MarketField cannot be empty")

        val = value.strip().upper()
        normalized = _ALIASES.get(val, val)

        if normalized != val:
            warnings.warn(
                f"MarketField alias detected: '{value}' → '{normalized}'",
                category=UserWarning,
                stacklevel=2,
            )

        if normalized not in _MARKET_FIELDS:
            raise ValueError(f"Unsupported MarketField: {value}")

        return normalized

    @classmethod
    def exists(cls, value: str) -> bool:
        try:
            cls.from_str(value)
            return True
        except ValueError:
            return False

    @classmethod
    def all(cls) -> frozenset[str]:
        return _MARKET_FIELDS


# esponi costanti per autocomplete (senza duplicare logica)
for _f in _MARKET_FIELDS:
    setattr(MarketField, _f.upper(), _f)


# ============================================================
# StaticField (helper, non per forza Enum)
# ============================================================

class StaticField:
    """
    Helper per i campi statici / semi-statici / bulk / storici.

    - Fonte unica: fields.yaml (sezione `info` + `aliases`)
    - `from_str` restituisce il nome canonico in UPPERCASE (string)
    - per BLOOMBERG accetta anche campi non configurati (ritorna stringa normalizzata)
    - `is_reference` / `is_bulk` / `is_historical` leggono da YAML
    - attributi UPPERCASE creati dinamicamente per autocomplete:
        es. StaticField.NAV == "NAV"
    """

    @classmethod
    def from_str(cls, value: str,
                 source: Union[DataSource, str] = DataSource.ORACLE) -> str:
        if not value:
            raise ValueError("StaticField cannot be empty")

        if isinstance(source, str):
            source = DataSource(source)

        val = value.strip().upper()
        normalized = _ALIASES.get(val, val)

        if normalized in _INFO_ALL:
            return normalized

        # non è nel catalogo ufficiale
        if source == DataSource.BLOOMBERG:
            # accettiamo campi custom come stringhe normalizzate in UPPERCASE
            return normalized.upper()

        raise ValueError(f"Unsupported StaticField: {value}")

    # ------------- categorie -------------

    @classmethod
    def is_reference(cls, field: str) -> bool:
        return field.strip().upper() in _INFO_REF

    @classmethod
    def is_bulk(cls, field: str) -> bool:
        return field.strip().upper() in _INFO_BULK

    @classmethod
    def is_historical(cls, field: str) -> bool:
        return field.strip().upper() in _INFO_HIST

    @classmethod
    def is_general(cls, field: str) -> bool:
        return field.strip().upper() in _INFO_GENERAL

    @classmethod
    def category(cls, field: str) -> str:
        f = field.strip().upper()
        if f in _INFO_BULK:
            return "bulk"
        if f in _INFO_REF:
            return "reference"
        if f in _INFO_HIST:
            return "historical"
        if f in _INFO_GENERAL:
            return "general"
        return "unknown"

    @classmethod
    def all(cls) -> frozenset[str]:
        return _INFO_ALL


# anche qui: costanti per autocomplete, derivate dal YAML
for _f in _INFO_ALL:
    setattr(StaticField, _f.upper(), _f)
