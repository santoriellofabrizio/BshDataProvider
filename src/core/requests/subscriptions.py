from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Callable, Optional, Dict, Any

from ruamel.yaml import YAML

from core.enums.currencies import CurrencyEnum
from core.instruments.instruments import Instrument, FutureInstrument, CDXIndexInstrument, SwapInstrument, \
    FxForwardInstrument
from core.enums.datasources import DataSource
from core.enums.instrument_types import InstrumentType
from core.requests.requests import BaseMarketRequest, BaseRequest


# ============================================================
# BASE BUILDER
# ============================================================
class BaseSubscriptionBuilder(ABC):
    """
    Gestisce la creazione delle subscription in modo unificato.
    - Config YAML opzionale (per override)
    - Metodo statico validate()
    - Costruzione di default in base al tipo strumento e provider
    """

    _config: Dict[str, Any] | None = None
    _default_path = Path("config/subscriptions.yaml")

    def __init__(self):
        self._helper = None

    # ------------------------------------------------------------
    # CONFIG MANAGEMENT
    # ------------------------------------------------------------
    @classmethod
    def load_config(cls, path: Path | str | None = None) -> Dict[str, Any]:
        if cls._config is not None:
            return cls._config
        p = Path(path) if path else cls._default_path
        if not p.exists():
            cls._config = {}
            return cls._config
        yaml = YAML(typ="safe")
        try:
            with p.open("r", encoding="utf-8") as f:
                cls._config = yaml.load(f) or {}
        except Exception as e:
            print(f"[WARN] Failed to load YAML config {p}: {e}")
            cls._config = {}
        return cls._config

    @classmethod
    def set_config(cls, config: Dict[str, Any] | None):
        cls._config = config or {}

    # ------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------
    @staticmethod
    def _id_matches(candidate: str, instrument: Instrument) -> bool:
        if not candidate or instrument is None:
            return False
        cand = str(candidate).strip().lower()
        for attr in ("id", "ticker", "isin"):
            val = getattr(instrument, attr, None)
            if val and str(val).strip().lower() == cand:
                return True
        return False

    @classmethod
    def _get_subscription_from_config(cls, provider: str, instrument: Instrument) -> Optional[str]:
        cfg = cls.load_config()
        if not cfg:
            return None
        section = cfg.get(provider) or cfg.get(provider.lower())
        if not section or not isinstance(section, dict):
            return None
        for attr in ("id", "ticker", "isin"):
            val = getattr(instrument, attr, None)
            if val:
                entry = section.get(str(val))
                if entry and isinstance(entry, dict):
                    return entry.get("subscription")
        return None

    # ------------------------------------------------------------
    # ENTRY POINT
    # ------------------------------------------------------------
    @classmethod
    @abstractmethod
    def build_subscription(cls, request: BaseMarketRequest) -> str | Callable:
        raise NotImplementedError


# ============================================================
# TIMESCALE BUILDER
# ============================================================
class TimescaleSubscriptionBuilder(BaseSubscriptionBuilder):
    """Costruisce la subscription per Timescale."""

    @classmethod
    def build_subscription(cls, request: BaseRequest) -> str | Callable:
        inst = request.instrument
        cfg = cls._get_subscription_from_config("timescale", inst)
        if cfg:
            return cfg

        sub = getattr(request, "subscription", None)
        if isinstance(sub, str) or callable(sub):
            return sub

        itype = inst.type if isinstance(inst.type, InstrumentType) else InstrumentType.from_str(str(inst.type))
        match itype:
            case InstrumentType.ETP:
                sub = inst.isin
                if not sub:
                    raise ValueError(f"Please specificy ISIN or SUBSCRIPTION for {inst.id}")
                return sub
            case InstrumentType.FUTURE:
                sub = inst.timescale_root or inst.id
                return partial(get_active_timescale_future, sub)
            case InstrumentType.CURRENCYPAIR | InstrumentType.CURRENCYPAIR:
                if inst.id and len(inst.id) == 3:
                    return f"EUR{inst.id.upper()}"
                return inst.id
            case _:
                return inst.id


# ============================================================
# BLOOMBERG BUILDER
# ============================================================
class BloombergSubscriptionBuilder(BaseSubscriptionBuilder):
    """Costruisce la subscription per Bloomberg."""

    @classmethod
    def build_subscription(cls, request: BaseRequest) -> str | Callable:
        inst = request.instrument
        cfg = cls._get_subscription_from_config("bloomberg", inst)
        if cfg:
            return cfg

        sub = getattr(request, "subscription", None)
        if isinstance(sub, str) or callable(sub):
            return sub

        itype = inst.type if isinstance(inst.type, InstrumentType) else InstrumentType.from_str(str(inst.type))

        match itype:
            # ============================================================
            # ETP / STOCK
            # ============================================================
            case InstrumentType.ETP | InstrumentType.STOCK:
                isin = inst.isin
                ticker = inst.ticker
                market = getattr(request, "market", None)
                if market:
                    if isin:
                        return f"{isin} {market} EQUITY"
                    if ticker:
                        return f"{ticker} {market} EQUITY"
                if isin:
                    return f"{isin} ISIN"
                raise ValueError(f"For {itype.name} request specify ISIN or MARKET or SUBSCRIPTION")

            # ============================================================
            # FUTURE
            # ============================================================
            case InstrumentType.FUTURE:
                inst: FutureInstrument
                id_ = inst.id
                if id_.endswith(("INDEX", "COMDTY")):
                    return id_
                root = inst.root
                suffix = inst.suffix
                if not inst.future_underlying and not suffix:
                    raise ValueError("For futures please specify underlying type or set autocomplete=True")

                suffix = suffix or "INDEX" if inst.future_underlying.upper() == "EQUITY" else "COMDTY"
                if inst.is_active_form:
                    return f"{root or id_}A {suffix}"
                else:
                    return partial(get_active_bbg_future, bbg_root=root, suffix=suffix)
            # ============================================================
            # CURRENCYPAIR
            # ============================================================
            case InstrumentType.CURRENCYPAIR:
                sub_id = inst.id
                return f"EUR{sub_id} Curncy" if len(sub_id) == 3 else f"{sub_id} Curncy"

            case InstrumentType.SWAP:
                inst: SwapInstrument
                uid = inst.id.upper()
                tenor = inst.tenor
                tkr = inst.ticker.upper() if inst.ticker else ""
                known_prefixes = ("EUSWI", "USSWIT", "ILSWI", "EUSW", "USOSFRC")
                if tkr and tkr.startswith(known_prefixes):
                    return tkr if tkr.endswith("CURNCY") else f"{tkr} Curncy"
                if tenor:
                    tenor = tenor.replace("Y", "").replace("M", "")

                if uid.startswith("EUZCISWAP") and tenor:
                    return f"EUSWI{tenor} Curncy"

                if uid.startswith("USZCISWAP") and tenor:
                    return f"USSWIT{tenor} Curncy"

                return uid if uid.endswith("CURNCY") else f"{uid} Curncy"

            case InstrumentType.CDXINDEX:
                inst: CDXIndexInstrument
                if inst.id.upper().endswith("CORP"):
                    return inst.id
                else:
                    index_name = inst.index_name
                    tenor = inst.tenor
                    is_active_form = inst.series or inst.is_active_form
                    if index_name:
                        if is_active_form and  tenor:
                                return f"{index_name}  GEN {tenor} Corp"
                        return partial(get_active_cdx_components, index_name=index_name, suffix="Corp")
                    else:
                        raise NotImplementedError("subsciption rule of cdx not Implemented. ")

            case InstrumentType.FXFWD:
                inst: FxForwardInstrument
                base_currency = inst.base_currency
                quoted_currency = inst.quoted_currency
                tenor = inst.tenor
                base_currency_str = base_currency.value if base_currency else "EUR"
                if not quoted_currency:
                    raise NotImplementedError("subsciption rule of cdx must have quoted_currency. ")
                quoted_currency_str = quoted_currency.value if quoted_currency != CurrencyEnum.USD else ""

                return f"{base_currency_str}{quoted_currency_str}{tenor} BGN Curncy"


            # ============================================================
            # DEFAULT
            # ============================================================
            case _:
                raise ValueError(f"Unsupported type {itype} for subscription build.")


# ============================================================
# ORACLE BUILDER
# ============================================================
class OracleSubscriptionBuilder(BaseSubscriptionBuilder):
    """Costruisce la subscription per Oracle."""

    @classmethod
    def build_subscription(cls, request: BaseRequest, helper=None) -> str | Callable:
        inst = request.instrument
        if inst:
             match inst.type:
                case InstrumentType.ETP | InstrumentType.STOCK:
                        inst = request.instrument
                        if inst.isin:
                            return inst.isin
                case InstrumentType.CDXINDEX:
                    return inst.ticker_root or inst.ticker

                case InstrumentType.FUTURE:
                    return inst.root

        return inst.id

# ============================================================
# UNIVERSAL BUILDER DISPATCHER
# ============================================================
class SubscriptionBuilder:
    """Dispatcher per costruire la subscription in base al provider."""

    @staticmethod
    def build(request: BaseRequest) -> str | Callable:
        match request.source:
            case DataSource.BLOOMBERG:
                return BloombergSubscriptionBuilder.build_subscription(request)
            case DataSource.TIMESCALE:
                return TimescaleSubscriptionBuilder.build_subscription(request)
            case DataSource.ORACLE:
                return OracleSubscriptionBuilder.build_subscription(request)
            case _:
                return getattr(request, "subscription", None)


# ============================================================
# UTILS
# ============================================================
def _futures_month_code(expiry) -> str:
    if isinstance(expiry, str):
        expiry = datetime.strptime(expiry, "%Y-%m-%d")
    month_codes = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
    code = month_codes[expiry.month - 1]
    year_code = str(expiry.year)[-1]
    return f"{code}{year_code}"


def get_active_future(inst, current_date) -> str:
    if isinstance(current_date, datetime):
        current_date = current_date.date()
    month = ((current_date.month - 1) // 3 + 1) * 3
    year = current_date.year
    if month > 12:
        month -= 12
        year += 1
    exp_code = f"{year}{month:02d}"
    return f"{inst.id}{exp_code}"

def get_active_timescale_future(ts_root, current_date) -> str:

    if isinstance(current_date, datetime):
        current_date = current_date.date()
    month = ((current_date.month - 1) // 3 + 1) * 3
    year = current_date.year
    if month > 12:
        month -= 12
        year += 1
    exp_code = f"{year}{month:02d}"
    return f"{ts_root}{exp_code}"

def get_active_bbg_future(bbg_root: str, current_date, suffix) -> str:
    """
    Restituisce il codice del future Bloomberg attivo per la data specificata.

    Esempio:
        get_active_bbg_future("ES", date(2025, 11, 4)) -> 'ESZ25'
    """
    if isinstance(current_date, datetime):
        current_date = current_date.date()

    # Mappa mesi → codici Bloomberg
    month_codes = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
                   7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}

    month = ((current_date.month - 1) // 3 + 1) * 3
    year = current_date.year
    if month > 12:
        month -= 12
        year += 1

    code = month_codes[month]
    year_suffix = str(year)[-1:]  # ultime due cifre

    return f"{bbg_root}{code}{year_suffix} {suffix}"

from datetime import date

def get_active_cdx_components(index_name: str, tenor: str, suffix: str, current_date: date = None):
    """
    Restituisce i componenti attivi per un indice CDXINDEX/iTraxx:
    {
        "index_name": ...,
        "tenor": ...,
        "series": ...,
        "suffix": ...
    }
    """
    index_name = index_name.upper()
    tenor = tenor.upper()
    suffix = suffix.strip()

    if current_date is None:
        current_date = date.today()

    # Serie base: S40 = 20 settembre 2023
    base_series = 40
    base_date = date(2023, 9, 20)

    # Trova quanti roll di marzo/settembre ci sono tra base_date e current_date
    def count_rolls(d1, d2):
        c = 0
        t = d1
        while t < d2:
            if t < date(t.year, 3, 20):
                t = date(t.year, 3, 20)
            elif t < date(t.year, 9, 20):
                t = date(t.year, 9, 20)
            else:
                t = date(t.year + 1, 3, 20)
            if t <= d2:
                c += 1
        return c

    series = base_series + count_rolls(base_date, current_date) if current_date >= base_date \
             else base_series - count_rolls(current_date, base_date)

    return f"{index_name} {series} {tenor} {suffix}"



# Auto-load YAML config
BaseSubscriptionBuilder.load_config()
