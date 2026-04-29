from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Callable, Optional, Dict, Any, Union, Literal

from ruamel.yaml import YAML

from sfm_data_provider.core.enums.currencies import CurrencyEnum
from sfm_data_provider.core.instruments.instruments import Instrument, FutureInstrument, CDXIndexInstrument, SwapInstrument, \
    FxForwardInstrument
from sfm_data_provider.core.enums.datasources import DataSource
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.requests.requests import BaseMarketRequest, BaseRequest


# ============================================================
# LIGHTWEIGHT WRAPPER
# ============================================================
class _SimpleRequest:
    """
    Wrapper minimale che avvolge un Instrument nudo in un oggetto
    compatibile con l'interfaccia attesa dai builder.
    Creato da _normalize_request quando l'input è un Instrument diretto.
    """

    market: None = None
    tenor: None = None
    subscription: None = None

    def __init__(self, instrument: Instrument, source: DataSource | None = None):
        self.instrument = instrument
        self.source = source


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
    # NORMALIZE
    # ------------------------------------------------------------
    @staticmethod
    def _normalize_request(
        request_or_instrument: Union["BaseRequest", Instrument],
        source: DataSource | None = None,
    ) -> tuple[Union["BaseRequest", _SimpleRequest], Instrument]:
        """
        Accetta indifferentemente un BaseRequest o un Instrument nudo.
        Ritorna sempre (request, instrument).

        Se viene passato un Instrument, viene wrappato in un _SimpleRequest
        con il source indicato, senza alterare la logica dei builder.
        """
        if isinstance(request_or_instrument, Instrument):
            req = _SimpleRequest(request_or_instrument, source=source)
            return req, request_or_instrument
        req = request_or_instrument
        return req, req.instrument

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
    def build_subscription(
        cls,
        request: Union["BaseRequest", Instrument],
        source: DataSource | None = None,
    ) -> str | Callable:
        raise NotImplementedError


# ============================================================
# TIMESCALE BUILDER
# ============================================================
class TimescaleSubscriptionBuilder(BaseSubscriptionBuilder):
    """Costruisce la subscription per Timescale."""

    @classmethod
    def build_subscription(
        cls,
        request: Union[BaseRequest, Instrument],
        source: DataSource = DataSource.TIMESCALE,
    ) -> str | Callable:
        request, inst = cls._normalize_request(request, source)

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
                rolling = 'third-friday' if inst.future_underlying == "EQUITY" else 'first-friday' #todo improve that rule
                return partial(get_active_timescale_future, sub, rolling=rolling)
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
    def build_subscription(
        cls,
        request: Union[BaseRequest, Instrument],
        source: DataSource = DataSource.BLOOMBERG,
    ) -> str | Callable:
        request, inst = cls._normalize_request(request, source)

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

                if inst.is_active_form:
                    return f"{root or id_}A {suffix}"
                else:
                    return partial(get_active_bbg_future, bbg_root=root, suffix=inst.suffix)

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

                return tkr if tkr.endswith("CURNCY") else f"{tkr} Curncy"

            case InstrumentType.CDXINDEX:
                inst: CDXIndexInstrument
                if inst.id.upper().endswith("CORP"):
                    return inst.id
                else:
                    index_name = inst.index_name
                    tenor = inst.tenor
                    is_active_form = inst.series or inst.is_active_form
                    if index_name:
                        if is_active_form and tenor:
                            return f"{index_name}  GEN {tenor} Corp"
                        return partial(get_active_cdx_components, index_name=index_name, suffix="Corp")
                    else:
                        raise NotImplementedError("subsciption rule of cdx not Implemented. ")

            case InstrumentType.FXFWD:
                inst: FxForwardInstrument
                base_currency = inst.base_currency
                quoted_currency = inst.quoted_currency
                tenor = inst.tenor
                if isinstance(base_currency, list) and len(base_currency) == 1:
                    base_currency = base_currency[0]
                base_currency_str = getattr(base_currency, 'value', base_currency) if base_currency else "EUR"
                if not quoted_currency:
                    raise NotImplementedError("subsciption rule of cdx must have quoted_currency. ")
                quoted_currency_str = quoted_currency.value if quoted_currency != CurrencyEnum.USD else ""

                return f"{base_currency_str}{quoted_currency_str}{tenor} BGN Curncy"

            # ============================================================
            # DEFAULT
            # ============================================================
            case InstrumentType.INDEX:
                if "INDEX" in inst.id.upper():
                    return inst.id
                return f"{inst.ticker or inst.id} INDEX"

            case _:
                raise NotImplementedError(f"subsciption rule of {itype} not Implemented. ")


# ============================================================
# ORACLE BUILDER
# ============================================================
class OracleSubscriptionBuilder(BaseSubscriptionBuilder):
    """Costruisce la subscription per Oracle."""

    @classmethod
    def build_subscription(
        cls,
        request: Union[BaseRequest, Instrument],
        source: DataSource = DataSource.ORACLE,
    ) -> str | Callable:
        request, inst = cls._normalize_request(request, source)

        if inst:
            match inst.type:
                case InstrumentType.ETP | InstrumentType.STOCK:
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
    def build(
        request: Union[BaseRequest, Instrument],
        source: DataSource | None = None,
    ) -> str | Callable:
        # Se è un Instrument nudo il caller deve specificare source
        if isinstance(request, Instrument):
            if source is None:
                raise ValueError(
                    "Quando si passa un Instrument diretto è necessario specificare `source`."
                )
            match source:
                case DataSource.BLOOMBERG:
                    return BloombergSubscriptionBuilder.build_subscription(request, source)
                case DataSource.TIMESCALE:
                    return TimescaleSubscriptionBuilder.build_subscription(request, source)
                case DataSource.ORACLE:
                    return OracleSubscriptionBuilder.build_subscription(request, source)
                case _:
                    return None

        # Caso normale: request porta già source
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


from datetime import datetime, date, timedelta


def get_n_friday(year, month, n_of_friday: int = 3):
    """Calcola il terzo venerdì di un dato mese e anno."""
    # Partiamo dal primo giorno del mese
    first_day = date(year, month, 1)
    # Calcoliamo quanto manca al primo venerdì (venerdì = 4 in weekday())
    # (4 - first_day.weekday() + 7) % 7
    first_friday = first_day + timedelta(days=(4 - first_day.weekday() + 7) % 7)
    # Il terzo venerdì è 14 giorni dopo il primo
    return first_friday + timedelta(days=7 * (n_of_friday - 1))


def get_active_timescale_future(ts_root, current_date, rolling: Literal['first-friday', 'third-friday']) -> str:
    if isinstance(current_date, datetime):
        current_date = current_date.date()

    year = current_date.year
    # Identifica il mese di scadenza del trimestre attuale (3, 6, 9, 12)
    month = ((current_date.month - 1) // 3 + 1) * 3

    # Calcola la data esatta della scadenza per il trimestre corrente
    if rolling == f'first-friday':
        expiry_date = get_n_friday(year, month, 1)
    else:
        expiry_date = get_n_friday(year, month, 3)

    # Se oggi è DOPO il terzo venerdì del mese di scadenza, passa al trimestre dopo
    if current_date >= expiry_date:
        month += 3
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

    month_codes = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
                   7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}

    month = ((current_date.month - 1) // 3 + 1) * 3
    year = current_date.year
    if month > 12:
        month -= 12
        year += 1

    code = month_codes[month]
    year_suffix = str(year)[-1:]

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

    base_series = 40
    base_date = date(2023, 9, 20)

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