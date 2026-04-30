"""
requests.py — Unified request model for market and static data.

This module defines the core request classes used throughout the BSH data system.
Each request represents a logical data query (market prices, static info, reference data, etc.)
and encapsulates all parameters required by the data providers (Timescale, Bloomberg, Oracle).

Request hierarchy:
    - **BaseRequest**: Generic foundation shared by all request types.
    - **BaseMarketRequest**: Time-series or dynamic data (prices, NAV, fair value...).
    - **BaseStaticRequest**: Static or semi-static data (TER, currency, PCF, etc.).

Logical request types (provider-agnostic):
    - **DailyRequest**: Daily snapshots, EOD prices ecc...
    - **IntradayRequest**: High-frequency or tick-level data.
    - **ReferenceRequest**: Instrument metadata (ISIN, ticker, currency, description...).
    - **BulkRequest**: Batch Bulk data (es: FX_COMPOSITION) for a single field across many instruments.
    - **HistoricalRequest**: Historical static or reference data-> NAVs.
    - **GeneralRequest**: Generic or global requests without a specific instrument, es: all ETFP isins.

Each request automatically:
    - Generates a unique `request_id` for tracking and caching
    - Normalizes fields to UPPERCASE, dates, and frequency
    - Validates source compatibility (e.g. ORACLE, BLOOMBERG, TIMESCALE)
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import time, datetime, date
from typing import Any, Callable, Dict, List, Optional, Union, Literal

import datetime as dt
from sfm_data_provider.core.enums.datasources import DataSource
from sfm_data_provider.core.enums.fields import MarketField, StaticField
from sfm_data_provider.core.enums.frequency import Frequency
from sfm_data_provider.core.enums.markets import Market

RequestType = Literal['general', 'historical', 'reference', 'intraday', 'daily', 'bulk']


# ============================================================
# BASE REQUEST (fundamentale)
# ============================================================

@dataclass
class BaseRequest:
    """Richiesta generica di mercato o statica."""
    fields: Union[str, List[str]]
    instrument: Optional[instrument] = None
    source: Optional[DataSource] = None
    request_type: Optional[RequestType] = None

    def __repr__(self) -> str:
        """Rappresentazione leggibile della richiesta, con tutti gli attributi."""
        cls_name = self.__class__.__name__
        attrs = asdict(self)
        # Formattazione compatta degli attributi
        formatted = []
        for k, v in attrs.items():
            if isinstance(v, list):
                v_str = "[" + ", ".join(map(str, v)) + "]"
            else:
                v_str = repr(v)
            formatted.append(f"{k}={v_str}")
        return f"{cls_name}({', '.join(formatted)})"

    def __post_init__(self):
        """Crea un identificativo univoco per la richiesta."""
        instr = self.instrument
        instr_id = getattr(self.instrument, "id") or "GENERAL" if instr else None
        field_part = ",".join(sorted(self.fields)) if isinstance(self.fields, list) else str(self.fields)
        self.request_id = f"{instr_id}:{field_part}"

    def _get_instr_id(self) -> str:
        """Helper per ottenere l'ID dello strumento in modo sicuro."""
        if self.instrument and hasattr(self.instrument, 'id'):
            return str(self.instrument.id) or "GENERAL"
        return "GENERAL"

    def __lt__(self, other):
        """Consente il sorting basato sull'ID dello strumento."""
        if not isinstance(other, BaseRequest):
            return NotImplemented
        return (self._get_instr_id(), self.request_id) < (other._get_instr_id(), other.request_id)


# ============================================================
# STATIC REQUEST (snapshot / semi-static)
# ============================================================

@dataclass(kw_only=True)
class BaseStaticRequest(BaseRequest):
    """
    Richiesta per dati statici o semi-statici (es. ETF, Futures, Swaps, CDXINDEX).
    Usata da OracleProvider o BloombergProvider per interrogazioni non time-series.
    
    Tutti i campi vengono normalizzati in UPPERCASE tramite StaticField.from_str().
    """

    start: Optional[dt.date] = None
    end: Optional[dt.date] = field(default_factory=lambda: dt.date.today())
    extra_params: Dict[str, Any] = field(default_factory=dict)
    subscription: Optional[str, Callable] = None
    request_type: Optional[RequestType] = None
    market: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        # --- Validazioni ---
        if not self.instrument and self.request_type in ["reference", "historical"]:
            raise ValueError("Missing instrument in BaseStaticRequest")
        if not self.fields:
            raise ValueError("Missing fields in BaseStaticRequest")

        # --- Normalizza fields con StaticField per validazione e normalizzazione UPPERCASE ---
        source = self.source or DataSource.ORACLE
        if isinstance(self.fields, list):
            self.fields = [StaticField.from_str(f, source=source) for f in self.fields]
        else:
            self.fields = StaticField.from_str(self.fields, source=source)

        # --- Parsing date ---
        if isinstance(self.start, str):
            self.start = dt.datetime.strptime(self.start, "%Y-%m-%d").date()
        if isinstance(self.end, str):
            self.end = dt.datetime.strptime(self.end, "%Y-%m-%d").date()

    @property
    def isin(self) -> str:
        return getattr(self.instrument, "id", None)

    def __repr__(self):
        name = getattr(self.instrument, "name", self.isin or "UNKNOWN")
        src = self.source.name if isinstance(self.source, DataSource) else str(self.source)
        return f"<BaseStaticRequest {self.fields} ({name}) [{src}]>"


# ============================================================
# MARKET REQUEST (dinamica / time-series)
# ============================================================

@dataclass(kw_only=True)
class BaseMarketRequest(BaseRequest):
    """
    Base class per tutte le richieste di dati di mercato.
    Ogni request rappresenta un singolo strumento,
    ma può essere gestita in batch dal client.
    
    Tutti i campi vengono normalizzati in UPPERCASE tramite MarketField.from_str().
    """

    market: Optional[Market | str] = None
    start: Union[datetime, date, str, None] = None
    end: Union[datetime, date, str, None] = None
    frequency: Union[str, Frequency] = "1d"
    subscription: Optional[Union[str, Callable[[], str]]] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        super().__post_init__()
        # --- Normalizza fields con MarketField per validazione e normalizzazione UPPERCASE ---
        if isinstance(self.fields, str):
            self.fields = [MarketField.from_str(self.fields)]
        elif isinstance(self.fields, list):
            self.fields = [MarketField.from_str(f) for f in self.fields]
        else:
            raise TypeError("fields must be a string or list of strings")

        # --- Frequency ---
        if isinstance(self.frequency, str):
            self.frequency = Frequency.from_str(self.frequency)

        # --- Garantisce dict ---
        self.extra_params = self.extra_params or {}

        # --- Date parsing ---
        self.start = self._parse_date(self.start)
        self.end = self._parse_date(self.end)

        # --- Normalizza market ---
        if isinstance(self.market, str):
            self.market = Market.from_str(self.market, self.source.value)

    @property
    def currency(self) -> Optional[str]:
        return self.instrument.currency if self.instrument else None

    @staticmethod
    def _parse_date(value: Union[str, datetime, date, None]) -> Optional[Union[datetime, date]]:
        """Parsing robusto di date e datetime."""
        if value is None:
            return None
        if isinstance(value, (datetime, date)):
            return value
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            raise TypeError(f"Invalid date string format: {value}")
        raise TypeError(f"Invalid date type: {type(value)}")


# ============================================================
# LOGICAL REQUEST TYPES (provider-agnostic)
# ============================================================

@dataclass(kw_only=True)
class DailyRequest(BaseMarketRequest):
    """Richiesta dati giornalieri (prezzi, NAV, fair value, ecc.)."""
    snapshot_time: Optional[time] = None
    seconds_sampling: Optional[int] = None  # per Timescale
    adjustment_mode: Optional[str] = None  # per Bloomberg
    request_type: Optional[RequestType] = 'daily'

    def __post_init__(self):
        super().__post_init__()
        self.frequency = Frequency.DAILY
        self.request_type = self.request_type or 'daily'
        self.extra_params.update({
            "snapshot_time": self.snapshot_time,
            "seconds_sampling": self.seconds_sampling,
            "adjustment_mode": self.adjustment_mode,
        })


@dataclass(kw_only=True)
class IntradayRequest(BaseMarketRequest):
    """Richiesta dati is_intraday (tick, 1m, 5m, ecc.)."""
    interval: str = "1m"
    event: Optional[str] = None  # es. TRADE, BID, ASK (solo Bloomberg)
    request_type: RequestType = "intraday"

    def __post_init__(self):
        super().__post_init__()
        self.frequency = Frequency.from_str(self.frequency)
        self.extra_params.update({
            "interval": self.interval,
            "event": self.event,
        })


@dataclass(kw_only=True)
class ReferenceRequest(BaseStaticRequest):
    """Richiesta dati anagrafici (ISIN, ticker, descrizione, ecc.)."""
    fields: List[str] = field(default_factory=lambda: ["ISIN", "TICKER", "CURRENCY", "NAME"])
    request_type: RequestType = "reference"
    market: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        self.request_type = "reference"
        if not self._validate_field():
            raise ValueError(f"{self.fields} not valid for {self.source} as REFERENCE_REQUEST")

    def _validate_field(self):
        match self.source:
            case DataSource.BLOOMBERG:
                return True
            case DataSource.ORACLE:
                return True
            case DataSource.TIMESCALE:
                return True


@dataclass(kw_only=True)
class BulkRequest(BaseStaticRequest):
    """Richiesta dati bulk (FX_COMPOSITION, PCF_COMPOSITION, ecc.)."""

    def __post_init__(self):
        super().__post_init__()
        self.request_type: RequestType = "bulk"
        if not isinstance(self.fields, str) and len(self.fields) > 1:
            raise ValueError("BulkRequest only accepts one field at a time")

        if not self._validate_field():
            raise ValueError(f"{self.fields} not valid for {self.source} as BULK_REQUEST")

    def _validate_field(self):
        match self.source:
            case DataSource.BLOOMBERG:
                return True
            case DataSource.ORACLE:
                return True
            case DataSource.TIMESCALE:
                return True


@dataclass(kw_only=True)
class GeneralRequest(BaseStaticRequest):
    """Richiesta generica senza strumento specifico."""

    def __post_init__(self):
        super().__post_init__()
        self.request_type: RequestType = "general"
        if self.instrument:
            raise ValueError("GeneralRequest doesn't accept instrument")


@dataclass(kw_only=True)
class HistoricalRequest(BaseStaticRequest):
    """Richiesta dati storici statici (NAV, carry, ecc.)."""

    def __post_init__(self):
        super().__post_init__()
        self.request_type: RequestType = "historical"
        if not isinstance(self.fields, str) and len(self.fields) > 1:
            raise ValueError("HistoricalRequest only accepts one field at a time")
        if not self._validate_field():
            raise ValueError(f"{self.fields} not valid for {self.source} as HISTORICAL_REQUEST")

    def _validate_field(self):
        match self.source:
            case DataSource.BLOOMBERG:
                return True
            case DataSource.ORACLE:
                return True
            case DataSource.TIMESCALE:
                return True
