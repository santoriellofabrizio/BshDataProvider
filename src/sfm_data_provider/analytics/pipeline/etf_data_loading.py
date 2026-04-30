"""
ETF Data Pipeline — caricamento dati lazy per analisi ETF.

Typical usage::

    pipeline = DataPipeline(
        api=BshData(...),
        instruments=["IE00B4L5Y983", "IE00B5BMR087"],
        start="2024-01-01",
        end="2024-06-01",
    )

    prices  = pipeline.prices           # lazy: scaricato al primo accesso
    fx_comp = pipeline.fx_composition
    pipeline.load_all()                 # scarica tutto in una volta

    # Overrides (chainable, invalidano il cache lazy):
    pipeline.override_ter({"IE00B4L5Y983": 0.002}).override_fx_composition(my_df)

    # Inject più override in un colpo solo:
    pipeline.set_data(ter={"IE00B4L5Y983": 0.002}, fx_prices=my_fx_df)

    # Copia con date diverse senza ri-settare gli override:
    extended = pipeline.with_date_range("2020-01-01", "2024-01-01")
    extended.load_all()

    # Config esplicita per tuning avanzato:
    config = PipelineConfig(
        snapshot_time=time(17, 30),
        intraday_start_time=time(9, 30),
        intraday_end_time=time(17),
        ter_scale=0.5,
        base_currency="USD",
        etf_market="EURONEXT",
    )
    pipeline = DataPipeline(api, instruments, "2024-01-01", "2024-06-01", config=config)
"""

from __future__ import annotations

import copy
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd

from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument
from sfm_data_provider.interface.bshdata import BshData

logger = logging.getLogger(__name__)

# Keys accepted by set_data()
_DATA_KEYS = frozenset({
    "ter", "fx_composition", "fx_forward_composition",
    "dividends", "fx_prices", "fx_forward_prices", "ytm",
})


@dataclass
class PipelineConfig:
    """
    Configurazione completa per DataPipeline.

    Timing
    ------
    snapshot_time
        Ora di chiusura usata per il look-up prezzi giornalieri (default 17:00).
    fx_snapshot_time
        Ora snapshot per i tassi FX spot. ``None`` → usa ``snapshot_time``.
    intraday_start_time / intraday_end_time
        Finestra delle barre intraday da includere (default 09:30–17:00).

    FX
    --
    base_currency
        Valuta base per i rate FX (default ``"EUR"``).
    fx_composition_ref_date
        Data usata per leggere le esposizioni valutarie. ``None`` → usa ``end``.
    fx_forward_tenor
        Tenor per il carry forward (default ``"1M"``).

    Aggiustamenti
    -------------
    ter_scale
        Moltiplica ogni TER per questo fattore prima di applicarlo.
        Utile per periodi sub-annuali (es. ``252/365``) o sensitivity analysis.

    Sorgenti & mercati
    ------------------
    etf_market
        Codice mercato per le chiamate API ETF (default ``"EURONEXT"``).
    etf_source / fx_source / fx_forward_source
        Provider dati (``'timescale'``, ``'bloomberg'``, …).
    etf_fallbacks
        Lista ordinata di provider di fallback tentati sui risultati parziali.
    """

    # --- Time window ---
    start: Union[str, date, datetime] = None
    end:   Union[str, date, datetime] = None

    # --- Timing ---
    frequency:           str  = "daily"
    snapshot_time:       time = field(default_factory=lambda: time(17, 0))
    fx_snapshot_time:    Optional[time] = None
    intraday_start_time: time = field(default_factory=lambda: time(9,  30))
    intraday_end_time:   time = field(default_factory=lambda: time(17,  0))

    # --- FX ---
    base_currency:           str            = "EUR"
    fx_composition_ref_date: Optional[date] = None
    fx_forward_tenor:        str            = "1M"

    # --- Aggiustamenti ---
    ter_scale: float = 1.0

    # --- Sorgenti ---
    etf_source:        str                  = "timescale"
    etf_market:        str                  = "EURONEXT"
    fx_source:         str                  = "timescale"
    fx_forward_source: str                  = "bloomberg"
    etf_fallbacks:     Optional[List[dict]] = None

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def effective_fx_snapshot_time(self) -> time:
        """Ora snapshot FX; cade su ``snapshot_time`` se non impostata."""
        return self.fx_snapshot_time or self.snapshot_time

    @property
    def effective_fx_ref_date(self) -> Optional[date]:
        """Data riferimento FX composition; cade su ``end`` se non impostata."""
        if self.fx_composition_ref_date is not None:
            return self.fx_composition_ref_date
        return pd.to_datetime(self.end).date() if self.end is not None else None


class DataPipeline:
    """
    Layer di data loading per ETF/Future/FX.

    Ogni dataset è lazy: viene scaricato solo al primo accesso (o dopo un override).
    Accetta un oggetto ``PipelineConfig`` per il tuning avanzato, oppure i singoli
    parametri per comodità (backward compatible).

    Args:
        api: Istanza di BshData.
        instruments: Lista di ISIN (str) o oggetti Instrument.
        start: Data di inizio.
        end: Data di fine.
        config: ``PipelineConfig`` opzionale. I parametri ``frequency``,
            ``snapshot_time``, ecc. passati direttamente vengono applicati
            sopra il config (override per comodità).
        frequency: Shortcut per ``config.frequency``.
        snapshot_time: Shortcut per ``config.snapshot_time``.
        fx_forward_tenor: Shortcut per ``config.fx_forward_tenor``.
        etf_source / fx_source / fx_forward_source: Shortcut sorgenti.
        etf_fallbacks: Shortcut fallback.
    """

    def __init__(
        self,
        api,
        instruments: Union[List[str], List],
        start: Union[str, date, datetime],
        end: Union[str, date, datetime],
        config: Optional[PipelineConfig] = None,
        # Shortcut params (applicati sopra config se forniti)
        frequency: Optional[str] = None,
        snapshot_time: Optional[time] = None,
        fx_forward_tenor: Optional[str] = None,
        etf_source: Optional[str] = None,
        fx_source: Optional[str] = None,
        fx_forward_source: Optional[str] = None,
        etf_fallbacks: Optional[List[dict]] = None,
    ):
        self.api: BshData = api

        self.config = config or PipelineConfig()
        self.config.start = start
        self.config.end   = end

        # Shortcut overrides
        if frequency        is not None: self.config.frequency         = frequency
        if snapshot_time    is not None: self.config.snapshot_time     = snapshot_time
        if fx_forward_tenor is not None: self.config.fx_forward_tenor  = fx_forward_tenor
        if etf_source       is not None: self.config.etf_source        = etf_source
        if fx_source        is not None: self.config.fx_source         = fx_source
        if fx_forward_source is not None: self.config.fx_forward_source = fx_forward_source
        if etf_fallbacks    is not None: self.config.etf_fallbacks     = etf_fallbacks

        if instruments and hasattr(instruments[0], "id"):
            self.instrument_objects: List[Instrument] = instruments
            self.instrument_ids = [inst.id for inst in instruments]
        else:
            self.instrument_ids = list(instruments)
            self.instrument_objects = self.api.market.build_instruments(self.instrument_ids, autocomplete=True)

        self.instruments_by_type: Dict[InstrumentType, List[Instrument]] = defaultdict(list)
        for inst in self.instrument_objects:
            self.instruments_by_type[inst.type].append(inst)

        # Unified override store (set_data / override_*)
        self._overrides: Dict[str, Any] = {}

        # Lazy cache
        self._prices:                  Optional[pd.DataFrame] = None
        self._fx_composition:          Optional[pd.DataFrame] = None
        self._fx_forward_composition:  Optional[pd.DataFrame] = None
        self._fx_prices:               Optional[pd.DataFrame] = None
        self._fx_forward_prices:       Optional[pd.DataFrame] = None
        self._dividends:               Optional[pd.DataFrame] = None
        self._ter:                     Optional[dict]         = None
        self._repo:                    Optional[pd.DataFrame] = None
        self._ytm:                     Optional[pd.DataFrame] = None

    # ============================================================
    # OVERRIDE — set_data + metodi individuali (backward compat)
    # ============================================================

    def set_data(self, **kwargs) -> "DataPipeline":
        """
        Inietta dati pre-costruiti bypassando una o più chiamate API (fluent).

        Chiavi accettate:
            ``ter``                    – dict {instrument_id: ter_annuale}
            ``fx_composition``         – DataFrame (strumenti × valute, pesi spot)
            ``fx_forward_composition`` – DataFrame (strumenti × valute, pesi forward)
            ``dividends``              – DataFrame (date × strumenti)
            ``fx_prices``              – DataFrame (date × coppie valutarie, spot)
            ``fx_forward_prices``      – DataFrame (date × coppie valutarie, forward)
            ``ytm``                    – DataFrame (date × strumenti, ytm)

        Esempio::

            pipeline.set_data(
                ter={"IE00B4L5Y983": 0.002},
                fx_prices=my_fx_df,
            )
        """
        unknown = set(kwargs) - _DATA_KEYS
        if unknown:
            raise ValueError(f"Chiavi sconosciute: {unknown}.  Valide: {sorted(_DATA_KEYS)}")
        self._overrides.update(kwargs)
        # Invalida i cache corrispondenti
        for key in kwargs:
            attr = f"_{key}" if key != "ter" else "_ter"
            if hasattr(self, attr):
                setattr(self, attr, None)
        logger.info("Data overrides impostati: %s", list(kwargs))
        return self

    def override_ter(self, ter: dict) -> "DataPipeline":
        return self.set_data(ter=ter)

    def override_ytm(self, ytm: pd.DataFrame) -> "DataPipeline":
        return self.set_data(ytm=ytm)

    def override_fx_composition(self, df: pd.DataFrame) -> "DataPipeline":
        self._fx_prices = None  # le valute necessarie possono cambiare
        return self.set_data(fx_composition=df)

    def override_fx_forward_composition(self, df: pd.DataFrame) -> "DataPipeline":
        self._fx_forward_prices = None
        return self.set_data(fx_forward_composition=df)

    def override_dividends(self, df: pd.DataFrame) -> "DataPipeline":
        return self.set_data(dividends=df)

    def override_fx_prices(self, df: pd.DataFrame) -> "DataPipeline":
        return self.set_data(fx_prices=df)

    def override_fx_forward_prices(self, df: pd.DataFrame) -> "DataPipeline":
        return self.set_data(fx_forward_prices=df)

    # ============================================================
    # LAZY PROPERTIES
    # ============================================================

    @property
    def prices(self) -> pd.DataFrame:
        if self._prices is None:
            self._prices = self._fetch_prices()
        return self._prices

    @property
    def fx_composition(self) -> Optional[pd.DataFrame]:
        if self._fx_composition is None:
            self._fx_composition = (
                self._overrides.get("fx_composition")
                or self._fetch_fx_composition()
            )
        return self._fx_composition

    @property
    def fx_forward_composition(self) -> Optional[pd.DataFrame]:
        if self._fx_forward_composition is None:
            self._fx_forward_composition = (
                self._overrides.get("fx_forward_composition")
                or self._fetch_fx_forward_composition()
            )
        return self._fx_forward_composition

    @property
    def fx_prices(self) -> Optional[pd.DataFrame]:
        if self._fx_prices is None:
            raw = (
                self._overrides.get("fx_prices")
                or self._fetch_fx_prices(self._currencies_from(self.fx_composition))
            )
            if raw is None:
                raw = pd.DataFrame(1, columns=[self.config.base_currency], index=self.prices.index)
            if isinstance(raw, pd.Series):
                raw = raw.to_frame()
            self._fx_prices = raw
        return self._fx_prices

    @property
    def fx_forward_prices(self) -> Optional[pd.DataFrame]:
        if self._fx_forward_prices is None:
            raw = (
                self._overrides.get("fx_forward_prices")
                or self._fetch_fx_forward_prices(self._currencies_from(self.fx_forward_composition))
            )
            if isinstance(raw, pd.Series):
                raw = raw.to_frame()
            self._fx_forward_prices = raw
        return self._fx_forward_prices

    @property
    def ytm(self) -> Optional[pd.DataFrame]:
        if self._ytm is None:
            self._ytm = self._overrides.get("ytm") or self._fetch_ytm()
        return self._ytm

    @property
    def dividends(self) -> Optional[pd.DataFrame]:
        if self._dividends is None:
            ids = [i.id for i in self.instrument_objects if i.type in (InstrumentType.ETP, InstrumentType.STOCK)]
            self._dividends = (
                self._overrides.get("dividends")
                or self.api.info.get_dividends(id=ids, start=self.config.start, end=self.config.end)
            )
        return self._dividends

    @property
    def ter(self) -> Optional[dict]:
        if self._ter is None:
            ids = [i.id for i in self.instrument_objects if i.type == InstrumentType.ETP]
            base = self.api.info.get_ter(id=ids) or {}
            override = self._overrides.get("ter") or {}
            merged = {**base, **override}  # override vince sui conflitti
            if self.config.ter_scale != 1.0 and merged:
                merged = {k: v * self.config.ter_scale for k, v in merged.items()}
            self._ter = merged
        return self._ter

    @property
    def repo(self) -> Optional[pd.DataFrame]:
        if self._repo is None:
            ccys = [i.currency.value for i in self.instrument_objects if i.type == InstrumentType.FUTURE]
            ids  = [i.id            for i in self.instrument_objects if i.type == InstrumentType.FUTURE]
            self._repo = self.api.market.get_daily_repo_rates(
                self.config.start, self.config.end, currencies=ccys, ids=ids
            )
        return self._repo

    def get_instruments(self, mode: Literal["dict", "list"] = "dict"):
        if mode == "dict":
            return {i.id: i for i in self.instrument_objects}
        return self.instrument_objects

    # ============================================================
    # EAGER LOAD
    # ============================================================

    def load_all(self) -> "DataPipeline":
        """Scarica tutti i dataset in sequenza. Ritorna self per chaining."""
        _ = self.prices
        _ = self.fx_composition
        _ = self.fx_forward_composition
        _ = self.fx_prices
        _ = self.fx_forward_prices
        _ = self.dividends
        _ = self.ter
        _ = self.ytm
        _ = self.repo
        return self

    # ============================================================
    # UTILITIES
    # ============================================================

    def with_date_range(
        self,
        start: Union[str, date, datetime],
        end:   Union[str, date, datetime],
    ) -> "DataPipeline":
        """
        Ritorna una nuova pipeline *non caricata* con un range di date diverso.

        Config e override sono preservati; tutti i dati lazy sono azzerati.
        Chiama ``.load_all()`` sul risultato.

        Esempio::

            full_history = pipeline.with_date_range("2018-01-01", "2024-01-01")
            full_history.load_all()
        """
        other = copy.copy(self)
        other.config = copy.copy(self.config)
        other.config.start = start
        other.config.end   = end
        other._overrides = dict(self._overrides)
        # Azzera tutti i cache lazy
        for attr in ("_prices", "_fx_composition", "_fx_forward_composition",
                     "_fx_prices", "_fx_forward_prices", "_dividends",
                     "_ter", "_repo", "_ytm"):
            setattr(other, attr, None)
        return other

    # ============================================================
    # PRIVATE FETCHERS
    # ============================================================

    def _fetch_prices(self) -> pd.DataFrame:
        if self.config.frequency.lower() in ("daily", "1d"):
            return self._fetch_daily_prices()
        return self._fetch_intraday_prices()

    def _fetch_daily_prices(self) -> pd.DataFrame:
        prices = []
        for typ, instruments in self.instruments_by_type.items():
            match typ:
                case InstrumentType.ETP | InstrumentType.FUTURE:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.config.start,
                        end=self.config.end,
                        fields="mid",
                        market="EUREX" if typ == InstrumentType.FUTURE else self.config.etf_market,
                        snapshot_time=self.config.snapshot_time,
                        source=self.config.etf_source,
                        fallbacks=self.config.etf_fallbacks or [{"source": "bloomberg"}],
                    ))
                case InstrumentType.CDXINDEX | InstrumentType.STOCK | InstrumentType.SWAP:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.config.start,
                        end=self.config.end,
                        fields="mid",
                        snapshot_time=self.config.snapshot_time,
                        source="bloomberg",
                    ))
                case InstrumentType.INDEX:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.config.start,
                        end=self.config.end,
                        fields="px_last",
                        source="bloomberg",
                    ))
        return pd.concat(prices, axis=1)

    def _fetch_intraday_prices(self) -> pd.DataFrame:
        prices = []
        for typ, instruments in self.instruments_by_type.items():
            match typ:
                case InstrumentType.ETP | InstrumentType.FUTURE:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.config.start,
                        end=self.config.end,
                        start_time=self.config.intraday_start_time,
                        end_time=self.config.intraday_end_time,
                        frequency=self.config.frequency,
                        fields="mid",
                        market="EUREX" if typ == InstrumentType.FUTURE else self.config.etf_market,
                        source=self.config.etf_source,
                    ))
                case InstrumentType.CDXINDEX | InstrumentType.STOCK | InstrumentType.SWAP:
                    raise ValueError("Bloomberg intraday downloads disabled.")
                case InstrumentType.INDEX:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.config.start,
                        end=self.config.end,
                        fields="px_last",
                        source="bloomberg",
                    ))
        return pd.concat(prices, axis=1)

    def _fetch_fx_composition(self) -> Optional[pd.DataFrame]:
        try:
            ids = [i.id for i in self.instruments_by_type[InstrumentType.ETP]]
            return self.api.info.get_fx_composition(id=ids, fx_fxfwrd="fx")
        except Exception as e:
            logger.warning("FX composition fetch failed: %s", e)
        return None

    def _fetch_fx_forward_composition(self) -> Optional[pd.DataFrame]:
        try:
            ids = [i.id for i in self.instruments_by_type[InstrumentType.ETP]]
            return self.api.info.get_fx_composition(id=ids, fx_fxfwrd="fxfwrd")
        except Exception as e:
            logger.warning("FX forward composition fetch failed: %s", e)
        return None

    def _fetch_ytm(self) -> pd.DataFrame:
        ytm = []
        for typ, instruments in self.instruments_by_type.items():
            if typ == InstrumentType.ETP:
                fixed_income = [i for i in instruments
                                if getattr(i, "underlying_type", None) in ("FIXED INCOME", "MONEY MARKET")]
                if fixed_income:
                    ytm.append(self.api.info.get_etf_ytm(
                        id=[f.id for f in fixed_income],
                        source="timescale",
                        start=self.config.start,
                        end=self.config.end,
                    ))
            if typ == InstrumentType.FUTURE:
                fixed_income = [i for i in instruments
                                if getattr(i, "future_underlying", None) in ("FIXED INCOME", "MONEY MARKET")]
                if fixed_income:
                    ytm.append(self.api.info.get_future_ytm(
                        id=[i.id for i in fixed_income],
                        start=self.config.start,
                        end=self.config.end,
                    ))
        return pd.concat(ytm, axis=1) if ytm else pd.DataFrame()

    def _fetch_fx_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        pairs = [f"{self.config.base_currency}{ccy}" for ccy in currencies]
        if self.config.frequency.lower() in ("daily", "1d"):
            return self.api.market.get_daily_currency(
                id=pairs,
                start=self.config.start,
                end=self.config.end,
                snapshot_time=self.config.effective_fx_snapshot_time,
                source=self.config.fx_source,
                fallbacks=[{"source": "bloomberg"}],
            )
        return self.api.market.get(
            type=InstrumentType.CURRENCYPAIR,
            id=pairs,
            start=self.config.start,
            end=self.config.end,
            start_time=self.config.intraday_start_time,
            end_time=self.config.intraday_end_time,
            frequency=self.config.frequency,
            fields="mid",
            source=self.config.fx_source,
        )

    def _fetch_fx_forward_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        return self.api.market.get_daily_fx_forward(
            quoted_currency=currencies,
            start=self.config.start,
            end=self.config.end,
            tenor=self.config.fx_forward_tenor,
            source=self.config.fx_forward_source,
        )

    def _currencies_from(self, composition: Optional[pd.DataFrame]) -> List[str]:
        if composition is None:
            return []
        base = self.config.base_currency
        for isin in self.instrument_ids:
            if isin in composition.index:
                return [c for c in composition.columns.tolist() if c != base]
        return [c for c in composition.index.tolist() if c != base]

    @staticmethod
    def _to_multiindex(df: Optional[pd.DataFrame], field: str) -> Optional[pd.DataFrame]:
        """Wrappa colonne flat in MultiIndex (field, id). No-op se già MultiIndex o None."""
        if df is None or isinstance(df.columns, pd.MultiIndex):
            return df
        df = df.copy()
        df.columns = pd.MultiIndex.from_tuples(
            [(field, c) for c in df.columns], names=["field", "id"]
        )
        return df

    # ============================================================

    def __repr__(self) -> str:
        loaded = [
            name for name in ("prices", "fx_composition", "fx_forward_composition",
                               "fx_prices", "fx_forward_prices", "dividends", "ter", "ytm")
            if getattr(self, f"_{name}") is not None
        ]
        status = ", ".join(loaded) if loaded else "nothing loaded"
        return (
            f"DataPipeline(\n"
            f"  instruments = {len(self.instrument_ids)}\n"
            f"  period      = {self.config.start} → {self.config.end}\n"
            f"  frequency   = {self.config.frequency}\n"
            f"  loaded      = [{status}]\n"
            f")"
        )
