"""
EtfDataLoading — layer di caricamento dati per ETF.

Carica lazily ogni dataset al primo accesso (prezzi, FX, dividendi, TER).
Non costruisce l'Adjuster: quella responsabilità rimane a EtfDataPipeline.

Typical usage:
    loader = EtfDataLoading(api, instruments=["IE00B4L5Y983"], start="2024-01-01", end="2024-06-01")

    prices        = loader.prices           # daily ETF prices
    fx_comp       = loader.fx_composition   # FX exposure weights
    fx_px         = loader.fx_prices        # spot FX rates
    divs          = loader.dividends
    ter           = loader.ter

    # Or load everything at once:
    loader.load_all()

    # Overrides (invalidate lazy cache):
    loader.override_ter({"IE00B4L5Y983": 0.002}).override_fx_composition(my_df)
"""

import logging
from datetime import date, datetime, time
from typing import Optional, Union, List

import pandas as pd

logger = logging.getLogger(__name__)


class EtfDataLoading:
    """
    Layer di data loading per ETF. Ogni dataset è lazy: viene scaricato
    solo quando acceduto per la prima volta (o dopo un override).
    """

    def __init__(
        self,
        api,
        instruments: Union[List[str], List],
        start: Union[str, date, datetime],
        end: Union[str, date, datetime],
        frequency: str = "daily",
        snapshot_time: time = time(17, 0),
        intraday_start_time: time = time(9, 0),
        intraday_end_time: time = time(17, 30),
        fx_forward_tenor: str = "1M",
        etf_source: str = "timescale",
        fx_source: str = "timescale",
        fx_forward_source: str = "bloomberg",
        etf_fallbacks: Optional[List[dict]] = None,
    ):
        self.api = api
        self.start = start
        self.end = end
        self.frequency = frequency
        self.snapshot_time = snapshot_time
        self.intraday_start_time = intraday_start_time
        self.intraday_end_time = intraday_end_time
        self.fx_forward_tenor = fx_forward_tenor
        self.etf_source = etf_source
        self.fx_source = fx_source
        self.fx_forward_source = fx_forward_source
        self.etf_fallbacks = etf_fallbacks

        if instruments and hasattr(instruments[0], "id"):
            self.instrument_objects = instruments
            self.instrument_ids = [inst.id for inst in instruments]
        else:
            self.instrument_ids = list(instruments)
            self.instrument_objects = None

        # Overrides (impostati prima dell'accesso alle property)
        self._override_ter: Optional[dict] = None
        self._override_fx_composition: Optional[pd.DataFrame] = None
        self._override_fx_forward_composition: Optional[pd.DataFrame] = None
        self._override_dividends: Optional[pd.DataFrame] = None
        self._override_fx_prices: Optional[pd.DataFrame] = None
        self._override_fx_forward_prices: Optional[pd.DataFrame] = None

        # Lazy cache interno
        self._prices: Optional[pd.DataFrame] = None
        self._fx_composition: Optional[pd.DataFrame] = None
        self._fx_forward_composition: Optional[pd.DataFrame] = None
        self._fx_prices: Optional[pd.DataFrame] = None
        self._fx_forward_prices: Optional[pd.DataFrame] = None
        self._dividends: Optional[pd.DataFrame] = None
        self._ter: Optional[dict] = None

    # ============================================================
    # OVERRIDE (chainable, invalidano il dato lazy corrispondente)
    # ============================================================

    def override_ter(self, ter: dict) -> "EtfDataLoading":
        self._override_ter = ter
        self._ter = None
        return self

    def override_fx_composition(self, df: pd.DataFrame) -> "EtfDataLoading":
        self._override_fx_composition = df
        self._fx_composition = None
        self._fx_prices = None  # le valute necessarie possono cambiare
        return self

    def override_fx_forward_composition(self, df: pd.DataFrame) -> "EtfDataLoading":
        self._override_fx_forward_composition = df
        self._fx_forward_composition = None
        self._fx_forward_prices = None
        return self

    def override_dividends(self, df: pd.DataFrame) -> "EtfDataLoading":
        self._override_dividends = df
        self._dividends = None
        return self

    def override_fx_prices(self, df: pd.DataFrame) -> "EtfDataLoading":
        self._override_fx_prices = df
        self._fx_prices = None
        return self

    def override_fx_forward_prices(self, df: pd.DataFrame) -> "EtfDataLoading":
        self._override_fx_forward_prices = df
        self._fx_forward_prices = None
        return self

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
            self._fx_composition = self._override_fx_composition or self._fetch_fx_composition()
        return self._fx_composition

    @property
    def fx_forward_composition(self) -> Optional[pd.DataFrame]:
        if self._fx_forward_composition is None:
            self._fx_forward_composition = (
                self._override_fx_forward_composition or self._fetch_fx_forward_composition()
            )
        return self._fx_forward_composition

    @property
    def fx_prices(self) -> Optional[pd.DataFrame]:
        if self._fx_prices is None:
            self._fx_prices = self._override_fx_prices or self._fetch_fx_prices(
                self._currencies_from(self.fx_composition)
            )
        return self._fx_prices

    @property
    def fx_forward_prices(self) -> Optional[pd.DataFrame]:
        if self._fx_forward_prices is None:
            self._fx_forward_prices = self._override_fx_forward_prices or self._fetch_fx_forward_prices(
                self._currencies_from(self.fx_forward_composition)
            )
        return self._fx_forward_prices

    @property
    def dividends(self) -> Optional[pd.DataFrame]:
        if self._dividends is None:
            self._dividends = self._override_dividends or self.api.info.get_dividends(
                id=self.instrument_ids, start=self.start, end=self.end
            )
        return self._dividends

    @property
    def ter(self) -> Optional[dict]:
        if self._ter is None:
            base = self.api.info.get_ter(id=self.instrument_ids) or {}
            if self._override_ter:
                base.update(self._override_ter)
            self._ter = base
        return self._ter

    # ============================================================
    # EAGER LOAD
    # ============================================================

    def load_all(self) -> "EtfDataLoading":
        """Scarica tutti i dataset in sequenza. Ritorna self per chaining."""
        _ = self.prices
        _ = self.fx_composition
        _ = self.fx_forward_composition
        _ = self.fx_prices
        _ = self.fx_forward_prices
        _ = self.dividends
        _ = self.ter
        return self

    # ============================================================
    # PRIVATE FETCHERS
    # ============================================================

    def _fetch_prices(self) -> pd.DataFrame:
        if self.frequency.lower() in ("daily", "1d"):
            return self.api.market.get_daily_etf(
                id=self.instrument_ids,
                start=self.start,
                end=self.end,
                snapshot_time=self.snapshot_time,
                source=self.etf_source,
                fallbacks=self.etf_fallbacks,
            )
        return self._fetch_intraday_prices()

    def _fetch_intraday_prices(self) -> pd.DataFrame:
        dates = pd.date_range(
            pd.to_datetime(self.start).date(),
            pd.to_datetime(self.end).date(),
            freq="B",
        )
        frames = []
        for d in dates:
            try:
                df = self.api.market.get_intraday_etf(
                    date=d.date(),
                    id=self.instrument_ids,
                    frequency=self.frequency,
                    start_time=self.intraday_start_time,
                    end_time=self.intraday_end_time,
                    source=self.etf_source,
                )
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as e:
                logger.warning(f"Intraday load failed {d.date()}: {e}")
        if not frames:
            raise ValueError("No intraday prices loaded")
        return pd.concat(frames).sort_index()

    def _fetch_fx_composition(self) -> Optional[pd.DataFrame]:
        try:
            data = self.api.info.get_currency_exposure(id=self.instrument_ids)
            if data is not None:
                return data.pivot(index="index", columns="CURRENCY", values="WEIGHT").fillna(0)
        except Exception as e:
            logger.warning(f"FX composition fetch failed: {e}")
        return None

    def _fetch_fx_forward_composition(self) -> Optional[pd.DataFrame]:
        try:
            data = self.api.info.get_currency_exposure(id=self.instrument_ids)
            if data is not None:
                return data.pivot(
                    index="index", columns="CURRENCY", values="WEIGHT_FX_FORWARD"
                ).fillna(0)
        except Exception as e:
            logger.warning(f"FX forward composition fetch failed: {e}")
        return None

    def _fetch_fx_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        return self.api.market.get_daily_currency(
            id=[f"EUR{ccy}" for ccy in currencies],
            start=self.start,
            end=self.end,
            source=self.fx_source,
        )

    def _fetch_fx_forward_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        return self.api.market.get_daily_fx_forward(
            quoted_currency=currencies,
            start=self.start,
            end=self.end,
            tenor=self.fx_forward_tenor,
            source=self.fx_forward_source,
        )

    @staticmethod
    def _currencies_from(composition: Optional[pd.DataFrame]) -> List[str]:
        if composition is None:
            return []
        return [c for c in composition.columns.tolist() if c != "EUR"]

    # ============================================================

    def __repr__(self) -> str:
        loaded = []
        for name in ("prices", "fx_composition", "fx_forward_composition",
                     "fx_prices", "fx_forward_prices", "dividends", "ter"):
            if getattr(self, f"_{name}") is not None:
                loaded.append(name)
        status = ", ".join(loaded) if loaded else "nothing loaded yet"
        return (
            f"EtfDataLoading(n={len(self.instrument_ids)}, "
            f"freq={self.frequency}, {self.start}→{self.end}, [{status}])"
        )
