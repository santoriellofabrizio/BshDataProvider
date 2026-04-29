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
from collections import defaultdict
from datetime import date, datetime, time
from typing import Optional, Union, List, Literal

import pandas as pd

from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument
from sfm_data_provider.interface.bshdata import BshData

logger = logging.getLogger(__name__)


class DataPipeline:
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
        snapshot_time: time = time(17),
        fx_forward_tenor: str = "1M",
        etf_source: str = "timescale",
        fx_source: str = "timescale",
        fx_forward_source: str = "bloomberg",
        etf_fallbacks: Optional[List[dict]] = None,
    ):

        self.api: BshData = api
        self.start = start
        self.end = end
        self.frequency = frequency
        self.snapshot_time = snapshot_time
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
            self.instrument_objects = self.api.market.build_instruments(self.instrument_ids, autocomplete=True)

        self.instruments_by_type = defaultdict(list)
        for inst in self.instrument_objects:
            self.instruments_by_type[inst.type].append(inst)

        # Overrides (impostati prima dell'accesso alle property)
        self._override_ter: Optional[dict] = None
        self._override_fx_composition: Optional[pd.DataFrame] = None
        self._override_fx_forward_composition: Optional[pd.DataFrame] = None
        self._override_dividends: Optional[pd.DataFrame] = None
        self._override_fx_prices: Optional[pd.DataFrame] = None
        self._override_fx_forward_prices: Optional[pd.DataFrame] = None
        self._override_ytm: Optional[pd.DataFrame] = None

        # Lazy cache interno
        self._prices: Optional[pd.DataFrame] = None
        self._fx_composition: Optional[pd.DataFrame] = None
        self._fx_forward_composition: Optional[pd.DataFrame] = None
        self._fx_prices: Optional[pd.DataFrame] = None
        self._fx_forward_prices: Optional[pd.DataFrame] = None
        self._dividends: Optional[pd.DataFrame] = None
        self._ter: Optional[dict] = None
        self._repo: Optional[pd.DataFrame] = None
        self._ytm: Optional[pd.DataFrame] = None

    # ============================================================
    # OVERRIDE (chainable, invalidano il dato lazy corrispondente)
    # ============================================================

    def override_ter(self, ter: dict) -> "DataPipeline":
        self._override_ter = ter
        self._ter = None
        return self

    def override_ytm(self, ytm: pd.DataFrame) -> "DataPipeline":
        self._override_ytm = ytm
        self._ytm = None
        return self

    def override_fx_composition(self, df: pd.DataFrame) -> "DataPipeline":
        self._override_fx_composition = df
        self._fx_composition = None
        self._fx_prices = None  # le valute necessarie possono cambiare
        return self

    def override_fx_forward_composition(self, df: pd.DataFrame) -> "DataPipeline":
        self._override_fx_forward_composition = df
        self._fx_forward_composition = None
        self._fx_forward_prices = None
        return self

    def override_dividends(self, df: pd.DataFrame) -> "DataPipeline":
        self._override_dividends = df
        self._dividends = None
        return self

    def override_fx_prices(self, df: pd.DataFrame) -> "DataPipeline":
        self._override_fx_prices = df
        self._fx_prices = None
        return self

    def override_fx_forward_prices(self, df: pd.DataFrame) -> "DataPipeline":
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
            raw = self._override_fx_prices or self._fetch_fx_prices(
                self._currencies_from(self.fx_composition)
            )
            if raw is None:
                raw = pd.DataFrame(1, columns=['EUR'], index=self.prices.index)
            if isinstance(raw, pd.Series): raw = raw.to_frame()
            self._fx_prices = raw
        return self._fx_prices

    @property
    def fx_forward_prices(self) -> Optional[pd.DataFrame]:
        if self._fx_forward_prices is None:
            raw = self._override_fx_forward_prices or self._fetch_fx_forward_prices(
                self._currencies_from(self.fx_forward_composition)
            )
            if isinstance(raw, pd.Series): raw = raw.to_frame()
            self._fx_forward_prices = raw
        return self._fx_forward_prices

    @property
    def ytm(self) -> Optional[pd.DataFrame]:
        if self._ytm is None:
            raw = self._override_ytm or self._fetch_ytm(
            )
            self._ytm = raw
        return self._ytm

    @property
    def dividends(self) -> Optional[pd.DataFrame]:
        if self._dividends is None:
            ids = [i.id for i in self.instrument_objects if i.type in (InstrumentType.ETP, InstrumentType.STOCK)]
            self._dividends = self._override_dividends or self.api.info.get_dividends(
                id=ids, start=self.start, end=self.end
            )
        return self._dividends

    @property
    def ter(self) -> Optional[pd.Series]:
        if self._ter is None:
            ids = [i.id for i in self.instrument_objects if i.type == InstrumentType.ETP]
            base = self.api.info.get_ter(id=ids)
            if self._override_ter:
                base.update(self._override_ter)
            self._ter = base
        return self._ter

    @property
    def repo(self) -> Optional[pd.DataFrame]:
        if self._repo is None:
            ccys = [i.currency.value for i in self.instrument_objects if i.type == InstrumentType.FUTURE]
            ids = [i.id for i in self.instrument_objects if i.type == InstrumentType.FUTURE]
            self._repo = self.api.market.get_daily_repo_rates(self.start, self.end, currencies=ccys, ids=ids)
        return self._repo

    def get_instruments(self, mode: Literal['dict','list'] = 'dict'):
        if mode == 'dict':
            return {i.id: i for i in self.instrument_objects}
        else:
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
    # PRIVATE FETCHERS
    # ============================================================

    def _fetch_prices(self) -> pd.DataFrame:
        if self.frequency.lower() in ("daily", "1d"):
            return self._fetch_daily_prices()
        return self._fetch_intraday_prices()

    def _fetch_daily_prices(self) -> pd.DataFrame:
        prices = []
        for type, instruments in self.instruments_by_type.items():
            match type:
                case InstrumentType.ETP | InstrumentType.FUTURE:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.start,
                        end=self.end,
                        fields='mid',
                        market="EUREX" if type == InstrumentType.FUTURE else "EURONEXT",
                        snapshot_time=self.snapshot_time,
                        source='timescale',
                        fallbacks=[{"source": "bloomberg"}]
                    ))
                case InstrumentType.CDXINDEX | InstrumentType.STOCK | InstrumentType.SWAP:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.start,
                        end=self.end,
                        fields='mid',
                        snapshot_time=self.snapshot_time,
                        source='bloomberg'
                    ))
                case InstrumentType.INDEX:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.start,
                        end=self.end,
                        fields='px_last',
                        source='bloomberg'))
        return pd.concat(prices, axis=1)

    def _fetch_intraday_prices(self) -> pd.DataFrame:

        prices = []
        for type, instruments in self.instruments_by_type.items():
            match type:
                case InstrumentType.ETP | InstrumentType.FUTURE:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.start,
                        end=self.end,
                        start_time=time(9,30),
                        end_time=time(17,),
                        frequency=self.frequency,
                        fields='mid',
                        market="EUREX" if type == InstrumentType.FUTURE else "EURONEXT",
                        source='timescale',
                    ))
                case InstrumentType.CDXINDEX | InstrumentType.STOCK | InstrumentType.SWAP:
                    raise ValueError("avoid stupid blooomberg downloads!")
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.start,
                        end=self.end,
                        fields='mid',
                        frequency=self.frequency,
                        source='bloomberg'
                    ))
                case InstrumentType.INDEX:
                    prices.append(self.api.market.get(
                        instruments=instruments,
                        start=self.start,
                        end=self.end,
                        fields='px_last',
                        source='bloomberg'))
                    #todo oversample
        return pd.concat(prices, axis=1)

    def _fetch_fx_composition(self) -> Optional[pd.DataFrame]:
        try:
            ids = [i.id for i in self.instruments_by_type[InstrumentType.ETP]]
            data = self.api.info.get_fx_composition(id=ids, fx_fxfwrd='fx')
            if data is not None:
                return data
        except Exception as e:
            logger.warning(f"FX composition fetch failed: {e}")
        return None

    def _fetch_fx_forward_composition(self) -> Optional[pd.DataFrame]:
        try:
            ids = [i.id for i in self.instruments_by_type[InstrumentType.ETP]]
            data = self.api.info.get_fx_composition(id=ids, fx_fxfwrd='fxfwrd')
            if data is not None:
                return data
        except Exception as e:
            logger.warning(f"FX forward composition fetch failed: {e}")
        return None

    def _fetch_ytm(self):

        fixed_income = [i for i in self.instrument_objects
                        if getattr(i, 'underlying_type', None) in ["FIXED INCOME", "MONEY MARKET"]]

        if fixed_income:
            return self.api.info.get_etp_fields(
                fields='ytm', instruments=fixed_income, source="timescale",
                start=self.start, end=self.end,
            )
        return None

    def _fetch_fx_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        return self.api.market.get_daily_currency(
            id=[f"EUR{ccy}" for ccy in currencies],
            start=self.start,
            end=self.end,
            source=self.fx_source,
            fallbacks=[{'source': 'bloomberg'}],
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

    def _currencies_from(self, composition: Optional[pd.DataFrame]) -> List[str]:
        if composition is None:
            return []
        for i in self.instrument_ids:
            if i in composition.index:
                return [c for c in composition.columns.tolist() if c != "EUR"]
        return [c for c in composition.index.tolist() if c != "EUR"]

    @staticmethod
    def _to_multiindex(df: Optional[pd.DataFrame], field: str) -> Optional[pd.DataFrame]:
        """Wrappa colonne flat in MultiIndex (field, id). No-op se già MultiIndex o None."""
        if df is None or isinstance(df.columns, pd.MultiIndex):
            return df
        df = df.copy()
        df.columns = pd.MultiIndex.from_tuples(
            [(field, c) for c in df.columns],
            names=["field", "id"],
        )
        return df

    # ============================================================

    def __repr__(self) -> str:
        loaded = []
        for name in ("prices", "fx_composition", "fx_forward_composition",
                     "fx_prices", "fx_forward_prices", "dividends", "ter", "ytm"):
            if getattr(self, f"_{name}") is not None:
                loaded.append(name)
        status = ", ".join(loaded) if loaded else "nothing loaded yet"
        return (
            f"EtfDataLoading(n={len(self.instrument_ids)}, "
            f"freq={self.frequency}, {self.start}→{self.end}, [{status}])"
        )
