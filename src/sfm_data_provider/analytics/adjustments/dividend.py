"""
Dividend adjustment component.

Formula: dividend_normalized = (dividend × fx_fund) / (price × fx_trading)
Dividends treated as occurring at midnight (date boundary).
Uses last cum-dividend price before midnight for normalization.
"""
from datetime import date, datetime
from typing import Union, List, Optional
from functools import cached_property
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)


class DividendComponent(Component):

    def __init__(self, dividends: pd.DataFrame,
                 instrument_prices: pd.DataFrame,
                 fx_prices: pd.DataFrame,
                 target: Optional[List[str]] = None):
        super().__init__(target)
        self.fx_prices = fx_prices
        self.prices = instrument_prices
        self.dividends_raw = dividends.fillna(0.0)
        self._instruments_cache = None

    def is_applicable(self, instrument: Instrument) -> bool:
        if instrument.id not in self.dividends_raw.columns:
            return False

        if instrument.type == InstrumentType.STOCK:
            return True

        if instrument.type == InstrumentType.ETP:
            return not isinstance(instrument, Instrument) or (
                    instrument.payment_policy is None
                    or instrument.payment_policy in {"DIST", "INC"}
            )

        return False

    @cached_property
    def _normalized_dividends(self) -> dict[str, dict[pd.Timestamp, float]]:
        """Cache normalized dividends per instrument (div_timestamp -> adjustment)"""
        if self._instruments_cache is None:
            return {}

        cache = {}
        for inst in self._instruments_cache.values():
            if not self.should_apply(inst):
                continue

            divs = self.dividends_raw[inst.id]
            div_dates = divs[divs != 0].index
            if len(div_dates) == 0:
                continue

            fund_ccy = self._get_currency(inst, 'fund_currency')
            trading_ccy = self._get_currency(inst, 'currency')
            adjustments = {}

            prices_tz = getattr(self.prices.index, 'tz', None)
            inst_in_prices = inst.id in self.prices.columns

            for d in div_dates:
                div_dt = pd.Timestamp(d)
                if prices_tz is not None:
                    if div_dt.tz is None:
                        div_dt = div_dt.tz_localize(prices_tz)
                    else:
                        div_dt = div_dt.tz_convert(prices_tz)

                div_amt = divs.loc[d]
                if pd.isna(div_amt) or div_amt == 0:
                    continue

                if not inst_in_prices:
                    continue

                # Use last price before dividend (midnight event) - O(log N) via searchsorted
                idx = self.prices.index.searchsorted(div_dt, side='left') - 1
                if idx < 0:
                    continue

                last_cum_timestamp = self.prices.index[idx]
                price = self.prices.loc[last_cum_timestamp, inst.id]
                if pd.isna(price) or price <= 0:
                    continue

                div_eur = self._convert_to_eur(div_amt, fund_ccy, last_cum_timestamp, self.fx_prices)
                price_eur = self._convert_to_eur(price, trading_ccy, last_cum_timestamp, self.fx_prices)
                if div_eur is not None and price_eur is not None and price_eur > 0:
                    adjustments[div_dt] = div_eur / price_eur

            cache[inst.id] = adjustments

        return cache

    def calculate_adjustment(
        self,
        instruments: dict[str, Instrument],
        dates: Union[List[date], List[datetime]],
        **kwargs
    ) -> pd.DataFrame:
        if self._instruments_cache is None:
            self._instruments_cache = instruments

        dates_dt = self._normalize_dates(dates)
        result = pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))

        is_intraday = dates_dt and any(d.hour != 0 or d.minute != 0 for d in dates_dt)

        if is_intraday:
            dates_arr = pd.DatetimeIndex(dates_dt)
            dates_tz = dates_arr.tz
        else:
            dates_set = set(dates_dt)

        for inst_id, adjustments in self._normalized_dividends.items():
            if inst_id not in result.columns:
                continue

            if is_intraday:
                # Intraday: use searchsorted to find the interval in O(log N)
                for div_dt, adjustment in adjustments.items():
                    ts = pd.Timestamp(div_dt)
                    if dates_tz is not None:
                        ts = ts.tz_localize(dates_tz) if ts.tz is None else ts.tz_convert(dates_tz)
                    idx = dates_arr.searchsorted(ts, side='left')
                    if 0 < idx < len(dates_arr):
                        result.loc[dates_dt[idx], inst_id] += adjustment
            else:
                # Daily: apply at midnight (normalized date)
                for div_dt, adjustment in adjustments.items():
                    div_date = div_dt.normalize()
                    if div_date in dates_set:
                        result.loc[div_date, inst_id] = adjustment

        return result

    def _get_currency(self, inst: Instrument, attr: str) -> str:
        if attr == 'fund_currency' and isinstance(inst, Instrument):
            if inst.fund_currency:
                return self._to_str(inst.fund_currency)

        return self._to_str(inst.currency)

    def _convert_to_eur(
        self,
        amount: float,
        ccy: str,
        timestamp: datetime,
        fx_prices: pd.DataFrame
    ) -> Optional[float]:
        ccy = self._to_str(ccy)

        if ccy == 'EUR':
            return amount

        col = next((c for c in fx_prices.columns if str(c).upper() == ccy or str(c).upper() == f"EUR{ccy}"), None)

        if col is None:
            return None

        if timestamp not in fx_prices.index:
            return None

        fx = fx_prices.loc[timestamp, col]

        if pd.isna(fx):
            return None
        if fx <= 0:
            return None

        return amount * fx

    @staticmethod
    def _to_str(obj) -> str:
        return str(getattr(obj, 'value', obj)).upper()
