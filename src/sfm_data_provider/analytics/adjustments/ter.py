"""
TER (Total Expense Ratio) component for ETF adjustments.

Formula: adjustment = -ter_annual × year_fraction
TER applied as event at midnight (date boundary).
"""
from datetime import date, datetime
from typing import Union, List, Optional
from functools import cached_property

import numpy as np
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.analytics.adjustments.common import calculate_year_fractions
from sfm_data_provider.analytics.adjustments.return_calculations import ReturnType
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)


class TerComponent(Component):

    LOWER_SANITY_CHECK = 0.0
    UPPER_SANITY_CHECK = 0.01

    def __init__(
        self,
        ters: dict[str, float] | pd.Series | pd.DataFrame,
        target: Optional[List[str]] = None,
    ):
        super().__init__(target)

        if isinstance(ters, pd.DataFrame):
            ters = ters.iloc[:, 0]

        if isinstance(ters, pd.Series):
            ters = ters.to_dict()

        self.ters = {}

        for instrument_id, ter in ters.items():
            if not isinstance(ter, (int, float)):
                raise TypeError(f"TER for {instrument_id} must be numeric, got {type(ter)}")

            if ter > self.UPPER_SANITY_CHECK:
                logger.warning(f"TER for {instrument_id}: {ter*100:.2f}% seems high. Dividing by 100")
                ter = ter / 100.0

            if ter <= self.LOWER_SANITY_CHECK:
                logger.warning(f"TER for {instrument_id} is {ter*100:.4f}%")

            self.ters[instrument_id] = float(ter)

        self._instruments_cache = None
        self._dates_cache = None

    def is_applicable(self, instrument: Instrument) -> bool:
        return instrument.type == InstrumentType.ETP and instrument.id in self.ters

    @cached_property
    def _ter_adjustments(self) -> dict[str, dict[pd.Timestamp, float]]:
        """Cache TER adjustments per instrument (midnight_timestamp -> adjustment)"""
        if self._instruments_cache is None or self._dates_cache is None:
            return {}

        cache = {}
        unique_dates = pd.Index(self._dates_cache).normalize().unique()
        year_fractions = calculate_year_fractions(unique_dates, shifted=False)
        yf_dict = dict(zip(unique_dates, year_fractions))

        for inst in self._instruments_cache.values():
            if not self.should_apply(inst):
                continue

            ter = self.ters[inst.id]
            adjustments = {}

            if self.return_calculator.return_type == ReturnType.LOGARITHMIC:
                ter = np.log(1 + ter)

            for midnight_date in unique_dates:
                midnight_ts = pd.Timestamp(midnight_date)
                yf = yf_dict[midnight_date]
                adjustments[midnight_ts] = -ter * yf

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
        if self._dates_cache is None:
            self._dates_cache = self._normalize_dates(dates)

        dates_dt = self._normalize_dates(dates)
        result = pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))

        is_intraday = dates_dt and any(d.hour != 0 or d.minute != 0 for d in dates_dt)

        for inst_id, adjustments in self._ter_adjustments.items():
            if inst_id not in result.columns:
                continue

            if is_intraday:
                # Intraday: find interval containing midnight
                for midnight_ts, adjustment in adjustments.items():
                    if hasattr(dates_dt, 'tz') and dates_dt.tz is not None:
                        if midnight_ts.tz is None:
                            midnight_ts = midnight_ts.tz_localize(dates_dt.tz)
                        else:
                            midnight_ts = midnight_ts.tz_convert(dates_dt.tz)

                    for i in range(1, len(dates_dt)):
                        t1_ts = pd.Timestamp(dates_dt[i - 1])
                        t2_ts = pd.Timestamp(dates_dt[i])
                        if t1_ts < midnight_ts <= t2_ts:
                            result.loc[dates_dt[i], inst_id] = adjustment
                            break
            else:
                # Daily: apply at midnight (normalized date)
                for midnight_ts, adjustment in adjustments.items():
                    midnight_date = midnight_ts.normalize()
                    if midnight_date in dates_dt:
                        result.loc[midnight_date, inst_id] = adjustment

        return result