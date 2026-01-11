"""
FX Forward carry adjustment component.

Formula: rate_diff = (fwd - spot) / spot
carry_adjustment = Σ(rate_diff[ccy] × weight[ccy] × year_fraction) - rate_diff[trading_ccy] × year_fraction
Carry applied as midnight event.
"""
from datetime import date, datetime
from typing import Union, List, Optional
from functools import cached_property
import pandas as pd
import numpy as np
import logging
import re

from analytics.adjustments.common import calculate_year_fractions, add_time_tag
from analytics.adjustments.component import Component
from core.enums.instrument_types import InstrumentType
from core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)


class FxForwardCarryComponent(Component):

    MAX_REASONABLE_RATE_DIFF = 0.15
    TENOR_MONTHS = {'1M': 1, '2M': 2, '3M': 3, '6M': 6, '9M': 9, '1Y': 12, '12M': 12}

    def __init__(
        self,
        fwd_composition: pd.DataFrame,
        fx_forward_prices: pd.DataFrame,
        tenor: str,
        fx_spot_prices: pd.DataFrame,
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        super().__init__(target)
        self.fwd_composition = fwd_composition.fillna(0.0)
        self.tenor = tenor
        self.settlement_days = settlement_days

        fx_fwd_normalized = self._normalize_fx_columns(fx_forward_prices, "forward")
        self.fx_forward_prices = self._transform_fx_forward_prices(fx_fwd_normalized, tenor)
        self.fx_spot_prices = self._normalize_fx_columns(fx_spot_prices, "spot")

        self._instruments_cache = None
        self._dates_cache = None

    def is_applicable(self, instrument: Instrument) -> bool:
        if instrument.type != InstrumentType.ETP:
            return False
        if instrument.id not in self.fwd_composition.index:
            return False
        if hasattr(instrument, 'currency_hedged'):
            is_hedged = instrument.currency_hedged
            if is_hedged is None:
                is_hedged = False
            if is_hedged:
                return False
        return True

    @cached_property
    def _carry_adjustments(self) -> dict[str, dict[pd.Timestamp, float]]:
        """Cache carry adjustments per instrument (midnight_timestamp -> adjustment)"""
        if self._instruments_cache is None or self._dates_cache is None:
            return {}

        rate_diffs = self._calculate_rate_differentials(self.fx_forward_prices, self.fx_spot_prices, self.tenor)
        if rate_diffs.empty:
            return {}

        applicable_ids = [
            inst.id for inst in self._instruments_cache.values()
            if self.should_apply(inst) and inst.id in self.fwd_composition.index
        ]
        if not applicable_ids:
            return {}

        unique_dates = pd.Index(self._dates_cache).normalize().unique()
        common_dates = rate_diffs.index.intersection(unique_dates)
        if len(common_dates) == 0:
            return {}

        year_fractions = calculate_year_fractions(unique_dates, shifted=True, settlement_days=self.settlement_days)
        yf_dict = dict(zip(unique_dates, year_fractions))

        comp_matrix = self.fwd_composition.loc[applicable_ids]
        common_currencies = comp_matrix.columns.intersection(rate_diffs.columns)
        if len(common_currencies) == 0:
            return {}

        comp_matrix = comp_matrix[common_currencies]
        rate_diffs_common = rate_diffs.loc[common_dates, common_currencies]

        cache = {}
        for inst_id in applicable_ids:
            adjustments = {}
            for midnight_date in common_dates:
                midnight_ts = pd.Timestamp(midnight_date)
                yf = yf_dict[midnight_date]
                rate_diffs_date = rate_diffs_common.loc[midnight_date]

                weighted_rate = (comp_matrix.loc[inst_id] * rate_diffs_date).sum()

                trading_ccy = str(self._instruments_cache[inst_id].currency)
                if trading_ccy != 'EUR' and trading_ccy in rate_diffs_date.index:
                    weighted_rate -= rate_diffs_date[trading_ccy]

                adjustments[midnight_ts] = -weighted_rate * yf

            cache[inst_id] = adjustments

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

        for inst_id, adjustments in self._carry_adjustments.items():
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

    def _transform_fx_forward_prices(self, fx_fwd_prices: pd.DataFrame, tenor: str) -> pd.DataFrame:
        tenor_upper = tenor.upper().strip()

        if tenor_upper not in self.TENOR_MONTHS:
            logger.warning(f"Unknown tenor '{tenor}'. Assuming 1M")
            months = 1
        else:
            months = self.TENOR_MONTHS[tenor_upper]

        annualization_factor = 12 / months
        fx_fwd_annualized = fx_fwd_prices * annualization_factor
        fx_fwd_decimal = fx_fwd_annualized / 10000.0

        all_values = pd.to_numeric(fx_fwd_decimal.values.flatten(), errors='coerce')
        all_values = all_values[~np.isnan(all_values) & (all_values != 0)]

        if len(all_values) > 0:
            max_val = np.max(np.abs(all_values))
            if max_val > 0.15:
                logger.warning(f"Max rate {max_val:.4f} seems high")

        return fx_fwd_decimal

    @staticmethod
    def _normalize_fx_columns(fx_prices: pd.DataFrame, label: str = "prices") -> pd.DataFrame:
        normalized_columns = {}
        columns_to_invert = []

        if isinstance(fx_prices, pd.Series):
            fx_prices = fx_prices.to_frame()

        for col in fx_prices.columns:
            col_str = str(col).upper().strip()
            parts = re.split(r'[\s_]+', col_str)
            base_ticker = parts[0]

            if len(base_ticker) == 3:
                normalized_columns[col] = base_ticker
                continue

            if len(base_ticker) == 6 and base_ticker.startswith('EUR'):
                currency = base_ticker[-3:]
                normalized_columns[col] = currency
                continue

            if len(base_ticker) == 6 and base_ticker.endswith('EUR'):
                currency = base_ticker[:3]
                normalized_columns[col] = currency
                columns_to_invert.append(col)
                logger.warning(f"'{col}' is inverted. Inverting prices -> {currency}")
                continue

            logger.warning(f"'{col}' doesn't match expected format. Keeping as-is")
            normalized_columns[col] = col

        fx_normalized = fx_prices.copy()

        for col in columns_to_invert:
            fx_normalized[col] = 1.0 / fx_normalized[col]
            fx_normalized[col].replace([np.inf, -np.inf], np.nan, inplace=True)

        fx_normalized = fx_normalized.rename(columns=normalized_columns)
        duplicates = fx_normalized.columns[fx_normalized.columns.duplicated()].tolist()

        if duplicates:
            logger.warning(f"Duplicate currency codes: {duplicates}")
            fx_normalized = fx_normalized.loc[:, ~fx_normalized.columns.duplicated()]

        return fx_normalized

    def _calculate_rate_differentials(
        self,
        fx_fwd_prices: pd.DataFrame,
        fx_spot_prices: pd.DataFrame,
        tenor: str,
    ) -> pd.DataFrame:
        common_dates = fx_fwd_prices.index.intersection(fx_spot_prices.index)
        common_currencies = fx_fwd_prices.columns.intersection(fx_spot_prices.columns)

        if len(common_dates) == 0:
            logger.error(f"No date overlap for tenor {tenor}")
            return pd.DataFrame()

        if len(common_currencies) == 0:
            logger.error(f"No currency overlap for tenor {tenor}")
            return pd.DataFrame()

        fwd_aligned = fx_fwd_prices.loc[common_dates, common_currencies]
        spot_aligned = fx_spot_prices.loc[common_dates, common_currencies]
        rate_diffs = fwd_aligned / spot_aligned

        max_abs_diff = rate_diffs.abs().max().max()
        if np.isnan(max_abs_diff) or np.isinf(max_abs_diff):
            logger.warning("Rate differentials contain NaN or Inf")
        elif max_abs_diff > self.MAX_REASONABLE_RATE_DIFF:
            logger.warning(f"Rate differentials exceed {self.MAX_REASONABLE_RATE_DIFF:.1%} (max: {max_abs_diff:.1%})")

        return rate_diffs

    def is_updatable(self) -> bool:
        return True

    @property
    def updatable_fields(self) -> set[str]:
        return {"fx_forward_prices", "fx_prices"}

    def append_data(
        self,
        timestamp: pd.Timestamp,
        *,
        fx_forward_prices: Optional[pd.Series] = None,
        fx_prices: Optional[pd.Series] = None
    ) -> None:
        if fx_forward_prices is not None:
            fx_forward_prices = add_time_tag(fx_forward_prices, timestamp)

            new_fx_fwd = self._normalize_fx_columns(fx_forward_prices, "forward")
            new_fx_fwd_transformed = self._transform_fx_forward_prices(new_fx_fwd, self.tenor)

            self.fx_forward_prices = pd.concat([self.fx_forward_prices, new_fx_fwd_transformed]).sort_index()
            self.fx_forward_prices = self.fx_forward_prices[~self.fx_forward_prices.index.duplicated(keep='last')]

        if fx_prices is not None:
            fx_prices = add_time_tag(fx_prices, timestamp)
            new_fx_spot = self._normalize_fx_columns(fx_prices, "spot")
            self.fx_spot_prices = pd.concat([self.fx_spot_prices, new_fx_spot]).sort_index()
            self.fx_spot_prices = self.fx_spot_prices[~self.fx_spot_prices.index.duplicated(keep='last')]

    def save_state(self) -> dict:
        return {
            'fx_forward_prices': self.fx_forward_prices.copy(),
            'fx_spot_prices': self.fx_spot_prices.copy()
        }

    def restore_state(self, state: dict) -> None:
        self.fx_forward_prices = state['fx_forward_prices'].copy()
        self.fx_spot_prices = state['fx_spot_prices'].copy()