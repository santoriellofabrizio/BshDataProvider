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

from sfm_data_provider.analytics.adjustments.common import calculate_year_fractions, add_time_tag
from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument

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

        # Vectorized: (n_inst × n_ccy) @ (n_ccy × n_dates) -> (n_inst × n_dates)
        comp_np = comp_matrix.values
        rates_np = rate_diffs_common.values  # (n_dates, n_ccy)
        weighted_np = comp_np @ rates_np.T   # (n_inst, n_dates)

        ccy_index = {c: i for i, c in enumerate(common_currencies)}
        yf_array = np.array([yf_dict[d] for d in common_dates])

        for i, inst_id in enumerate(applicable_ids):
            trading_ccy = str(self._instruments_cache[inst_id].currency)
            if trading_ccy != 'EUR' and trading_ccy in ccy_index:
                weighted_np[i] -= rates_np[:, ccy_index[trading_ccy]]

        adjustments_np = -weighted_np * yf_array  # (n_inst, n_dates)

        cache = {
            inst_id: {
                pd.Timestamp(d): float(adjustments_np[i, j])
                for j, d in enumerate(common_dates)
            }
            for i, inst_id in enumerate(applicable_ids)
        }

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
        is_intraday = dates_dt and any(d.hour != 0 or d.minute != 0 for d in dates_dt)

        if is_intraday:
            # Fast path: searchsorted maps each midnight to its enclosing interval in O(n log n),
            # then a single vectorised assignment fills all instruments at once.
            # All instruments share the same midnight key ordering (built from common_dates),
            # so values_matrix[:, j] is the column vector for midnight j across all instruments.
            inst_list = list(instruments.keys())
            inst_to_idx = {iid: i for i, iid in enumerate(inst_list)}
            result_arr = np.zeros((len(dates_dt), len(inst_list)), dtype=float)

            if self._carry_adjustments:
                dates_ns = pd.DatetimeIndex(dates_dt).asi8  # int64 ns, computed once

                # All instruments share the same midnight keys — use first as sample
                sample_adj = next(iter(self._carry_adjustments.values()))
                midnight_ns = pd.DatetimeIndex(list(sample_adj.keys())).asi8

                # searchsorted(side='left') returns i s.t. dates[i-1] < midnight <= dates[i]
                raw_idx = np.searchsorted(dates_ns, midnight_ns, side='left')
                valid = (raw_idx > 0) & (raw_idx < len(dates_dt))
                valid_rows = raw_idx[valid]
                valid_idxs = np.where(valid)[0]

                if valid_rows.size > 0:
                    applicable_ids = [iid for iid in self._carry_adjustments
                                      if inst_to_idx.get(iid, -1) >= 0]
                    if applicable_ids:
                        col_arr = np.array([inst_to_idx[iid] for iid in applicable_ids])
                        values_matrix = np.array(
                            [list(self._carry_adjustments[iid].values()) for iid in applicable_ids],
                            dtype=float,
                        )  # shape: (n_applicable, n_midnight)
                        for k, mid_idx in enumerate(valid_idxs):
                            result_arr[valid_rows[k], col_arr] = values_matrix[:, mid_idx]

            result = pd.DataFrame(result_arr, index=dates_dt, columns=inst_list)
        else:
            # Daily: build a numpy array by positional index — avoids all pandas
            # column-label assignment overhead (~0.3ms × 800 cols = 240ms).
            inst_list = list(instruments.keys())
            inst_to_idx = {inst_id: i for i, inst_id in enumerate(inst_list)}
            dates_ts_list = list(pd.DatetimeIndex(dates_dt))
            result_arr = np.zeros((len(dates_ts_list), len(inst_list)), dtype=float)
            for inst_id, adjustments in self._carry_adjustments.items():
                col_idx = inst_to_idx.get(inst_id, -1)
                if col_idx < 0:
                    continue
                for k, ts in enumerate(dates_ts_list):
                    result_arr[k, col_idx] = adjustments.get(ts, 0.0)
            result = pd.DataFrame(result_arr, index=dates_dt, columns=inst_list)

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