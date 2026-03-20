"""
FX Spot exposure adjustment component.

Adjusts returns for currency exposure, accounting for trading currency.
Formula: fx_correction = I·(fx_return[ccy] × weight[ccy]) - fx_return[trading_ccy]

Updatable Fields: fx_prices
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.common import normalize_fx_columns, add_time_tag
from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)


class FxSpotComponent(Component):

    LOWER_SANITY_CHECK = -0.1
    UPPER_SANITY_CHECK = 1.1

    def __init__(self, fx_composition: pd.DataFrame, fx_prices: pd.DataFrame, target: Optional[List[str]] = None):
        super().__init__(target)
        self.fx_composition = fx_composition.fillna(0.0).copy()
        self._fx_prices = normalize_fx_columns(fx_prices)
        self._adjustments_cache: Optional[pd.DataFrame] = None
        self._fx_returns_cache: Optional[pd.DataFrame] = None

        for instrument_id in self.fx_composition.index:
            total = self.fx_composition.loc[instrument_id].sum()
            if not (self.LOWER_SANITY_CHECK <= total <= self.UPPER_SANITY_CHECK):
                logger.warning(f"{instrument_id} composition sums to {total:.2%}")

            eur_weight = 1.0 - total
            if abs(eur_weight) > 0.001 and 'EUR' in self.fx_composition.columns:
                self.fx_composition.loc[instrument_id, 'EUR'] += eur_weight

    def is_updatable(self) -> bool:
        return True

    @property
    def updatable_fields(self) -> set[str]:
        return {"fx_prices"}

    def append_data(self, timestamp: pd.Timestamp, *, fx_prices: Optional[pd.Series] = None, **kwargs) -> None:
        if fx_prices is None:
            return

        fx_prices = add_time_tag(fx_prices, timestamp)

        self._fx_prices = pd.concat([self._fx_prices, normalize_fx_columns(fx_prices)]).sort_index()
        self._fx_prices = self._fx_prices[~self._fx_prices.index.duplicated(keep='last')]

        if self._fx_returns_cache is not None and len(self._fx_returns_cache) > 0:
            self._fx_returns_cache = self._fx_returns_cache.iloc[:-1]

        if self._adjustments_cache is not None and len(self._adjustments_cache) > 0:
            self._adjustments_cache = self._adjustments_cache.iloc[:-1]

    def save_state(self) -> dict:
        # Save only lengths + last rows to avoid copying full DataFrames.
        # apply_temp_data always appends one row to _fx_prices and drops the
        # last row of each cache; calculate_adjustment may then extend them.
        # Restoring by truncating to saved length + reinserting last row is sufficient.
        fx_ret_last = (
            self._fx_returns_cache.iloc[[-1]].copy()
            if self._fx_returns_cache is not None and not self._fx_returns_cache.empty
            else None
        )
        adj_last = (
            self._adjustments_cache.iloc[[-1]].copy()
            if self._adjustments_cache is not None and not self._adjustments_cache.empty
            else None
        )
        return {
            'fx_prices_len': len(self._fx_prices),
            'fx_returns_cache_len': len(self._fx_returns_cache) if self._fx_returns_cache is not None else None,
            'fx_returns_cache_last': fx_ret_last,
            'adjustments_cache_len': len(self._adjustments_cache) if self._adjustments_cache is not None else None,
            'adjustments_cache_last': adj_last,
        }

    def restore_state(self, state: dict) -> None:
        self._fx_prices = self._fx_prices.iloc[:state['fx_prices_len']]

        fx_len = state['fx_returns_cache_len']
        if fx_len is None:
            self._fx_returns_cache = None
        elif fx_len == 0:
            self._fx_returns_cache = self._fx_returns_cache.iloc[:0]
        else:
            self._fx_returns_cache = pd.concat([
                self._fx_returns_cache.iloc[:fx_len - 1],
                state['fx_returns_cache_last'],
            ])

        adj_len = state['adjustments_cache_len']
        if adj_len is None:
            self._adjustments_cache = None
        elif adj_len == 0:
            self._adjustments_cache = self._adjustments_cache.iloc[:0]
        else:
            self._adjustments_cache = pd.concat([
                self._adjustments_cache.iloc[:adj_len - 1],
                state['adjustments_cache_last'],
            ])

    def apply_temp_data(self, *, timestamp: pd.Timestamp, fx_prices: Optional[pd.DataFrame | pd.Series] = None, **kwargs) -> None:
        if fx_prices is None:
            return

        if isinstance(fx_prices, pd.Series):
            fx_prices = fx_prices.to_frame(timestamp).T

        new_fx_prices = normalize_fx_columns(fx_prices)
        self._fx_prices = pd.concat([self._fx_prices, new_fx_prices]).drop_duplicates().sort_index()

        if self._fx_returns_cache is not None and len(self._fx_returns_cache) > 0:
            self._fx_returns_cache = self._fx_returns_cache.iloc[:-1]

        if self._adjustments_cache is not None and len(self._adjustments_cache) > 0:
            self._adjustments_cache = self._adjustments_cache.iloc[:-1]

    def is_applicable(self, instrument: Instrument) -> bool:
        if instrument.type not in [InstrumentType.STOCK, InstrumentType.ETP]:
            return False
        return instrument.id in self.fx_composition.index

    def calculate_adjustment(
        self,
        instruments: dict[str, Instrument],
        dates: Union[List[date], List[datetime]],
        **kwargs,
    ) -> pd.DataFrame:

        self.validate_input(instruments, dates)
        dates_dt = self._normalize_dates(dates)
        instrument_ids = list(instruments.keys())

        applicable_ids = [inst.id for inst in instruments.values()
                          if self.should_apply(inst) and inst.id in self.fx_composition.index]

        if not applicable_ids:
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        if self._adjustments_cache is not None:
            cached_dates = set(self._adjustments_cache.index)
            missing_dates = [d for d in dates_dt if d not in cached_dates]
            first_idx_to_calc = self._adjustments_cache.index[-1]
        else:
            missing_dates = dates_dt
            first_idx_to_calc = self._fx_prices.index[0]

        if not missing_dates:
            return self._adjustments_cache.loc[dates_dt, instrument_ids].copy()

        fx_returns = self.return_calculator.calculate_returns(self._fx_prices.loc[first_idx_to_calc:]).iloc[1:]
        fx_returns = fx_returns.where(fx_returns.notna(), 0.0)

        common_dates = fx_returns.index.intersection(missing_dates)
        if len(common_dates) == 0:
            logger.error(f"No date overlap: FX {fx_returns.index.min()} to {fx_returns.index.max()}, Requested {missing_dates[0]} to {missing_dates[-1]}")
            result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
            self.validate_output(result)
            return result

        comp_matrix = self.fx_composition.loc[applicable_ids]
        common_currencies = comp_matrix.columns.intersection(fx_returns.columns)

        if len(common_currencies) == 0:
            logger.error(f"No currency overlap")
            result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
            self.validate_output(result)
            return result

        comp_matrix = comp_matrix[common_currencies]
        fx_ret_matrix = fx_returns.loc[common_dates, common_currencies]
        weighted_fx = comp_matrix @ fx_ret_matrix.T
        result_applicable = weighted_fx.T

        for inst_id in applicable_ids:
            trading_ccy = str(instruments[inst_id].currency)
            if trading_ccy != 'EUR' and trading_ccy in fx_returns.columns:
                result_applicable[inst_id] = result_applicable[inst_id] - fx_returns[trading_ccy]

        new_adjustments = pd.DataFrame(0.0, index=missing_dates, columns=instrument_ids, dtype='float64')
        for inst_id in applicable_ids:
            new_adjustments.loc[common_dates, inst_id] = result_applicable[inst_id].astype('float64')

        if self._adjustments_cache is None:
            self._adjustments_cache = new_adjustments
        else:
            self._adjustments_cache = pd.concat([self._adjustments_cache, new_adjustments]).sort_index()

        result = self._adjustments_cache.loc[dates_dt, instrument_ids].copy()
        self.validate_output(result)
        return result