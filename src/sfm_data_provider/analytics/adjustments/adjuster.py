"""
Adjuster: orchestrate components, manage state, delegate cache to components.
"""
from contextlib import contextmanager
from datetime import date, datetime
from typing import Dict, Optional, Literal, Union
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.common import add_time_tag
from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.analytics.adjustments.return_calculations import ReturnCalculator
from sfm_data_provider.core.instruments.instrument_factory import InstrumentFactory
from sfm_data_provider.core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)
pd.set_option('future.no_silent_downcasting', True)


class Adjuster:
    """
    Orchestrate components, manage prices, delegate caching to components.

    Usage:
        adjuster = Adjuster(prices).add(TerComponent()).add(FxSpotComponent())
        adjuster.append_update(prices=new_prices, fx_prices=new_fx)
        adjustments = adjuster.calculate()

        # Live (temporary, no storage)
        live_adj = adjuster.live_update(prices=live_prices, fx_prices=live_fx)
        clean_rets = adjuster.live_update(prices=live_prices, fx_prices=live_fx).clean_returns()
    """
    MAX_SIGNIFICANT_DIGITS = 8

    def __init__(
            self,
            prices: pd.DataFrame,
            instruments: Optional[Dict[str, Instrument]] = None,
            is_intraday: bool = True,
            return_type: Literal["percentage", "logarithmic", "absolute"] = "percentage",
    ):

        self.intraday = is_intraday
        self.return_calculator = ReturnCalculator(return_type)
        self._prices = self._validate_and_transpose(prices)

        self.instrument_ids = self._prices.columns.tolist()
        self.instruments = instruments if instruments is not None else self._fetch_instruments_from_factory()
        self._validate_instruments()

        self.components: list[Component] = []
        self._subscriptions: dict[str, list[Component]] = {}

        logger.info(f"Adjuster: {len(self.instrument_ids)} instruments, {len(self._prices)} dates")

    @property
    def prices(self) -> pd.DataFrame:
        return self._prices

    def _prepare_new_prices(self, prices: pd.DataFrame) -> pd.DataFrame:
        prices = self._validate_and_transpose(prices)
        return prices.reindex(columns=self.instrument_ids, fill_value=None)

    def _validate_and_transpose(self, prices: pd.DataFrame) -> pd.DataFrame:

        if not isinstance(prices, pd.DataFrame):
            raise TypeError(f"element must be DataFrame, got {type(prices)}")
        if prices.empty:
            raise ValueError(f"element is empty")
        needs_transpose = isinstance(prices.columns, pd.DatetimeIndex) or (
                isinstance(prices.index[0], str) and isinstance(prices.columns[0], (date, pd.Timestamp))
        )
        if needs_transpose:
            prices = prices.T
        if not isinstance(prices.index, pd.DatetimeIndex):
            prices.index = pd.to_datetime(prices.index)
        prices.index = prices.index.normalize() if not self.intraday else prices.index
        return prices

    def _fetch_instruments_from_factory(self) -> Dict[str, Instrument]:
        factory = InstrumentFactory()
        return factory.get_many(self.instrument_ids)

    def _validate_instruments(self):
        missing = set(self.instrument_ids) - set(self.instruments.keys())
        if missing:
            raise ValueError(f"Missing instruments: {missing}")

    def add(self, component: Component) -> 'Adjuster':
        component.set_return_calculator(self.return_calculator)
        component.is_intraday = self.intraday
        self.components.append(component)
        if component.is_updatable():
            for field in component.updatable_fields:
                self._subscriptions.setdefault(field, []).append(component)
        return self

    def calculate_adjustment(self, dates: Optional[list] = None) -> pd.DataFrame:
        dates = dates or self._prices.index.tolist()
        if not self.intraday:
            dates = [d.date() if isinstance(d, (datetime, pd.Timestamp)) else d for d in dates]

        adjustments = pd.DataFrame(0.0, index=dates, columns=self.instrument_ids)
        for component in self.components:
            try:
                adjustments += component.calculate_adjustment(
                    instruments=self.instruments,
                    dates=dates
                ).fillna(0)
            except Exception as e:
                logger.error(f"{component.__class__.__name__} failed: {e}", exc_info=True)
        return adjustments.round(self.MAX_SIGNIFICANT_DIGITS)

    def get_clean_returns(self, dates: Optional[list] = None, cumulative: bool = False) -> pd.DataFrame:
        """
        Calculate clean returns (raw returns + adjustments).

        Args:
            cumulative: bool used to indicate if return cumulation is needed
            dates: Optional list of dates to calculate returns for

        Returns:
            DataFrame with clean returns (cumulated or not)
        """
        raw_returns = self.return_calculator.calculate_returns(self._prices)
        adjustments = self.calculate_adjustment(dates).iloc[::-1]
        if dates is not None:
            raw_returns = raw_returns.loc[dates]
        cleaned = raw_returns.add(adjustments.reindex(raw_returns.index, columns=raw_returns.columns), fill_value=0.0)
        if cumulative:
            cleaned = self.return_calculator.accumulate_returns_forward(cleaned)
        return cleaned

    def append_update(self, timestamp: pd.Timestamp = None,
                      prices: Optional[pd.Series] = None,
                      **component_data) -> 'Adjuster':

        timestamp = pd.Timestamp.now() if self.intraday else pd.Timestamp.now().normalize()
        if prices is not None:
            new_prices = self._prepare_new_prices(
                add_time_tag(prices, timestamp)
            )
            self._prices = (pd.concat([self._prices, new_prices]).
                            drop_duplicates().
                            sort_index())

        self._update_components(timestamp, component_data, temp=False)
        return self

    @contextmanager
    def live_update(self, prices: Optional[pd.DataFrame | pd.Series] = None, **component_data):
        """
        Context manager for temporary updates without persisting changes.
        Automatically restores state on exit.

        Usage:
            with adjuster.live_update(prices=live_prices, fx_prices=live_fx):
                live_adj = adjuster.calculate()
                live_rets = adjuster.clean_returns()

        Args:
            prices: Live prices (Series or DataFrame)
            **component_data: Component-specific data to update

        Yields:
            Self (Adjuster) to allow method calls within the context
        """
        timestamp = pd.Timestamp.now() if self.intraday else pd.Timestamp.now().normalize()
        prices = add_time_tag(prices, timestamp)
        # Save current state
        snapshot = (self._prices.copy(), {id(c): c.save_state() for c in self.components if c.is_updatable()})

        try:
            if prices is not None: # Apply temporary updates
                prices = self._prepare_new_prices(prices)
                self._prices = pd.concat([self._prices, prices]).drop_duplicates().sort_index()

            self._update_components(timestamp, component_data, temp=True)

            yield self

        finally:
            # Restore state
            self._prices, component_states = snapshot
            for comp in self.components:
                if id(comp) in component_states:
                    comp.restore_state(component_states[id(comp)])

    def _update_components(self, timestamp: pd.Timestamp, component_data: dict, temp: bool = False):
        if not component_data:
            return
        method = 'apply_temp_data' if temp else 'append_data'
        for field, data in component_data.items():
            if field not in self._subscriptions:
                continue
            for component in self._subscriptions[field]:
                try:
                    getattr(component, method)(**{field: data}, timestamp=timestamp)
                except Exception as e:
                    logger.error(f"{component.__class__.__name__} update failed: {e}", exc_info=True)

    def get_breakdown(self, ticker: Optional[str] = None) -> Union[dict[str, pd.DataFrame], pd.DataFrame]:
        """Get adjustments by component. Returns dict or single-ticker DataFrame."""
        dates = self._prices.index.tolist()

        if not self.intraday:
            dates = [d.date() if isinstance(d, (datetime, pd.Timestamp)) else d for d in dates]

        bdwn = {c.__class__.__name__: c.calculate_adjustment(self.instruments, dates) for c in self.components}
        return pd.concat([df[ticker] for df in bdwn.values()], axis=1, keys=bdwn.keys()) if ticker else bdwn

    def __repr__(self) -> str:
        return (f"Adjuster(instruments={len(self.instrument_ids)},"
                f" dates={len(self._prices)},"
                f" components={len(self.components)})")
