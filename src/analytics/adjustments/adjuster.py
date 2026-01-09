"""
Adjuster orchestrator for return adjustments.
"""
from contextlib import contextmanager
from datetime import date, datetime
from typing import Dict, Optional, Union, Literal

import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol
from analytics.adjustments.return_calculations import ReturnCalculator
from core.instruments.instrument_factory import InstrumentFactory
from core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)
pd.set_option('future.no_silent_downcasting', True)


class Adjuster:
    """
    Orchestrates adjustment calculations with efficient incremental updates.

    Update Modes:
    -------------
    1. Append Mode: Permanently store new data and calculate incrementally
       - Use: append_update(prices=..., fx_prices_intraday=..., recalc_last_n=1)
       - Stores: New prices + new adjustments in cache
       - Calculates: Only new dates (plus recalc_last_n previous dates)

    2. Live Update Mode: Temporary calculation without storage
       - Use: live_update(prices=..., fx_prices_intraday=...)
       - Stores: Nothing (all temporary)
       - Calculates: New dates only, then discards
       - Returns: Calculated adjustments

    Usage:
    ------
        # Setup
        ter = TerComponent(ter_data)
        fx_spot = FxSpotComponent(fx_comp, fx_prices_intraday)
        intraday_adjuster = Adjuster(prices).add(ter).add(fx_spot)

        # Append mode: Store new end-of-day data
        intraday_adjuster.append_update(
            prices=new_eod_prices,
            fx_prices_intraday=new_fx_prices,
            recalc_last_n=1
        )
        adjustments = intraday_adjuster.calculate()

        # Live mode: Intraday updates without storage
        live_adj = intraday_adjuster.live_update(
            prices=live_prices,
            fx_prices_intraday=live_fx_prices
        )
    """

    # Class constants
    MAX_SIGNIFICANT_DIGITS = 8
    VALID_FILL_METHODS = frozenset({'ffill', 'bfill', 'time', 'linear', None})
    VALID_RETURN_TYPES = frozenset({'percentage', 'logarithmic', 'absolute'})

    def __init__(
            self,
            prices: pd.DataFrame,
            instruments: Optional[Dict[str, InstrumentProtocol]] = None,
            intraday: bool = True,
            fill_method: Optional[Literal['ffill', 'bfill', 'time', 'linear']] = None,
            return_type: Literal["percentage", "logarithmic", "absolute"] = "percentage",
    ):
        """
        Initialize intraday_adjuster.

        Args:
            prices: DataFrame with instrument prices (dates × instruments)
            instruments: Optional dict[instrument_id -> Instrument]. If None, fetches from InstrumentFactory
            intraday: If True, preserve intraday timestamps; if False, normalize to dates
            fill_method: How to handle NaN values ('ffill', 'bfill', 'linear', 'time', or None)
            return_type: Type of returns ("percentage", "logarithmic", "absolute")
        """
        # Validate inputs
        if fill_method not in self.VALID_FILL_METHODS:
            raise ValueError(f"fill_method must be one of {self.VALID_FILL_METHODS}, got '{fill_method}'")
        if return_type not in self.VALID_RETURN_TYPES:
            raise ValueError(f"return_type must be one of {self.VALID_RETURN_TYPES}, got '{return_type}'")

        self.intraday = intraday
        self.return_calculator = ReturnCalculator(return_type)

        # Validate and normalize prices
        self._prices = self._validate_and_transpose(prices, "prices")

        # Handle missing values (bug fix: was using 'prices' instead of 'self._prices')
        if fill_method is not None:
            self._prices = self._apply_fill_method(self._prices, fill_method)

        # Normalize dates if not intraday
        if not self.intraday:
            self._prices.index = self._prices.index.normalize()

        # Extract instrument IDs
        self.instrument_ids = self._prices.columns.tolist()

        # Get instruments
        self.instruments = instruments if instruments is not None else self._fetch_instruments_from_factory()
        self._validate_instruments()

        # Component registry
        self.components: list[Component] = []
        self._subscriptions: dict[str, list[Component]] = {}

        # Adjustments cache
        self._adjustments: Optional[pd.DataFrame] = None

        logger.info(
            f"Adjuster initialized: {len(self.instrument_ids)} instruments, "
            f"{len(self._prices)} {'timestamps' if self.intraday else 'dates'}"
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def prices(self) -> pd.DataFrame:
        """Current prices DataFrame"""
        return self._prices

    # =========================================================================
    # Helper Methods (extracted to reduce duplication)
    # =========================================================================

    def _ensure_dataframe(self, data: Optional[pd.DataFrame | pd.Series]) -> Optional[pd.DataFrame]:
        """Convert Series to single-row DataFrame with current timestamp."""
        if data is None:
            return None
        if isinstance(data, pd.Series):
            ts = pd.Timestamp.now() if self.intraday else pd.Timestamp.now().normalize()
            return data.to_frame(ts).T
        return data

    def _normalize_dates_for_calc(self, dates: Optional[list]) -> list:
        """Normalize dates based on intraday setting."""
        if dates is None:
            return self._prices.index.tolist()
        if not self.intraday:
            return [d.date() if isinstance(d, (datetime, pd.Timestamp)) else d for d in dates]
        return dates

    def _prepare_new_prices(self, prices: pd.DataFrame) -> pd.DataFrame:
        """Validate, normalize index, and align columns for new prices."""

        if isinstance(prices, pd.Series):
            ts = self._get_current_timestamp()
            prices = prices.to_frame(ts).T

        prices = self._validate_and_transpose(prices, "prices")

        # Handle non-temporal index
        if not isinstance(prices.index, pd.DatetimeIndex):
            ts = self._get_current_timestamp()
            prices.index = pd.DatetimeIndex([ts])
            logger.debug(f"Non-temporal index detected, using timestamp={ts}")
        elif not self.intraday:
            prices.index = prices.index.normalize()

        # Align columns
        return prices.reindex(columns=self.instrument_ids, fill_value=None)

    def _get_current_timestamp(self) -> pd.Timestamp:
        """Get current timestamp, normalized if not intraday."""
        ts = pd.Timestamp.now()
        self._last_update_timestamp = ts if self.intraday else ts.normalize()
        return self._last_update_timestamp

    # =========================================================================
    # Validation Methods
    # =========================================================================

    @staticmethod
    def _validate_and_transpose(df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Validate DataFrame and transpose if needed (dates should be index)."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{name} must be DataFrame, got {type(df)}")
        if df.empty:
            raise ValueError(f"{name} is empty")

        # Check if needs transposing
        needs_transpose = (
                isinstance(df.columns, pd.DatetimeIndex) or
                (isinstance(df.index[0], str) and isinstance(df.columns[0], (date, pd.Timestamp)))
        )

        if needs_transpose:
            df = df.T
            logger.debug(f"{name}: Transposed to {df.shape}")

        # Convert index to DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                raise ValueError(f"{name} index must be dates. Error: {e}")

        return df

    @staticmethod
    def _apply_fill_method(prices: pd.DataFrame, fill_method: str) -> pd.DataFrame:
        """Apply fill method to handle missing values."""
        try:
            if fill_method in ('ffill', 'bfill'):
                result = getattr(prices, fill_method)()
            else:
                result = prices.interpolate(method=fill_method)

            remaining_nans = result.isna().sum().sum()
            logger.info(f"Handled NaNs via '{fill_method}' ({remaining_nans} remaining)")
            return result
        except Exception as e:
            raise ValueError(f"Invalid fill_method: '{fill_method}'") from e

    def _fetch_instruments_from_factory(self) -> Dict[str, Instrument]:
        """Fetch instruments from InstrumentFactory."""
        logger.debug(f"Fetching {len(self.instrument_ids)} instruments...")
        factory = InstrumentFactory()
        instruments = factory.get_many(self.instrument_ids)
        logger.info(f"Fetched {len(instruments)} instruments")
        return instruments

    def _validate_instruments(self):
        """Validate all required instruments are present."""
        missing = set(self.instrument_ids) - set(self.instruments.keys())
        if missing:
            raise ValueError(f"Missing instruments: {missing}")

    # =========================================================================
    # Component Management
    # =========================================================================

    def add(self, component: Component) -> 'Adjuster':
        """Add component and register its subscriptions."""
        component.set_return_calculator(self.return_calculator)
        component.is_intraday = self.intraday

        self.components.append(component)

        # Register subscriptions for updatable components
        if component.is_updatable():
            for field in component.updatable_fields:
                self._subscriptions.setdefault(field, []).append(component)
                logger.debug(f"{component.__class__.__name__} subscribed to '{field}'")

        logger.debug(f"Added: {component.__class__.__name__}")
        return self

    # =========================================================================
    # Calculation Methods
    # =========================================================================

    def calculate(self, dates: Optional[list] = None) -> pd.DataFrame:
        """Calculate adjustments using cache when possible."""
        calc_dates = self._normalize_dates_for_calc(dates)

        # Find dates not in cache
        if self._adjustments is None:
            missing_dates = calc_dates
        else:
            cached_dates = set(self._adjustments.index)
            missing_dates = [d for d in calc_dates if d not in cached_dates]

        # Calculate missing dates
        if missing_dates:
            cache_hits = len(calc_dates) - len(missing_dates)
            logger.debug(f"Calculating {len(missing_dates)} missing dates (cache hit: {cache_hits})")

            new_adj = self._calculate_for_dates(missing_dates)

            if self._adjustments is None:
                self._adjustments = new_adj
            else:
                self._adjustments = pd.concat([self._adjustments, new_adj]).sort_index()
        else:
            logger.debug(f"All {len(calc_dates)} dates in cache (100% hit rate)")

        return self._adjustments.loc[calc_dates].copy()

    def _calculate_for_dates(self, dates: list) -> pd.DataFrame:
        """Calculate adjustments for specific dates only."""
        # Early exit
        if not dates:
            return pd.DataFrame(columns=self.instrument_ids)

        adjustments = pd.DataFrame(0.0, index=dates, columns=self.instrument_ids)

        if not self.components:
            return adjustments

        for component in self.components:
            comp_name = component.__class__.__name__
            logger.debug(f"Calculating {comp_name} for {len(dates)} dates...")

            try:
                component_adj = component.calculate_adjustment(
                    instruments=self.instruments,
                    dates=dates,
                    prices=self._prices,
                ).fillna(0)
                adjustments += component_adj
            except Exception as e:
                logger.error(f"{comp_name} failed: {e}", exc_info=True)

        return adjustments.round(self.MAX_SIGNIFICANT_DIGITS)

    # =========================================================================
    # Returns Calculation
    # =========================================================================

    def clean_returns(
            self,
            dates: Optional[list] = None,
            cumulative: bool = False,
            live_prices: Optional[pd.DataFrame | pd.Series] = None,
            **live_component_data,
    ) -> pd.DataFrame:
        """
        Calculate clean returns by applying adjustments to raw returns.

        Args:
            dates: Optional subset of dates
            cumulative: If True, return forward cumulative returns
            live_prices: Optional live prices for temporary calculation
            **live_component_data: Optional live component data (fx_prices_intraday, etc.)

        Returns:
            DataFrame(dates × instruments) with clean returns
        """
        # Live mode
        if live_prices is not None or live_component_data:
            live_prices = self._prepare_new_prices(live_prices)
            with self._live_context(prices=live_prices, **live_component_data):
                return self._compute_clean_returns(dates, cumulative)

        return self._compute_clean_returns(dates, cumulative)

    def get_raw_returns(self,
                        cumulative: bool = False,
                        live_prices: Optional[pd.DataFrame | pd.Series] = None,
                        **live_component_data,
                        ) -> pd.DataFrame | None:

        if live_prices is not None or live_component_data:
            live_prices = self._prepare_new_prices(live_prices)
            with self._live_context(prices=live_prices, **live_component_data):
                rets = self.return_calculator.calculate_returns(self._prices)
                return (1 + rets).iloc[::-1].cumprod().iloc[::-1] - 1 if cumulative else rets

        rets = self.return_calculator.calculate_returns(self._prices)
        return (1 + rets).iloc[::-1].cumprod().iloc[::-1] - 1 if cumulative else rets

    def _compute_clean_returns(self, dates: Optional[list], cumulative: bool) -> pd.DataFrame:
        """Core clean returns calculation logic."""
        raw_returns = self.return_calculator.calculate_returns(self._prices)
        adjustments = self.calculate(dates)

        if dates is not None:
            raw_returns = raw_returns.loc[dates]

        cleaned = raw_returns.add(
            adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
            fill_value=0.0
        )

        if cumulative:
            cleaned = (1 + cleaned).iloc[::-1].cumprod().iloc[::-1] - 1

        return cleaned

    def clean_prices(
            self,
            backpropagate: bool = False,
            rebase: bool = False,
            dates: Optional[list] = None
    ) -> pd.DataFrame:
        """Calculate clean prices from clean returns."""
        rets = self.clean_returns(dates)
        px = self._prices.loc[dates] if dates else self._prices

        if backpropagate:
            return self.return_calculator.returns_to_prices(rets[::-1], px.iloc[-1])[::-1]

        result = self.return_calculator.returns_to_prices(rets, px.iloc[0])
        return result / result.iloc[0] if rebase else result

    # =========================================================================
    # Breakdown / Analysis
    # =========================================================================

    def get_breakdown(
            self,
            ticker: Optional[str] = None,
            dates: Optional[list] = None
    ) -> Union[dict[str, pd.DataFrame], pd.DataFrame]:
        """Get adjustments by component."""
        calc_dates = self._normalize_dates_for_calc(dates)
        breakdown = {}

        for component in self.components:
            comp_name = component.__class__.__name__
            try:
                breakdown[comp_name] = component.calculate_adjustment(
                    instruments=self.instruments,
                    dates=calc_dates,
                    prices=self._prices,
                )
            except Exception as e:
                logger.error(f"{comp_name} failed: {e}")
                breakdown[comp_name] = pd.DataFrame(0.0, index=calc_dates, columns=self.instrument_ids)

        if ticker:
            return pd.concat(
                [df[ticker] for df in breakdown.values()],
                axis=1,
                keys=breakdown.keys()
            )
        return breakdown

    # =========================================================================
    # Update Methods
    # =========================================================================

    def append_update(
            self,
            prices: Optional[pd.DataFrame] = None,
            recalc_last_n: int = 1,
            **component_data
    ) -> 'Adjuster':
        """
        Append new data permanently and calculate incrementally.

        Args:
            prices: New prices to append
            recalc_last_n: Number of previous dates to recalculate
                          -1 = full recalculation, 0 = only new dates, N = new + last N
            **component_data: Component updates (fx_prices_intraday, dividends, etc.)

        Returns:
            Self for method chaining
        """
        if component_data and prices is None:
            logger.warning("Updating component data without new prices.")

        new_dates = []

        # Handle prices update
        if prices is not None:
            new_prices = self._prepare_new_prices(prices)
            new_dates = new_prices.index.difference(self._prices.index).tolist()

            # Append permanently
            self._prices = pd.concat([self._prices, new_prices]).drop_duplicates().sort_index()
            logger.debug(f"Appended {len(new_dates)} new dates (total: {len(self._prices)} rows)")

        # Update components permanently
        self._update_components(component_data, temp=False)

        # Calculate adjustments for new dates
        if new_dates:
            dates_to_calc = self._get_dates_to_calculate(new_dates, recalc_last_n)
            logger.debug(f"Calculating {len(dates_to_calc)} dates (new={len(new_dates)}, recalc={recalc_last_n})")

            new_adjustments = self._calculate_for_dates(dates_to_calc)

            # Update cache
            if self._adjustments is None:
                self._adjustments = new_adjustments
            else:
                if recalc_last_n != 0:
                    self._adjustments = self._adjustments.loc[~self._adjustments.index.isin(dates_to_calc)]
                self._adjustments = pd.concat([self._adjustments, new_adjustments]).sort_index()

            logger.debug(f"Adjustments cache: {len(self._adjustments)} dates")

        return self

    def live_update(
            self,
            prices: Optional[pd.DataFrame | pd.Series] = None,
            **component_data
    ) -> pd.DataFrame:
        """
        Calculate adjustments with live data (no storage).

        Args:
            prices: Live prices
            **component_data: Live component data

        Returns:
            Adjustments DataFrame (dates × instruments)
        """
        prices = self._ensure_dataframe(prices)

        with self._live_context(prices=prices, **component_data) as live_dates:
            if not live_dates:
                logger.warning("No live dates to calculate")
                return pd.DataFrame(0.0, index=[], columns=self.instrument_ids)

            adjustments = self._calculate_for_dates(live_dates)
            logger.debug(f"Live calculation for {len(live_dates)} dates (no storage)")
            return adjustments

    def _get_dates_to_calculate(self, new_dates: list, recalc_last_n: int) -> list:
        """Determine which dates to calculate based on recalc policy."""
        if recalc_last_n == -1:
            return self._prices.index.tolist()

        if recalc_last_n == 0 or self._adjustments is None or len(self._adjustments) == 0:
            return new_dates

        last_n_dates = self._adjustments.index[-recalc_last_n:].tolist()
        return sorted(set(last_n_dates + new_dates))

    def _update_components(self, component_data: dict, temp: bool = False):
        """Update components with new data."""
        if not component_data:
            return

        updated = []
        method = 'apply_temp_data' if temp else 'append_data'

        for field, data in component_data.items():
            if field not in self._subscriptions:
                logger.warning(f"No components subscribed to '{field}'")
                continue

            for component in self._subscriptions[field]:
                try:
                    getattr(component, method)(**{field: data}, timestamp=self._last_update_timestamp)
                    comp_name = component.__class__.__name__
                    if comp_name not in updated:
                        updated.append(comp_name)
                except Exception as e:
                    logger.error(f"Failed to update {component.__class__.__name__}: {e}", exc_info=True)

        if updated:
            mode = "temporary" if temp else "permanent"
            logger.debug(f"Updated components ({mode}): {', '.join(updated)}")

    # =========================================================================
    # Live Context Manager
    # =========================================================================

    @contextmanager
    def _live_context(self, prices: Optional[pd.DataFrame] = None, **component_data):
        """
        Context manager for temporary calculations.

        Saves state, applies temp data, yields live dates, then restores.
        """
        # Save state (tuple for simplicity)
        snapshot = (
            self._prices.copy(),
            self._adjustments.copy() if self._adjustments is not None else None,
            {id(c): c.save_state() for c in self.components if c.is_updatable()}
        )

        live_dates = []

        try:
            # Apply temporary prices
            if prices is not None:
                prices = self._prepare_new_prices(prices)
                live_dates = prices.index.tolist()
                self._prices = pd.concat([self._prices, prices]).drop_duplicates().sort_index()

            # Apply temporary component data
            self._update_components(component_data, temp=True)

            yield live_dates

        finally:
            # Restore state
            self._prices, self._adjustments, component_states = snapshot
            for comp in self.components:
                if id(comp) in component_states:
                    comp.restore_state(component_states[id(comp)])

    # =========================================================================
    # Dunder Methods
    # =========================================================================

    def __repr__(self) -> str:
        return (
            f"Adjuster("
            f"instruments={len(self.instrument_ids)}, "
            f"dates={len(self._prices)}, "
            f"components={len(self.components)})"
        )
