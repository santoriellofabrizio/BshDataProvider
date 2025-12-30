"""
Adjuster orchestrator for return adjustments.
"""
from contextlib import contextmanager
from datetime import date, datetime
import pandas as pd
import logging
from typing import Dict, Optional, Union, List, Literal

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
       - Use: append_update(prices=..., fx_prices=..., recalc_last_n=1)
       - Stores: New prices + new adjustments in cache
       - Calculates: Only new dates (plus recalc_last_n previous dates)

    2. Live Update Mode: Temporary calculation without storage
       - Use: live_update(prices=..., fx_prices=...)
       - Stores: Nothing (all temporary)
       - Calculates: New dates only, then discards
       - Returns: Calculated adjustments

    Usage:
    ------
        # Setup
        ter = TerComponent(ter_data)
        fx_spot = FxSpotComponent(fx_comp, fx_prices)
        adjuster = Adjuster(prices).add(ter).add(fx_spot)

        # Append mode: Store new end-of-day data
        adjuster.append_update(
            prices=new_eod_prices,
            fx_prices=new_fx_prices,
            recalc_last_n=1  # Recalc last date + new dates
        )
        adjustments = adjuster.calculate()

        # Live mode: Intraday updates without storage
        live_adj = adjuster.live_update(
            prices=live_prices,
            fx_prices=live_fx_prices
        )
        # Next call uses historical data (live data auto-discarded)
    """

    MAX_NUMBER_OF_SIGNIFICANT_DIGITS = 8

    def __init__(
            self,
            prices: pd.DataFrame,
            instruments: Optional[Dict[str, InstrumentProtocol]] = None,
            intraday: bool = True,
            fill_method: Optional[Literal['ffill', 'bfill', 'time', 'linear']] = None,
            return_type: Literal["percentage", "logarithmic", "absolute"] = "percentage",
    ):
        """
        Initialize adjuster.

        Args:
            prices: DataFrame with instrument prices
                    - Index: dates (DatetimeIndex or convertible)
                    - Columns: instrument IDs
            instruments: Optional dict[instrument_id -> Instrument object]
                        If None, fetches from InstrumentFactory
            intraday: If True, preserve intraday timestamps; if False, normalize to dates
            fill_method: How to handle NaN values in prices
                        - None: Keep NaN (propagates through calculations)
                        - 'ffill': Forward fill missing prices
                        - 'bfill': Backward fill missing prices
                        - 'linear': Linear interpolation
                        - 'time': Time-weighted interpolation (requires DatetimeIndex)
                        - 'index': Index-based interpolation
                        - 'polynomial': Polynomial interpolation
                        - 'spline': Spline interpolation
                        See pandas.DataFrame.interpolate() for all interpolation options
            return_type: Type of returns to calculate
                        - "percentage": (P_t - P_{t-1}) / P_{t-1}
                        - "logarithmic": log(P_t / P_{t-1})
                        - "absolute": P_t - P_{t-1}
        """
        self.intraday = intraday

        # Create return calculator
        self.return_calculator = ReturnCalculator(return_type)

        # Validate and normalize DataFrame
        self._prices = self._validate_and_transpose(prices, "prices")

        # Handle missing values
        if fill_method is not None:
            self._prices = self._handle_missing(prices, fill_method)
        # Normalize dates if not intraday
        if not self.intraday:
            self._prices.index = self._prices.index.normalize()

        # Extract instrument IDs
        self.instrument_ids = self._prices.columns.tolist()

        # Get instruments
        if instruments is None:
            self.instruments = self._fetch_instruments_from_factory()
        else:
            self.instruments = instruments

        # Validate
        self._validate_instruments()

        # Component registry
        self.components: list[Component] = []
        self._subscriptions: dict[str, list[Component]] = {}  # field -> [components]

        # Adjustments cache (always valid)
        self._adjustments: pd.DataFrame | None = None  # dates × instruments

        logger.info(
            f"Adjuster initialized: {len(self.instrument_ids)} instruments, "
            f"{len(self._prices.index)} {'timestamps' if self.intraday else 'dates'}"
        )

    @property
    def prices(self) -> pd.DataFrame:
        """Get current prices"""
        return self._prices

    @staticmethod
    def _validate_and_transpose(df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Validate DataFrame and transpose if needed."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{name} must be DataFrame, got {type(df)}")

        if df.empty:
            raise ValueError(f"{name} is empty")

        # Check if needs transposing
        needs_transpose = False

        if isinstance(df.columns, pd.DatetimeIndex):
            needs_transpose = True
        elif (isinstance(df.index[0], str) and
              isinstance(df.columns[0], (date, pd.Timestamp))):
            needs_transpose = True

        if needs_transpose:
            df = df.T
            logger.info(f"{name}: Transposed to {df.shape}")

        # Convert index to DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                raise ValueError(f"{name} index must be dates. Error: {e}")

        return df

    def _fetch_instruments_from_factory(self) -> Dict[str, Instrument]:
        """Fetch instruments from InstrumentFactory."""
        logger.debug(f"Fetching {len(self.instrument_ids)} instruments...")
        factory = InstrumentFactory()
        instruments = factory.get_many(self.instrument_ids)
        logger.info(f"Fetched {len(instruments)} instruments")
        return instruments

    def _validate_instruments(self):
        """Validate instruments."""
        missing = set(self.instrument_ids) - set(self.instruments.keys())
        if missing:
            raise ValueError(f"Missing instruments: {missing}")

    def add(self, component: Component) -> 'Adjuster':
        """
        Add component and register its subscriptions.

        Args:
            component: Component with its own data

        Returns:
            Self for chaining
        """
        # Inject return calculator into component
        component.set_return_calculator(self.return_calculator)
        component.is_intraday = self.intraday

        self.components.append(component)

        # Register subscriptions if component is updatable
        if component.is_updatable():
            for field in component.updatable_fields:
                if field not in self._subscriptions:
                    self._subscriptions[field] = []
                self._subscriptions[field].append(component)
                logger.debug(f"{component.__class__.__name__} subscribed to '{field}'")

        logger.debug(f"Added: {component.__class__.__name__}")
        return self

    def calculate(self, dates: Union[list[date], list[datetime], None] = None) -> pd.DataFrame:
        """
        Calculate adjustments using cache when possible.

        Args:
            dates: Optional subset of dates

        Returns:
            DataFrame(dates × instruments) with total adjustments
        """
        if dates is None:
            calc_dates = self.prices.index.tolist()
        else:
            calc_dates = dates
            if not self.intraday:
                calc_dates = [d.date() if isinstance(d, (datetime, pd.Timestamp)) else d for d in calc_dates]

        # Find dates not in cache
        if self._adjustments is None:
            missing_dates = calc_dates
        else:
            missing_dates = [d for d in calc_dates if d not in self._adjustments.index]

        # Calculate missing dates
        if len(missing_dates) > 0:
            logger.debug(
                f"Adjuster: Calculating {len(missing_dates)} missing dates"
                f" (cache hit: {len(calc_dates) - len(missing_dates)})")
            new_adj = self._calculate_for_dates(missing_dates)

            if self._adjustments is None:
                self._adjustments = new_adj
            else:
                self._adjustments = pd.concat([self._adjustments, new_adj])
                self._adjustments = self._adjustments.sort_index()
        else:
            logger.debug(f"Adjuster: All {len(calc_dates)} dates in cache (100% hit rate)")

        # Return requested dates from cache
        return self._adjustments.loc[calc_dates].copy()

    def get_breakdown(self, dates: Union[list[date], list[datetime], None] = None) -> dict[str, pd.DataFrame]:
        """Get adjustments by component."""
        if dates is None:
            calc_dates = self.prices.index.tolist()
        else:
            calc_dates = dates
            if not self.intraday:
                calc_dates = [
                    d.date() if isinstance(d, (datetime, pd.Timestamp)) else d
                    for d in calc_dates
                ]

        breakdown = {}

        for component in self.components:
            comp_name = component.__class__.__name__

            try:
                breakdown[comp_name] = component.calculate_adjustment(
                    instruments=self.instruments,
                    dates=calc_dates,
                    prices=self.prices,
                )
            except Exception as e:
                logger.error(f"{comp_name} failed: {e}")
                breakdown[comp_name] = pd.DataFrame(
                    0.0, index=calc_dates, columns=self.instrument_ids
                )

        return breakdown

    def clean_returns(
            self,
            dates: Union[list[date], list[datetime], None] = None,
            cumulative: bool = False,
            live_prices: Optional[pd.DataFrame | pd.Series] = None,
            **live_component_data,
    ) -> pd.DataFrame:
        """
        Calculate clean returns by applying adjustments to raw returns.

        Raw returns are calculated using the return calculator.

        IMPORTANT:
        - The first return is always 0 (no previous price to compare to)
        - NaN values in prices will result in NaN returns (unless fill_missing was set in constructor)

        Args:
            dates: Optional subset of dates to calculate for
            cumulative: If True, return forward cumulative returns (from each date T to end)
            live_prices: Optional live prices for temporary calculation (no storage)
            live_component_data: Optional live component data (fx_prices, dividends, etc.) for temporary calculation

        Returns:
            DataFrame(dates × instruments) with clean returns (forward cumulative if specified)

        Example:
            # Standard calculation with historical data
            clean = adjuster.clean_returns()

            # Live calculation with intraday prices
            live_clean = adjuster.clean_returns(
                live_prices=live_prices,
                live_component_data={'fx_prices': fx_live}
            )
        """
        # Handle live updates
        if live_prices is not None or live_component_data:
            if isinstance(live_prices, pd.Series):
                ts = pd.Timestamp.now() if self.intraday else pd.Timestamp.now().normalize()
                live_prices = live_prices.to_frame(ts).T
            live_component_data = live_component_data or {}
            with self._live_context(prices=live_prices, **live_component_data) as live_dates:
                # Calculate with live data
                raw_returns = self.return_calculator.calculate_returns(self.prices)
                adjustments = self.calculate(dates)
                if dates is not None:
                    raw_returns = raw_returns.loc[dates]

                cleaned = raw_returns.add(
                    adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
                    fill_value=0.0
                )

                cleaned.iloc[0] = 0.0

                if cumulative:
                    cleaned = (1 + cleaned).iloc[::-1].cumprod().iloc[::-1] - 1

                return cleaned

        # Standard path: use historical data
        raw_returns = self.return_calculator.calculate_returns(self.prices)
        adjustments = self.calculate(dates)
        if dates is not None:
            raw_returns = raw_returns.loc[dates]

        cleaned = raw_returns.add(
            adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
            fill_value=0.0
        )

        # Ensure first return is always 0 (first price has no previous price for return calculation)
        # This aligns with return calculator behavior and ensures clean price reconstruction works correctly
        cleaned.iloc[0] = 0.0

        if cumulative:
            cleaned = (1 + cleaned).iloc[::-1].cumprod().iloc[::-1] - 1

        return cleaned

    def clean_prices(
            self,
            backpropagate: bool = True,
            rebase: bool = False,
            dates: Union[list[date], list[datetime], None] = None,
    ) -> pd.DataFrame:
        """
        Reconstruct clean prices from adjustments.

        Args:
            rebase: IF true, initial prices are set to 1.
            backpropagate: If True, start from last price and work backwards.
                          If False, start from first price and work forwards.
            dates: Optional subset of dates to calculate for

        Returns:
            DataFrame(dates × instruments) with clean prices

        Example:
            # Backpropagate from last known price (default)
            clean_prices = adjuster.get_clean_prices(backpropagate=True)

            # Forward propagate from first price
            clean_prices = adjuster.get_clean_prices(backpropagate=False)
        """
        # Get clean returns
        clean_returns = self.clean_returns(dates)

        # Determine which prices to use as anchor
        if dates is not None:
            anchor_prices = self.prices.loc[dates]
        else:
            anchor_prices = self.prices

        if backpropagate:
            # Start from last price and work backwards
            # Get last known prices as anchor
            last_prices = anchor_prices.iloc[-1].copy()

            # Reverse the returns
            reversed_returns = clean_returns.iloc[::-1]

            # Calculate cumulative backwards using return calculator
            # For percentage: (1 + r).cumprod()
            # For log: exp(r.cumsum())
            # For absolute: Need special handling
            if self.return_calculator.return_type.value == "percentage":
                cumulative_factor = (1 + reversed_returns).cumprod()
            elif self.return_calculator.return_type.value == "logarithmic":
                import numpy as np
                cumulative_factor = np.exp(reversed_returns.cumsum())
            else:  # absolute
                # For absolute returns, backpropagation is not straightforward
                # Use forward propagation instead
                logger.warning("Backpropagation not supported for absolute returns, using forward propagation")
                first_prices = anchor_prices.iloc[0].copy()
                result = self.return_calculator.returns_to_prices(clean_returns, first_prices)
                if rebase:
                    result = result / last_prices
                return result

            # Reverse back to original order
            cumulative_factor = cumulative_factor.iloc[::-1]

            # Vectorized: clean_prices[t] = last_price / cumulative_factor[last] * cumulative_factor[t]
            # Normalize cumulative_factor so that last row = 1
            normalized_factor = cumulative_factor / cumulative_factor.iloc[-1]

            # Multiply by last prices (broadcasting)
            clean_prices = normalized_factor.multiply(last_prices, axis=1)
        else:
            # Start from first price and work forwards (use return calculator)
            first_prices = anchor_prices.iloc[0].copy()
            clean_prices = self.return_calculator.returns_to_prices(clean_returns, first_prices)
            if rebase:
                clean_prices = clean_prices / first_prices
        return clean_prices

    def append_update(
            self,
            prices: Optional[pd.DataFrame] = None,
            recalc_last_n: int = 1,
            **component_data
    ) -> 'Adjuster':
        """
        Append new data permanently and calculate incrementally.

        Args:
            prices: New prices to append (DataFrame with dates × instruments)
            recalc_last_n: Number of previous dates to recalculate
                          -1 = full recalculation from start
                           0 = only new dates
                           1 = new dates + last stored date (default)
                           N = new dates + last N stored dates
            **component_data: Component updates (fx_prices, dividends, etc.)

        Returns:
            Self for method chaining

        Example:
            # Append new end-of-day data
            adjuster.append_update(
                prices=new_eod_prices,
                fx_prices=new_fx_prices,
                recalc_last_n=1
            )

            # Full recalculation
            adjuster.append_update(
                prices=new_prices,
                recalc_last_n=-1
            )

        Workflow:
            1. Append new prices to permanent storage
            2. Update components with new data (permanent)
            3. Determine dates to calculate (new + recalc_last_n)
            4. Calculate adjustments for those dates
            5. Update adjustments cache
        """
        # Validate: warn if updating components without prices
        if component_data and prices is None:
            logger.warning(
                "Updating component data without new prices. "
                "Components may not have dates to calculate on."
            )

        new_dates = []

        # Handle prices update
        if prices is not None:
            new_prices = self._validate_and_transpose(prices, "prices")

            # Handle non-temporal index
            if not isinstance(new_prices.index, pd.DatetimeIndex):
                if self.intraday:
                    timestamp = pd.Timestamp.now()
                else:
                    timestamp = pd.Timestamp.now().normalize()
                new_prices.index = [timestamp]
                logger.debug(f"Adjuster: Non-temporal index detected, using timestamp={timestamp}")
            elif not self.intraday:
                new_prices.index = new_prices.index.normalize()

            # Align columns
            new_prices = new_prices.reindex(columns=self.instrument_ids, fill_value=None)

            # Get truly new dates
            new_dates = new_prices.index.difference(self._prices.index).tolist()

            # Permanently append new prices
            self._prices = pd.concat([self._prices, new_prices]).drop_duplicates().sort_index()
            logger.debug(f"Adjuster: Appended {len(new_dates)} new dates (now {len(self._prices)} rows)")

        # Update components permanently
        if component_data:
            updated_components = []

            for field, data in component_data.items():
                if field in self._subscriptions:
                    for component in self._subscriptions[field]:
                        try:
                            component.append_data(**{field: data})
                            comp_name = component.__class__.__name__
                            if comp_name not in updated_components:
                                updated_components.append(comp_name)
                        except Exception as e:
                            logger.error(
                                f"Failed to update {component.__class__.__name__} with {field}: {e}",
                                exc_info=True
                            )
                else:
                    logger.warning(f"No components subscribed to field '{field}'")

            if updated_components:
                logger.info(f"Updated components (permanent): {', '.join(updated_components)}")

        # Calculate adjustments for new dates (incremental)
        if new_dates:
            dates_to_calc = self._get_dates_to_calculate(new_dates, recalc_last_n)
            logger.debug(f"Adjuster: Calculating adjustments for {len(dates_to_calc)} dates "
                         f"(new={len(new_dates)}, recalc_last_n={recalc_last_n})")

            new_adjustments = self._calculate_for_dates(dates_to_calc)

            # Update cache
            if self._adjustments is None:
                self._adjustments = new_adjustments
            else:
                # Remove old dates that we recalculated
                if recalc_last_n > 0 or recalc_last_n == -1:
                    self._adjustments = self._adjustments.loc[~self._adjustments.index.isin(dates_to_calc)]

                self._adjustments = pd.concat([self._adjustments, new_adjustments]).sort_index()

            logger.debug(f"Adjuster: Adjustments cache now has {len(self._adjustments)} dates")

        return self

    def live_update(
            self,
            prices: Optional[pd.DataFrame | pd.Series] = None,
            **component_data
    ) -> pd.DataFrame:
        """
        Calculate adjustments with live data (no storage).

        Args:
            prices: Live prices (DataFrame with dates × instruments)
            **component_data: Live component data (fx_prices, etc.)

        Returns:
            Adjustments DataFrame (dates × instruments)

        Example:
            # Intraday update without storage
            live_adj = adjuster.live_update(
                prices=live_prices,
                fx_prices=live_fx_prices
            )
            clean_returns = adjuster.clean_returns()  # Uses live data

            # Next call uses historical data (live data auto-discarded)
            historical = adjuster.calculate()

        Workflow:
            1. Save current state
            2. Temporarily apply new data
            3. Calculate adjustments
            4. Restore original state
            5. Return adjustments
        """
        if isinstance(prices, pd.Series):
            ts = pd.Timestamp.now() if self.intraday else pd.Timestamp.now().normalize()
            prices = prices.to_frame(ts).T

        with self._live_context(prices=prices, **component_data) as live_dates:
            # Calculate adjustments for live dates only
            if live_dates:
                adjustments = self._calculate_for_dates(live_dates)
                logger.debug(f"Adjuster: Live calculation for {len(live_dates)} dates (no storage)")
                return adjustments
            else:
                logger.warning("Adjuster: No live dates to calculate")
                return pd.DataFrame(0.0, index=[], columns=self.instrument_ids)

    def _get_dates_to_calculate(self, new_dates: list, recalc_last_n: int) -> list:
        """
        Determine which dates to calculate based on recalc policy.

        Args:
            new_dates: New dates being added
            recalc_last_n: Lookback parameter
                -1: Full recalc (all dates in prices)
                 0: Only new dates
                 N: New dates + last N dates from cache

        Returns:
            List of dates to calculate
        """
        if recalc_last_n == -1:
            # Full recalculation
            return self._prices.index.tolist()

        if recalc_last_n == 0:
            # Only new dates
            return new_dates

        # Partial recalc: new + last N from cache
        if self._adjustments is None or len(self._adjustments) == 0:
            # No cache, calculate all new dates
            return new_dates

        # Get last N dates from cache
        last_n_dates = self._adjustments.index[-recalc_last_n:].tolist()

        # Combine and deduplicate
        all_dates = last_n_dates + new_dates
        return sorted(set(all_dates))

    @contextmanager
    def _live_context(self, prices=None, **component_data):
        """
        Context manager for temporary calculations.

        Saves current state, applies temp data, yields live dates, then restores.

        Args:
            prices: Temporary prices
            **component_data: Temporary component data

        Yields:
            list: Live dates to calculate
        """
        # Save state
        saved_prices = self._prices.copy()
        saved_adjustments = self._adjustments.copy() if self._adjustments is not None else None
        saved_component_states = self._save_component_states()

        live_dates = []

        try:
            # Apply temporary prices
            if prices is not None:
                new_prices = self._validate_and_transpose(prices, "prices")

                # Handle non-temporal index
                if not isinstance(new_prices.index, pd.DatetimeIndex):
                    if self.intraday:
                        timestamp = pd.Timestamp.now()
                    else:
                        timestamp = pd.Timestamp.now().normalize()
                    new_prices.index = [timestamp]
                elif not self.intraday:
                    new_prices.index = new_prices.index.normalize()

                # Align columns
                new_prices = new_prices.reindex(columns=self.instrument_ids, fill_value=None)

                # Get live dates (only new ones)
                live_dates = new_prices.index.tolist()

                # Temporarily extend prices
                self._prices = pd.concat([self._prices, new_prices]).drop_duplicates().sort_index()

            # Apply temporary component data
            for field, data in component_data.items():
                if field in self._subscriptions:
                    for component in self._subscriptions[field]:
                        try:
                            component.apply_temp_data(**{field: data})
                        except Exception as e:
                            logger.error(
                                f"Failed to apply temp data to {component.__class__.__name__}: {e}",
                                exc_info=True
                            )

            yield live_dates

        finally:
            # Restore original state
            self._prices = saved_prices
            self._adjustments = saved_adjustments
            self._restore_component_states(saved_component_states)

    def _save_component_states(self) -> dict:
        """Save component data states for restoration"""
        states = {}
        for comp in self.components:
            if comp.is_updatable():
                states[id(comp)] = comp.save_state()
        return states

    def _restore_component_states(self, states: dict):
        """Restore component data states"""
        for comp in self.components:
            if id(comp) in states:
                comp.restore_state(states[id(comp)])

    def _calculate_for_dates(self, dates: list) -> pd.DataFrame:
        """
        Calculate adjustments for specific dates only.

        Args:
            dates: List of dates to calculate adjustments for

        Returns:
            DataFrame(dates × instruments) with adjustments
        """
        # Initialize result
        adjustments = pd.DataFrame(0.0, index=dates, columns=self.instrument_ids)

        # Sum from each component
        for component in self.components:
            comp_name = component.__class__.__name__
            logger.debug(f"Calculating {comp_name} for {len(dates)} dates...")

            try:
                component_adj = component.calculate_adjustment(
                    instruments=self.instruments,
                    dates=dates,
                    prices=self.prices,  # Full prices, component filters internally
                ).fillna(0)

                adjustments += component_adj

            except Exception as e:
                logger.error(f"{comp_name} failed: {e}", exc_info=True)

        # Round to avoid floating point precision issues
        adjustments = adjustments.round(self.MAX_NUMBER_OF_SIGNIFICANT_DIGITS)

        return adjustments

    @staticmethod
    def _handle_missing(prices, fill_method):
        if fill_method is not None:
            if (nan_count_before := prices.isna().sum().sum()) > 0:
                if fill_method == 'ffill':
                    prices = prices.ffill()
                    nan_count_after = prices.isna().sum().sum()
                    logger.info(f"Adjuster: Filled {nan_count_before - nan_count_after} NaN values using ffill "
                                f"({nan_count_after} NaN remaining)")

                elif fill_method == 'bfill':
                    prices = prices.bfill()
                    nan_count_after = prices.isna().sum().sum()
                    logger.info(f"Adjuster: Filled {nan_count_before - nan_count_after} NaN values using bfill "
                                f"({nan_count_after} NaN remaining)")
                else:
                    # Interpolation methods
                    try:
                        prices = prices.interpolate(method=fill_method)
                        nan_count_after = prices.isna().sum().sum()
                        logger.info(
                            f"Adjuster: Interpolated {nan_count_before - nan_count_after}"
                            f" NaN values using '{fill_method}' "
                            f"({nan_count_after} NaN remaining)"
                        )
                    except Exception as e:
                        logger.error(f"Adjuster: Interpolation failed with method '{fill_method}': {e}")
                        raise ValueError(f"Invalid fill_method: '{fill_method}'") from e

            return prices

    def __repr__(self) -> str:
        return (
            f"Adjuster("
            f"instruments={len(self.instrument_ids)}, "
            f"dates={len(self.prices.index)}, "
            f"components={len(self.components)})"
        )
