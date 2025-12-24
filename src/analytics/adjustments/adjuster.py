"""
Adjuster orchestrator for return adjustments.

NEW DESIGN: Components own their data (no fx_prices in Adjuster).
"""
from datetime import date, datetime
import pandas as pd
import logging
from typing import Dict, Optional, Union, List

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol
from core.instruments.instrument_factory import InstrumentFactory

logger = logging.getLogger(__name__)
pd.set_option('future.no_silent_downcasting', True)


class Adjuster:
    """
    Orchestrates adjustment calculations across multiple instruments.

    NEW Usage - Components own their data:
        # Components receive their own data
        ter = TerComponent(ter_data)
        fx_spot = FxSpotComponent(fx_comp, fx_prices)
        fx_fwd = FxForwardCarryComponent(fwd_comp, fwd_prices, "1M", fx_prices)

        # Option 1: Individual adds
        adjuster = (
            Adjuster(prices)
            .add(ter)
            .add(fx_spot)
            .add(fx_fwd)
        )

        # Option 2: Component chain (your preferred syntax)
        adjuster = Adjuster(prices).add_chain(
            ter.add(
                fx_spot.add(
                    fx_fwd.add(
                        DividendComponent(divs)
                    )
                )
            )
        )

        # Calculate
        adjustments = adjuster.calculate()
    """

    MAX_NUMBER_OF_SIGNIFICANT_DIGITS = 8

    def __init__(
            self,
            prices: pd.DataFrame,
            instruments: Optional[Dict[str, InstrumentProtocol]] = None,
            intraday: bool = False,
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
        """
        self.intraday = intraday

        # Validate and normalize DataFrame
        self._prices = self._validate_and_transpose(prices, "prices")

        # Normalize dates if not intraday
        if not self.intraday:
            self._prices.index = self._prices.index.normalize()

        # Temporary prices (for append=False updates)
        self._temp_prices = None

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
        """Get current prices (temp or permanent)"""
        return self._temp_prices if self._temp_prices is not None else self._prices

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

    def _fetch_instruments_from_factory(self) -> Dict[str, InstrumentProtocol]:
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

        # If temp prices are set, calculate fresh (no cache)
        if self._temp_prices is not None:
            logger.debug("Adjuster: Using temp prices, bypassing cache")
            adjustments = self._calculate_for_dates(calc_dates)

            # Clear temp prices and temp component data after use
            self._temp_prices = None
            logger.debug("Adjuster: Cleared temp prices after calculation")

            return adjustments

        # Otherwise, use cache
        # Find dates not in cache
        if self._adjustments is None:
            missing_dates = calc_dates
        else:
            missing_dates = [d for d in calc_dates if d not in self._adjustments.index]

        # Calculate missing dates
        if len(missing_dates) > 0:
            logger.debug(f"Adjuster: Calculating {len(missing_dates)} missing dates (cache hit: {len(calc_dates) - len(missing_dates)})")
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
    ) -> pd.DataFrame:
        """
        Calculate clean returns by applying adjustments to raw returns.

        Raw returns are calculated from self.prices using pct_change().

        Args:
            dates: Optional subset of dates to calculate for

        Returns:
            DataFrame(dates × instruments) with clean returns
        """

        raw_returns = self.prices.pct_change(fill_method=None).fillna(0.0)
        adjustments = self.calculate(dates)
        if dates is not None:
            raw_returns = raw_returns.loc[dates]

        cleaned = raw_returns.add(
            adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
            fill_value=0.0
        )

        return cleaned

    def clean_prices(
            self,
            backpropagate: bool = True,
            dates: Union[list[date], list[datetime], None] = None,
    ) -> pd.DataFrame:
        """
        Reconstruct clean prices from adjustments.

        Args:
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

            # Calculate cumulative product backwards: P[t] = P[t+1] / (1 + r[t+1])
            cumulative_factor = (1 + reversed_returns).cumprod()

            # Reverse back to original order
            cumulative_factor = cumulative_factor.iloc[::-1]

            # Vectorized: clean_prices[t] = last_price / cumulative_factor[last] * cumulative_factor[t]
            # Normalize cumulative_factor so that last row = 1
            normalized_factor = cumulative_factor / cumulative_factor.iloc[-1]

            # Multiply by last prices (broadcasting)
            clean_prices = normalized_factor.multiply(last_prices, axis=1)
        else:
            # Start from first price and work forwards
            first_prices = anchor_prices.iloc[0].copy()
            cumulative_factor = (1 + clean_returns).cumprod()
            clean_prices = cumulative_factor.multiply(first_prices, axis=1)

        return clean_prices

    def update(
            self,
            append: bool = False,
            prices: Optional[pd.DataFrame] = None,
            **kwargs
    ) -> 'Adjuster':
        """
        Update prices and component data (append or temporary).

        IMPORTANT: Provide prices when updating components, as they need
        new dates to calculate adjustments on.

        Args:
            append: If True, append to existing data (permanent).
                   If False, use for next calculation only (temporary).
            prices: New instrument prices (DataFrame with dates × instruments)
            **kwargs: Component data (fx_prices, dividends, etc.)

        Returns:
            Self for method chaining

        Example:
            # Permanent update: append new data
            adjuster.update(
                append=True,
                prices=new_prices,
                fx_prices=new_fx_prices
            )

            # Temporary update: live data for one calculation
            adjuster.update(
                append=False,
                prices=live_prices,
                fx_prices=live_fx_prices
            )
            result = adjuster.calculate()  # Uses live data
            result2 = adjuster.calculate()  # Back to permanent data

        Notes:
            - Registry pattern: O(1) lookup for component updates
            - Components subscribe via updatable_fields property
        """
        # Validate: warn if updating components without prices
        if kwargs and prices is None and append:
            logger.warning(
                "Updating component data without new prices. "
                "Components may not have dates to calculate on."
            )

        # Handle prices update with proper timestamp
        new_dates_for_calc = None
        if prices is not None:
            new_prices = self._validate_and_transpose(prices, "prices")

            # Handle case where index is not temporal (e.g., single row with instrument cols)
            if not isinstance(new_prices.index, pd.DatetimeIndex):
                # Create timestamp: now() if intraday, today's date otherwise
                if self.intraday:
                    timestamp = pd.Timestamp.now()
                else:
                    timestamp = pd.Timestamp.now().normalize()

                # Rebuild with proper timestamp index
                new_prices.index = [timestamp]
                logger.debug(f"Adjuster: Non-temporal index detected, using timestamp={timestamp}")
            elif not self.intraday:
                new_prices.index = new_prices.index.normalize()

            # Align columns with existing instruments (fill missing with last known price)
            new_prices = new_prices.reindex(columns=self.instrument_ids, fill_value=None)

            if append:
                # Get dates that are actually new
                new_dates_for_calc = new_prices.index.difference(self._prices.index).tolist()

                # Permanently append new prices
                self._prices = pd.concat([self._prices, new_prices]).drop_duplicates().sort_index()
                self._temp_prices = None  # Clear any temp data
                logger.debug(f"Adjuster: Appended {len(new_dates_for_calc)} new dates (now {len(self._prices)} rows)")
            else:
                # Store temporarily for next calculation only
                self._temp_prices = new_prices
                logger.debug(f"Adjuster: Temp prices ({len(new_prices)} rows) for next calculation")

        # Update subscribed components using registry (O(1) lookup)
        if kwargs:
            updated_components = []

            for field, data in kwargs.items():
                # O(1) lookup: get components subscribed to this field
                if field in self._subscriptions:
                    for component in self._subscriptions[field]:
                        try:
                            component.update(append=append, **{field: data})
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
                mode = "permanent" if append else "temporary"
                logger.info(f"Updated components ({mode}): {', '.join(updated_components)}")

        # Calculate incremental adjustments for new dates (append mode only)
        if append and new_dates_for_calc and len(new_dates_for_calc) > 0:
            logger.debug(f"Adjuster: Calculating adjustments for {len(new_dates_for_calc)} new dates")

            new_adjustments = self._calculate_for_dates(new_dates_for_calc)

            # Append to existing adjustments
            if self._adjustments is None:
                self._adjustments = new_adjustments
            else:
                self._adjustments = pd.concat([self._adjustments, new_adjustments])
                self._adjustments = self._adjustments.sort_index()

            logger.debug(f"Adjuster: Adjustments cache now has {len(self._adjustments)} dates")

        return self  # Enable method chaining

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

    def __repr__(self) -> str:
        return (
            f"Adjuster("
            f"instruments={len(self.instrument_ids)}, "
            f"dates={len(self.prices.index)}, "
            f"components={len(self.components)})"
        )
