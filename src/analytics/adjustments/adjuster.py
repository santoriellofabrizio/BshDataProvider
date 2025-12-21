"""
Adjuster orchestrator for return adjustments.
"""
from datetime import date
import pandas as pd
import logging
from typing import Dict, Optional

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol
from core.instruments.instrument_factory import InstrumentFactory

logger = logging.getLogger(__name__)


class Adjuster:
    """
    Orchestrates adjustment calculations across multiple instruments.

    Usage:
        # Prepare data
        prices = pd.DataFrame(...)      # dates × instruments
        fx_prices = pd.DataFrame(...)   # dates × currencies

        # Option 1: Explicit instruments
        instruments = factory.get_many(['IWDA LN', 'VWRL LN'])
        adjuster = Adjuster(prices, fx_prices, instruments)

        # Option 2: Auto-fetch from factory (lazy)
        adjuster = Adjuster(prices, fx_prices)  # Uses InstrumentFactory

        # Build pipeline
        adjuster.add(TerComponent(ters)).add(YtmComponent(ytms))

        # Calculate
        adjustments = adjuster.calculate()  # DataFrame(dates × instruments)
    """

    def __init__(
        self,
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
        instruments: Optional[Dict[str, InstrumentProtocol]] = None,
    ):
        """
        Initialize adjuster.

        Args:
            prices: DataFrame with instrument prices
                    - Index: dates (DatetimeIndex or convertible)
                    - Columns: instrument IDs
            fx_prices: DataFrame with FX spot rates (EUR base)
                       - Index: dates (DatetimeIndex or convertible)
                       - Columns: currency codes
            instruments: Optional dict[instrument_id → Instrument object]
                        If None, fetches from InstrumentFactory

        Raises:
            ValueError: If DataFrames malformed or incompatible
        """
        # Validate and normalize DataFrames
        self.prices = self._validate_and_transpose(prices, "prices")
        self.fx_prices = self._validate_and_transpose(fx_prices, "fx_prices")

        # Extract instrument IDs from columns
        self.instrument_ids = self.prices.columns.tolist()

        # Get instruments (explicit or from factory)
        if instruments is None:
            self.instruments = self._fetch_instruments_from_factory()
        else:
            self.instruments = instruments

        # Validate instruments
        self._validate_instruments()

        # Component registry
        self.components: list[Component] = []

        logger.info(
            f"Adjuster initialized: {len(self.instrument_ids)} instruments, "
            f"{len(self.prices.index)} dates"
        )

    def _validate_and_transpose(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """
        Validate DataFrame structure and transpose if needed.

        Expected format:
            - Index: dates (DatetimeIndex preferred)
            - Columns: instruments/currencies (strings)

        Auto-transposes if:
            - Columns look like dates
            - Index looks like instruments/currencies

        Args:
            df: Input DataFrame
            name: Name for error messages

        Returns:
            Validated DataFrame (dates × items)

        Raises:
            ValueError: If DataFrame malformed
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{name} must be DataFrame, got {type(df)}")

        if df.empty:
            raise ValueError(f"{name} is empty")

        # Check if needs transposing
        needs_transpose = False

        # Heuristic 1: Columns are dates
        if isinstance(df.columns, pd.DatetimeIndex):
            needs_transpose = True
            logger.debug(f"{name}: Transposing (columns are DatetimeIndex)")

        # Heuristic 2: Index looks like strings, columns like dates
        elif (isinstance(df.index[0], str) and
              isinstance(df.columns[0], (date, pd.Timestamp))):
            needs_transpose = True
            logger.debug(f"{name}: Transposing (index=str, columns=dates)")

        # Transpose if needed
        if needs_transpose:
            df = df.T
            logger.info(f"{name}: Transposed to {df.shape} (dates × items)")

        # Convert index to DatetimeIndex if needed
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
                logger.debug(f"{name}: Converted index to DatetimeIndex")
            except Exception as e:
                raise ValueError(
                    f"{name} index must be dates or convertible to DatetimeIndex. "
                    f"Got: {type(df.index[0])}. Error: {e}"
                )

        # Validate structure
        if len(df.index) == 0:
            raise ValueError(f"{name} has no dates in index")

        if len(df.columns) == 0:
            raise ValueError(f"{name} has no items in columns")

        return df

    def _fetch_instruments_from_factory(self) -> Dict[str, InstrumentProtocol]:
        """
        Fetch instruments from InstrumentFactory.

        Uses factory's internal registry (shared with API calls).

        Returns:
            Dict[instrument_id → Instrument object]

        Raises:
            ImportError: If InstrumentFactory not available
        """

        logger.debug(f"Fetching {len(self.instrument_ids)} instruments from factory...")

        factory = InstrumentFactory()
        instruments = factory.get_many(self.instrument_ids)

        logger.info(f"Fetched {len(instruments)} instruments from InstrumentFactory")

        return instruments

    def _validate_instruments(self):
        """Validate that all instruments in prices have metadata"""
        missing = set(self.instrument_ids) - set(self.instruments.keys())
        if missing:
            raise ValueError(
                f"Missing instruments in metadata: {missing}. "
                f"Provide instruments for all columns in prices."
            )

    def add(self, component: Component) -> 'Adjuster':
        """
        Add component to calculation pipeline (fluent API).

        Args:
            component: Component instance with data

        Returns:
            Self for chaining

        Example:
            adjuster.add(TerComponent(ters)).add(YtmComponent(ytms))
        """
        self.components.append(component)
        logger.debug(f"Added component: {component.__class__.__name__}")
        return self

    def calculate(self, dates: list[date] | None = None) -> pd.DataFrame:
        """
        Calculate adjustments for all instruments.

        Args:
            dates: Optional subset of dates (default: all dates in prices.index)

        Returns:
            DataFrame(dates × instruments) with total adjustments
        """
        calc_dates = dates or self.prices.index.tolist()

        # Initialize result
        adjustments = pd.DataFrame(0.0, index=calc_dates, columns=self.instrument_ids)

        # Sum contributions from each component
        for component in self.components:
            comp_name = component.__class__.__name__
            logger.debug(f"Calculating {comp_name}...")

            try:
                component_adj = component.calculate_batch(
                    instruments=self.instruments,
                    dates=calc_dates,
                    prices=self.prices,
                    fx_prices=self.fx_prices,
                )

                # Add to total
                adjustments += component_adj

                # Log stats
                non_zero = (component_adj != 0).sum().sum()
                logger.info(
                    f"{comp_name}: {non_zero} non-zero adjustments "
                    f"(mean={component_adj.mean().mean():.6f})"
                )

            except Exception as e:
                logger.error(
                    f"{comp_name} failed completely: {e}",
                    exc_info=True
                )
                # Continue with other components

        return adjustments

    def get_breakdown(self, dates: list[date] | None = None) -> dict[str, pd.DataFrame]:
        """
        Get adjustments broken down by component.

        Args:
            dates: Optional subset of dates (default: all dates in prices.index)

        Returns:
            Dict[component_name → DataFrame(dates × instruments)]

        Example:
            breakdown = adjuster.get_breakdown()
            ter_adj = breakdown['TerComponent']
            ytm_adj = breakdown['YtmComponent']
        """
        calc_dates = dates or self.prices.index.tolist()
        breakdown = {}

        for component in self.components:
            comp_name = component.__class__.__name__

            try:
                breakdown[comp_name] = component.calculate_batch(
                    instruments=self.instruments,
                    dates=calc_dates,
                    prices=self.prices,
                    fx_prices=self.fx_prices,
                )
            except Exception as e:
                logger.error(f"{comp_name} failed: {e}")
                breakdown[comp_name] = pd.DataFrame(
                    0.0, index=calc_dates, columns=self.instrument_ids
                )

        return breakdown

    def clean_returns(
        self,
        raw_returns: pd.DataFrame,
        dates: list[date] | None = None,
    ) -> pd.DataFrame:
        """
        Clean raw returns by applying adjustments.

        Args:
            raw_returns: DataFrame(dates × instruments) with raw returns
            dates: Optional subset of dates

        Returns:
            DataFrame(dates × instruments) with cleaned returns

        Formula:
            clean_return = raw_return + adjustment
        """
        adjustments = self.calculate(dates)

        # Align and add
        cleaned = raw_returns.add(
            adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
            fill_value=0.0
        )

        return cleaned

    def __repr__(self) -> str:
        return (
            f"Adjuster("
            f"instruments={len(self.instrument_ids)}, "
            f"dates={len(self.prices.index)}, "
            f"components={len(self.components)})"
        )