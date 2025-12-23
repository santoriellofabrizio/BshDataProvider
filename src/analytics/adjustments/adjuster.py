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
            instruments: Optional dict[instrument_id → Instrument object]
                        If None, fetches from InstrumentFactory
            intraday: If True, preserve intraday timestamps; if False, normalize to dates
        """
        self.intraday = intraday
        
        # Validate and normalize DataFrame
        self.prices = self._validate_and_transpose(prices, "prices")
        
        # Normalize dates if not intraday
        if not self.intraday:
            self.prices.index = self.prices.index.normalize()

        # Extract instrument IDs
        self.instrument_ids = self.prices.columns.tolist()

        # Get instruments
        if instruments is None:
            self.instruments = self._fetch_instruments_from_factory()
        else:
            self.instruments = instruments

        # Validate
        self._validate_instruments()

        # Component registry
        self.components: list[Component] = []

        logger.info(
            f"Adjuster initialized: {len(self.instrument_ids)} instruments, "
            f"{len(self.prices.index)} {'timestamps' if self.intraday else 'dates'}"
        )

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
        Add component (fluent API).

        Args:
            component: Component with its own data

        Returns:
            Self for chaining
        """
        self.components.append(component)
        logger.debug(f"Added: {component.__class__.__name__}")
        return self

    def add_chain(self, component: Component) -> 'Adjuster':
        """
        Add component chain (unwraps builder pattern).

        Args:
            component: Root component (may have children)

        Returns:
            Self for chaining

        Example:
            chain = (
                TerComponent(ter)
                .add(FxSpotComponent(fx_comp, fx_prices))
                .add(DividendComponent(divs))
            )
            adjuster.add_chain(chain)
        """
        all_components = component.get_chain()
        
        for comp in all_components:
            self.components.append(comp)
            logger.debug(f"Added from chain: {comp.__class__.__name__}")
        
        logger.info(f"Added {len(all_components)} components from chain")
        return self

    def calculate(self, dates: Union[list[date], list[datetime], None] = None) -> pd.DataFrame:
        """
        Calculate adjustments.

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
                calc_dates = [
                    d.date() if isinstance(d, (datetime, pd.Timestamp)) else d
                    for d in calc_dates
                ]

        # Initialize result
        adjustments = pd.DataFrame(0.0, index=calc_dates, columns=self.instrument_ids)

        # Sum from each component
        for component in self.components:
            comp_name = component.__class__.__name__
            logger.debug(f"Calculating {comp_name}...")

            try:
                # NEW: No fx_prices passed
                component_adj = component.calculate_adjustment(
                    instruments=self.instruments,
                    dates=calc_dates,
                    prices=self.prices,
                ).fillna(0)

                adjustments += component_adj

                # Log stats
                non_zero = (component_adj != 0).sum().sum()
                logger.info(
                    f"{comp_name}: {non_zero} non-zero "
                    f"(mean={component_adj.mean().mean():.6f})"
                )

            except Exception as e:
                logger.error(f"{comp_name} failed: {e}", exc_info=True)

        return adjustments

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
        raw_returns: pd.DataFrame,
        dates: Union[list[date], list[datetime], None] = None,
    ) -> pd.DataFrame:
        """Clean raw returns by applying adjustments."""
        adjustments = self.calculate(dates)
        
        cleaned = raw_returns.add(
            adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
            fill_value=0.0
        )
        
        return cleaned

    def update(
        self,
        prices: Optional[pd.DataFrame] = None,
        fx_prices: Optional[pd.DataFrame] = None,
        **kwargs
    ) -> 'Adjuster':
        """
        Update data for live mode (fluent API).
        
        Updates both the adjuster's prices and delegates to all components
        for their specific data updates.
        
        Args:
            prices: New instrument prices (DataFrame with dates × instruments)
            fx_prices: New FX prices (DataFrame with dates × currencies)
            **kwargs: Other component-specific data (dividends, ters, etc.)
        
        Returns:
            Self for method chaining
        
        Example:
            # Update and calculate in one line (fluent API)
            adjustments = adjuster.update(
                prices=new_prices,
                fx_prices=new_fx
            ).calculate()
            
            # Full chain
            breakdown = adjuster.update(prices=new_prices).get_breakdown()
            
            # Traditional style still works
            adjuster.update(prices=new_prices)
            adjustments = adjuster.calculate()
        
        Notes:
            - Only provided arguments are updated (None means no update)
            - Components decide what to update based on kwargs keys
            - Invalid/missing data is handled gracefully per component
        """
        # Update adjuster's prices
        if prices is not None:
            self.prices = self._validate_and_transpose(prices, "prices")
            if not self.intraday:
                self.prices.index = self.prices.index.normalize()
            logger.debug(f"Adjuster: Updated prices to {len(self.prices)} rows")
        
        # Prepare component update data
        update_data = {}
        if fx_prices is not None:
            update_data['fx_prices'] = fx_prices
        update_data.update(kwargs)
        
        # Update all components
        if update_data:
            updated_components = []
            for component in self.components:
                try:
                    component.update_data(**update_data)
                    updated_components.append(component.__class__.__name__)
                except Exception as e:
                    logger.error(
                        f"Failed to update {component.__class__.__name__}: {e}",
                        exc_info=True
                    )
            
            if updated_components:
                logger.info(f"Updated components: {', '.join(updated_components)}")

        return self  # Enable method chaining

    def __repr__(self) -> str:
        return (
            f"Adjuster("
            f"instruments={len(self.instrument_ids)}, "
            f"dates={len(self.prices.index)}, "
            f"components={len(self.components)})"
        )
