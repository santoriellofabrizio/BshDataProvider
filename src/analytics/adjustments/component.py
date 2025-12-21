"""
Base component for adjustments.
"""
from abc import ABC, abstractmethod
from datetime import date
import pandas as pd
import logging

from analytics.adjustments.protocols import InstrumentProtocol

logger = logging.getLogger(__name__)


class Component(ABC):
    """
    Abstract base for adjustment components.

    Components are PURE CALCULATIONS:
    - No I/O (data passed in)
    - No state (stateless)
    - Deterministic (same input → same output)

    Subclasses implement:
    - is_applicable(): Filter logic based on instrument
    - calculate_batch(): Vectorized calculation
    """

    @abstractmethod
    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if component applies to this instrument.

        Args:
            instrument: Instrument object with metadata

        Returns:
            True if component should be applied

        Examples:
            # YTM only for Fixed Income
            return instrument.underlying_type in ['FIXED INCOME', 'MONEY MARKET']

            # Dividend only for DIST/INC
            return instrument.payment_policy in ['DIST', 'INC']

            # TER only if data available
            return instrument.id in self.ters
        """
        pass

    @abstractmethod
    def calculate_batch(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: list[date],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate adjustments for multiple instruments (vectorized).

        Args:
            instruments: Dict mapping instrument_id → Instrument object
            dates: List of dates
            prices: DataFrame(dates × instruments) - instrument prices
            fx_prices: DataFrame(dates × currencies) - FX rates (EUR base)

        Returns:
            DataFrame(dates × instruments) with adjustments

        Note:
            - Use instruments.keys() for instrument IDs
            - Filter by is_applicable(instrument) internally
            - Return 0.0 for non-applicable instruments
            - Handle missing data gracefully (log warning, return 0.0)
        """
        pass

    def _safe_calculate(
        self,
        instrument: InstrumentProtocol,
        dates: list[date],
        calculation_func,
    ) -> pd.Series:
        """
        Safe wrapper for calculations with error handling.

        Args:
            instrument: Instrument being calculated
            dates: Dates for calculation
            calculation_func: Function that returns Series

        Returns:
            Series with adjustments (zeros on error)
        """
        try:
            return calculation_func()
        except Exception as e:
            logger.warning(
                f"{self.__class__.__name__} failed for {instrument.id}: {e}",
                exc_info=True
            )
            return pd.Series(0.0, index=dates)