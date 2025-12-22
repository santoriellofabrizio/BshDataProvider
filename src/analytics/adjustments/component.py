"""
Base component for adjustments.
"""
from abc import ABC, abstractmethod
from datetime import date, datetime
import pandas as pd
import logging
from typing import Union, List, Optional

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
    - calculate_adjustment(): Vectorized calculation
    
    Attributes:
        target: Optional set of instrument IDs to apply adjustments to.
                If None, applies to all applicable instruments.
    """
    
    def __init__(self, target: Optional[List[str]] = None):
        """
        Initialize component.
        
        Args:
            target: Optional list of instrument IDs to restrict adjustments.
                   If None, applies to all applicable instruments.
                   If provided, only instruments in both target and applicable set
                   will receive adjustments.
        """
        self.target = set(target) if target is not None else None

    @abstractmethod
    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if component applies to this instrument (domain logic only).

        Args:
            instrument: Instrument object with metadata

        Returns:
            True if component should be applied based on instrument characteristics
            (ignores target filter - that's handled by should_apply)

        Examples:
            # YTM only for Fixed Income
            return instrument.underlying_type in ['FIXED INCOME', 'MONEY MARKET']

            # Dividend only for DIST/INC
            return instrument.payment_policy in ['DIST', 'INC']

            # TER only if data available
            return instrument.id in self.ters
        """
        pass

    def should_apply(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if component should apply to this instrument (domain + target filter).
        
        Combines is_applicable() with target filter.
        Use this in calculate_adjustment(), NOT is_applicable() directly.
        
        Args:
            instrument: Instrument to check
        
        Returns:
            True if component should be applied (passes both filters)
        """
        # Target filter first (cheaper check)
        if self.target is not None and instrument.id not in self.target:
            return False
        
        # Then component-specific domain logic
        return self.is_applicable(instrument)
    
    @abstractmethod
    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate adjustments for multiple instruments (vectorized).

        Args:
            instruments: Dict mapping instrument_id → Instrument object
            dates: List of dates or datetimes
                   ALL dates are normalized to datetime (midnight if date)
            prices: DataFrame(dates × instruments) - instrument prices
            fx_prices: DataFrame(dates × currencies) - FX rates (EUR base)

        Returns:
            DataFrame(dates × instruments) with adjustments

        Implementation Pattern:
            # 1. Normalize dates to datetime
            dates_dt = self._normalize_dates(dates)
            
            # 2. Filter applicable instruments (USE should_apply, NOT is_applicable)
            applicable_ids = [
                inst.id for inst in instruments.values()
                if self.should_apply(inst)
            ]
            
            # 3. Validate data availability
            if not applicable_ids:
                logger.debug("No applicable instruments")
                return pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))
            
            # 4. Calculate adjustments
            # ... your logic ...
            
            # 5. Return result
            return result
        """
        pass

    def _safe_calculate(
        self,
        instrument: InstrumentProtocol,
        dates: Union[List[date], List[datetime]],
        calculation_func,
    ) -> pd.Series:
        """
        Safe wrapper for calculations with error handling.

        Args:
            instrument: Instrument being calculated
            dates: Dates or datetimes for calculation
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
            dates_dt = self._normalize_dates(dates)
            return pd.Series(0.0, index=dates_dt)
    
    # ========================================================================
    # UTILITY METHODS - Parsing, validation, normalization
    # ========================================================================
    
    @staticmethod
    def _normalize_dates(dates: Union[List[date], List[datetime]]) -> List[datetime]:
        """
        Normalize all dates to datetime objects (midnight if date).
        
        Args:
            dates: List of date or datetime objects
        
        Returns:
            List of datetime objects
        
        Example:
            date(2024, 1, 15) → datetime(2024, 1, 15, 0, 0, 0)
            datetime(2024, 1, 15, 16, 30) → datetime(2024, 1, 15, 16, 30)
        """
        normalized = []
        for d in dates:
            if isinstance(d, datetime):
                normalized.append(d)
            elif isinstance(d, date):
                # Convert date to datetime at midnight
                normalized.append(datetime.combine(d, datetime.min.time()))
            elif isinstance(d, pd.Timestamp):
                normalized.append(d.to_pydatetime())
            else:
                raise TypeError(
                    f"Expected date or datetime, got {type(d).__name__}: {d}"
                )
        return normalized
