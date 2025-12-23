"""
Base component for adjustments with builder pattern support.
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
    Abstract base for adjustment components with builder pattern.

    Components are PURE CALCULATIONS:
    - No I/O (data passed in __init__)
    - No state (stateless)
    - Deterministic (same input → same output)

    Subclasses implement:
    - is_applicable(): Filter logic based on instrument
    - calculate_adjustment(): Vectorized calculation
    
    Builder Pattern:
        Components can be chained for fluent API:
        
        chain = (
            TerComponent(ter)
            .add(FxSpotComponent(weights, fx_prices))
            .add(FxForwardCarryComponent(...))
            .add(DividendComponent(divs))
        )
        adjuster.add_chain(chain)
    
    Attributes:
        target: Optional set of instrument IDs to apply adjustments to
        _children: List of child components for builder pattern
    """
    
    def __init__(self, target: Optional[List[str]] = None):
        """
        Initialize component.
        
        Args:
            target: Optional list of instrument IDs to restrict adjustments.
                   If None, applies to all applicable instruments.
        """
        self.target = set(target) if target is not None else None
        self._children: List['Component'] = []  # For builder pattern

    @abstractmethod
    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if component applies to this instrument (domain logic only).

        Args:
            instrument: Instrument object with metadata

        Returns:
            True if component should be applied based on instrument characteristics

        Examples:
            # YTM only for Fixed Income
            return instrument.underlying_type in ['FIXED INCOME', 'MONEY MARKET']

            # Dividend only for DIST/INC
            return instrument.payment_policy in ['DIST', 'INC']
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
        # Check domain logic first
        if not self.is_applicable(instrument):
            return False
        
        # If no target filter, apply to all applicable
        if self.target is None:
            return True
        
        # Check if in target set
        return instrument.id in self.target

    @abstractmethod
    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate adjustments (vectorized).

        NEW SIGNATURE: No fx_prices parameter!
        Each component stores its own data dependencies.

        Args:
            instruments: Dict[instrument_id → Instrument object]
            dates: List of dates (date or datetime objects)
            prices: DataFrame(dates × instruments) with prices

        Returns:
            DataFrame(dates × instruments) with adjustments

        Notes:
            - MUST use self._normalize_dates(dates) as first line
            - MUST use self.should_apply(inst) for filtering
            - Handle date mismatches gracefully
            - Return 0.0 for non-applicable instruments
            - Include comprehensive logging
        """
        pass

    # ========================================================================
    # BUILDER PATTERN METHODS
    # ========================================================================

    def add(self, component: 'Component') -> 'Component':
        """
        Add a child component (builder pattern).
        
        Returns the PARENT (self) for continued chaining.
        
        Args:
            component: Child component to add
        
        Returns:
            self (parent) for chaining
        
        Example:
            chain = (
                TerComponent(ter)
                .add(FxSpotComponent(weights, fx_prices))
                .add(FxForwardCarryComponent(...))
                .add(DividendComponent(divs))
            )
            
            adjuster.add_chain(chain)
        """
        self._children.append(component)
        logger.debug(f"{self.__class__.__name__}.add({component.__class__.__name__})")
        return self  # Return parent for chaining

    def get_chain(self) -> List['Component']:
        """
        Get flattened list of all components in chain.
        
        Recursively collects self + all children + their children.
        
        Returns:
            List of all components in the chain
        
        Example:
            # After: ter.add(fx).add(div)
            chain = ter.get_chain()
            # Returns: [ter, fx, div]
        """
        result = [self]  # Start with self
        
        # Recursively add all children
        for child in self._children:
            result.extend(child.get_chain())
        
        return result

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================

    @staticmethod
    def _normalize_dates(dates: Union[List[date], List[datetime]]) -> List[datetime]:
        """
        Normalize dates to datetime objects.
        
        MANDATORY: Call this as FIRST LINE in calculate_adjustment().
        
        Args:
            dates: List of date or datetime objects
        
        Returns:
            List of datetime objects
        
        Example:
            def calculate_adjustment(self, instruments, dates, prices):
                # FIRST LINE - MANDATORY
                dates_dt = self._normalize_dates(dates)
                
                # Now use dates_dt everywhere
                result = pd.DataFrame(0.0, index=dates_dt, columns=...)
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
                    f"Expected date or datetime, got {type(d)}. "
                    "All dates must be date or datetime objects."
                )
        
        return normalized

    def update_data(self, **kwargs) -> None:
        """
        Update component data for live mode.
        
        Base implementation: no-op (components are stateless by default).
        Override in subclasses that store mutable data (e.g., prices, fx_prices).
        
        Args:
            **kwargs: Component-specific data to update
        
        Example:
            # In FxSpotComponent
            def update_data(self, **kwargs):
                if 'fx_prices' in kwargs:
                    self.fx_prices = kwargs['fx_prices']
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
