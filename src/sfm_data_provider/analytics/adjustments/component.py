"""
Base component for adjustments with builder pattern support.
"""
from abc import ABC, abstractmethod
from datetime import date, datetime
import pandas as pd
import logging
from typing import Union, List, Optional, TYPE_CHECKING

from sfm_data_provider.core.instruments.instruments import Instrument

if TYPE_CHECKING:
    from sfm_data_provider.analytics.adjustments.return_calculations import ReturnCalculator

logger = logging.getLogger(__name__)


class Component(ABC):
    """
    Abstract base for adjustment components with builder pattern.

    Components are PURE CALCULATIONS:
    - No I/O (data passed in __init__)
    - No state (stateless)
    - Deterministic (same input -> same output)

    Subclasses implement:
    - is_applicable(): Filter logic based on instrument
    - calculate_adjustment(): Vectorized calculation
    
    Builder Pattern:
        Components can be chained for fluent API:
        
        chain = (
            TerComponent(ter)
            .add(FxSpotComponent(weights, fx_prices_intraday))
            .add(FxForwardCarryComponent(...))
            .add(DividendComponent(divs))
        )
        intraday_adjuster.add_chain(chain)
    
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
        self._return_calculator: Optional['ReturnCalculator'] = None  # Injected by Adjuster
        self.is_intraday = None

    def is_updatable(self) -> bool:
        """
        Check if this component can receive data updates.

        Returns:
            True if component supports updates (has updatable_fields property)

        Note:
            Default is False. Override to return True in updatable components.
        """
        return False

    @abstractmethod
    def is_applicable(self, instrument: Instrument) -> bool:
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

    def should_apply(self, instrument: Instrument) -> bool:
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

    @staticmethod
    def validate_input(instruments: dict[str, Instrument], dates: Union[List[date], List[datetime]]) -> None:
        if not instruments:
            raise ValueError("instruments cannot be empty")
        if not dates:
            raise ValueError("dates cannot be empty")

    @staticmethod
    def validate_output(result: pd.DataFrame) -> None:
        """
        Validate output data, raise ValueError if invalid.

        Default implementation checks for basic sanity.
        Override for component-specific validation.

        Args:
            result: DataFrame with calculated adjustments

        Raises:
            ValueError: If validation fails
        """
        if result.empty:
            raise ValueError("Result DataFrame is empty")
        if result.isna().all().all():
            raise ValueError("Result contains only NaN values")

    @abstractmethod
    def calculate_adjustment(
            self,
            instruments: dict[str, Instrument],
            dates: Union[List[date], List[datetime]],
    ) -> pd.DataFrame:
        """
        Calculate adjustments (vectorized).

        NEW SIGNATURE: No fx_prices_intraday parameter!
        Each component stores its own data dependencies.

        Args:
            instruments: Dict[instrument_id -> Instrument object]
            dates: List of dates (date or datetime objects)

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
    # RETURN CALCULATOR INJECTION
    # ========================================================================

    def set_return_calculator(self, calculator: 'ReturnCalculator') -> None:
        """
        Inject return calculator (called by Adjuster.add()).

        Args:
            calculator: ReturnCalculator instance from parent Adjuster

        Note:
            This is called automatically when component is added to Adjuster.
            Components should not call this directly.
        """
        self._return_calculator = calculator
        logger.debug(f"{self.__class__.__name__}: ReturnCalculator injected (type={calculator.return_type.value})")

    @property
    def return_calculator(self) -> 'ReturnCalculator':
        """
        Access to return calculator.

        Returns:
            ReturnCalculator instance

        Raises:
            RuntimeError: If calculator not set (component not added to Adjuster)

        Usage:
            # In component's calculate_adjustment()
            returns = self.return_calculator.calculate_returns(prices)
        """
        if self._return_calculator is None:
            raise RuntimeError(
                f"{self.__class__.__name__}: ReturnCalculator not set. "
                "Component must be added to Adjuster via .add()"
            )
        return self._return_calculator

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
                .add(FxSpotComponent(weights, fx_prices_intraday))
                .add(FxForwardCarryComponent(...))
                .add(DividendComponent(divs))
            )

            intraday_adjuster.add_chain(chain)
        """
        self._children.append(component)
        logger.debug(f"{self.__class__.__name__}.add({component.__class__.__name__})")
        return self  # Return parent for chaining

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
        return pd.to_datetime(dates).to_pydatetime().tolist()

    def append_data(self, **kwargs) -> None:
        """
        Append new data permanently (implemented by updatable components).

        Base implementation: no-op (components without updatable data).
        Override in updatable components with explicit parameters.

        Args:
            **kwargs: Component-specific data

        Example:
            # In FxSpotComponent
            def append_data(self, *, fx_prices_intraday: Optional[pd.DataFrame] = None):
                if fx_prices_intraday is not None:
                    self._fx_prices = pd.concat([self._fx_prices, fx_prices_intraday])
        """
        pass

    def save_state(self) -> dict:
        """
        Save current state for restoration (implemented by updatable components).

        Used by Adjuster's live_update context manager to save/restore state.

        Returns:
            dict: Saved state (component-specific structure)

        Example:
            # In FxSpotComponent
            def _save_state(self) -> dict:
                return {'fx_prices_intraday': self._fx_prices.copy()}
        """
        return {}

    def restore_state(self, state: dict) -> None:
        """
        Restore saved state (implemented by updatable components).

        Used by Adjuster's live_update context manager to restore original state.

        Args:
            state: Previously saved state from _save_state()

        Example:
            # In FxSpotComponent
            def _restore_state(self, state: dict) -> None:
                self._fx_prices = state['fx_prices_intraday']
        """
        pass

    def apply_temp_data(self, **kwargs) -> None:
        """
        Temporarily modify data without permanent storage.

        Used by Adjuster's live_update context manager to apply temporary data.
        State will be automatically restored after calculation.

        Args:
            **kwargs: Component-specific temporary data

        Example:
            # In FxSpotComponent
            def _apply_temp_data(self, *, fx_prices_intraday: Optional[pd.DataFrame] = None):
                if fx_prices_intraday is not None:
                    # Temporarily extend fx_prices_intraday
                    self._fx_prices = pd.concat([self._fx_prices, fx_prices_intraday])
        """
        pass

    @property
    def updatable_fields(self) -> set[str]:
        """
        Declare updatable fields (subscription model).

        Base implementation returns empty set (not updatable).
        Override in updatable components.

        Returns:
            Set of field names this component can update
        """
        return set()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
