"""
Return calculation logic centralized in one class.

Handles different return types (percentage, logarithmic, absolute) and provides
consistent methods for calculating returns, accumulating them, and reconstructing prices.
"""
from enum import Enum
from typing import Literal
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class ReturnType(Enum):
    """Supported return calculation types"""
    PERCENTAGE = "percentage"
    LOGARITHMIC = "logarithmic"
    ABSOLUTE = "absolute"


class ReturnCalculator:
    """
    Centralizes all return calculation logic.

    Handles:
    - Return calculation from prices
    - Return accumulation over time
    - Price reconstruction from returns
    - Conversion between return types (if needed in future)

    Usage:
        # Create calculator
        calc = ReturnCalculator(return_type="percentage")

        # Calculate returns
        returns = calc.calculate_returns(prices)

        # Accumulate returns
        cumulative = calc.accumulate_returns(returns)

        # Reconstruct prices
        prices = calc.returns_to_prices(returns, initial_price)
    """

    def __init__(self, return_type: Literal["percentage", "logarithmic", "absolute"] = "percentage"):
        """
        Initialize return calculator.

        Args:
            return_type: Type of returns to calculate
                        - "percentage": (P_t - P_{t-1}) / P_{t-1}
                        - "logarithmic": log(P_t / P_{t-1})
                        - "absolute": P_t - P_{t-1}
        """
        self.return_type = ReturnType(return_type)
        logger.debug(f"ReturnCalculator initialized with type={self.return_type.value}")

    def calculate_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate returns from prices.

        Args:
            prices: DataFrame(dates × instruments) with prices

        Returns:
            DataFrame(dates × instruments) with returns
            First row will be NaN (no previous price)

        Formula:
            - Percentage: (P_t - P_{t-1}) / P_{t-1}
            - Logarithmic: log(P_t / P_{t-1})
            - Absolute: P_t - P_{t-1}
        """
        if self.return_type == ReturnType.PERCENTAGE:
            return prices.pct_change(fill_method=None)
        elif self.return_type == ReturnType.LOGARITHMIC:
            return np.log(prices / prices.shift(1))
        elif self.return_type == ReturnType.ABSOLUTE:
            return prices - prices.shift(1)
        else:
            raise ValueError(f"Return type {self.return_type.value} not supported")

    def accumulate_returns(self, returns: pd.DataFrame) -> pd.DataFrame:
        """
        Accumulate returns over time to get cumulative returns.

        Args:
            returns: DataFrame(dates × instruments) with returns

        Returns:
            DataFrame(dates × instruments) with cumulative returns

        Formula:
            - Percentage: (1 + r).cumprod() - 1
            - Logarithmic: r.cumsum()
            - Absolute: r.cumsum()

        Note:
            For percentage returns, cumulative return represents total return
            from start: (P_t - P_0) / P_0
        """
        if self.return_type == ReturnType.PERCENTAGE:
            return (1 + returns).cumprod() - 1
        elif self.return_type == ReturnType.LOGARITHMIC:
            return returns.cumsum()
        elif self.return_type == ReturnType.ABSOLUTE:
            return returns.cumsum()
        else:
            raise ValueError(f"Return type {self.return_type.value} not supported")

    def returns_to_prices(
        self,
        returns: pd.DataFrame,
        initial_price: pd.Series
    ) -> pd.DataFrame:
        """
        Reconstruct prices from returns.

        Args:
            returns: DataFrame(dates × instruments) with returns
            initial_price: Series(instruments) with starting prices

        Returns:
            DataFrame(dates × instruments) with reconstructed prices

        Formula:
            - Percentage: (1 + r).cumprod() * P_0
            - Logarithmic: exp(r.cumsum()) * P_0
            - Absolute: r.cumsum() + P_0

        Note:
            First row should have return = 0 to ensure P_0 is preserved
        """
        if self.return_type == ReturnType.PERCENTAGE:
            return (1 + returns).cumprod() * initial_price
        elif self.return_type == ReturnType.LOGARITHMIC:
            return np.exp(returns.cumsum()) * initial_price
        elif self.return_type == ReturnType.ABSOLUTE:
            return returns.cumsum() + initial_price
        else:
            raise ValueError(f"Return type {self.return_type.value} not supported")

    def __repr__(self) -> str:
        return f"ReturnCalculator(type={self.return_type.value})"
