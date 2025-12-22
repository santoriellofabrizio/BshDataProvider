"""
Input validation and inference utilities for adjustment components.

Provides centralized validation, automatic format detection, and transformation
for DataFrame inputs to ensure consistency across all components.
"""
from datetime import date, datetime
from typing import Literal, Optional, List, Union
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class DataFrameValidator:
    """
    Centralized validation and inference for adjustment data.

    Handles:
    - Automatic orientation detection (dates×items vs items×dates)
    - Auto-transposition to expected format
    - DatetimeIndex conversion
    - Missing data validation
    - Type checking

    Usage:
        # In component __init__
        self.data = DataFrameValidator.validate_timeseries(
            data,
            name="DividendComponent.dividends",
            expected_format="dates_x_items",
            auto_transpose=True
        )
    """

    @staticmethod
    def validate_timeseries(
            df: pd.DataFrame,
            name: str,
            expected_format: Literal["dates_x_items", "items_x_dates"] = "dates_x_items",
            auto_transpose: bool = True,
            allow_empty: bool = False,
    ) -> pd.DataFrame:
        """
        Validate and optionally transpose time-series DataFrame.

        Args:
            df: Input DataFrame
            name: Name for logging/errors (e.g., "DividendComponent.dividends")
            expected_format: Expected orientation
                - "dates_x_items": rows=dates, columns=instruments/currencies
                - "items_x_dates": rows=instruments/currencies, columns=dates
            auto_transpose: If True, auto-transpose if orientation is wrong
            allow_empty: If True, allow empty DataFrames

        Returns:
            Validated DataFrame in expected format with DatetimeIndex

        Raises:
            TypeError: If df is not a DataFrame
            ValueError: If DataFrame invalid or can't be fixed

        Example:
            >>> data = pd.DataFrame({
            ...     'INST_A': [1.0, 2.0],
            ...     'INST_B': [3.0, 4.0]
            ... }, index=['2024-01-01', '2024-01-02'])
            >>> validated = DataFrameValidator.validate_timeseries(
            ...     data, "test", "dates_x_items"
            ... )
            >>> validated.index  # DatetimeIndex
        """
        # Type check
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{name}: Must be DataFrame, got {type(df).__name__}")

        # Empty check
        if df.empty:
            if allow_empty:
                logger.warning(f"{name}: DataFrame is empty")
                return df
            else:
                raise ValueError(f"{name}: DataFrame is empty")

        # Infer current orientation
        detected_format = DataFrameValidator._infer_orientation(df, name)

        logger.debug(
            f"{name}: Detected format={detected_format}, expected={expected_format}"
        )

        # Check if orientation matches
        if detected_format == expected_format:
            # Already correct format
            logger.debug(f"{name}: Format OK ({expected_format})")
            validated = df.copy()

        elif auto_transpose:
            # Transpose to expected format
            logger.info(
                f"{name}: Auto-transposing from {detected_format} to {expected_format}"
            )
            validated = df.T.copy()

        else:
            raise ValueError(
                f"{name}: Wrong orientation. "
                f"Expected: {expected_format}, Got: {detected_format}. "
                "Set auto_transpose=True to fix automatically."
            )

        # Ensure DatetimeIndex (based on expected format)
        if expected_format == "dates_x_items":
            validated = DataFrameValidator._ensure_datetime_index(validated, name)

        # Validate shape
        if validated.shape[0] == 0:
            raise ValueError(f"{name}: No rows after validation")
        if validated.shape[1] == 0:
            raise ValueError(f"{name}: No columns after validation")

        logger.info(
            f"{name}: Validated successfully. Shape: {validated.shape} "
            f"({len(validated)} dates × {len(validated.columns)} items)"
        )

        return validated

    @staticmethod
    def validate_composition(
            df: pd.DataFrame,
            name: str,
            expected_columns: Optional[List[str]] = None,
            expected_index: Optional[List[str]] = None,
            allow_missing_columns: bool = True,
            allow_missing_index: bool = True,
    ) -> pd.DataFrame:
        """
        Validate composition DataFrame (instruments × attributes).

        Used for static mapping data like FX composition, FX forward composition, etc.

        Args:
            df: Input DataFrame (instruments × attributes)
            name: Name for logging/errors
            expected_columns: Optional list of expected attribute columns
            expected_index: Optional list of expected instrument IDs
            allow_missing_columns: If False, raise error if columns missing
            allow_missing_index: If False, raise error if index items missing

        Returns:
            Validated DataFrame

        Raises:
            TypeError: If df is not a DataFrame
            ValueError: If validation fails

        Example:
            >>> composition = pd.DataFrame({
            ...     'USD': [0.65, 0.60],
            ...     'GBP': [0.10, 0.15],
            ... }, index=['INST_A', 'INST_B'])
            >>> validated = DataFrameValidator.validate_composition(
            ...     composition,
            ...     "FxSpotComponent.fx_composition",
            ...     expected_columns=['USD', 'GBP', 'EUR']
            ... )
        """
        # Type check
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{name}: Must be DataFrame, got {type(df).__name__}")

        if df.empty:
            raise ValueError(f"{name}: DataFrame is empty")

        validated = df.copy()

        # Validate expected columns (attributes)
        if expected_columns:
            missing_cols = set(expected_columns) - set(validated.columns)
            if missing_cols:
                if allow_missing_columns:
                    logger.warning(
                        f"{name}: Missing columns: {sorted(missing_cols)}. "
                        f"Available: {sorted(validated.columns)}"
                    )
                else:
                    raise ValueError(
                        f"{name}: Missing required columns: {sorted(missing_cols)}. "
                        f"Available: {sorted(validated.columns)}"
                    )

        # Validate expected index (instruments)
        if expected_index:
            missing_idx = set(expected_index) - set(validated.index)
            if missing_idx:
                if allow_missing_index:
                    logger.warning(
                        f"{name}: Missing instruments in index: {sorted(missing_idx)}. "
                        f"Available: {sorted(validated.index)}"
                    )
                else:
                    raise ValueError(
                        f"{name}: Missing required instruments: {sorted(missing_idx)}. "
                        f"Available: {sorted(validated.index)}"
                    )

        logger.info(
            f"{name}: Validated composition. "
            f"Shape: {validated.shape} "
            f"({len(validated)} instruments × {len(validated.columns)} attributes)"
        )

        return validated

    @staticmethod
    def validate_series(
            series: pd.Series,
            name: str,
            expected_index: Optional[List[str]] = None,
            allow_missing: bool = True,
    ) -> pd.Series:
        """
        Validate Series (instrument → value mapping).

        Used for static data like TERs, future_currencies, etc.

        Args:
            series: Input Series
            name: Name for logging/errors
            expected_index: Optional list of expected instruments
            allow_missing: If False, raise error if instruments missing

        Returns:
            Validated Series

        Raises:
            TypeError: If not a Series
            ValueError: If validation fails

        Example:
            >>> ters = pd.Series({'INST_A': 0.002, 'INST_B': 0.0015})
            >>> validated = DataFrameValidator.validate_series(
            ...     ters,
            ...     "TerComponent.ters",
            ...     expected_index=['INST_A', 'INST_B', 'INST_C']
            ... )
        """
        # Type check
        if not isinstance(series, pd.Series):
            raise TypeError(f"{name}: Must be Series, got {type(series).__name__}")

        if series.empty:
            raise ValueError(f"{name}: Series is empty")

        validated = series.copy()

        # Validate expected index
        if expected_index:
            missing = set(expected_index) - set(validated.index)
            if missing:
                if allow_missing:
                    logger.warning(
                        f"{name}: Missing instruments: {sorted(missing)}. "
                        f"Available: {sorted(validated.index)}"
                    )
                else:
                    raise ValueError(
                        f"{name}: Missing required instruments: {sorted(missing)}. "
                        f"Available: {sorted(validated.index)}"
                    )

        logger.info(f"{name}: Validated Series with {len(validated)} entries")

        return validated

    @staticmethod
    def _infer_orientation(
            df: pd.DataFrame,
            name: str
    ) -> Literal["dates_x_items", "items_x_dates"]:
        """
        Infer DataFrame orientation using multiple heuristics.

        Heuristics (in order):
        1. Index is DatetimeIndex → dates_x_items
        2. Columns are DatetimeIndex → items_x_dates
        3. Index[0] is date/datetime → dates_x_items
        4. Columns[0] is date/datetime → items_x_dates
        5. Index is parseable as dates → dates_x_items
        6. Columns are parseable as dates → items_x_dates
        7. Fallback: dates_x_items (most common)

        Args:
            df: Input DataFrame
            name: Name for logging

        Returns:
            Detected orientation
        """
        # Heuristic 1: DatetimeIndex
        if isinstance(df.index, pd.DatetimeIndex):
            logger.debug(f"{name}: Index is DatetimeIndex → dates_x_items")
            return "dates_x_items"

        if isinstance(df.columns, pd.DatetimeIndex):
            logger.debug(f"{name}: Columns are DatetimeIndex → items_x_dates")
            return "items_x_dates"

        # Heuristic 2: First element type
        if len(df.index) > 0:
            first_idx = df.index[0]
            if isinstance(first_idx, (date, datetime, pd.Timestamp)):
                logger.debug(f"{name}: Index[0] is datetime → dates_x_items")
                return "dates_x_items"

        if len(df.columns) > 0:
            first_col = df.columns[0]
            if isinstance(first_col, (date, datetime, pd.Timestamp)):
                logger.debug(f"{name}: Columns[0] is datetime → items_x_dates")
                return "items_x_dates"

        # Heuristic 3: Try parsing as dates
        try:
            pd.to_datetime(df.index)
            logger.debug(f"{name}: Index parseable as dates → dates_x_items")
            return "dates_x_items"
        except:
            pass

        try:
            pd.to_datetime(df.columns)
            logger.debug(f"{name}: Columns parseable as dates → items_x_dates")
            return "items_x_dates"
        except:
            pass

        # Heuristic 4: Shape heuristic
        # If significantly more columns than rows, likely items_x_dates
        if df.shape[1] > df.shape[0] * 3:
            logger.debug(
                f"{name}: Many more columns than rows ({df.shape}) → items_x_dates"
            )
            return "items_x_dates"

        # Fallback: Most common format
        logger.warning(
            f"{name}: Could not definitively infer orientation for DataFrame {df.shape}. "
            "Assuming dates_x_items (most common). "
            "Use explicit expected_format if this is wrong."
        )
        return "dates_x_items"

    @staticmethod
    def _ensure_datetime_index(df: pd.DataFrame, name: str) -> pd.DataFrame:
        """
        Convert DataFrame index to DatetimeIndex if not already.

        Args:
            df: Input DataFrame
            name: Name for logging

        Returns:
            DataFrame with DatetimeIndex

        Raises:
            ValueError: If index cannot be converted to DatetimeIndex
        """
        if isinstance(df.index, pd.DatetimeIndex):
            logger.debug(f"{name}: Index already DatetimeIndex")
            return df

        try:
            df_copy = df.copy()
            df_copy.index = pd.to_datetime(df_copy.index)
            logger.debug(f"{name}: Converted index to DatetimeIndex")
            return df_copy
        except Exception as e:
            raise ValueError(
                f"{name}: Index must be dates or convertible to DatetimeIndex. "
                f"Got type: {type(df.index[0]).__name__ if len(df.index) > 0 else 'empty'}. "
                f"Error: {e}"
            )

    @staticmethod
    def check_missing_data(
            df: pd.DataFrame,
            name: str,
            max_missing_pct: float = 10.0,
            warn_only: bool = True,
    ) -> pd.DataFrame:
        """
        Check for missing data in DataFrame and optionally warn/error.

        Args:
            df: Input DataFrame
            name: Name for logging
            max_missing_pct: Maximum allowed percentage of missing values
            warn_only: If True, only warn; if False, raise error

        Returns:
            Original DataFrame (unchanged)

        Raises:
            ValueError: If missing data exceeds threshold and warn_only=False
        """
        total_cells = df.size
        missing_cells = df.isna().sum().sum()
        missing_pct = 100.0 * missing_cells / total_cells if total_cells > 0 else 0.0

        if missing_pct > max_missing_pct:
            msg = (
                f"{name}: {missing_cells} ({missing_pct:.1f}%) missing values. "
                f"Threshold: {max_missing_pct:.1f}%"
            )

            if warn_only:
                logger.warning(msg)
            else:
                raise ValueError(msg)

        elif missing_cells > 0:
            logger.debug(
                f"{name}: {missing_cells} ({missing_pct:.1f}%) missing values (OK)"
            )

        return df

    @staticmethod
    def validate_numeric_data(
            df: pd.DataFrame,
            name: str,
            allow_negative: bool = True,
            allow_zero: bool = True,
            allow_inf: bool = False,
    ) -> pd.DataFrame:
        """
        Validate that DataFrame contains valid numeric data.

        Args:
            df: Input DataFrame
            name: Name for logging
            allow_negative: If False, raise error if negative values found
            allow_zero: If False, raise error if zero values found
            allow_inf: If False, raise error if inf/-inf values found

        Returns:
            Original DataFrame (unchanged)

        Raises:
            ValueError: If validation fails
        """
        # Check for inf/-inf
        if not allow_inf:
            inf_mask = np.isinf(df.select_dtypes(include=[np.number]))
            inf_count = inf_mask.sum().sum()
            if inf_count > 0:
                raise ValueError(
                    f"{name}: Contains {inf_count} inf/-inf values (not allowed)"
                )

        # Check for negative values
        if not allow_negative:
            numeric_cols = df.select_dtypes(include=[np.number])
            neg_mask = numeric_cols < 0
            neg_count = neg_mask.sum().sum()
            if neg_count > 0:
                raise ValueError(
                    f"{name}: Contains {neg_count} negative values (not allowed)"
                )

        # Check for zero values
        if not allow_zero:
            numeric_cols = df.select_dtypes(include=[np.number])
            zero_mask = numeric_cols == 0
            zero_count = zero_mask.sum().sum()
            if zero_count > 0:
                logger.warning(
                    f"{name}: Contains {zero_count} zero values (check if expected)"
                )

        logger.debug(f"{name}: Numeric data validation passed")

        return df


# Convenience functions for common patterns

def validate_dividend_data(dividends: pd.DataFrame) -> pd.DataFrame:
    """Validate dividend data (dates × instruments)."""
    return DataFrameValidator.validate_timeseries(
        dividends,
        name="dividends",
        expected_format="dates_x_items",
        auto_transpose=True,
    )


def validate_ytm_data(ytm: pd.DataFrame) -> pd.DataFrame:
    """Validate YTM data (dates × instruments)."""
    return DataFrameValidator.validate_timeseries(
        ytm,
        name="ytm",
        expected_format="dates_x_items",
        auto_transpose=True,
    )


def validate_fx_composition(fx_composition: pd.DataFrame) -> pd.DataFrame:
    """Validate FX composition (instruments × currencies)."""
    return DataFrameValidator.validate_composition(
        fx_composition,
        name="fx_composition",
        allow_missing_columns=True,
        allow_missing_index=True,
    )


def validate_fx_prices(fx_prices: pd.DataFrame) -> pd.DataFrame:
    """Validate FX prices (dates × currencies)."""
    return DataFrameValidator.validate_timeseries(
        fx_prices,
        name="fx_prices",
        expected_format="dates_x_items",
        auto_transpose=True,
    )


def validate_ter_data(ters: Union[dict, pd.Series]) -> pd.Series:
    """Validate TER data (instrument → value)."""
    if isinstance(ters, dict):
        ters = pd.Series(ters)

    return DataFrameValidator.validate_series(
        ters,
        name="ters",
        allow_missing=True,
    )