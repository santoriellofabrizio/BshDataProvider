"""
Date utilities for adjustments.

Note: For business day calculations, use HolidayManager from bshdata.core.utils
"""
import logging
from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_year_fractions(
        business_dates: list[date] | pd.DatetimeIndex,
        shifted: bool = False,
        settlement_days: int = 2,
        number_of_days_in_year: int = 365,
) -> pd.Series:
    """
    Calculate year fractions for pro-rata calculations.

    Args:
        number_of_days_in_year: number of days in a year
        business_dates: List of dates or DatetimeIndex
        shifted: If True, use shifted fractions (for settlement lag)
        settlement_days: Settlement lag (T+1=1, T+2=2)

    Returns:
        Series(dates) with year fractions

    Formula:
        Standard: (date - prev_date).days / 365
        Shifted: (next_date - date).days / 365  (for T+N settlement)

    Example:
        # Standard fractions
        dates = pd.date_range('2024-01-01', '2024-01-10')
        fractions = calculate_year_fractions(dates)

        # Shifted for T+2 settlement (e.g., for YTM)
        fractions_shifted = calculate_year_fractions(dates, shifted=True, settlement_days=2)
    """
    if isinstance(business_dates, pd.DatetimeIndex):
        dates_idx = business_dates.sort_values()
        if len(dates_idx) == 0:
            return pd.Series(dtype=float)
    else:
        if not business_dates:
            return pd.Series(dtype=float)
        dates_idx = pd.DatetimeIndex(sorted(business_dates))

    n = len(dates_idx)

    if shifted:
        days = np.ones(n, dtype=float)
        valid_n = n - settlement_days
        if valid_n > 0:
            next_dates = dates_idx[settlement_days:]
            prev_dates = dates_idx[settlement_days - 1: n - 1]
            days[:valid_n] = (next_dates - prev_dates).days
        fractions = pd.Series(days / number_of_days_in_year, index=dates_idx)
    else:
        days = np.zeros(n, dtype=float)
        if n > 1:
            days[1:] = (dates_idx[1:] - dates_idx[:-1]).days
        fractions = pd.Series(days / number_of_days_in_year, index=dates_idx)

    return fractions


def normalize_fx_columns(fx_prices: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize FX price columns from tickers to currency codes.

    Converts:
        EURUSD -> USD
        EURGBP -> GBP
        EURJPY -> JPY
        USD    -> USD (unchanged, warning)
        USDEUR -> USD (inverted, warning)

    Args:
        fx_prices: DataFrame with FX prices

    Returns:
        DataFrame with normalized column names and inverted prices where needed
    """
    if isinstance(fx_prices, pd.Series):
        fx_prices = fx_prices.to_frame()
    normalized_columns = {}
    columns_to_invert = []  # Track which columns need 1/price

    for col in fx_prices.columns:
        col_str = str(col)

        # Case 1: 6-char EUR-based ticker (EURUSD, EURGBP, etc.)
        if len(col_str) == 6 and col_str.startswith('EUR'):
            # Extract quote currency (last 3 chars)
            currency = col_str[-3:]
            normalized_columns[col] = currency
            logger.debug(f"Normalized FX column: {col} -> {currency}")

        # Case 2: 6-char inverted ticker (USDEUR, GBPEUR, etc.)
        elif len(col_str) == 6 and col_str.endswith('EUR'):
            # Extract base currency (first 3 chars)
            currency = col_str[:3]
            normalized_columns[col] = currency
            columns_to_invert.append(col)
            logger.warning(
                f"FX column '{col}' is inverted (base currency is not EUR). "
                f"Inverting prices: 1/{col} -> {currency}"
            )

        # Case 3: 3-char currency code (USD, GBP, etc.)
        elif len(col_str) == 3:
            normalized_columns[col] = col_str
            logger.info(
                f"FX column '{col}' is a currency code without EUR base indication. "
                f"Assuming it represents EUR{col} (e.g., EUR/{col} rate)."
            )

        # Case 4: Other formats - warn and keep as-is
        else:
            logger.info(
                f"FX column '{col}' doesn't match expected format (EURCCY, CCYEUR, or CCY). "
                "Keeping as-is."
            )
            normalized_columns[col] = col

    # Create normalized DataFrame
    fx_normalized = fx_prices.copy()

    # Invert prices for inverted tickers (USDEUR -> 1/USDEUR)
    for col in columns_to_invert:
        logger.info(f"Inverting FX prices for {col}: new values = 1 / old values")
        fx_normalized[col] = 1.0 / fx_normalized[col]
        # Replace inf with NaN (in case of zero prices)
        fx_normalized[col].replace([np.inf, -np.inf], np.nan, inplace=True)

    # Rename columns
    fx_normalized = fx_normalized.rename(columns=normalized_columns)

    # Log duplicates if any
    duplicates = fx_normalized.columns[fx_normalized.columns.duplicated()].tolist()
    if duplicates:
        logger.warning(
            f"Duplicate currency codes after normalization: {duplicates}. "
            "Keeping first occurrence."
        )
        fx_normalized = fx_normalized.loc[:, ~fx_normalized.columns.duplicated()]

    logger.debug(
        f"FX columns normalized: {list(fx_prices.columns)} -> {list(fx_normalized.columns)}"
    )

    return fx_normalized


def add_time_tag(series: pd.Series, timestamp: pd.Timestamp) -> pd.DataFrame:
    return series.to_frame(timestamp).T
