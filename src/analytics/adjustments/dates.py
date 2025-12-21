"""
Date utilities for adjustments.

Note: For business day calculations, use HolidayManager from bshdata.core.utils
"""
from datetime import date
import pandas as pd


def calculate_year_fractions(
    dates: list[date] | pd.DatetimeIndex,
    shifted: bool = False,
    settlement_days: int = 2,
) -> pd.Series:
    """
    Calculate year fractions for pro-rata calculations.

    Args:
        dates: List of dates or DatetimeIndex
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
    # Convert to list if DatetimeIndex
    if isinstance(dates, pd.DatetimeIndex):
        dates = dates.date.tolist()

    if not dates:
        return pd.Series(dtype=float)

    dates_sorted = sorted(dates)
    fractions = pd.Series(0.0, index=dates_sorted)

    if shifted:
        # Shifted fractions (for forward settlement)
        for i, date in enumerate(dates_sorted):
            if i < len(dates_sorted) - settlement_days:
                next_date = dates_sorted[i + settlement_days]
                days = (next_date - date).days
            else:
                # Fallback for end dates
                days = 1

            fractions[date] = days / 365.0
    else:
        # Standard fractions
        for i, date in enumerate(dates_sorted):
            if i == 0:
                # First date: assume 1 day
                days = 1
            else:
                prev_date = dates_sorted[i - 1]
                days = (date - prev_date).days

            fractions[date] = days / 365.0

    return fractions