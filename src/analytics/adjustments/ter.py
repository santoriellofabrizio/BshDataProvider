"""
TER (Total Expense Ratio) component for ETF adjustments.

Logic: TER is applied as an event at midnight (date boundary).
For intraday data, applies adjustment to the period that CROSSES that date.
For daily data, applies adjustment on that date.

Formula: adjustment = -ter_annual × year_fraction
The TER value is annualized and proportionato to the time period.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.common import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType


logger = logging.getLogger(__name__)


class TerComponent(Component):
    """
    TER (Total Expense Ratio) component for ETF adjustments.

    Treats TER as a daily cost applied at midnight.
    For period returns crossing a date, applies the TER adjustment to that period.
    """

    LOWER_SANITY_CHECK = 0.0     # 0%
    UPPER_SANITY_CHECK = 0.01    # 1% (above this, probably in wrong format)

    def __init__(
        self,
        ters: dict[str, float] | pd.Series | pd.DataFrame,
        target: Optional[List[str]] = None,
    ):
        """
        Args:
            ters: Dict, Series, or DataFrame mapping instrument_id -> annual TER (decimal)
                  E.g., 0.0020 = 0.20%, NOT 0.20 = 20%
            target: Optional list of instrument IDs to apply TER adjustments to.
                   If None, applies to all ETPs with TER data.

        Example:
            ter_comp = TerComponent(ters_dict)
            ter_comp = TerComponent(ters_dict, target=['IWDA LN', 'VWRL LN'])
        """
        super().__init__(target)

        # Parse input into dict
        if isinstance(ters, pd.DataFrame):
            ters = ters.iloc[:, 0]

        if isinstance(ters, pd.Series):
            ters = ters.to_dict()

        self.ters = {}

        # Validate ALL entries
        for instrument_id, ter in ters.items():
            if not isinstance(ter, (int, float)):
                raise TypeError(f"TER for {instrument_id} must be numeric, got {type(ter)}")

            # Sanity check
            if ter > self.UPPER_SANITY_CHECK:
                logger.warning(
                    f"TER for {instrument_id}: {ter*100:.2f}% seems high. "
                    f"Dividing by 100"
                )
                ter = ter / 100.0

            if ter <= self.LOWER_SANITY_CHECK:
                logger.warning(f"TER for {instrument_id} is {ter*100:.4f}%. Probably wrong.")

            self.ters[instrument_id] = float(ter)

        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.ters.keys())
            if missing_data:
                logger.warning(
                    f"TerComponent: Target contains {len(missing_data)} instruments "
                    f"without TER data: {sorted(missing_data)[:5]}"
                    f"{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero TER adjustments."
                )

        logger.info(
            f"TerComponent: {len(self.ters)} instruments with TER data"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """Check if applicable (ETP with TER data)."""
        return (
            instrument.type == InstrumentType.ETP and
            instrument.id in self.ters
        )

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate TER adjustments (event-driven at midnight)."""
        # 1. Normalize dates to datetime
        dates_dt = self._normalize_dates(dates)
        result = pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))

        # 2. Filter applicable
        applicable = [i for i in instruments.values() if self.should_apply(i)]

        # 3. Early return if no applicable
        if not applicable:
            logger.debug(
                f"TerComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return result

        logger.debug(
            f"TerComponent: Processing {len(applicable)}/{len(instruments)} instruments"
        )

        # 4. Detect if intraday mode
        is_intraday = self._is_intraday_mode(dates_dt)

        if is_intraday:
            logger.debug("TerComponent: Using intraday mode (period returns)")
            result = self._calculate_intraday(applicable, dates_dt)
        else:
            logger.debug("TerComponent: Using daily mode")
            result = self._calculate_daily(applicable, dates_dt)

        # 5. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.debug(f"TerComponent: ZERO non-zero adjustments for {len(applicable)} instruments")
        else:
            mean_adj = result[result != 0].mean().mean()
            logger.debug(
                f"TerComponent: Generated {non_zero} non-zero adjustments, "
                f"mean TER impact: {mean_adj:.6f}"
            )

        return result

    def _calculate_daily(
        self,
        applicable: List[InstrumentProtocol],
        dates_dt: List[datetime],
    ) -> pd.DataFrame:
        """Calculate TER adjustments for daily data.

        Each date gets TER × year_fraction applied.
        """
        result = pd.DataFrame(0.0, index=dates_dt, columns=[i.id for i in applicable])

        # Calculate year fractions for each date
        year_fractions = calculate_year_fractions(dates_dt, shifted=False)

        for inst in applicable:
            ter = self.ters[inst.id]
            result[inst.id] = -ter * year_fractions

        return result

    def _calculate_intraday(
        self,
        applicable: List[InstrumentProtocol],
        dates_dt: List[datetime],
    ) -> pd.DataFrame:
        """Calculate TER adjustments for intraday data (period returns).

        For each unique date, TER is applied at midnight to the period crossing that boundary.
        The adjustment value is -ter × year_fraction_for_that_date.
        """
        result = pd.DataFrame(0.0, index=dates_dt, columns=[i.id for i in applicable])

        # Extract unique dates and calculate year fractions for them
        unique_dates = pd.Index(dates_dt).normalize().unique()
        year_fractions = calculate_year_fractions(unique_dates, shifted=False)

        # Map unique_dates to their year_fractions
        yf_dict = dict(zip(unique_dates, year_fractions))

        for inst in applicable:
            ter = self.ters[inst.id]

            # For each unique date, apply TER to the period crossing midnight
            for ter_date in unique_dates:
                ter_timestamp = pd.Timestamp(ter_date)
                yf = yf_dict[ter_date]

                # Preserve timezone if dates are tz-aware
                if hasattr(dates_dt, 'tz') and dates_dt.tz is not None:
                    if ter_timestamp.tz is None:
                        ter_timestamp = ter_timestamp.tz_localize(dates_dt.tz)
                    else:
                        ter_timestamp = ter_timestamp.tz_convert(dates_dt.tz)

                # Find the period that crosses this midnight
                for i in range(1, len(dates_dt)):
                    t1 = dates_dt[i - 1]
                    t2 = dates_dt[i]

                    t1_ts = pd.Timestamp(t1)
                    t2_ts = pd.Timestamp(t2)

                    # TER applies if period crosses the date boundary
                    if t1_ts < ter_timestamp <= t2_ts:
                        adjustment = -ter * yf
                        result.loc[t2, inst.id] = adjustment

                        logger.debug(
                            f"TerComponent: {inst.id}: Applied TER {ter*100:.4f}% "
                            f"(period {t1} -> {t2}, yf={yf:.6f})"
                        )

                        break

        return result

    @staticmethod
    def _is_intraday_mode(dates_dt: List[datetime]) -> bool:
        """Detect if operating in intraday mode (non-midnight timestamps)."""
        if not dates_dt:
            return False

        return any(d.hour != 0 or d.minute != 0 for d in dates_dt)

    def __repr__(self) -> str:
        return f"TerComponent(instruments={len(self.ters)})"