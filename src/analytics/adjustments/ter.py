"""
TER (Total Expense Ratio) component for ETF adjustments.
"""
from datetime import date
from typing import Literal
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType


logger = logging.getLogger(__name__)


class TerComponent(Component):
    """
    TER adjustment component.

    Formula: adjustment = -ter_annual × year_fraction_shifted
    """

    LOWER_SANITY_CHECK = 0.0     # 0%
    UPPER_SANITY_CHECK = 0.01    # 1% (above this, probably in wrong format)

    def __init__(
        self,
        ters: dict[str, float] | pd.Series,
        shifted_settlement: Literal["T+1", "T+2", "T+3"] = "T+2",
    ):
        """
        Args:
            ters: Dict or Series mapping instrument_id → annual TER (decimal)
                  E.g., 0.0020 = 0.20%, NOT 0.20 = 20%
            shifted_settlement: Settlement convention (T+1, T+2, T+3)
        """
        # Parse settlement days
        self.settlement_days = int(shifted_settlement.replace("T+", ""))

        # Convert Series to dict first
        if isinstance(ters, pd.Series):
            ters = ters.to_dict()

        self.ters = {}

        # Validate ALL entries (dict or converted Series)
        for instrument_id, ter in ters.items():
            # Type check
            if not isinstance(ter, (int, float)):
                raise TypeError(f"TER for {instrument_id} must be numeric, got {type(ter)}")

            # Sanity check: if > 1%, probably in percentage format (0.20 instead of 0.0020)
            if ter > self.UPPER_SANITY_CHECK:
                logger.warning(
                    f"TER for {instrument_id}: {ter*100:.2f}% seems high. "
                    f"Dividing by 100 (assuming percentage format)"
                )
                ter = ter / 100.0

            # Check if still reasonable after scaling
            if ter <= self.LOWER_SANITY_CHECK:
                logger.warning(f"TER for {instrument_id} is {ter*100:.4f}%. Probably wrong.")

            self.ters[instrument_id] = float(ter)

        logger.info(
            f"TerComponent: {len(self.ters)} instruments, "
            f"settlement={shifted_settlement}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """Check if applicable (ETP with data)."""
        return (
            instrument.type == InstrumentType.ETP and
            instrument.id in self.ters
        )

    def calculate_batch(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: list[date],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate TER adjustments (vectorized with shifted year fractions)."""
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))

        # Calculate year fractions with settlement shift
        year_fractions = calculate_year_fractions(
            dates,
            shifted=True,
            settlement_days=self.settlement_days
        )

        # Filter applicable
        applicable = [i for i in instruments.values() if self.is_applicable(i)]

        if not applicable:
            return result

        logger.debug(f"TerComponent: {len(applicable)}/{len(instruments)} instruments")

        # Vectorized calculation
        for inst in applicable:
            result[inst.id] = -self.ters[inst.id] * year_fractions

        return result

    def __repr__(self) -> str:
        return f"TerComponent({len(self.ters)} instruments)"