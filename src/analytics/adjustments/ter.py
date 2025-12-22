"""
TER (Total Expense Ratio) component for ETF adjustments.
"""
from datetime import date, datetime
from typing import Literal, Union, List, Optional
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
        target: Optional[List[str]] = None,
    ):
        """
        Args:
            ters: Dict or Series mapping instrument_id → annual TER (decimal)
                  E.g., 0.0020 = 0.20%, NOT 0.20 = 20%
            shifted_settlement: Settlement convention (T+1, T+2, T+3)
            target: Optional list of instrument IDs to apply TER adjustments to.
                   If None, applies to all ETPs with TER data.
                   If provided, only instruments in both target and ters will be adjusted.
        
        Example:
            # Apply to all ETPs with TER data
            ter_comp = TerComponent(ters)
            
            # Apply only to specific ETPs
            ter_comp = TerComponent(ters, target=['IWDA LN', 'VWRL LN'])
        """
        # Initialize base with target
        super().__init__(target)
        
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

        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.ters.keys())
            if missing_data:
                logger.warning(
                    f"TerComponent: Target contains {len(missing_data)} instruments "
                    f"without TER data: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero TER adjustments."
                )
        
        logger.info(
            f"TerComponent: {len(self.ters)} instruments with TER data, "
            f"settlement={shifted_settlement}"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
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
        dates: Union[List[date], List[datetime]],
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

        # Filter applicable (use _should_apply to respect target filter)
        applicable = [i for i in instruments.values() if self._should_apply(i)]

        if not applicable:
            logger.debug(
                f"TerComponent: No applicable instruments. "
                f"Total instruments: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return result

        logger.info(
            f"TerComponent: Processing {len(applicable)}/{len(instruments)} instruments"
        )

        # Vectorized calculation
        for inst in applicable:
            result[inst.id] = -self.ters[inst.id] * year_fractions
        
        # Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"TerComponent: Produced ZERO non-zero adjustments for "
                f"{len(applicable)} instruments. Verify TER data."
            )
        else:
            logger.debug(
                f"TerComponent: Generated {non_zero} non-zero adjustments, "
                f"mean TER impact: {result.mean().mean():.6f}"
            )

        return result

    def __repr__(self) -> str:
        return f"TerComponent({len(self.ters)} instruments)"