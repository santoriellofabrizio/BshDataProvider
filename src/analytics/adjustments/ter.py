"""
TER (Total Expense Ratio) component for ETF adjustments.
"""
from datetime import date, datetime
from typing import Literal, Union, List, Optional
import pandas as pd
import logging

from analytics.adjustments.common import calculate_year_fractions
from analytics.adjustments.component import Component

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

        if isinstance(ters, pd.DataFrame):
            ters = ters.iloc[:, 0]

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

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate TER adjustments (vectorized with shifted year fractions)."""
        # 1. Validate input
        self.validate_input(instruments, dates, prices)

        # 2. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        instrument_ids = list(instruments.keys())

        # 2. Filter applicable (USE should_apply)
        applicable = [i for i in instruments.values() if self.should_apply(i)]

        # 3. Early return if no applicable
        if not applicable:
            logger.debug(
                f"TerComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 4. Log processing
        logger.info(
            f"TerComponent: Processing {len(applicable)}/{len(instruments)} instruments"
        )

        # 5. Calculate year fractions with settlement shift
        year_fractions = calculate_year_fractions(
            dates_dt,
            shifted=True,
            settlement_days=self.settlement_days
        )

        # 6. Create result DataFrame
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 7. Vectorized calculation
        for inst in applicable:
            result[inst.id] = -self.ters[inst.id] * year_fractions
        
        # 8. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"TerComponent: Produced ZERO non-zero adjustments for "
                f"{len(applicable)} instruments. Verify TER data."
            )
        else:
            mean_adj = result[[i.id for i in applicable]].mean().mean()
            logger.debug(
                f"TerComponent: Generated {non_zero} non-zero adjustments, "
                f"mean TER impact: {mean_adj:.6f}"
            )

        # 9. Validate output
        self.validate_output(result)

        return result

    def __repr__(self) -> str:
        return f"TerComponent(instruments={len(self.ters)})"
