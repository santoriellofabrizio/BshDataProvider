"""
CDX (Credit Default Swap) carry adjustment component.

Adjusts for carry cost of credit default swap positions.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType

logger = logging.getLogger(__name__)


class CdxComponent(Component):
    """
    CDX carry adjustment component for credit default swaps.

    Formula:
        adjustment = cdx_spread × year_fraction_shifted

    Similar to repo but for credit instruments - spread represents carry cost.

    Usage:
        # CDX spreads per instrument (basis points → decimal)
        cdx_spreads = pd.DataFrame({
            'CDX_INDEX_1': [0.0120, 0.0125],  # 120-125 bps
            'CDX_INDEX_2': [0.0095, 0.0098],  # 95-98 bps
        }, index=dates)

        adjuster.add(CdxComponent(cdx_spreads, settlement_days=2))
    """

    def __init__(
        self,
        cdx_spreads: pd.DataFrame,
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        """
        Initialize CDX component.

        Args:
            cdx_spreads: DataFrame(dates × instruments)
                        Index: dates
                        Columns: instrument IDs
                        Values: CDX spreads in decimal (0.0120 = 120 bps)
            settlement_days: Settlement lag (T+1=1, T+2=2, T+3=3)
            target: Optional list of instrument IDs to apply adjustments to

        Example:
            # Apply to all instruments with CDX data
            cdx_comp = CdxComponent(cdx_spreads)

            # Apply only to specific indices
            cdx_comp = CdxComponent(cdx_spreads, target=['CDX_IG', 'CDX_HY'])
        """
        super().__init__(target)

        self.cdx_spreads = cdx_spreads.fillna(0.0)
        self.settlement_days = settlement_days

        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.cdx_spreads.columns)
            if missing_data:
                logger.warning(
                    f"CdxComponent: Target contains {len(missing_data)} instruments "
                    f"without CDX spread data: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero CDX carry adjustments."
                )

        logger.info(
            f"CdxComponent: {len(self.cdx_spreads.columns)} instruments with CDX spreads, "
            f"T+{settlement_days} settlement"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check applicability (domain logic only).

        Applicable if:
        - INDEX (credit index)
        - Has CDX spread data
        """
        if instrument.type != InstrumentType.INDEX:
            return False

        return instrument.id in self.cdx_spreads.columns

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate CDX carry adjustments."""
        # 1. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        instrument_ids = list(instruments.keys())

        # 2. Filter applicable (USE should_apply)
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst) and inst.id in self.cdx_spreads.columns
        ]

        # 3. Early return if no applicable
        if not applicable_ids:
            logger.debug(
                f"CdxComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 4. Log processing
        logger.info(
            f"CdxComponent: Processing {len(applicable_ids)}/{len(instruments)} instruments"
        )

        # 5. Calculate shifted year fractions
        year_fractions_shifted = calculate_year_fractions(
            dates_dt,
            shifted=True,
            settlement_days=self.settlement_days
        )

        # 6. Get CDX spreads for applicable instruments
        cdx_applicable = self.cdx_spreads[applicable_ids]

        # 7. Align dates
        common_dates = cdx_applicable.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"CdxComponent: ZERO adjustments - no date overlap. "
                f"CDX dates: {cdx_applicable.index.min()} to {cdx_applicable.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 8. Vectorized calculation: spread × year_fraction_shifted
        cdx_aligned = cdx_applicable.loc[common_dates]
        year_frac_aligned = year_fractions_shifted.loc[common_dates]

        # Multiply each column by year_fractions (positive carry for credit exposure)
        result_applicable = cdx_aligned.mul(year_frac_aligned, axis=0)

        # 9. Create full result DataFrame
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
        result.loc[common_dates, applicable_ids] = result_applicable

        # 10. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"CdxComponent: Produced ZERO non-zero adjustments for "
                f"{len(applicable_ids)} instruments. Verify CDX spread data."
            )
        else:
            mean_adj = result[applicable_ids].mean().mean()
            logger.debug(
                f"CdxComponent: Generated {non_zero} non-zero adjustments, "
                f"mean CDX carry impact: {mean_adj:.6f}"
            )

        return result

    def __repr__(self) -> str:
        return f"CdxComponent(instruments={len(self.cdx_spreads.columns)})"
