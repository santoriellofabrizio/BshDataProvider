"""
YTM (Yield to Maturity) adjustment component for Fixed Income ETF.

Calculates yield carry cost for bond ETFs.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.common import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol, EtfInstrumentProtocol
from core.enums.instrument_types import InstrumentType

logger = logging.getLogger(__name__)


class YtmComponent(Component):
    """
    YTM (Yield to Maturity) adjustment component.

    Calculates yield carry cost for Fixed Income instruments (ETF, Future, Index)
    using shifted year fractions to account for settlement lag.

    Formula:
        adjustment = -ytm × year_fraction_shifted

    Usage:
        # API returns DataFrame (dates × instruments)
        ytm = pd.DataFrame({
            'AGGH LN': [0.045, 0.046, 0.045],  # ETF
            'FUTURE_ISIN': [0.042, 0.043],     # Future
            'INDEX_ISIN': [0.038, 0.039],      # Index
        }, index=dates)

        intraday_adjuster.add(YtmComponent(ytm, settlement_days=2))
    """

    def __init__(
        self, 
        ytm: pd.DataFrame, 
        settlement_days: int = 2,
        target: Optional[List[str]] = None
    ):
        """
        Initialize YTM component.

        Args:
            ytm: DataFrame(dates × instruments)
                 Index: dates
                 Columns: instrument IDs
                 Values: YTM in decimal (0.045 = 4.5%)
                 Sparse OK (NaN = 0)
            settlement_days: Settlement lag (T+1=1, T+2=2, T+3=3)
            target: Optional list of instrument IDs to apply adjustments to
        
        Example:
            # Apply to all instruments with YTM data
            ytm_comp = YtmComponent(ytm_data)
            
            # Apply only to specific bonds
            ytm_comp = YtmComponent(ytm_data, target=['BOND_A', 'BOND_B'])
        """
        super().__init__(target)
        
        self.ytm_series = ytm.fillna(0.0)
        self.settlement_days = settlement_days
        
        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.ytm_series.columns)
            if missing_data:
                logger.warning(
                    f"YtmComponent: Target contains {len(missing_data)} instruments "
                    f"without YTM data: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero YTM adjustments."
                )

        logger.info(
            f"YtmComponent: {len(self.ytm_series.columns)} instruments with YTM data, "
            f"T+{settlement_days} settlement"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if YTM applicable (domain logic only).

        Applicable if:
        - ETP with FIXED INCOME or MONEY MARKET underlying
        - FUTURE with FIXED INCOME underlying
        - INDEX with FIXED INCOME type
        - Has YTM data
        """
        if instrument.id not in self.ytm_series.columns:
            return False

        # ETP (ETF): Check underlying_type
        if instrument.type == InstrumentType.ETP:
            if isinstance(instrument, EtfInstrumentProtocol):
                underlying = instrument.underlying_type
                if underlying and underlying not in ['FIXED INCOME', 'MONEY MARKET']:
                    return False
            return True

        # FUTURE: Check underlying_type attribute
        if instrument.type == InstrumentType.FUTURE:
            if hasattr(instrument, 'underlying_type'):
                underlying = instrument.underlying_type
                if underlying and underlying == 'FIXED INCOME':
                    return True
                return False
            return True

        # INDEX: Check index_type attribute
        if instrument.type == InstrumentType.INDEX:
            if hasattr(instrument, 'index_type'):
                index_type = instrument.index_type
                if index_type and index_type == 'FIXED INCOME':
                    return True
                return False
            return True

        return False

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate YTM adjustments with vectorized operations."""
        # 1. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        instrument_ids = list(instruments.keys())

        # 2. Filter applicable (USE should_apply, NOT is_applicable)
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst) and inst.id in self.ytm_series.columns
        ]

        # 3. Early return if no applicable
        if not applicable_ids:
            logger.debug(
                f"YtmComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 4. Log processing
        logger.debug(
            f"YtmComponent: Processing {len(applicable_ids)}/{len(instruments)} instruments"
        )

        # 5. Calculate shifted year fractions once (T+2 settlement)
        year_fractions_shifted = calculate_year_fractions(dates_dt, shifted=True, settlement_days=self.settlement_days)

        # 6. Get YTM data for applicable instruments
        ytm_applicable = self.ytm_series[applicable_ids]

        # 7. Align dates
        common_dates = ytm_applicable.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"YtmComponent: ZERO adjustments - no date overlap. "
                f"YTM dates: {ytm_applicable.index.min()} to {ytm_applicable.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 8. Vectorized calculation: -YTM × year_fraction_shifted
        ytm_aligned = ytm_applicable.loc[common_dates]
        fractions_aligned = year_fractions_shifted.loc[common_dates]

        # Multiply each column by year_fractions (negative for cost)
        result_applicable = -ytm_aligned.mul(fractions_aligned, axis=0)

        # Fill result
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
        result.loc[common_dates, applicable_ids] = result_applicable

        # 9. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"YtmComponent: Produced ZERO non-zero adjustments for "
                f"{len(applicable_ids)} instruments. Verify YTM data."
            )
        else:
            mean_adj = result[applicable_ids].mean().mean()
            logger.debug(
                f"YtmComponent: Generated {non_zero} non-zero adjustments, "
                f"mean YTM impact: {mean_adj:.6f}"
            )

        return result

    def __repr__(self) -> str:
        return (
            f"YtmComponent(instruments={len(self.ytm_series.columns)}, "
            f"T+{self.settlement_days})"
        )
