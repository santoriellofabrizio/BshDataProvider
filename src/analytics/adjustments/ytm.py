"""
YTM (Yield to Maturity) adjustment component for Fixed Income ETF.

Calculates yield carry cost for bond ETFs.
"""
from datetime import date, datetime
from typing import Union, List
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
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

        adjuster.add(YtmComponent(ytm, settlement_days=2))
    """

    def __init__(self, ytm: pd.DataFrame, settlement_days: int = 2):
        """
        Initialize YTM component.

        Args:
            ytm: DataFrame(dates × instruments)
                 Index: dates
                 Columns: instrument IDs
                 Values: YTM in decimal (0.045 = 4.5%)
                 Sparse OK (NaN = 0)
            settlement_days: Settlement lag (T+1=1, T+2=2, T+3=3)
        """
        self.ytm_series = ytm.fillna(0.0)
        self.settlement_days = settlement_days

        logger.info(
            f"YtmComponent initialized: {len(self.ytm_series.columns)} instruments, "
            f"T+{settlement_days} settlement"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if YTM applicable.

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
                    logger.debug(
                        f"{instrument.id} is {underlying}, not Fixed Income, skipping YTM"
                    )
                    return False
            return True

        # FUTURE: Check underlying_type attribute
        if instrument.type == InstrumentType.FUTURE:
            if hasattr(instrument, 'underlying_type'):
                underlying = instrument.underlying_type
                if underlying and underlying == 'FIXED INCOME':
                    return True
                logger.debug(
                    f"{instrument.id} is {underlying}, not Fixed Income Future, skipping YTM"
                )
                return False
            # If no underlying_type, check if has YTM data (assume applicable)
            return True

        # INDEX: Check index_type attribute
        if instrument.type == InstrumentType.INDEX:
            if hasattr(instrument, 'index_type'):
                index_type = instrument.index_type
                if index_type and index_type == 'FIXED INCOME':
                    return True
                logger.debug(
                    f"{instrument.id} is {index_type}, not Fixed Income Index, skipping YTM"
                )
                return False
            # If no index_type, check if has YTM data (assume applicable)
            return True

        return False

    def calculate_batch(
            self,
            instruments: dict[str, InstrumentProtocol],
            dates: Union[List[date], List[datetime]],
            prices: pd.DataFrame,
            fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate YTM adjustments with vectorized operations."""
        instrument_ids = list(instruments.keys())
        result = pd.DataFrame(0.0, index=dates, columns=instrument_ids)

        # Filter applicable
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.is_applicable(inst) and inst.id in self.ytm_series.columns
        ]

        if not applicable_ids:
            logger.debug("No applicable instruments for YtmComponent")
            return result

        logger.debug(f"YtmComponent: {len(applicable_ids)}/{len(instruments)} instruments")

        # Calculate shifted year fractions once (T+2 settlement)
        year_fractions_shifted = calculate_year_fractions(
            dates,
            shifted=True,
            settlement_days=self.settlement_days
        )

        # Get YTM data for applicable instruments
        ytm_applicable = self.ytm_series[applicable_ids]

        # Align dates
        common_dates = ytm_applicable.index.intersection(dates)
        if len(common_dates) == 0:
            logger.warning("No common dates between YTM data and requested dates")
            return result

        # Vectorized calculation: -YTM × year_fraction_shifted
        # ytm_applicable: dates × instruments
        # year_fractions_shifted: Series (dates)
        # Broadcasting: DataFrame × Series (broadcasts across columns)

        ytm_aligned = ytm_applicable.loc[common_dates]
        fractions_aligned = year_fractions_shifted.loc[common_dates]

        # Multiply each column by year_fractions (negative for cost)
        result_applicable = -ytm_aligned.mul(fractions_aligned, axis=0)

        # Fill result
        result.loc[common_dates, applicable_ids] = result_applicable

        return result

    def __repr__(self) -> str:
        return (
            f"YtmComponent(instruments={len(self.ytm_series.columns)}, "
            f"T+{self.settlement_days})"
        )