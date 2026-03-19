"""
CDX (Credit Default Swap Index) component for carry adjustment.

Calculates carry cost for CDX instruments based on spread and time to maturity.
"""
from datetime import date, datetime
from typing import Union, List
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType

logger = logging.getLogger(__name__)


class CdxComponent(Component):
    """
    CDX (Credit Default Swap Index) carry adjustment component.
    
    Calculates carry cost for CDX based on:
    - Spread (in basis points)
    - Time to maturity (rolling 5Y standard)
    - Roll dates (March 20, September 20)
    
    Formula:
        carry = (spread / 10000) × (1 / time_to_maturity_days) × 365 × year_fraction
    
    Usage:
        # CDX spreads in basis points
        cdx_spreads = pd.DataFrame({
            'CDX_ISIN_1': [120.5, 122.0, 119.8],  # bp
            'CDX_ISIN_2': [85.2, 86.1, 84.9],
        }, index=dates)
        
        adjuster.add(CdxComponent(cdx_spreads, tenor='5Y'))
    """
    
    # Roll dates for CDX (March 20, September 20)
    ROLL_DATES = {
        'MAR': (3, 20),
        'SEP': (9, 20),
    }
    
    def __init__(self, cdx_spreads: pd.DataFrame, tenor: str = '5Y'):
        """
        Initialize CDX component.
        
        Args:
            cdx_spreads: DataFrame(dates × instruments)
                        Index: dates
                        Columns: CDX instrument IDs
                        Values: spreads in basis points (120.5 = 120.5 bp)
            tenor: CDX tenor (default: '5Y')
                  Currently only '5Y' is supported
        """
        self.cdx_spreads = cdx_spreads.fillna(0.0)
        self.tenor = tenor
        
        # Validate tenor
        if tenor != '5Y':
            raise ValueError(f"Only '5Y' tenor is supported, got '{tenor}'")
        
        # Parse tenor to years
        self.tenor_years = int(tenor.replace('Y', ''))
        
        logger.info(
            f"CdxComponent initialized: {len(self.cdx_spreads.columns)} instruments, "
            f"tenor={tenor}"
        )
    
    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if CDX carry applicable.
        
        Applicable if:
        - Instrument type is CDX or CREDIT_INDEX
        - Has spread data
        """
        if instrument.id not in self.cdx_spreads.columns:
            return False
        
        # Check instrument type
        # Note: Adjust based on your InstrumentType enum
        if hasattr(InstrumentType, 'CDX'):
            return instrument.type == InstrumentType.CDX
        
        # Fallback: check if instrument has 'cdx' or 'credit' in type name
        type_name = str(instrument.type).lower() if instrument.type else ''
        return 'cdx' in type_name or 'credit' in type_name
    
    def calculate_batch(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate CDX carry adjustments."""
        instrument_ids = list(instruments.keys())
        result = pd.DataFrame(0.0, index=dates, columns=instrument_ids)
        
        # Filter applicable
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.is_applicable(inst) and inst.id in self.cdx_spreads.columns
        ]
        
        if not applicable_ids:
            logger.debug("No applicable instruments for CdxComponent")
            return result
        
        logger.debug(f"CdxComponent: {len(applicable_ids)}/{len(instruments)} instruments")
        
        # Calculate standard year fractions (no settlement shift for CDX)
        year_fractions = calculate_year_fractions(
            dates,
            shifted=False,
            settlement_days=0
        )
        
        # Get spreads for applicable instruments
        spreads_applicable = self.cdx_spreads[applicable_ids]
        
        # Align dates
        common_dates = spreads_applicable.index.intersection(dates)
        if len(common_dates) == 0:
            logger.warning("No common dates between CDX spreads and requested dates")
            return result
        
        # Calculate carry for each date
        for calc_date in common_dates:
            # Convert to date if datetime
            date_obj = calc_date.date() if isinstance(calc_date, (datetime, pd.Timestamp)) else calc_date
            
            # Calculate time to maturity
            time_to_maturity_days = self._get_time_to_maturity(date_obj)
            
            # Get spreads for this date
            spreads_today = spreads_applicable.loc[calc_date]
            
            # Formula: carry = (spread / 10000) × (1 / ttm_days) × 365 × year_fraction
            # Note: × 365 converts daily rate to annual, then × year_fraction scales to period
            carry_rate = (spreads_today / 10000.0) * (1.0 / time_to_maturity_days) * 365.0
            
            # Apply year fraction
            year_frac = year_fractions.loc[calc_date]
            carry_today = carry_rate * year_frac
            
            # Fill result (positive = carry benefit)
            result.loc[calc_date, applicable_ids] = carry_today
        
        return result
    
    def _get_time_to_maturity(self, current_date: date) -> int:
        """
        Calculate time to maturity for CDX.
        
        CDX rolls on March 20 and September 20.
        For 5Y tenor: maturity is 5 years from last roll date.
        
        Args:
            current_date: Current date
        
        Returns:
            Days to maturity
        """
        year = current_date.year
        march_20 = date(year, 3, 20)
        sept_20 = date(year, 9, 20)
        
        # Determine creation (last roll) date
        if current_date < march_20:
            # Before March 20 → last roll was September 20 of previous year
            creation_date = date(year - 1, 9, 20)
        elif current_date < sept_20:
            # Between March 20 and September 20 → last roll was March 20 this year
            creation_date = march_20
        else:
            # After September 20 → last roll was September 20 this year
            creation_date = sept_20
        
        # Maturity = creation_date + tenor_years
        maturity_date = date(
            creation_date.year + self.tenor_years,
            creation_date.month,
            creation_date.day
        )
        
        # Time to maturity in days
        ttm_days = (maturity_date - current_date).days
        
        # Ensure positive
        if ttm_days <= 0:
            logger.warning(
                f"CDX time to maturity is {ttm_days} days for {current_date}. "
                "Using 1 day minimum."
            )
            ttm_days = 1
        
        return ttm_days
    
    def __repr__(self) -> str:
        return (
            f"CdxComponent(instruments={len(self.cdx_spreads.columns)}, "
            f"tenor={self.tenor})"
        )
