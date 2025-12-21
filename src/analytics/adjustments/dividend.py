"""
Dividend adjustment component for ETF and Stock.
"""
from datetime import date, datetime
from typing import Union, List
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol, EtfInstrumentProtocol
from core.enums.instrument_types import InstrumentType


logger = logging.getLogger(__name__)


def to_str(obj) -> str:
    """Convert CurrencyEnum or str to uppercase string."""
    return str(getattr(obj, 'value', obj))


class DividendComponent(Component):
    """
    Dividend adjustment component.

    Formula: dividend_normalized = (dividend × fx_fund) / (price × fx_trading)
    
    Intraday Logic:
    - For period returns: applies adjustment when date changes between t1 and t2
    - Dividends are treated as occurring at midnight (date boundary)
    """

    def __init__(self, dividends: pd.DataFrame):
        """
        Args:
            dividends: DataFrame(dates × instruments) with dividend amounts
                      Index: dates (ex-dividend dates)
                      Values: dividend amounts in instrument's fund currency
        """
        self.dividends_raw = dividends.fillna(0.0)
        
        total_events = (self.dividends_raw != 0).sum().sum()
        logger.info(
            f"DividendComponent: {len(dividends.columns)} instruments, "
            f"{total_events} events"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """Check if applicable (STOCK or ETP with DIST/INC policy)."""
        if instrument.id not in self.dividends_raw.columns:
            return False

        if instrument.type == InstrumentType.STOCK:
            return True

        if instrument.type == InstrumentType.ETP:
            if isinstance(instrument, EtfInstrumentProtocol):
                policy = instrument.payment_policy
                return policy is None or policy in ['DIST', 'INC']
            return True

        return False

    def calculate_batch(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate dividend adjustments.
        
        Auto-detects intraday mode and applies appropriate logic:
        - Daily mode: applies adjustment on ex-dividend date
        - Intraday mode: applies adjustment to period containing ex-dividend datetime
        """
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))

        applicable = [i for i in instruments.values() if self.is_applicable(i)]
        if not applicable:
            return result

        logger.debug(f"DividendComponent: {len(applicable)}/{len(instruments)} instruments")
        
        # Detect if we're in intraday mode
        is_intraday = self._is_intraday_mode(dates)
        
        if is_intraday:
            logger.debug("DividendComponent: Using intraday mode (period returns)")
            return self._calculate_intraday(instruments, applicable, dates, prices, fx_prices)
        else:
            logger.debug("DividendComponent: Using daily mode")
            return self._calculate_daily(instruments, applicable, dates, prices, fx_prices)

    def _is_intraday_mode(self, dates: Union[List[date], List[datetime]]) -> bool:
        """
        Detect if we're operating in intraday mode.
        
        Intraday mode if:
        - dates contains datetime/Timestamp objects
        - AND at least one timestamp has non-zero hour/minute
        """
        if not dates:
            return False
        
        first_date = dates[0]
        
        # Check if it's a datetime type
        if not isinstance(first_date, (datetime, pd.Timestamp)):
            return False
        
        # Check if any timestamp has non-zero time
        # (normalized dates have hour=0, minute=0)
        return first_date.hour != 0 or first_date.minute != 0
    
    def _calculate_daily(self, instruments, applicable, dates, prices, fx_prices) -> pd.DataFrame:
        """
        Calculate dividend adjustments for daily data.
        
        Applies adjustment on the ex-dividend date.
        """
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))
        
        for inst in applicable:
            result[inst.id] = self._normalize(inst, dates, prices, fx_prices)
        
        return result
    
    def _calculate_intraday(
        self, 
        instruments, 
        applicable, 
        dates: List[datetime], 
        prices: pd.DataFrame, 
        fx_prices: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate dividend adjustments for intraday data (period returns).
        
        Logic:
        1. For each dividend event on date D
        2. Find period returns (t1 → t2) where date(t1) != date(t2) and date(t2) == D
        3. Apply adjustment to those periods (dividend occurs at midnight)
        
        Example:
            14-01 16:00 → 15-01 09:00: date changes to 15-01 → apply div if exists on 15-01
            15-01 09:00 → 15-01 14:00: same date → no div adjustment
        """
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))
        
        for inst in applicable:
            inst_id = inst.id
            
            # Get dividend events for this instrument
            divs = self.dividends_raw[inst_id]
            div_dates = divs[divs != 0].index
            
            if len(div_dates) == 0:
                continue
            
            # Get currencies
            fund_ccy = self._get_ccy(inst, 'fund_currency')
            trading_ccy = self._get_ccy(inst, 'currency')
            
            # For each consecutive pair of timestamps
            for i in range(1, len(dates)):
                t1 = dates[i - 1]
                t2 = dates[i]
                
                # Check if date changed
                date_t1 = t1.date()
                date_t2 = t2.date()
                
                if date_t1 == date_t2:
                    # Same date, no dividend in this period
                    continue
                
                # Date changed: check if there's a dividend on date_t2
                # (or any date between date_t1 and date_t2)
                for div_date in div_dates:
                    # Convert to date for comparison
                    div_date_only = pd.Timestamp(div_date).date()
                    
                    # Dividend applies if it's between t1 and t2 (inclusive of t2, exclusive of t1)
                    if date_t1 < div_date_only <= date_t2:
                        div_amt = divs.loc[div_date]
                        if pd.isna(div_amt) or div_amt == 0:
                            continue
                        
                        logger.debug(
                            f"{inst_id}: Dividend ${div_amt:.4f} on {div_date_only}, "
                            f"applying to period {t1} → {t2}"
                        )
                        
                        # Calculate adjustment using price at t1 (before date change)
                        if inst_id not in prices.columns or t1 not in prices.index:
                            logger.warning(f"{inst_id}: No price at {t1}, skipping")
                            continue
                        
                        price = prices.loc[t1, inst_id]
                        if pd.isna(price) or price <= 0:
                            logger.warning(f"{inst_id}: Invalid price at {t1}: {price}")
                            continue
                        
                        # Convert to EUR
                        div_eur = self._to_eur(div_amt, fund_ccy, t1, fx_prices)
                        price_eur = self._to_eur(price, trading_ccy, t1, fx_prices)
                        
                        if div_eur and price_eur and price_eur > 0:
                            adjustment = div_eur / price_eur
                            result.loc[t2, inst_id] += adjustment
                            logger.debug(f"{inst_id}: Adjustment at {t2}: +{adjustment:.6f}")
        
        return result

    def _normalize(self, inst, dates, prices, fx_prices) -> pd.Series:
        """Normalize dividends for single instrument."""
        normalized = pd.Series(0.0, index=dates)

        divs = self.dividends_raw[inst.id]
        div_dates = divs[divs != 0].index

        if len(div_dates) == 0:
            return normalized

        # Get currencies
        fund_ccy = self._get_ccy(inst, 'fund_currency')
        trading_ccy = self._get_ccy(inst, 'currency')

        for d in div_dates:
            if d not in dates:
                continue

            div_amt = divs.loc[d]
            if pd.isna(div_amt) or div_amt == 0:
                continue

            # Get price
            if inst.id not in prices.columns or d not in prices.index:
                continue

            price = prices.loc[d, inst.id]
            if pd.isna(price) or price <= 0:
                continue

            # Convert to EUR
            div_eur = self._to_eur(div_amt, fund_ccy, d, fx_prices)
            price_eur = self._to_eur(price, trading_ccy, d, fx_prices)

            if div_eur and price_eur and price_eur > 0:
                normalized.loc[d] = div_eur / price_eur

        return normalized

    def _get_ccy(self, inst, attr: str) -> str:
        """Get currency from instrument attribute."""
        # Try fund_currency first if attr is fund_currency
        if attr == 'fund_currency' and isinstance(inst, EtfInstrumentProtocol):
            if inst.fund_currency:
                return to_str(inst.fund_currency)

        # Fallback to currency
        return to_str(inst.currency)

    def _to_eur(self, amount: float, ccy: str, date: date, fx_prices: pd.DataFrame) -> float | None:
        """Convert amount to EUR."""
        ccy = to_str(ccy)

        if ccy == 'EUR':
            return amount

        # Find column (case-insensitive)
        col = next((c for c in fx_prices.columns if str(c).upper() == ccy), None)

        if col is None or date not in fx_prices.index:
            return None

        fx = fx_prices.loc[date, col]

        return amount * fx if not pd.isna(fx) and fx > 0 else None

    def __repr__(self) -> str:
        total = (self.dividends_raw != 0).sum().sum()
        return f"DividendComponent({len(self.dividends_raw.columns)} instruments, {total} events)"