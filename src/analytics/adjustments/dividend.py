"""
Dividend adjustment component for ETF and Stock.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import numpy as np
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol, EtfInstrumentProtocol
from core.enums.instrument_types import InstrumentType


logger = logging.getLogger(__name__)


class DividendComponent(Component):
    """
    Dividend adjustment component.

    Formula: dividend_normalized = (dividend × fx_fund) / (price × fx_trading)
    
    Logic:
    - Dividends are treated as occurring at midnight (date boundary)
    - Uses last cum-dividend price before midnight for normalization
    """

    def __init__(self, dividends: pd.DataFrame, target: Optional[List[str]] = None):
        """
        Args:
            dividends: DataFrame(dates × instruments) with dividend amounts
                      Index: dates (ex-dividend dates)
                      Values: dividend amounts in instrument's fund currency
            target: Optional list of instrument IDs to apply adjustments to
        
        Example:
            # Apply to all instruments with dividend data
            div_comp = DividendComponent(dividends)
            
            # Apply only to specific stocks/ETFs
            div_comp = DividendComponent(dividends, target=['STOCK_A', 'ETF_B'])
        """
        super().__init__(target)
        
        self.dividends_raw = dividends.fillna(0.0)
        
        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.dividends_raw.columns)
            if missing_data:
                logger.warning(
                    f"DividendComponent: Target contains {len(missing_data)} instruments "
                    f"without dividend data: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero dividend adjustments."
                )
        
        total_events = (self.dividends_raw != 0).sum().sum()
        logger.info(
            f"DividendComponent: {len(dividends.columns)} instruments, "
            f"{total_events} dividend events"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
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

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate dividend adjustments.
        
        Automatically detects if operating in intraday mode based on timestamps.
        Applies correct logic for either daily or intraday period returns.
        """
        # 1. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        result = pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))

        # 2. Filter applicable (USE should_apply)
        applicable = [i for i in instruments.values() if self.should_apply(i)]
        
        # 3. Early return if no applicable
        if not applicable:
            logger.debug(
                f"DividendComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return result

        # 4. Log processing
        logger.info(
            f"DividendComponent: Processing {len(applicable)}/{len(instruments)} instruments"
        )
        
        # 5. Detect if we're in intraday mode
        is_intraday = self._is_intraday_mode(dates_dt)
        
        if is_intraday:
            logger.debug("DividendComponent: Using intraday mode (period returns)")
            result = self._calculate_intraday(instruments, applicable, dates_dt, prices, fx_prices)
        else:
            logger.debug("DividendComponent: Using daily mode")
            result = self._calculate_daily(instruments, applicable, dates_dt, prices, fx_prices)
        
        # 6. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.debug(
                f"DividendComponent: ZERO non-zero adjustments for "
                f"{len(applicable)} instruments (expected if no dividends in period)"
            )
        else:
            mean_adj = result[result != 0].mean().mean()
            logger.debug(
                f"DividendComponent: Generated {non_zero} non-zero adjustments, "
                f"mean dividend impact: {mean_adj:.6f}"
            )
        
        return result
    
    # ========================================================================
    # CALCULATION LOGIC - Daily vs Intraday
    # ========================================================================
    
    def _calculate_daily(
        self, 
        instruments: dict[str, InstrumentProtocol],
        applicable: List[InstrumentProtocol],
        dates_dt: List[datetime], 
        prices: pd.DataFrame, 
        fx_prices: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate dividend adjustments for daily data.
        
        Applies adjustment on the ex-dividend date.
        """
        result = pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))
        
        for inst in applicable:
            result[inst.id] = self._normalize_dividends(inst, dates_dt, prices, fx_prices)
        
        return result
    
    def _calculate_intraday(
        self, 
        instruments: dict[str, InstrumentProtocol],
        applicable: List[InstrumentProtocol],
        dates_dt: List[datetime], 
        prices: pd.DataFrame, 
        fx_prices: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate dividend adjustments for intraday data (period returns).
        
        Logic:
        1. For each dividend event on date D at midnight
        2. Find the LAST price timestamp BEFORE midnight of D (cum-dividend price)
        3. Apply adjustment to the period that CROSSES midnight of D
        """
        result = pd.DataFrame(0.0, index=dates_dt, columns=list(instruments.keys()))
        
        for inst in applicable:
            inst_id = inst.id
            
            # Get dividend events for this instrument
            divs = self.dividends_raw[inst_id]
            div_dates = divs[divs != 0].index
            
            if len(div_dates) == 0:
                continue
            
            # Get currencies
            fund_ccy = self._get_currency(inst, 'fund_currency')
            trading_ccy = self._get_currency(inst, 'currency')
            
            # For each dividend event
            for div_date in div_dates:
                div_date_only = pd.Timestamp(div_date).date()
                div_amt = divs.loc[div_date]
                
                if pd.isna(div_amt) or div_amt == 0:
                    continue
                
                # Find LAST available price timestamp BEFORE div_date_only midnight
                div_midnight = pd.Timestamp(div_date_only)
                
                # Get all timestamps before the dividend
                price_timestamps_before = [
                    ts for ts in prices.index 
                    if ts < div_midnight and inst_id in prices.columns
                ]
                
                if len(price_timestamps_before) == 0:
                    logger.warning(
                        f"DividendComponent: {inst_id}: No price before dividend {div_date_only}. "
                        "Skipping dividend adjustment."
                    )
                    continue
                
                # Get last cum-dividend price
                last_cum_timestamp = max(price_timestamps_before)
                price = prices.loc[last_cum_timestamp, inst_id]
                
                # Validate price (EXPLICIT None check, not implicit!)
                if pd.isna(price) or price <= 0:
                    logger.warning(
                        f"DividendComponent: {inst_id}: Invalid price at {last_cum_timestamp}: {price}. "
                        "Skipping dividend adjustment."
                    )
                    continue
                
                logger.debug(
                    f"DividendComponent: {inst_id}: Dividend ${div_amt:.4f} on {div_date_only}, "
                    f"using cum-div price at {last_cum_timestamp}: ${price:.2f}"
                )
                
                # Convert to EUR using the cum-dividend timestamp
                div_eur = self._convert_to_eur(div_amt, fund_ccy, last_cum_timestamp, fx_prices)
                price_eur = self._convert_to_eur(price, trading_ccy, last_cum_timestamp, fx_prices)
                
                # EXPLICIT None checks (not implicit truthiness!)
                if div_eur is None or price_eur is None or price_eur <= 0:
                    logger.warning(
                        f"DividendComponent: {inst_id}: Failed EUR conversion for dividend {div_date_only}. "
                        f"div_eur={div_eur}, price_eur={price_eur}"
                    )
                    continue
                
                # Calculate adjustment
                adjustment = div_eur / price_eur
                
                # Find the period that crosses this dividend date
                for i in range(1, len(dates_dt)):
                    t1 = dates_dt[i - 1]
                    t2 = dates_dt[i]
                    
                    date_t1 = t1.date()
                    date_t2 = t2.date()
                    
                    # Dividend applies if period crosses the dividend date
                    if date_t1 < div_date_only <= date_t2:
                        result.loc[t2, inst_id] += adjustment
                        
                        logger.debug(
                            f"DividendComponent: {inst_id}: Applied adjustment at {t2}: "
                            f"+{adjustment:.6f} (period {t1} → {t2})"
                        )
                        
                        break  # Only apply to first crossing period
        
        return result
    
    # ========================================================================
    # UTILITY METHODS - Parsing, validation, normalization
    # ========================================================================
    
    def _is_intraday_mode(self, dates_dt: List[datetime]) -> bool:
        """
        Detect if operating in intraday mode.
        
        Intraday mode if at least one timestamp has non-zero hour/minute.
        """
        if not dates_dt:
            return False
        
        # Check if any timestamp has non-zero time (normalized dates are all midnight)
        return any(d.hour != 0 or d.minute != 0 for d in dates_dt)
    
    def _normalize_dividends(
        self, 
        inst: InstrumentProtocol, 
        dates_dt: List[datetime], 
        prices: pd.DataFrame, 
        fx_prices: pd.DataFrame
    ) -> pd.Series:
        """Normalize dividends for single instrument (daily mode)."""
        normalized = pd.Series(0.0, index=dates_dt)

        divs = self.dividends_raw[inst.id]
        div_dates = divs[divs != 0].index

        if len(div_dates) == 0:
            return normalized

        # Get currencies
        fund_ccy = self._get_currency(inst, 'fund_currency')
        trading_ccy = self._get_currency(inst, 'currency')

        for d in div_dates:
            # Find matching datetime in dates_dt
            d_dt = pd.Timestamp(d)
            if d_dt not in dates_dt:
                continue

            div_amt = divs.loc[d]
            if pd.isna(div_amt) or div_amt == 0:
                continue

            # Get price
            if inst.id not in prices.columns or d_dt not in prices.index:
                continue

            price = prices.loc[d_dt, inst.id]
            if pd.isna(price) or price <= 0:
                continue

            # Convert to EUR (EXPLICIT None checks!)
            div_eur = self._convert_to_eur(div_amt, fund_ccy, d_dt, fx_prices)
            price_eur = self._convert_to_eur(price, trading_ccy, d_dt, fx_prices)

            if div_eur is not None and price_eur is not None and price_eur > 0:
                normalized.loc[d_dt] = div_eur / price_eur

        return normalized

    def _get_currency(self, inst: InstrumentProtocol, attr: str) -> str:
        """Get currency from instrument attribute."""
        # Try fund_currency first if attr is fund_currency
        if attr == 'fund_currency' and isinstance(inst, EtfInstrumentProtocol):
            if inst.fund_currency:
                return self._to_str(inst.fund_currency)

        # Fallback to trading currency
        return self._to_str(inst.currency)

    def _convert_to_eur(
        self, 
        amount: float, 
        ccy: str, 
        timestamp: datetime, 
        fx_prices: pd.DataFrame
    ) -> Optional[float]:
        """
        Convert amount to EUR using FX rate at given timestamp.
        
        Args:
            amount: Amount to convert
            ccy: Currency code (3 chars)
            timestamp: Datetime for FX rate lookup
            fx_prices: DataFrame with FX rates (EUR base)
        
        Returns:
            Converted amount in EUR, or None if conversion fails
        """
        ccy = self._to_str(ccy)

        if ccy == 'EUR':
            return amount

        # Find column (case-insensitive)
        col = next((c for c in fx_prices.columns if str(c).upper() == ccy), None)

        if col is None:
            logger.debug(f"DividendComponent: No FX rate for {ccy}")
            return None
        
        if timestamp not in fx_prices.index:
            logger.debug(f"DividendComponent: No FX rate at {timestamp} for {ccy}")
            return None

        fx = fx_prices.loc[timestamp, col]

        # EXPLICIT checks (not implicit truthiness!)
        if pd.isna(fx):
            return None
        if fx <= 0:
            return None
        
        return amount * fx

    @staticmethod
    def _to_str(obj) -> str:
        """Convert CurrencyEnum or str to uppercase string."""
        return str(getattr(obj, 'value', obj)).upper()

    def __repr__(self) -> str:
        total = (self.dividends_raw != 0).sum().sum()
        return f"DividendComponent({len(self.dividends_raw.columns)} instruments, {total} events)"
