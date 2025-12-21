"""
Dividend adjustment component for ETF and Stock.
"""
from datetime import date
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
    """

    def __init__(self, dividends: pd.DataFrame):
        """
        Args:
            dividends: DataFrame(dates × instruments) with dividend amounts
        """
        self.dividends_raw = dividends.fillna(0.0)
        total_events = (self.dividends_raw != 0).sum().sum()
        logger.info(f"DividendComponent: {len(dividends.columns)} instruments, {total_events} events")

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
        dates: list[date],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate dividend adjustments."""
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))

        applicable = [i for i in instruments.values() if self.is_applicable(i)]
        if not applicable:
            return result

        logger.debug(f"DividendComponent: {len(applicable)}/{len(instruments)} instruments")

        for inst in applicable:
            result[inst.id] = self._normalize(inst, dates, prices, fx_prices)

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