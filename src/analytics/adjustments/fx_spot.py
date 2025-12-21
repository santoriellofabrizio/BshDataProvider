"""
FX Spot exposure adjustment component for ETF and Stock.

Adjusts for currency exposure mismatch between portfolio and trading currency.
"""
from datetime import date, datetime
from typing import Union, List
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType
from core.enums.currencies import CurrencyEnum


logger = logging.getLogger(__name__)


class FxSpotComponent(Component):
    """
    FX Spot exposure adjustment component.

    Adjusts returns for currency exposure when portfolio composition differs
    from trading currency using vectorized matrix operations.

    Formula:
        fx_correction = Σ(fx_return[ccy] × weight[ccy]) - fx_return[trading_ccy]

    Usage:
        # API returns DataFrame (instruments × currencies)
        fx_composition = pd.DataFrame({
            'USD': [0.65, 0.60],
            'GBP': [0.10, 0.15],
            'JPY': [0.05, 0.00],
        }, index=['IWDA LN', 'VWRL LN'])

        adjuster.add(FxSpotComponent(fx_composition))
    """

    # Sanity check bounds for composition sum
    LOWER_SANITY_CHECK = -0.1  # Allow 10% negative for short positions
    UPPER_SANITY_CHECK = 1.1   # Allow 10% over for rounding/leverage

    def __init__(self, fx_composition: pd.DataFrame):
        """
        Initialize FX Spot component.

        Args:
            fx_composition: DataFrame(instruments × currencies)
                           Index: instrument IDs
                           Columns: currency codes (USD, GBP, etc.)
                           Values: weights in decimal (0.65 = 65%)
                           Sparse OK (NaN treated as 0)

        Example:
            #           USD   GBP   JPY   EUR
            # IWDA LN  0.65  0.10  0.05   NaN
            # VWRL LN  0.60  0.15   NaN  0.25
        """
        # Fill NaN with 0 and validate currencies
        self.fx_composition = fx_composition.fillna(0.0).copy()

        # Validate currency codes
        for ccy in self.fx_composition.columns:
            if not CurrencyEnum.exists(str(ccy)):
                logger.warning(f"Unknown currency in composition: {ccy}")

        # Sanity check per instrument
        for instrument_id in self.fx_composition.index:
            total = self.fx_composition.loc[instrument_id].sum()

            if not (self.LOWER_SANITY_CHECK <= total <= self.UPPER_SANITY_CHECK):
                logger.warning(
                    f"FX composition for {instrument_id} sums to {total:.2%}, "
                    f"expected [{self.LOWER_SANITY_CHECK:.0%}, {self.UPPER_SANITY_CHECK:.0%}]"
                )

            # Add EUR remainder if needed
            eur_weight = 1.0 - total
            if abs(eur_weight) > 0.001 and 'EUR' in self.fx_composition.columns:
                self.fx_composition.loc[instrument_id, 'EUR'] += eur_weight

        logger.info(
            f"FxSpotComponent initialized: {len(self.fx_composition)} instruments, "
            f"{len(self.fx_composition.columns)} currencies"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check applicability.

        Applicable if:
        - STOCK or ETP
        - Has FX composition
        - NOT currency_hedged (default False)
        """
        if instrument.type not in [InstrumentType.STOCK, InstrumentType.ETP]:
            return False

        if instrument.id not in self.fx_composition.index:
            return False

        # Check currency_hedged attribute
        if hasattr(instrument, 'currency_hedged'):
            is_hedged = instrument.currency_hedged
            if is_hedged is None:
                is_hedged = False

            if is_hedged:
                logger.debug(f"{instrument.id} is currency hedged, skipping FX spot")
                return False

        return True

    def calculate_batch(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate FX spot adjustments using vectorized matrix operations.

        Performance: O(N×M×T) → O(N×M + M×T) with matrix multiplication
        """
        instrument_ids = list(instruments.keys())

        # Filter applicable instruments
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.is_applicable(inst) and inst.id in self.fx_composition.index
        ]

        if not applicable_ids:
            logger.debug("No applicable instruments for FxSpotComponent")
            return pd.DataFrame(0.0, index=dates, columns=instrument_ids)

        logger.debug(f"FxSpotComponent: {len(applicable_ids)}/{len(instruments)} instruments")

        # Calculate FX returns (dates × currencies)
        fx_returns = fx_prices.pct_change().fillna(0.0)

        # Ensure dates alignment
        common_dates = fx_returns.index.intersection(dates)
        if len(common_dates) == 0:
            logger.warning("No common dates between fx_prices and requested dates")
            return pd.DataFrame(0.0, index=dates, columns=instrument_ids)

        # Get composition matrix for applicable instruments
        comp_matrix = self.fx_composition.loc[applicable_ids]  # N × M

        # Align currencies between composition and fx_returns
        common_currencies = comp_matrix.columns.intersection(fx_returns.columns)
        comp_matrix = comp_matrix[common_currencies]
        fx_ret_matrix = fx_returns[common_currencies]  # T × M

        # Matrix multiplication: (N × M) @ (M × T)^T = N × T
        # weighted_fx: instruments × dates
        weighted_fx = comp_matrix @ fx_ret_matrix.T

        # Transpose to dates × instruments
        result_applicable = weighted_fx.T

        # Subtract trading currency return per instrument
        for inst_id in applicable_ids:
            trading_ccy = str(instruments[inst_id].currency)

            if trading_ccy != 'EUR' and trading_ccy in fx_returns.columns:
                # Subtract trading currency return
                result_applicable[inst_id] = (
                    result_applicable[inst_id] - fx_returns[trading_ccy]
                )

        # Create full result DataFrame
        result = pd.DataFrame(0.0, index=dates, columns=instrument_ids)

        # Fill with calculated values (align dates)
        for inst_id in applicable_ids:
            result.loc[common_dates, inst_id] = result_applicable[inst_id]

        return result

    def __repr__(self) -> str:
        return f"FxSpotComponent(instruments={len(self.fx_composition)})"