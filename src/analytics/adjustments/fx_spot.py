"""
FX Spot exposure adjustment component for ETF and Stock.

Adjusts for currency exposure mismatch between portfolio and trading currency.
"""
from datetime import date, datetime
from typing import Union, List, Optional
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

    def __init__(self, fx_composition: pd.DataFrame, target: Optional[List[str]] = None):
        """
        Initialize FX Spot component.

        Args:
            fx_composition: DataFrame(instruments × currencies)
                           Index: instrument IDs
                           Columns: currency codes (USD, GBP, etc.)
                           Values: weights in decimal (0.65 = 65%)
                           Sparse OK (NaN treated as 0)
            target: Optional list of instrument IDs to apply adjustments to

        Example:
            #           USD   GBP   JPY   EUR
            # IWDA LN  0.65  0.10  0.05   NaN
            # VWRL LN  0.60  0.15   NaN  0.25
            
            # Apply to all instruments
            fx_comp = FxSpotComponent(fx_composition)
            
            # Apply only to specific instruments
            fx_comp = FxSpotComponent(fx_composition, target=['IWDA LN'])
        """
        super().__init__(target)
        
        # Fill NaN with 0 and validate currencies
        self.fx_composition = fx_composition.fillna(0.0).copy()

        # Validate currency codes
        for ccy in self.fx_composition.columns:
            if not CurrencyEnum.exists(str(ccy)):
                logger.warning(f"FxSpotComponent: Unknown currency in composition: {ccy}")

        # Sanity check per instrument
        for instrument_id in self.fx_composition.index:
            total = self.fx_composition.loc[instrument_id].sum()

            if not (self.LOWER_SANITY_CHECK <= total <= self.UPPER_SANITY_CHECK):
                logger.warning(
                    f"FxSpotComponent: {instrument_id} composition sums to {total:.2%}, "
                    f"expected [{self.LOWER_SANITY_CHECK:.0%}, {self.UPPER_SANITY_CHECK:.0%}]"
                )

            # Add EUR remainder if needed
            eur_weight = 1.0 - total
            if abs(eur_weight) > 0.001 and 'EUR' in self.fx_composition.columns:
                self.fx_composition.loc[instrument_id, 'EUR'] += eur_weight
        
        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.fx_composition.index)
            if missing_data:
                logger.warning(
                    f"FxSpotComponent: Target contains {len(missing_data)} instruments "
                    f"without FX composition: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero FX spot adjustments."
                )

        logger.info(
            f"FxSpotComponent: {len(self.fx_composition)} instruments, "
            f"{len(self.fx_composition.columns)} currencies"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check applicability (domain logic only).

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
                return False

        return True

    def calculate_adjustment(
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
        # 1. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        instrument_ids = list(instruments.keys())

        # 2. Filter applicable (USE should_apply)
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst) and inst.id in self.fx_composition.index
        ]

        # 3. Early return if no applicable
        if not applicable_ids:
            logger.debug(
                f"FxSpotComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 4. Log processing
        logger.info(
            f"FxSpotComponent: Processing {len(applicable_ids)}/{len(instruments)} instruments"
        )

        # 5. Calculate FX returns (dates × currencies)
        fx_returns = fx_prices.pct_change().fillna(0.0)

        # Ensure dates alignment
        common_dates = fx_returns.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"FxSpotComponent: ZERO adjustments - no date overlap. "
                f"FX dates: {fx_returns.index.min()} to {fx_returns.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 6. Get composition matrix for applicable instruments
        comp_matrix = self.fx_composition.loc[applicable_ids]  # N × M

        # Align currencies between composition and fx_returns
        common_currencies = comp_matrix.columns.intersection(fx_returns.columns)
        if len(common_currencies) == 0:
            logger.error(
                f"FxSpotComponent: No currency overlap. "
                f"Composition currencies: {list(comp_matrix.columns)}, "
                f"FX currencies: {list(fx_returns.columns)}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
        
        comp_matrix = comp_matrix[common_currencies]
        fx_ret_matrix = fx_returns[common_currencies]  # T × M

        # 7. Matrix multiplication: (N × M) @ (M × T)^T = N × T
        # weighted_fx: instruments × dates
        weighted_fx = comp_matrix @ fx_ret_matrix.T

        # Transpose to dates × instruments
        result_applicable = weighted_fx.T

        # 8. Subtract trading currency return per instrument
        for inst_id in applicable_ids:
            trading_ccy = str(instruments[inst_id].currency)

            if trading_ccy != 'EUR' and trading_ccy in fx_returns.columns:
                # Subtract trading currency return
                result_applicable[inst_id] = (
                    result_applicable[inst_id] - fx_returns[trading_ccy]
                )

        # 9. Create full result DataFrame
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # Fill with calculated values (align dates)
        for inst_id in applicable_ids:
            result.loc[common_dates, inst_id] = result_applicable[inst_id]

        # 10. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.debug(
                f"FxSpotComponent: ZERO non-zero adjustments for "
                f"{len(applicable_ids)} instruments (expected if no FX movement)"
            )
        else:
            mean_adj = result[applicable_ids].mean().mean()
            logger.debug(
                f"FxSpotComponent: Generated {non_zero} non-zero adjustments, "
                f"mean FX impact: {mean_adj:.6f}"
            )

        return result

    def __repr__(self) -> str:
        return f"FxSpotComponent(instruments={len(self.fx_composition)})"
