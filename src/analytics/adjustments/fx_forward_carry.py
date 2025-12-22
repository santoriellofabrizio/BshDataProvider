"""
FX Forward carry adjustment component.

Adjusts for currency carry cost from FX forward positions.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import numpy as np
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType

logger = logging.getLogger(__name__)


class FxForwardCarryComponent(Component):
    """
    FX Forward carry adjustment component.

    Calculates carry cost for FX forward positions by computing implied
    interest rate differentials from forward points.

    Formula:
        rate_differential = (fwd_price - spot_price) / spot_price
        carry_adjustment = Σ(rate_diff[ccy] × weight[ccy] × year_fraction) - 
                          rate_diff[trading_ccy] × year_fraction

    Usage:
        # API returns:
        # 1. FX forward composition (instruments × currencies)
        # 2. FX forward prices dict (tenor → DataFrame of prices)
        # 3. FX spot prices (for rate differential calculation)

        fwd_composition = pd.DataFrame({
            'USD': [0.65, 0.60],
            'GBP': [0.10, 0.15],
        }, index=['FUND_A', 'FUND_B'])

        fwd_prices = {
            '1M': pd.DataFrame(...),  # dates × currencies
            '3M': pd.DataFrame(...),
        }

        adjuster.add(FxForwardCarryComponent(
            fwd_composition=fwd_composition,
            fx_forward_prices=fwd_prices,
            fx_spot_prices=fx_spot_prices,
            settlement_days=2
        ))
    """

    # Sanity check for rate differentials (±10% = reasonable annual rate diff)
    MAX_REASONABLE_RATE_DIFF = 0.10

    def __init__(
        self,
        fwd_composition: pd.DataFrame,
        fx_forward_prices: dict[str, pd.DataFrame],
        fx_spot_prices: pd.DataFrame,
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        """
        Initialize FX Forward carry component.

        Args:
            fwd_composition: DataFrame(instruments × currencies)
                            Index: instrument IDs
                            Columns: currency codes
                            Values: weights (0.65 = 65% exposure)
            fx_forward_prices: Dict[tenor → DataFrame(dates × currencies)]
                              Tenors: '1M', '3M', '6M', etc.
                              Values: Forward prices (EUR base)
            fx_spot_prices: DataFrame(dates × currencies)
                           Spot prices for rate differential calculation
            settlement_days: Settlement lag (T+1=1, T+2=2, T+3=3)
            target: Optional list of instrument IDs to apply adjustments to

        Example:
            fwd_comp = pd.DataFrame({'USD': [0.65]}, index=['FUND_A'])
            fwd_prices = {'1M': pd.DataFrame({'USD': [1.121, 1.122]}, index=dates)}
            spot_prices = pd.DataFrame({'USD': [1.12, 1.12]}, index=dates)
            
            component = FxForwardCarryComponent(
                fwd_comp, fwd_prices, spot_prices, settlement_days=2
            )
        """
        super().__init__(target)

        self.fwd_composition = fwd_composition.fillna(0.0)
        self.fx_forward_prices = fx_forward_prices
        self.fx_spot_prices = fx_spot_prices
        self.settlement_days = settlement_days

        # Validate tenors
        if not self.fx_forward_prices:
            raise ValueError("fx_forward_prices dict is empty")

        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.fwd_composition.index)
            if missing_data:
                logger.warning(
                    f"FxForwardCarryComponent: Target contains {len(missing_data)} instruments "
                    f"without FX forward composition: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero FX forward carry adjustments."
                )

        logger.info(
            f"FxForwardCarryComponent: {len(self.fwd_composition)} instruments, "
            f"{len(self.fx_forward_prices)} tenors, "
            f"T+{settlement_days} settlement"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check applicability (domain logic only).

        Applicable if:
        - ETP (ETF)
        - Has FX forward composition
        - NOT currency_hedged (default False)
        """
        if instrument.type != InstrumentType.ETP:
            return False

        if instrument.id not in self.fwd_composition.index:
            return False

        # Check if currency hedged
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
        """Calculate FX forward carry adjustments."""
        # 1. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        instrument_ids = list(instruments.keys())

        # 2. Filter applicable (USE should_apply)
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst) and inst.id in self.fwd_composition.index
        ]

        # 3. Early return if no applicable
        if not applicable_ids:
            logger.debug(
                f"FxForwardCarryComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 4. Log processing
        logger.info(
            f"FxForwardCarryComponent: Processing {len(applicable_ids)}/{len(instruments)} instruments"
        )

        # 5. Calculate shifted year fractions
        year_fractions_shifted = calculate_year_fractions(
            dates_dt,
            shifted=True,
            settlement_days=self.settlement_days
        )

        # 6. Calculate rate differentials for each tenor
        rate_diffs_by_tenor = {}

        for tenor, fwd_prices_tenor in self.fx_forward_prices.items():
            rate_diffs = self._calculate_rate_differentials(
                fwd_prices_tenor,
                self.fx_spot_prices,
                tenor
            )
            rate_diffs_by_tenor[tenor] = rate_diffs

        # 7. Select primary tenor (use first available)
        primary_tenor = list(self.fx_forward_prices.keys())[0]
        rate_diffs = rate_diffs_by_tenor[primary_tenor]

        # Align dates
        common_dates = rate_diffs.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"FxForwardCarryComponent: ZERO adjustments - no date overlap. "
                f"Rate diff dates: {rate_diffs.index.min()} to {rate_diffs.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 8. Get composition matrix
        comp_matrix = self.fwd_composition.loc[applicable_ids]

        # Align currencies
        common_currencies = comp_matrix.columns.intersection(rate_diffs.columns)
        if len(common_currencies) == 0:
            logger.error(
                f"FxForwardCarryComponent: No currency overlap. "
                f"Composition: {list(comp_matrix.columns)}, "
                f"Rate diffs: {list(rate_diffs.columns)}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        comp_matrix = comp_matrix[common_currencies]
        rate_diffs_aligned = rate_diffs.loc[common_dates, common_currencies]

        # 9. Matrix multiplication: (N × M) @ (M × T)^T = N × T
        weighted_rates = comp_matrix @ rate_diffs_aligned.T
        result_applicable = weighted_rates.T  # dates × instruments

        # 10. Multiply by year fractions
        year_frac_aligned = year_fractions_shifted.loc[common_dates]
        result_applicable = result_applicable.mul(year_frac_aligned, axis=0)

        # 11. Subtract trading currency carry
        for inst_id in applicable_ids:
            trading_ccy = str(instruments[inst_id].currency)

            if trading_ccy != 'EUR' and trading_ccy in rate_diffs.columns:
                trading_carry = (
                    rate_diffs_aligned[trading_ccy] * year_frac_aligned
                )
                result_applicable[inst_id] = (
                    result_applicable[inst_id] - trading_carry
                )

        # 12. Create full result
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
        result.loc[common_dates, applicable_ids] = result_applicable

        # 13. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.debug(
                f"FxForwardCarryComponent: ZERO non-zero adjustments for "
                f"{len(applicable_ids)} instruments (expected if no rate differentials)"
            )
        else:
            mean_adj = result[applicable_ids].mean().mean()
            logger.debug(
                f"FxForwardCarryComponent: Generated {non_zero} non-zero adjustments, "
                f"mean carry impact: {mean_adj:.6f}"
            )

        return result

    # ========================================================================
    # UTILITY METHODS - Parsing, validation, normalization
    # ========================================================================

    def _calculate_rate_differentials(
        self,
        fx_fwd_prices: pd.DataFrame,
        fx_spot_prices: pd.DataFrame,
        tenor: str,
    ) -> pd.DataFrame:
        """
        Calculate implied rate differentials from forward points.

        Formula:
            rate_diff = (fwd - spot) / spot

        Args:
            fx_fwd_prices: Forward prices (dates × currencies)
            fx_spot_prices: Spot prices (dates × currencies)
            tenor: Tenor string for logging

        Returns:
            DataFrame(dates × currencies) with rate differentials

        Note:
            - Handles zero/NaN spot prices (returns 0)
            - Validates reasonable ranges (warns if >10% differential)
        """
        # Align columns
        common_currencies = fx_fwd_prices.columns.intersection(fx_spot_prices.columns)

        if len(common_currencies) == 0:
            logger.warning(
                f"FxForwardCarryComponent: No currency overlap for tenor {tenor}. "
                f"Forward: {list(fx_fwd_prices.columns)}, "
                f"Spot: {list(fx_spot_prices.columns)}"
            )
            return pd.DataFrame()

        fwd_aligned = fx_fwd_prices[common_currencies]
        spot_aligned = fx_spot_prices[common_currencies]

        # Align dates
        common_dates = fwd_aligned.index.intersection(spot_aligned.index)

        if len(common_dates) == 0:
            logger.warning(
                f"FxForwardCarryComponent: No date overlap for tenor {tenor}"
            )
            return pd.DataFrame()

        fwd_aligned = fwd_aligned.loc[common_dates]
        spot_aligned = spot_aligned.loc[common_dates]

        # Calculate forward differential (fwd - spot)
        fwd_diff = fwd_aligned - spot_aligned

        # Validate spot prices and handle division by zero
        spot_safe = spot_aligned.copy()
        spot_safe = spot_safe.replace(0, np.nan)  # Zero → NaN

        # Count missing/zero spots
        missing_count = spot_safe.isna().sum().sum()
        if missing_count > 0:
            logger.warning(
                f"FxForwardCarryComponent: {missing_count} spot prices missing/zero "
                f"for tenor {tenor}. Setting rate differentials to 0."
            )

        # Calculate rate differential: (fwd - spot) / spot
        rate_diffs = fwd_diff / spot_safe

        # Replace inf/-inf with NaN, then fill with 0
        rate_diffs = rate_diffs.replace([np.inf, -np.inf], np.nan).fillna(0.0)

        # Sanity check: warn if rate differentials exceed reasonable bounds
        max_abs_diff = rate_diffs.abs().max().max()
        if max_abs_diff > self.MAX_REASONABLE_RATE_DIFF:
            logger.warning(
                f"FxForwardCarryComponent: Rate differentials for tenor {tenor} "
                f"exceed {self.MAX_REASONABLE_RATE_DIFF:.1%} (max: {max_abs_diff:.1%}). "
                "Check if forward prices are in correct units (e.g., points vs price)."
            )

        logger.debug(
            f"FxForwardCarryComponent: Calculated rate diffs for tenor {tenor}, "
            f"{len(common_dates)} dates, {len(common_currencies)} currencies"
        )

        return rate_diffs

    def __repr__(self) -> str:
        return (
            f"FxForwardCarryComponent("
            f"instruments={len(self.fwd_composition)}, "
            f"tenors={list(self.fx_forward_prices.keys())})"
        )
