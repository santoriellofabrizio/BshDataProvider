"""
FX Forward carry adjustment component.

Adjusts for currency carry cost from FX forward positions.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import numpy as np
import logging
import re

from analytics.adjustments.common import calculate_year_fractions
from analytics.adjustments.component import Component
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

    Note:
        FX forward prices are assumed to be:
        - Monthly tenor (1M) → annualized by multiplying × 12
        - In basis points of rate differential → divided by 10,000
        - Expected final range: 0.0 to ~0.08 (0% to 8% annualized)
    """

    # Sanity check for rate differentials (±10% = reasonable annual rate diff)
    MAX_REASONABLE_RATE_DIFF = 0.10
    
    # Tenor mapping for annualization
    TENOR_MONTHS = {
        '1M': 1,
        '2M': 2,
        '3M': 3,
        '6M': 6,
        '9M': 9,
        '1Y': 12,
        '12M': 12,
    }

    def __init__(
        self,
        fwd_composition: pd.DataFrame,
        fx_forward_prices: pd.DataFrame,
        tenor: str,
        fx_spot_prices: pd.DataFrame,
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        """
        Initialize FX Forward carry component.

        Args:
            fwd_composition: DataFrame(instruments × currencies)
                            Index: instrument IDs
                            Columns: currency codes (USD, GBP, etc.)
                            Values: weights (0.65 = 65% exposure)
            fx_forward_prices: DataFrame(dates × currencies)
                              Forward prices in BASIS POINTS of rate differential
                              for specified tenor (typically 1M)
            tenor: Tenor string (e.g., '1M', '3M', '6M', '1Y')
            fx_spot_prices: DataFrame(dates × currencies)
                           Spot FX prices (EUR base, no transformation needed)
            settlement_days: Settlement lag (T+1=1, T+2=2, T+3=3)
            target: Optional list of instrument IDs to apply adjustments to
        """
        super().__init__(target)

        self.fwd_composition = fwd_composition.fillna(0.0)
        self.tenor = tenor
        self.settlement_days = settlement_days
        
        # Normalize FX forward columns (EURUSD 1M → USD)
        logger.debug("FxForwardCarryComponent: Normalizing FX forward columns")
        fx_fwd_normalized = self._normalize_fx_columns(fx_forward_prices, "forward")
        
        # Transform FX forward prices: annualize and convert from bp
        logger.debug(f"FxForwardCarryComponent: Transforming FX forward prices for tenor {tenor}")
        self.fx_forward_prices = self._transform_fx_forward_prices(fx_fwd_normalized, tenor)
        
        # Normalize FX spot columns (EURUSD → USD) - NO transformation needed
        logger.debug("FxForwardCarryComponent: Normalizing FX spot columns")
        self.fx_spot_prices = self._normalize_fx_columns(fx_spot_prices, "spot")

        # Validate target compatibility
        if self.target is not None:
            missing_data = self.target - set(self.fwd_composition.index)
            if missing_data:
                logger.warning(
                    f"FxForwardCarryComponent: Target contains {len(missing_data)} instruments "
                    f"without FX forward composition: {sorted(missing_data)[:5]}"
                    f"{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero FX forward carry adjustments."
                )

        logger.info(
            f"FxForwardCarryComponent: {len(self.fwd_composition)} instruments, "
            f"tenor={tenor}, "
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

        # 6. Calculate rate differentials (forward already transformed in __init__)
        rate_diffs = self._calculate_rate_differentials(
            self.fx_forward_prices,
            self.fx_spot_prices,
            self.tenor
        )

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

        # 7. Get composition matrix
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

        # 8. Matrix multiplication: (N × M) @ (M × T)^T = N × T
        weighted_rates = comp_matrix @ rate_diffs_aligned.T
        result_applicable = weighted_rates.T  # dates × instruments

        # 9. Multiply by year fractions
        year_frac_aligned = year_fractions_shifted.loc[common_dates]
        result_applicable = result_applicable.mul(year_frac_aligned, axis=0)

        # 10. Subtract trading currency carry
        for inst_id in applicable_ids:
            trading_ccy = str(instruments[inst_id].currency)

            if trading_ccy != 'EUR' and trading_ccy in rate_diffs.columns:
                trading_carry = (
                    rate_diffs_aligned[trading_ccy] * year_frac_aligned
                )
                result_applicable[inst_id] = (
                    result_applicable[inst_id] - trading_carry
                )
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids, dtype=np.float64)

        # Ensure compatible dtype and assign
        if not result_applicable.empty:
            result_applicable = result_applicable.astype(np.float64, copy=False)
            result.loc[common_dates, applicable_ids] = result_applicable

        # 12. Summary logging
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

        return - result

    # ========================================================================
    # UTILITY METHODS - Parsing, validation, normalization
    # ========================================================================

    def _transform_fx_forward_prices(
        self, 
        fx_fwd_prices: pd.DataFrame, 
        tenor: str
    ) -> pd.DataFrame:
        """
        Transform FX forward prices from basis points to annualized rates.
        
        Transformation:
            1. Annualize based on tenor (1M → × 12, 3M → × 4, etc.)
            2. Convert from basis points to decimal (÷ 10,000)
        
        Formula:
            annualized_rate = (monthly_bp × months_per_year) / 10,000
        
        Example:
            Input: 25 bp (1M tenor)
            Step 1: 25 × 12 = 300 bp (annualized)
            Step 2: 300 / 10,000 = 0.03 (3% annualized rate)
        
        Args:
            fx_fwd_prices: DataFrame with forward prices in bp (dates × currencies)
            tenor: Tenor string ('1M', '3M', '6M', '1Y')
        
        Returns:
            DataFrame with annualized rates in decimal format
        """
        # Parse tenor to get months
        tenor_upper = tenor.upper().strip()
        
        if tenor_upper not in self.TENOR_MONTHS:
            logger.warning(
                f"FxForwardCarryComponent: Unknown tenor '{tenor}'. "
                f"Supported: {list(self.TENOR_MONTHS.keys())}. "
                f"Assuming 1M (monthly)."
            )
            months = 1
        else:
            months = self.TENOR_MONTHS[tenor_upper]
        
        # Calculate annualization factor
        annualization_factor = 12 / months
        
        logger.info(
            f"FxForwardCarryComponent: Transforming forward prices - "
            f"tenor={tenor} ({months} months), "
            f"annualization factor={annualization_factor:.2f}x"
        )
        
        # Step 1: Annualize (multiply by 12/months)
        fx_fwd_annualized = fx_fwd_prices * annualization_factor
        
        # Step 2: Convert from basis points to decimal (divide by 10,000)
        fx_fwd_decimal = fx_fwd_annualized / 10000.0
        
        # Validation
        all_values = pd.to_numeric(fx_fwd_decimal.values.flatten(), errors='coerce')
        all_values = all_values[~np.isnan(all_values) & (all_values != 0)]
        
        if len(all_values) > 0:
            median_val = np.median(np.abs(all_values))
            min_val = np.min(np.abs(all_values))
            max_val = np.max(np.abs(all_values))
            
            logger.info(
                f"FxForwardCarryComponent: After transformation - "
                f"min={min_val:.6f}, median={median_val:.6f}, max={max_val:.6f} "
                f"(expected range: 0.0 to ~0.08)"
            )
            
            # Sanity checks
            if max_val > 0.15:
                logger.warning(
                    f"FxForwardCarryComponent: Max rate {max_val:.4f} (>{15}%) seems high. "
                    "Check if input is in correct format (basis points)."
                )
            
            if median_val > 0.10:
                logger.warning(
                    f"FxForwardCarryComponent: Median rate {median_val:.4f} (>{10}%) seems high. "
                    "Typical range is 0-8%."
                )
            
            if max_val < 0.001:
                logger.warning(
                    f"FxForwardCarryComponent: Max rate {max_val:.6f} (<0.1%) seems too low. "
                    "Check if input scaling is correct."
                )
        
        return fx_fwd_decimal

    @staticmethod
    def _normalize_fx_columns(fx_prices: pd.DataFrame, label: str = "prices") -> pd.DataFrame:
        """
        Normalize FX columns to currency codes.
        
        Robust parsing handles:
            'USD'           → 'USD' (already normalized)
            'EURUSD'        → 'USD' (extract quote currency)
            'EURUSD 1M'     → 'USD' (strip tenor)
            'EURUSD_1M'     → 'USD' (strip tenor with underscore)
            'EUR USD 1M'    → 'USD' (space-separated with tenor)
            'EUREUR'        → 'EUR' (EUR itself, keep as-is)
            'USDEUR'        → 'USD' (inverted, needs 1/price)
            'USDEUR 1M'     → 'USD' (inverted with tenor)
        
        Args:
            fx_prices: DataFrame with FX prices
            label: Label for logging (e.g., "forward", "spot")
        
        Returns:
            DataFrame with normalized column names
        """
        normalized_columns = {}
        columns_to_invert = []

        if isinstance(fx_prices, pd.Series):
            fx_prices = fx_prices.to_frame()
        
        for col in fx_prices.columns:
            col_str = str(col).upper().strip()
            
            # Parse column: extract base ticker and tenor
            # Pattern: split on space or underscore to separate tenor
            parts = re.split(r'[\s_]+', col_str)
            base_ticker = parts[0]
            tenor = parts[1] if len(parts) > 1 else None
            
            # Case 1: Already a 3-char currency code (USD, GBP, EUR, etc.)
            if len(base_ticker) == 3:
                normalized_columns[col] = base_ticker
                if tenor:
                    logger.debug(
                        f"FxForwardCarryComponent ({label}): '{col}' → '{base_ticker}' "
                        f"(tenor: {tenor})"
                    )
                continue
            
            # Case 2: 6-char EUR-based pair (EURUSD, EURGBP, EUREUR, etc.)
            if len(base_ticker) == 6 and base_ticker.startswith('EUR'):
                currency = base_ticker[-3:]  # Last 3 chars = quote currency
                normalized_columns[col] = currency
                
                if tenor:
                    logger.debug(
                        f"FxForwardCarryComponent ({label}): '{col}' → '{currency}' "
                        f"(tenor: {tenor})"
                    )
                else:
                    logger.debug(
                        f"FxForwardCarryComponent ({label}): '{col}' → '{currency}'"
                    )
                continue
            
            # Case 3: 6-char inverted pair (USDEUR, GBPEUR, etc.)
            if len(base_ticker) == 6 and base_ticker.endswith('EUR'):
                currency = base_ticker[:3]  # First 3 chars = base currency
                normalized_columns[col] = currency
                columns_to_invert.append(col)
                logger.warning(
                    f"FxForwardCarryComponent ({label}): '{col}' is inverted. "
                    f"Inverting prices: 1/{col} → {currency}"
                    f"{f' (tenor: {tenor})' if tenor else ''}"
                )
                continue
            
            # Case 4: Other format - keep as-is with warning
            logger.warning(
                f"FxForwardCarryComponent ({label}): '{col}' doesn't match expected format. "
                f"Expected: 'USD', 'EURUSD', 'EURUSD 1M', etc. Keeping as-is."
            )
            normalized_columns[col] = col
        
        # Create normalized DataFrame
        fx_normalized = fx_prices.copy()
        
        # Invert prices for inverted tickers
        for col in columns_to_invert:
            logger.info(
                f"FxForwardCarryComponent ({label}): Inverting prices for '{col}': "
                "new = 1 / old"
            )
            fx_normalized[col] = 1.0 / fx_normalized[col]
            fx_normalized[col].replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Rename columns
        fx_normalized = fx_normalized.rename(columns=normalized_columns)
        
        # Check for duplicates
        duplicates = fx_normalized.columns[fx_normalized.columns.duplicated()].tolist()
        if duplicates:
            logger.warning(
                f"FxForwardCarryComponent ({label}): Duplicate currency codes after normalization: {duplicates}. "
                "Keeping first occurrence."
            )
            fx_normalized = fx_normalized.loc[:, ~fx_normalized.columns.duplicated()]
        
        logger.info(
            f"FxForwardCarryComponent ({label}): Normalized columns: "
            f"{list(fx_prices.columns)} → {list(fx_normalized.columns)}"
        )
        
        return fx_normalized

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
            fx_fwd_prices: Forward prices (dates × currencies) - ALREADY TRANSFORMED
                          (annualized and in decimal format)
            fx_spot_prices: Spot prices (dates × currencies) - EUR base FX prices
            tenor: Tenor string for logging

        Returns:
            DataFrame(dates × currencies) with rate differentials

        Note:
            - Forward prices are RATE DIFFERENTIALS in annualized decimal format
            - Spot prices are standard FX prices (EUR base)
            - Since forwards are already rate diffs, we return them directly
        """
        # Forward prices are already rate differentials (annualized)
        # No need to calculate (fwd - spot) / spot
        # Just return the forward prices as-is
        
        logger.debug(
            f"FxForwardCarryComponent: Using forward rate differentials for tenor {tenor} "
            f"(already annualized and in decimal format)"
        )
        
        # Align columns with spot (for validation)
        common_currencies = fx_fwd_prices.columns.intersection(fx_spot_prices.columns)
        
        if len(common_currencies) == 0:
            logger.warning(
                f"FxForwardCarryComponent: No currency overlap for tenor {tenor}. "
                f"Forward: {list(fx_fwd_prices.columns)}, "
                f"Spot: {list(fx_spot_prices.columns)}"
            )
            return pd.DataFrame()
        
        # Use forward prices directly as rate differentials
        rate_diffs = fx_fwd_prices[common_currencies].copy()
        
        # Sanity check: warn if rate differentials exceed reasonable bounds
        max_abs_diff = rate_diffs.abs().max().max()
        if max_abs_diff > self.MAX_REASONABLE_RATE_DIFF:
            logger.warning(
                f"FxForwardCarryComponent: Rate differentials for tenor {tenor} "
                f"exceed {self.MAX_REASONABLE_RATE_DIFF:.1%} (max: {max_abs_diff:.1%}). "
                "Check if forward prices are in correct format."
            )
        
        logger.debug(
            f"FxForwardCarryComponent: Rate differentials for tenor {tenor}, "
            f"{len(rate_diffs)} dates, {len(common_currencies)} currencies"
        )
        
        return rate_diffs

    # ========================================================================
    # UPDATABLE INTERFACE - Support for progressive data updates
    # ========================================================================

    def is_updatable(self) -> bool:
        """This component supports data updates"""
        return True

    @property
    def updatable_fields(self) -> set[str]:
        """Declare updatable fields (subscription model)"""
        return {"fx_forward_prices", "fx_prices"}

    def append_data(
        self,
        *,
        fx_forward_prices: Optional[pd.DataFrame] = None,
        fx_prices: Optional[pd.DataFrame] = None
    ) -> None:
        """
        Append new FX forward and/or spot price data permanently.

        Args:
            fx_forward_prices: New FX forward price DataFrame (dates × currencies)
                              in BASIS POINTS format
            fx_prices: New FX spot price DataFrame (dates × currencies)

        Raises:
            ValueError: If data is invalid (not DataFrame or empty)
        """
        if fx_forward_prices is not None:
            # Validate
            if not isinstance(fx_forward_prices, pd.DataFrame):
                raise ValueError("fx_forward_prices must be DataFrame")
            if fx_forward_prices.empty:
                raise ValueError("fx_forward_prices cannot be empty")

            # Normalize columns
            new_fx_fwd = self._normalize_fx_columns(fx_forward_prices, "forward")

            # Transform (annualize and convert from bp)
            new_fx_fwd_transformed = self._transform_fx_forward_prices(new_fx_fwd, self.tenor)

            # Permanently append
            # Note: Don't use drop_duplicates() on values, only on index (to remove duplicate timestamps)
            self.fx_forward_prices = pd.concat(
                [self.fx_forward_prices, new_fx_fwd_transformed]
            ).sort_index()
            # Remove duplicate index entries (keep last)
            self.fx_forward_prices = self.fx_forward_prices[~self.fx_forward_prices.index.duplicated(keep='last')]

            logger.debug(
                f"FxForwardCarryComponent: Appended fx_forward_prices "
                f"(now {len(self.fx_forward_prices)} rows)"
            )

        if fx_prices is not None:
            # Validate
            if not isinstance(fx_prices, pd.DataFrame):
                raise ValueError("fx_prices must be DataFrame")
            if fx_prices.empty:
                raise ValueError("fx_prices cannot be empty")

            # Normalize columns
            new_fx_spot = self._normalize_fx_columns(fx_prices, "spot")

            # Permanently append
            # Note: Don't use drop_duplicates() on values, only on index (to remove duplicate timestamps)
            self.fx_spot_prices = pd.concat(
                [self.fx_spot_prices, new_fx_spot]
            ).sort_index()
            # Remove duplicate index entries (keep last)
            self.fx_spot_prices = self.fx_spot_prices[~self.fx_spot_prices.index.duplicated(keep='last')]

            logger.debug(
                f"FxForwardCarryComponent: Appended fx_prices "
                f"(now {len(self.fx_spot_prices)} rows)"
            )

    def save_state(self) -> dict:
        """Save current state for restoration (used by live_update)"""
        return {
            'fx_forward_prices': self.fx_forward_prices.copy(),
            'fx_spot_prices': self.fx_spot_prices.copy()
        }

    def restore_state(self, state: dict) -> None:
        """Restore saved state (used by live_update)"""
        self.fx_forward_prices = state['fx_forward_prices'].copy()
        self.fx_spot_prices = state['fx_spot_prices'].copy()
        logger.debug("FxForwardCarryComponent: State restored")

    def __repr__(self) -> str:
        return (
            f"FxForwardCarryComponent("
            f"instruments={len(self.fwd_composition)}, "
            f"tenor={self.tenor})"
        )
