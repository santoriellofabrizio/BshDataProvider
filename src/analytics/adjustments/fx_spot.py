"""
FX Spot exposure adjustment component for ETF and Stock.

Adjusts for currency exposure mismatch between portfolio and trading currency.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from analytics.adjustments.common import normalize_fx_columns
from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType
from core.enums.currencies import CurrencyEnum


logger = logging.getLogger(__name__)


class FxSpotComponent(Component):
    """
    FX Spot exposure adjustment component (UpdatableComponent).

    Adjusts returns for currency exposure when portfolio composition differs
    from trading currency using vectorized matrix operations.

    Formula:
        fx_correction = I·(fx_return[ccy] × weight[ccy]) - fx_return[trading_ccy]

    Updatable Fields:
        - fx_prices: FX price data

    Usage:
        # Initialize
        fx_comp = FxSpotComponent(fx_composition, fx_prices)
        adjuster = Adjuster(prices).add(fx_comp)

        # Append mode: permanently add new data
        adjuster.append_update(fx_prices=new_fx_prices)

        # Live mode: temporary calculation without storage
        adjuster.live_update(fx_prices=live_fx_prices)
    """

    # Sanity check bounds for composition sum
    LOWER_SANITY_CHECK = -0.1  # Allow 10% negative for short positions
    UPPER_SANITY_CHECK = 1.1   # Allow 10% over for rounding/leverage

    def __init__(self, fx_composition: pd.DataFrame, fx_prices: pd.DataFrame, target: Optional[List[str]] = None):
        """
        Initialize FX Spot component.

        Args:
            fx_composition: DataFrame(instruments × currencies)
                           Index: instrument IDs
                           Columns: currency codes (USD, GBP, etc.)
                           Values: weights in decimal (0.65 = 65%)
                           Sparse OK (NaN treated as 0)
            fx_prices: DataFrame(dates × currencies) with FX prices
            target: Optional list of instrument IDs to apply adjustments to
        """
        super().__init__(target)

        # Fill NaN with 0 and validate currencies
        self.fx_composition = fx_composition.fillna(0.0).copy()
        self._fx_prices = normalize_fx_columns(fx_prices)

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

    def is_updatable(self) -> bool:
        """This component supports data updates"""
        return True

    @property
    def updatable_fields(self) -> set[str]:
        """Declare updatable fields (subscription model)"""
        return {"fx_prices"}

    def append_data(self, *, fx_prices: Optional[pd.DataFrame] = None) -> None:
        """
        Append new FX price data permanently.

        Args:
            fx_prices: New FX price DataFrame (dates × currencies)

        Raises:
            ValueError: If fx_prices is invalid (not DataFrame or empty)
        """
        if fx_prices is None:
            return

        # Validate
        if not isinstance(fx_prices, pd.DataFrame):
            raise ValueError("fx_prices must be DataFrame")
        if fx_prices.empty:
            raise ValueError("fx_prices cannot be empty")

        # Normalize columns
        new_fx_prices = normalize_fx_columns(fx_prices)

        # Permanently append
        # Note: Don't use drop_duplicates() on values, only on index (to remove duplicate timestamps)
        self._fx_prices = pd.concat([self._fx_prices, new_fx_prices]).sort_index()
        # Remove duplicate index entries (keep last)
        self._fx_prices = self._fx_prices[~self._fx_prices.index.duplicated(keep='last')]
        logger.debug(f"FxSpotComponent: Appended fx_prices (now {len(self._fx_prices)} rows)")

    def save_state(self) -> dict:
        """Save current state for restoration"""
        return {'fx_prices': self._fx_prices.copy()}

    def restore_state(self, state: dict) -> None:
        """Restore saved state"""
        self._fx_prices = state['fx_prices']

    def apply_temp_data(self, *, fx_prices: Optional[pd.DataFrame] = None, **kwargs) -> None:
        """
        Temporarily extend fx_prices without permanent storage.

        Args:
            fx_prices: Temporary FX price data
        """
        if fx_prices is None:
            return

        # Validate
        if not isinstance(fx_prices, pd.DataFrame):
            raise ValueError("fx_prices must be DataFrame")
        if fx_prices.empty:
            raise ValueError("fx_prices cannot be empty")

        # Normalize columns
        new_fx_prices = normalize_fx_columns(fx_prices)

        # Temporarily extend (will be restored by context manager)
        self._fx_prices = pd.concat([self._fx_prices, new_fx_prices]).drop_duplicates().sort_index()
        logger.debug(f"FxSpotComponent: Temp fx_prices applied ({len(new_fx_prices)} rows)")

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
    ) -> pd.DataFrame:
        """
        Calculate FX spot adjustments using vectorized matrix operations.

        Performance: O(N×M×T) → O(N×M + M×T) with matrix multiplication
        """
        # 1. Validate input
        self.validate_input(instruments, dates, prices)

        # 2. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)

        instrument_ids = list(instruments.keys())

        # 3. Filter applicable (USE should_apply)
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst) and inst.id in self.fx_composition.index
        ]

        # 4. Early return if no applicable
        if not applicable_ids:
            logger.debug(
                f"FxSpotComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
            self.validate_output(result)
            return result

        # 5. Log processing
        logger.info(
            f"FxSpotComponent: Processing {len(applicable_ids)}/{len(instruments)} instruments"
        )

        # 6. Calculate FX returns using return calculator
        fx_returns = self.return_calculator.calculate_returns(self._fx_prices)
        fx_returns = fx_returns.where(fx_returns.notna(), 0.0)

        # Ensure dates alignment
        common_dates = fx_returns.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"FxSpotComponent: ZERO adjustments - no date overlap. "
                f"FX dates: {fx_returns.index.min()} to {fx_returns.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
            self.validate_output(result)
            return result

        # 7. Get composition matrix for applicable instruments
        comp_matrix = self.fx_composition.loc[applicable_ids]  # N × M

        # Align currencies between composition and fx_returns
        common_currencies = comp_matrix.columns.intersection(fx_returns.columns)
        if len(common_currencies) == 0:
            logger.error(f"FxSpotComponent: No currency overlap. Composition currencies: "
                         f"{list(comp_matrix.columns)}, FX currencies: {list(fx_returns.columns)}")

            result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
            self.validate_output(result)
            return result

        comp_matrix = comp_matrix[common_currencies]
        fx_ret_matrix = fx_returns[common_currencies]  # T × M

        # 8. Matrix multiplication: (N × M) @ (M × T)^T = N × T
        # weighted_fx: instruments × dates
        weighted_fx = comp_matrix @ fx_ret_matrix.T

        # Transpose to dates × instruments
        result_applicable = weighted_fx.T

        # 9. Subtract trading currency return per instrument
        for inst_id in applicable_ids:
            trading_ccy = str(instruments[inst_id].currency)

            if trading_ccy != 'EUR' and trading_ccy in fx_returns.columns:
                # Subtract trading currency return
                result_applicable[inst_id] = (
                    result_applicable[inst_id] - fx_returns[trading_ccy]
                )

        # 10. Create full result DataFrame
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids, dtype='float64')

        # Fill with calculated values (align dates)
        for inst_id in applicable_ids:
            result.loc[common_dates, inst_id] = result_applicable[inst_id].astype('float64')

        # 11. Summary logging
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

        # 12. Validate output
        self.validate_output(result)

        return result

    def __repr__(self) -> str:
        return f"FxSpotComponent(instruments={len(self.fx_composition)})"
