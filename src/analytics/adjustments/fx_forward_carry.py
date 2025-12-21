"""
FX Forward component for currency hedging costs.

Calculates carry cost from FX forwards using interest rate differentials
derived from forward prices.
"""
from datetime import date, datetime
from typing import Literal, Union, List
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType


logger = logging.getLogger(__name__)


# Tenor annualization factors
TENOR_FACTORS = {
    '1W': 52,
    '2W': 26,
    '1M': 12,
    '2M': 6,
    '3M': 4,
    '6M': 2,
    '9M': 4/3,
    '12M': 1,
}

# Unit conversion divisors
UNIT_DIVISORS = {
    'bp': 10000,      # basis points
    'pct': 100,       # percentage
    'decimal': 1,     # already decimal
    'pips': 10000,    # pips
}


def to_str(obj) -> str:
    """Convert CurrencyEnum or str to uppercase string."""
    return str(getattr(obj, 'value', obj)).upper()


class FxForwardComponent(Component):
    """
    FX Forward hedging cost component.

    Calculates carry cost from currency hedging using interest rate differentials
    derived from FX forward prices.

    Formula:
        1. Annualize based on tenor: fwd_ann = fwd × tenor_factor
        2. Convert to decimal: fwd_diff = fwd_ann / unit_divisor
        3. Get rate differential: rate_diff = fwd_diff / spot
           This gives: r_EUR - r_ccy
        4. Apply weights: cost = Σ(rate_diff[ccy] × weight[ccy]) × year_fraction
        5. Final adjustment: -cost (negative = cost to hedge)

    Theory:
        FX Forward Price = (Forward Rate - Spot Rate)
        Forward Rate = Spot × (1 + r_EUR) / (1 + r_ccy)
        Therefore: Price / Spot ≈ r_EUR - r_ccy

    Usage:
        # FX Forward 1M prices in basis points (DEFAULT)
        fx_fwd_prices = pd.DataFrame({
            'USD': [15.0, 16.0, 14.5],  # EUR rates 15bps higher → pay premium
            'GBP': [-5.0, -4.5, -5.2],  # EUR rates 5bps lower → receive discount
        }, index=dates)

        component = FxForwardComponent(
            fxfwd_composition,
            fx_fwd_prices,
            tenor="1M",    # Default
            unit="bp"      # Default
        )

        # For 3M forwards
        component_3m = FxForwardComponent(
            fxfwd_composition,
            fx_fwd_3m_prices,
            tenor="3M",
            unit="bp"
        )
    """

    def __init__(
        self,
        fxfwd_composition: pd.DataFrame,
        fx_fwd_prices: pd.DataFrame,
        tenor: str = "1M",
        unit: str = "bp",
        shifted_settlement: Literal["T+1", "T+2", "T+3"] = "T+2",
    ):
        """
        Initialize FX Forward component.

        Args:
            fxfwd_composition: DataFrame(instruments × currencies)
                              Hedged currency weights (0.65 = 65% hedged)
            fx_fwd_prices: DataFrame(dates × currencies)
                          FX Forward prices in specified unit
                          Positive = EUR rates higher (pay premium)
                          Negative = EUR rates lower (receive discount)
            tenor: Forward tenor (e.g., "1M", "3M", "6M", "12M")
                  Default: "1M"
            unit: Price unit - "bp" (basis points), "pct" (percentage),
                  "decimal", or "pips"
                  Default: "bp" (most common market convention)
            shifted_settlement: Settlement convention (T+1, T+2, T+3)

        Note:
            fx_spot_prices are passed from Adjuster in calculate_batch()
            to avoid duplication and ensure consistency.
        """
        self.fxfwd_composition = fxfwd_composition.fillna(0.0).copy()
        self.fx_fwd_prices = fx_fwd_prices.fillna(0.0).copy()
        self.settlement_days = int(shifted_settlement.replace("T+", ""))

        # Tenor and unit configuration
        self.tenor = tenor.upper()
        self.unit = unit.lower()

        if self.tenor not in TENOR_FACTORS:
            raise ValueError(f"Unknown tenor: {tenor}. Supported: {list(TENOR_FACTORS.keys())}")
        if self.unit not in UNIT_DIVISORS:
            raise ValueError(f"Unknown unit: {unit}. Supported: {list(UNIT_DIVISORS.keys())}")

        self.tenor_factor = TENOR_FACTORS[self.tenor]
        self.unit_divisor = UNIT_DIVISORS[self.unit]

        logger.info(
            f"FxForwardComponent: {len(self.fxfwd_composition)} instruments, "
            f"{len(self.fxfwd_composition.columns)} currencies, "
            f"tenor={self.tenor} (×{self.tenor_factor}), "
            f"unit={self.unit} (÷{self.unit_divisor}), "
            f"settlement={shifted_settlement}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if applicable.

        Applicable if:
        - STOCK or ETP
        - Has FX forward composition (hedged)
        - IS currency_hedged = True
        """
        if instrument.type not in [InstrumentType.STOCK, InstrumentType.ETP]:
            return False

        if instrument.id not in self.fxfwd_composition.index:
            return False

        # Check currency_hedged attribute (opposite of FxSpotComponent)
        if hasattr(instrument, 'currency_hedged'):
            is_hedged = instrument.currency_hedged
            if is_hedged is False:
                return False

            if not is_hedged:
                logger.debug(f"{instrument.id} is not hedged, skipping FX forward")
                return False
        else:
            # No attribute → assume not hedged
            return True

        return True

    def calculate_batch(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,  # ← FX SPOT prices from Adjuster
    ) -> pd.DataFrame:
        """
        Calculate FX forward carry costs.

        Args:
            instruments: Instrument objects
            dates: Calculation dates or datetimes
            prices: Price data (not used)
            fx_prices: FX SPOT prices (passed from Adjuster for consistency)

        Returns:
            DataFrame(dates × instruments) with forward carry adjustments
        """
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))

        # Filter applicable
        applicable_ids = [
            i.id for i in instruments.values()
            if self.is_applicable(i) and i.id in self.fxfwd_composition.index
        ]

        if not applicable_ids:
            logger.debug("No applicable instruments for FxForwardComponent")
            return result

        logger.debug(f"FxForwardComponent: {len(applicable_ids)}/{len(instruments)} instruments")

        # Calculate rate differentials from forward prices
        rate_diffs = self._calculate_rate_differentials(
            self.fx_fwd_prices,
            fx_prices
        )

        # Calculate year fractions with settlement shift
        year_fractions = calculate_year_fractions(
            dates,
            shifted=True,
            settlement_days=self.settlement_days
        )

        # Calculate adjustments
        result_applicable = self._calculate_forward_costs(
            applicable_ids,
            rate_diffs,
            year_fractions,
            dates
        )

        # Fill result
        for inst_id in applicable_ids:
            if inst_id in result_applicable.columns:
                result[inst_id] = result_applicable[inst_id]

        return result

    def _calculate_rate_differentials(
        self,
        fx_fwd_prices: pd.DataFrame,
        fx_spot_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate interest rate differentials from forward prices.

        Formula:
            1. fwd_ann = fwd × tenor_factor (e.g., 1M: ×12, 3M: ×4)
            2. fwd_diff = fwd_ann / unit_divisor (e.g., bp: ÷10000, pct: ÷100)
            3. rate_diff = fwd_diff / spot

        This gives: rate_diff ≈ r_EUR - r_ccy

        Args:
            fx_fwd_prices: Forward prices in specified unit and tenor
            fx_spot_prices: Spot FX rates

        Returns:
            DataFrame(dates × currencies) with rate differentials
        """
        # Step 1: Annualize based on tenor
        fx_fwd_annualized = fx_fwd_prices * self.tenor_factor

        # Step 2: Convert to decimal based on unit
        fx_fwd_diff = fx_fwd_annualized / self.unit_divisor

        # Step 3: Normalize by spot to get rate differential
        # Align dates and currencies
        common_dates = fx_fwd_diff.index.intersection(fx_spot_prices.index)
        common_ccys = fx_fwd_diff.columns.intersection(fx_spot_prices.columns)

        if len(common_dates) == 0:
            logger.warning("No common dates between forward and spot prices")
            return pd.DataFrame(0.0, index=fx_fwd_diff.index, columns=fx_fwd_diff.columns)

        if len(common_ccys) == 0:
            logger.warning("No common currencies between forward and spot prices")
            return pd.DataFrame(0.0, index=fx_fwd_diff.index, columns=fx_fwd_diff.columns)

        # Calculate rate differential
        rate_diffs = fx_fwd_diff.loc[common_dates, common_ccys] / fx_spot_prices.loc[common_dates, common_ccys]

        logger.debug(
            f"Calculated rate differentials for {len(common_ccys)} currencies, "
            f"{len(common_dates)} dates "
            f"(tenor={self.tenor}, unit={self.unit})"
        )

        return rate_diffs

    def _calculate_forward_costs(
        self,
        applicable_ids: list[str],
        rate_diffs: pd.DataFrame,
        year_fractions: pd.Series,
        dates: list[date],
    ) -> pd.DataFrame:
        """
        Calculate forward costs using rate differentials.

        Formula:
            cost[inst] = Σ(rate_diff[ccy] × weight[ccy]) × year_fraction
            adjustment = -cost  (negative = cost to hedge)
        """
        result = pd.DataFrame(0.0, index=dates, columns=applicable_ids)

        # Get composition for applicable instruments
        comp_matrix = self.fxfwd_composition.loc[applicable_ids]

        # Align dates
        common_dates = rate_diffs.index.intersection(dates)
        if len(common_dates) == 0:
            logger.warning("No common dates for forward cost calculation")
            return result

        # Align currencies
        common_ccys = comp_matrix.columns.intersection(rate_diffs.columns)
        comp_aligned = comp_matrix[common_ccys]
        rate_diffs_aligned = rate_diffs[common_ccys]

        # For each instrument and date, calculate weighted cost
        for inst_id in applicable_ids:
            composition = comp_aligned.loc[inst_id]

            for d in common_dates:
                if d not in year_fractions.index:
                    continue

                rate_diff_d = rate_diffs_aligned.loc[d]

                # Weighted sum of rate differentials
                weighted_rate = (composition * rate_diff_d).sum()

                # Apply year fraction (negative = cost)
                result.loc[d, inst_id] = -weighted_rate * year_fractions.loc[d]

        return result

    def __repr__(self) -> str:
        return (
            f"FxForwardComponent({len(self.fxfwd_composition)} instruments, "
            f"tenor={self.tenor}, unit={self.unit}, T+{self.settlement_days})"
        )