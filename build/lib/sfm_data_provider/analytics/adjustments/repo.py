"""
Repo adjustment component for Futures.

Adjusts for repo financing cost when holding futures positions.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.analytics.adjustments.common import calculate_year_fractions
from sfm_data_provider.analytics.adjustments import InstrumentProtocol
from sfm_data_provider.core.enums.instrument_types import InstrumentType

logger = logging.getLogger(__name__)


class RepoComponent(Component):
    """
    Repo adjustment component for futures financing cost.

    Formula:
        adjustment = -repo_rate × year_fraction_shifted

    Two modes:
    1. Direct repo rates per instrument
    2. Currency-based repo rates (mapped via future_currencies)

    Usage:
        # Mode 1: Direct repo rates per instrument
        repo_rates = pd.DataFrame({
            'FUTURE_1': [0.025, 0.026],  # dates × instruments
            'FUTURE_2': [0.028, 0.029],
        }, index=dates)

        intraday_adjuster.add(RepoComponent(repo_rates, mode='direct'))

        # Mode 2: Currency-based rates
        repo_rates = pd.DataFrame({
            'USD': [0.025, 0.026],  # dates × currencies
            'EUR': [0.020, 0.021],
        }, index=dates)

        future_currencies = pd.Series({
            'FUTURE_1': 'USD',
            'FUTURE_2': 'EUR',
        })

        intraday_adjuster.add(RepoComponent(
            repo_rates,
            mode='currency',
            future_currencies=future_currencies
        ))
    """

    def __init__(
        self,
        repo_rates: pd.DataFrame,
        mode: str = 'direct',
        future_currencies: Optional[Union[pd.Series, dict]] = None,
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        """
        Initialize Repo component.

        Args:
            repo_rates: DataFrame with repo rates
                       Mode 'direct': dates × instrument_ids (rates per instrument)
                       Mode 'currency': dates × currencies (rates per currency)
            mode: 'direct' or 'currency'
            future_currencies: Required if mode='currency'
                              Series mapping instrument_id -> currency code
            settlement_days: Settlement lag (T+1=1, T+2=2, T+3=3)
            target: Optional list of instrument IDs to apply adjustments to

        Example:
            # Direct mode
            repo_comp = RepoComponent(repo_rates_direct, mode='direct')

            # Currency mode with target
            repo_comp = RepoComponent(
                repo_rates_currency,
                mode='currency',
                future_currencies=future_currencies,
                target=['FUTURE_1', 'FUTURE_2']
            )
        """
        super().__init__(target)

        if mode not in ['direct', 'currency']:
            raise ValueError(f"mode must be 'direct' or 'currency', got '{mode}'")

        if mode == 'currency' and future_currencies is None:
            raise ValueError("future_currencies required when mode='currency'")

        if isinstance(future_currencies, dict):
            future_currencies = pd.Series(future_currencies)

        self.repo_rates = repo_rates.fillna(0.0)
        self.mode = mode
        self.future_currencies = future_currencies
        self.settlement_days = settlement_days

        # Validate target compatibility
        if self.target is not None:
            if mode == 'direct':
                missing_data = self.target - set(self.repo_rates.columns)
            else:  # currency mode
                # Check if target instruments have currency mapping
                missing_data = self.target - set(future_currencies.index if future_currencies is not None else [])
            
            if missing_data:
                logger.warning(
                    f"RepoComponent: Target contains {len(missing_data)} instruments "
                    f"without repo data: {sorted(missing_data)[:5]}{'...' if len(missing_data) > 5 else ''}. "
                    "These will receive zero repo adjustments."
                )

        logger.info(
            f"RepoComponent: mode={mode}, "
            f"{len(self.repo_rates.columns)} {'instruments' if mode == 'direct' else 'currencies'}, "
            f"T+{settlement_days} settlement"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check applicability (domain logic only).

        Applicable if:
        - FUTURE
        - Has repo data (direct or currency-mapped)
        """
        if instrument.type != InstrumentType.FUTURE:
            return False

        # Mode 1: Direct repo rates
        if self.mode == 'direct':
            return instrument.id in self.repo_rates.columns

        # Mode 2: Currency-based rates
        if self.future_currencies is not None and instrument.id in self.future_currencies.index:
            ccy = str(self.future_currencies[instrument.id]).upper()
            return ccy in self.repo_rates.columns

        return False

    def calculate_adjustment(
        self,
        instruments: dict[str, InstrumentProtocol],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Calculate repo adjustments."""
        # 1. Normalize dates to datetime (MANDATORY)
        dates_dt = self._normalize_dates(dates)
        
        instrument_ids = list(instruments.keys())

        # 2. Filter applicable (USE should_apply)
        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst)
        ]

        # 3. Early return if no applicable
        if not applicable_ids:
            logger.debug(
                f"RepoComponent: No applicable instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 4. Log processing
        logger.debug(
            f"RepoComponent: Processing {len(applicable_ids)}/{len(instruments)} instruments"
        )

        # 5. Calculate shifted year fractions
        year_fractions_shifted = calculate_year_fractions(dates_dt, shifted=True, settlement_days=self.settlement_days)

        # 6. Align dates
        common_dates = self.repo_rates.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"RepoComponent: ZERO adjustments - no date overlap. "
                f"Repo dates: {self.repo_rates.index.min()} to {self.repo_rates.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        # 7. Calculate adjustments based on mode
        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        year_frac_aligned = year_fractions_shifted.loc[common_dates]

        if self.mode == 'direct':
            # Mode 1: Direct repo rates per instrument
            for inst_id in applicable_ids:
                if inst_id not in self.repo_rates.columns:
                    logger.warning(f"RepoComponent: No repo data for {inst_id}")
                    continue

                repo_series = self.repo_rates.loc[common_dates, inst_id]
                result.loc[common_dates, inst_id] = -repo_series * year_frac_aligned

        else:
            # Mode 2: Currency-based rates
            for inst_id in applicable_ids:
                if self.future_currencies is None or inst_id not in self.future_currencies.index:
                    logger.warning(f"RepoComponent: No currency mapping for {inst_id}")
                    continue

                # Get currency (normalize to uppercase for matching)
                ccy_raw = self.future_currencies[inst_id]
                ccy = str(ccy_raw).upper()

                # Find matching column (case-insensitive)
                matched_col = None
                if ccy in self.repo_rates.columns:
                    matched_col = ccy
                else:
                    # Case-insensitive fallback
                    matches = [
                        c for c in self.repo_rates.columns
                        if str(c).upper() == ccy
                    ]
                    if matches:
                        matched_col = matches[0]
                        logger.debug(
                            f"RepoComponent: Matched currency '{ccy_raw}' to '{matched_col}' "
                            f"for {inst_id}"
                        )

                if matched_col is None:
                    logger.warning(
                        f"RepoComponent: No repo rate for currency '{ccy}' "
                        f"(instrument {inst_id})"
                    )
                    continue

                repo_series = self.repo_rates.loc[common_dates, matched_col]
                result.loc[common_dates, inst_id] = -repo_series * year_frac_aligned

        # 8. Summary logging
        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"RepoComponent: Produced ZERO non-zero adjustments for "
                f"{len(applicable_ids)} instruments. Verify repo data."
            )
        else:
            mean_adj = result[applicable_ids].mean().mean()
            logger.debug(
                f"RepoComponent: Generated {non_zero} non-zero adjustments, "
                f"mean repo impact: {mean_adj:.6f}"
            )

        return result

    def __repr__(self) -> str:
        return (
            f"RepoComponent("
            f"mode={self.mode}, "
            f"items={len(self.repo_rates.columns)})"
        )
