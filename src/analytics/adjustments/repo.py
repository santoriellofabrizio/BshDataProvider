"""
Repo component for futures financing benefit.

Calculates benefit from avoided financing costs when holding futures.
"""
from datetime import date
from typing import Literal
import pandas as pd
import logging

from analytics.adjustments.component import Component
from analytics.adjustments.dates import calculate_year_fractions
from analytics.adjustments.protocols import InstrumentProtocol
from core.enums.instrument_types import InstrumentType

logger = logging.getLogger(__name__)


class RepoComponent(Component):
    """
    Repo financing benefit component.

    Calculates benefit from avoided financing costs when holding futures
    instead of physical underlying.

    Formula:
        adjustment = + repo_rate × year_fraction
        (positive = benefit from not paying financing cost)

    Theory:
        When you hold a future instead of the physical asset:
        - You don't need to finance the full notional
        - You only post margin (typically 5-10% of notional)
        - You save the repo/financing cost → positive adjustment

    Usage:
        # Repo rates per currency
        repo_rates = pd.DataFrame({
            'EUR': [0.020, 0.021],  # 2.0% repo rate
            'USD': [0.025, 0.026],  # 2.5% repo rate
        }, index=dates)

        # Map futures to their currencies
        future_currencies = pd.Series({
            'FUTURE_ISIN_1': 'EUR',
            'FUTURE_ISIN_2': 'USD',
        })

        component = RepoComponent(future_currencies, repo_rates)

        # Or provide repo directly per instrument
        repo_per_instrument = pd.DataFrame({
            'FUTURE_ISIN_1': [0.020, 0.021],
            'FUTURE_ISIN_2': [0.025, 0.026],
        }, index=dates)

        component = RepoComponent(repo_data=repo_per_instrument)
    """

    def __init__(
            self,
            future_currencies: pd.Series | None = None,
            repo_rates: pd.DataFrame | None = None,
            repo_data: pd.DataFrame | None = None,
            shifted_settlement: Literal["T+1", "T+2", "T+3"] = "T+2",
    ):
        """
        Initialize Repo component.

        Two modes of operation:

        Mode 1 - Repo rates per currency (most common):
            future_currencies: Series mapping ISIN → Currency
            repo_rates: DataFrame(dates × currencies) with repo rates

        Mode 2 - Repo rates per instrument (direct):
            repo_data: DataFrame(dates × ISINs) with repo rates

        Args:
            future_currencies: Series mapping future ISIN to currency
            repo_rates: DataFrame(dates × currencies) with repo rates (decimal)
            repo_data: DataFrame(dates × ISINs) with repo rates (decimal)
                      Use this for instrument-specific repo rates
            shifted_settlement: Settlement convention (T+1, T+2, T+3)
        """
        # Validate input
        if repo_data is not None:
            # Mode 2: Direct repo per instrument
            self.mode = "direct"
            self.repo_data = repo_data.fillna(0.0)
            self.future_currencies = None
            self.repo_rates = None
            n_instruments = len(self.repo_data.columns)
        else:
            # Mode 1: Repo per currency
            if future_currencies is None or repo_rates is None:
                raise ValueError(
                    "Either provide repo_data OR both future_currencies and repo_rates"
                )
            self.mode = "currency"
            self.future_currencies = future_currencies
            self.repo_rates = repo_rates.fillna(0.0)
            self.repo_data = None
            n_instruments = len(self.future_currencies)

        self.settlement_days = int(shifted_settlement.replace("T+", ""))

        logger.info(
            f"RepoComponent: {n_instruments} futures, "
            f"mode={self.mode}, "
            f"settlement={shifted_settlement}"
        )

    def is_applicable(self, instrument: InstrumentProtocol) -> bool:
        """
        Check if repo benefit applies to this instrument.

        Applicable only to futures.
        """
        if instrument.type != InstrumentType.FUTURE:
            return False

        # Check if we have repo data
        if self.mode == "direct":
            return instrument.id in self.repo_data.columns
        else:
            return instrument.id in self.future_currencies.index

    def calculate_batch(
            self,
            instruments: dict[str, InstrumentProtocol],
            dates: list[date],
            prices: pd.DataFrame,
            fx_prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate repo financing benefit.

        Returns:
            DataFrame(dates × instruments) with repo adjustments
        """
        result = pd.DataFrame(0.0, index=dates, columns=list(instruments.keys()))

        # Filter applicable instruments
        applicable_ids = [
            i.id for i in instruments.values()
            if self.is_applicable(i)
        ]

        if not applicable_ids:
            logger.debug("No applicable instruments for RepoComponent")
            return result

        logger.debug(f"RepoComponent: {len(applicable_ids)}/{len(instruments)} instruments")

        # Calculate year fractions
        year_fractions = calculate_year_fractions(
            dates,
            shifted=True,
            settlement_days=self.settlement_days
        )

        if self.mode == "direct":
            # Mode 2: Direct repo per instrument
            common_dates = self.repo_data.index.intersection(dates)
            if len(common_dates) == 0:
                logger.warning("No common dates between repo data and calculation dates")
                return result

            for isin in applicable_ids:
                if isin in self.repo_data.columns:
                    repo_series = self.repo_data.loc[common_dates, isin]
                    year_frac_aligned = year_fractions.loc[common_dates]

                    # Positive adjustment (benefit)
                    result.loc[common_dates, isin] = repo_series * year_frac_aligned

        else:
            # Mode 1: Repo per currency
            common_dates = self.repo_rates.index.intersection(dates)
            if len(common_dates) == 0:
                logger.warning("No common dates between repo rates and calculation dates")
                return result

            # For each future, get repo rate for its currency
            for isin in applicable_ids:
                ccy = self.future_currencies[isin]

                if ccy in self.repo_rates.columns:
                    repo_series = self.repo_rates.loc[common_dates, ccy]
                    year_frac_aligned = year_fractions.loc[common_dates]

                    # Positive adjustment (benefit)
                    result.loc[common_dates, isin] = repo_series * year_frac_aligned
                else:
                    logger.warning(
                        f"No repo rate found for currency {ccy} (future {isin})"
                    )

        return result

    def __repr__(self) -> str:
        if self.mode == "direct":
            n = len(self.repo_data.columns)
            return f"RepoComponent({n} instruments, mode=direct)"
        else:
            n = len(self.future_currencies)
            n_ccy = len(self.repo_rates.columns)
            return f"RepoComponent({n} futures, {n_ccy} currencies, mode=currency)"