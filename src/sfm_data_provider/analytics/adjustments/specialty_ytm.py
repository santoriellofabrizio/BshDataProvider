"""
Specialty ETF carry adjustment component.

Computes the YTM (carry cost) for ETFs whose yield is derived from a combination
of an overnight interest rate and a credit spread, rather than read directly
from a bond index:

    ytm = overnight_rate[rate_currency] - cdx_spread[cdx_ticker] / 10_000

    adjustment = -ytm x year_fraction_shifted(T+settlement_days)

Background
----------
Certain "specialty" ETFs (e.g. UCITS ETF LU0321462870 - iTraxx Crossover) do
not have a standard fixed-income YTM.  Their carry is better approximated as:

    carry ~ ESTR - iTraxx-Xover-spread (bp converted to decimal)

In ReturnAdjustmentsLibrary this logic lived in
ETF._calculate_carry_specialties() as a hard-coded block for LU0321462870.
This component is the generic, data-driven replacement:

* Pass any overnight-rate DataFrame (dates x currencies) as overnight_rates.
* Pass any CDX/credit-spread DataFrame (dates x tickers, values in basis
  points) as cdx_spreads.
* Declare the per-instrument mapping as instrument_mapping:
  {isin: (cdx_ticker, rate_currency)}.

The component applies only to ETP instruments whose ISIN is present in
instrument_mapping.
"""
from datetime import date, datetime
from typing import Union, List, Optional, Dict, Tuple
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.analytics.adjustments.common import calculate_year_fractions
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)


class SpecialtyEtfCarryComponent(Component):
    """
    Carry adjustment for specialty ETFs whose YTM is CDX/overnight-rate derived.

    Formula
    -------
    For each instrument in instrument_mapping:

        ytm(t) = overnight_rates[t, rate_currency]
                 - cdx_spreads[t, cdx_ticker] / 10_000

        adjustment(t) = -ytm(t) x year_fraction_shifted(t, T+settlement_days)

    Parameters
    ----------
    overnight_rates : pd.DataFrame
        index = dates, columns = currency codes (e.g. 'EUR' for ESTR).
        Values are decimal rates (0.03 = 3%).
    cdx_spreads : pd.DataFrame
        index = dates, columns = CDX ticker labels (e.g. 'ITRX XOVER').
        Values are in basis points (100 bp = 1%).
    instrument_mapping : dict[str, tuple[str, str]]
        Maps each ISIN to (cdx_ticker, rate_currency).
        Example: {'LU0321462870': ('ITRX XOVER', 'EUR')}
    settlement_days : int, default 2
        Settlement lag (T+1=1, T+2=2, T+3=3).
    target : list[str] | None
        Optional whitelist of instrument IDs.

    Usage
    -----
        overnight_rates = pd.DataFrame({'EUR': [0.039, 0.039, 0.040]}, index=dates)
        cdx_spreads = pd.DataFrame({'ITRX XOVER': [330.5, 331.0, 329.0]}, index=dates)
        mapping = {'LU0321462870': ('ITRX XOVER', 'EUR')}
        adjuster.add(SpecialtyEtfCarryComponent(overnight_rates, cdx_spreads, mapping))
    """

    def __init__(
        self,
        overnight_rates: pd.DataFrame,
        cdx_spreads: pd.DataFrame,
        instrument_mapping: Dict[str, Tuple[str, str]],
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        super().__init__(target)

        if overnight_rates.empty:
            raise ValueError("SpecialtyEtfCarryComponent: overnight_rates cannot be empty")
        if cdx_spreads.empty:
            raise ValueError("SpecialtyEtfCarryComponent: cdx_spreads cannot be empty")
        if not instrument_mapping:
            raise ValueError("SpecialtyEtfCarryComponent: instrument_mapping cannot be empty")

        self._overnight_rates: pd.DataFrame = overnight_rates
        self._cdx_spreads: pd.DataFrame = cdx_spreads
        self._instrument_mapping: Dict[str, Tuple[str, str]] = instrument_mapping
        self.settlement_days = settlement_days

        missing_cdx: list[str] = []
        missing_rate: list[str] = []
        for isin, (cdx_ticker, rate_ccy) in instrument_mapping.items():
            if cdx_ticker not in cdx_spreads.columns:
                missing_cdx.append(f"{isin} -> {cdx_ticker}")
            if rate_ccy not in overnight_rates.columns:
                missing_rate.append(f"{isin} -> {rate_ccy}")

        if missing_cdx:
            logger.warning(
                f"SpecialtyEtfCarryComponent: missing CDX tickers in cdx_spreads: "
                f"{missing_cdx}. Affected instruments will receive zero adjustment."
            )
        if missing_rate:
            logger.warning(
                f"SpecialtyEtfCarryComponent: missing rate currencies in overnight_rates: "
                f"{missing_rate}. Affected instruments will receive zero adjustment."
            )

        if self.target is not None:
            unmapped = self.target - set(instrument_mapping.keys())
            if unmapped:
                logger.warning(
                    f"SpecialtyEtfCarryComponent: {len(unmapped)} target instruments "
                    f"have no mapping: {sorted(unmapped)[:5]}"
                    f"{'...' if len(unmapped) > 5 else ''}. Zero adjustment will be applied."
                )

        logger.info(
            f"SpecialtyEtfCarryComponent: {len(instrument_mapping)} mapped instruments, "
            f"T+{settlement_days} settlement"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: Instrument) -> bool:
        return (
            instrument.type == InstrumentType.ETP
            and instrument.id in self._instrument_mapping
        )

    def calculate_adjustment(
        self,
        instruments: dict[str, Instrument],
        dates: Union[List[date], List[datetime]],
        prices: pd.DataFrame = None,
    ) -> pd.DataFrame:
        dates_dt = self._normalize_dates(dates)
        instrument_ids = list(instruments.keys())

        applicable_ids = [
            inst.id for inst in instruments.values()
            if self.should_apply(inst)
        ]

        if not applicable_ids:
            logger.debug(
                f"SpecialtyEtfCarryComponent: No applicable specialty ETF instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        logger.debug(
            f"SpecialtyEtfCarryComponent: Processing "
            f"{len(applicable_ids)}/{len(instruments)} specialty ETF instruments"
        )

        ytm_df = self._compute_ytm(applicable_ids, dates_dt)

        if ytm_df.empty or ytm_df.shape[1] == 0:
            logger.warning(
                "SpecialtyEtfCarryComponent: YTM DataFrame is empty after computation. "
                "Check overnight_rates and cdx_spreads date coverage."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        year_fractions_shifted = calculate_year_fractions(
            dates_dt, shifted=True, settlement_days=self.settlement_days
        )

        common_dates = ytm_df.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"SpecialtyEtfCarryComponent: ZERO adjustments - no date overlap. "
                f"Rates dates: {self._overnight_rates.index.min()} to "
                f"{self._overnight_rates.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        computed_ids = [iid for iid in applicable_ids if iid in ytm_df.columns]
        ytm_aligned = ytm_df.loc[common_dates, computed_ids]
        fractions_aligned = year_fractions_shifted.loc[common_dates]
        result_applicable = -ytm_aligned.mul(fractions_aligned, axis=0)

        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
        result.loc[common_dates, computed_ids] = result_applicable

        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"SpecialtyEtfCarryComponent: ZERO non-zero adjustments for "
                f"{len(applicable_ids)} specialty ETFs. Verify input data."
            )
        else:
            mean_adj = result[computed_ids].mean().mean()
            logger.debug(
                f"SpecialtyEtfCarryComponent: {non_zero} non-zero adjustments, "
                f"mean impact: {mean_adj:.6f}"
            )

        return result

    def _compute_ytm(
        self, applicable_ids: List[str], dates_dt: List[datetime]
    ) -> pd.DataFrame:
        result_cols: dict[str, pd.Series] = {}

        rate_idx = pd.DatetimeIndex(self._overnight_rates.index)
        cdx_idx = pd.DatetimeIndex(self._cdx_spreads.index)
        source_dates = rate_idx.intersection(cdx_idx)

        if source_dates.empty:
            logger.warning(
                "SpecialtyEtfCarryComponent: overnight_rates and cdx_spreads share "
                "no common dates. Returning empty YTM DataFrame."
            )
            return pd.DataFrame(index=pd.DatetimeIndex([]))

        for isin in applicable_ids:
            cdx_ticker, rate_ccy = self._instrument_mapping[isin]

            if cdx_ticker not in self._cdx_spreads.columns:
                logger.warning(
                    f"SpecialtyEtfCarryComponent: CDX ticker '{cdx_ticker}' not found "
                    f"for {isin}. Assigning zero YTM."
                )
                result_cols[isin] = pd.Series(0.0, index=source_dates)
                continue

            if rate_ccy not in self._overnight_rates.columns:
                logger.warning(
                    f"SpecialtyEtfCarryComponent: Rate currency '{rate_ccy}' not found "
                    f"for {isin}. Assigning zero YTM."
                )
                result_cols[isin] = pd.Series(0.0, index=source_dates)
                continue

            overnight = self._overnight_rates.loc[source_dates, rate_ccy]
            cdx_bp = self._cdx_spreads.loc[source_dates, cdx_ticker]
            result_cols[isin] = overnight - cdx_bp / 10_000.0

        return pd.DataFrame(result_cols, index=source_dates)

    def __repr__(self) -> str:
        return (
            f"SpecialtyEtfCarryComponent("
            f"instruments={len(self._instrument_mapping)}, "
            f"T+{self.settlement_days})"
        )
