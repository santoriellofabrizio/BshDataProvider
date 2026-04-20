"""
Bond accrued interest adjustment component.

Approximates the daily carry of a bond by accruing YTM pro-rata:

    adjustment = -ytm × year_fraction_shifted

This is the same formula used by YtmComponent for fixed-income ETFs and
futures, but applied specifically to BOND instruments (InstrumentType.BOND).

Accepts either:
- A time-series DataFrame (dates × ISINs) — preferred when YTM varies
- A constant-YTM dict / Series (ISIN → float) — useful for static positions
  or when a full YTM time series is not available

Migration note
--------------
In ReturnAdjustmentsLibrary, Bond._get_daily_adjustments() used a hardcoded
4.1% YTM stub.  This component is the proper replacement: pass the actual
per-instrument YTM obtained from bshDataProvider or any external source.
"""
from datetime import date, datetime
from typing import Union, List, Optional
import pandas as pd
import logging

from sfm_data_provider.analytics.adjustments.component import Component
from sfm_data_provider.analytics.adjustments.common import calculate_year_fractions
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import Instrument

logger = logging.getLogger(__name__)


class BondAccruedInterestComponent(Component):
    """
    Accrued interest adjustment for bond instruments (InstrumentType.BOND).

    Computes the daily carry cost by accruing YTM over the settlement period:

        adjustment = -ytm × year_fraction_shifted(T+settlement_days)

    The negative sign means that accrued interest *reduces* the fair-value
    return (the price drops by the amount accrued on each coupon payment day),
    so the adjustment adds it back when computing clean returns.

    Parameters
    ----------
    ytm : pd.DataFrame | dict[str, float] | pd.Series
        Yield to maturity data.
        • DataFrame:   index = dates, columns = ISIN codes, values = decimal YTM.
                       Use this when YTM changes over time (e.g. floating rate bonds).
        • dict/Series: ISIN → constant annual YTM (decimal).
                       Internally converted to a DataFrame with a constant value
                       for all requested dates.
    settlement_days : int, default 2
        Settlement lag (T+1 = 1, T+2 = 2, T+3 = 3).
    target : list[str] | None
        Optional whitelist of instrument IDs.  If None, applies to all BOND
        instruments that have YTM data.

    Usage
    -----
    Time-series YTM (preferred)::

        ytm_df = pd.DataFrame(
            {'XS0000000001': [0.041, 0.041, 0.042],
             'XS0000000002': [0.035, 0.035, 0.034]},
            index=dates
        )
        adjuster.add(BondAccruedInterestComponent(ytm_df))

    Constant YTM (simple positions)::

        ytm_const = {'XS0000000001': 0.041, 'XS0000000002': 0.035}
        adjuster.add(BondAccruedInterestComponent(ytm_const))
    """

    def __init__(
        self,
        ytm: Union[pd.DataFrame, dict, pd.Series],
        settlement_days: int = 2,
        target: Optional[List[str]] = None,
    ):
        super().__init__(target)

        if isinstance(ytm, pd.DataFrame):
            self._ytm_series: pd.DataFrame = ytm.fillna(0.0)
            self._constant_mode = False
        else:
            if isinstance(ytm, pd.Series):
                ytm = ytm.to_dict()
            for isin, val in ytm.items():
                if not isinstance(val, (int, float)):
                    raise TypeError(
                        f"BondAccruedInterestComponent: YTM for {isin} must be numeric, "
                        f"got {type(val)}"
                    )
            self._constant_ytm: dict[str, float] = {k: float(v) for k, v in ytm.items()}
            self._ytm_series = pd.DataFrame()
            self._constant_mode = True

        self.settlement_days = settlement_days

        if self.target is not None and not self._constant_mode:
            missing = self.target - set(self._ytm_series.columns)
            if missing:
                logger.warning(
                    f"BondAccruedInterestComponent: {len(missing)} target instruments "
                    f"have no YTM data: {sorted(missing)[:5]}"
                    f"{'...' if len(missing) > 5 else ''}. Zero adjustment will be applied."
                )

        n_instruments = (
            len(self._constant_ytm) if self._constant_mode
            else len(self._ytm_series.columns)
        )
        logger.info(
            f"BondAccruedInterestComponent: {n_instruments} instruments, "
            f"T+{settlement_days} settlement, "
            f"mode={'constant' if self._constant_mode else 'time-series'}"
            f"{f', target={len(self.target)} instruments' if self.target else ''}"
        )

    def is_applicable(self, instrument: Instrument) -> bool:
        if instrument.type != InstrumentType.BOND:
            return False
        if self._constant_mode:
            return instrument.id in self._constant_ytm
        return instrument.id in self._ytm_series.columns

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
                f"BondAccruedInterestComponent: No applicable BOND instruments. "
                f"Total: {len(instruments)}"
                f"{f', target filter: {len(self.target)}' if self.target else ''}"
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        logger.debug(
            f"BondAccruedInterestComponent: Processing "
            f"{len(applicable_ids)}/{len(instruments)} bond instruments"
        )

        ytm_df = self._get_ytm_dataframe(applicable_ids, dates_dt)

        year_fractions_shifted = calculate_year_fractions(
            dates_dt, shifted=True, settlement_days=self.settlement_days
        )

        common_dates = ytm_df.index.intersection(dates_dt)
        if len(common_dates) == 0:
            logger.error(
                f"BondAccruedInterestComponent: ZERO adjustments — no date overlap. "
                f"YTM dates: {ytm_df.index.min()} to {ytm_df.index.max()}, "
                f"Requested: {dates_dt[0]} to {dates_dt[-1]}. "
                "Check data alignment."
            )
            return pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)

        ytm_aligned = ytm_df.loc[common_dates, applicable_ids]
        fractions_aligned = year_fractions_shifted.loc[common_dates]
        result_applicable = -ytm_aligned.mul(fractions_aligned, axis=0)

        result = pd.DataFrame(0.0, index=dates_dt, columns=instrument_ids)
        result.loc[common_dates, applicable_ids] = result_applicable

        non_zero = (result != 0).sum().sum()
        if non_zero == 0:
            logger.warning(
                f"BondAccruedInterestComponent: ZERO non-zero adjustments for "
                f"{len(applicable_ids)} bonds. Verify YTM data."
            )
        else:
            mean_adj = result[applicable_ids].mean().mean()
            logger.debug(
                f"BondAccruedInterestComponent: {non_zero} non-zero adjustments, "
                f"mean impact: {mean_adj:.6f}"
            )

        return result

    def _get_ytm_dataframe(
        self, applicable_ids: List[str], dates_dt: List[datetime]
    ) -> pd.DataFrame:
        if not self._constant_mode:
            return self._ytm_series
        return pd.DataFrame(
            {isin: self._constant_ytm[isin] for isin in applicable_ids
             if isin in self._constant_ytm},
            index=dates_dt,
        )

    def __repr__(self) -> str:
        n = (
            len(self._constant_ytm)
            if self._constant_mode
            else len(self._ytm_series.columns)
        )
        return (
            f"BondAccruedInterestComponent("
            f"instruments={n}, "
            f"T+{self.settlement_days}, "
            f"mode={'constant' if self._constant_mode else 'time-series'})"
        )
