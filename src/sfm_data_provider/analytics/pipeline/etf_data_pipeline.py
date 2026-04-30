"""
ETF Data Pipeline - Unified data loading and cleaning for ETF analysis.

Typical usage:

    >>> pipeline = EtfDataPipeline(
    ...     api=BshData(config_path="..."),
    ...     instruments=["IE00B4L5Y983", "IE00B5BMR087"],
    ...     start="2024-01-01",
    ...     end="2024-06-01",
    ... )
    >>> pipeline.load()
    >>> pipeline.clean_returns
    >>> pipeline.summary()

    # With overrides (fluent)
    >>> (
    ...     EtfDataPipeline(api, instruments, start, end)
    ...     .set_data(ter={"IE00B4L5Y983": 0.002})
    ...     .load()
    ... )

    # Variant with different date range (no reload of config/overrides)
    >>> long_pipeline  = pipeline.with_date_range("2020-01-01", "2024-01-01")
    >>> long_pipeline.load()
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, List, Literal, Optional, Union

import numpy as np
import pandas as pd

from sfm_data_provider.analytics.adjustments import Adjuster
from sfm_data_provider.analytics.adjustments.dividend import DividendComponent
from sfm_data_provider.analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from sfm_data_provider.analytics.adjustments.fx_spot import FxSpotComponent
from sfm_data_provider.analytics.adjustments.ter import TerComponent

logger = logging.getLogger(__name__)

# Keys accepted by set_data() / override_*()
_DATA_KEYS = frozenset({
    "ter", "fx_composition", "fx_forward_composition",
    "dividends", "fx_prices", "fx_forward_prices",
})


@dataclass
class PipelineConfig:
    """
    Full configuration for EtfDataPipeline.

    Time window
    -----------
    start, end
        Date range. Set automatically by EtfDataPipeline.__init__; exposed here
        so a config object can be reused across pipelines.

    Frequency & timing
    ------------------
    frequency
        ``'daily'`` / ``'1d'`` or an intraday bar size (``'1m'``, ``'5m'``,
        ``'15m'``, ``'1h'``).
    snapshot_time
        Daily close time used for ETF price look-up (default 17:00).
    fx_snapshot_time
        Snapshot time for FX spot rates. ``None`` → uses ``snapshot_time``.
        Override when FX OTC closes at a different time than the exchange.
    intraday_start_time / intraday_end_time
        Window of intraday bars to include (default 09:00–17:30).

    FX
    --
    base_currency
        Reference currency for FX rates and composition (default ``"EUR"``).
        Change to ``"USD"`` for USD-base analysis.
    fx_composition_ref_date
        Date used to look up FX exposures (e.g. last rebalancing date).
        ``None`` → uses ``end``.
    fx_forward_tenor
        Tenor for FX forward carry component (default ``"1M"``).

    Adjustments
    -----------
    adjust_ter / adjust_fx_spot / adjust_fx_forward / adjust_dividends
        Toggle each adjustment component.
    ter_scale : float
        Multiply every TER value by this factor before applying.
        Useful for partial-year periods (e.g. ``252/365`` for a calendar year
        expressed in business-day fraction) or sensitivity analysis.
    outlier_clip_sigma : float | None
        If set, winsorise clean returns at ± N × per-instrument standard
        deviation. Typical value: ``5.0``.  ``None`` → no clipping.

    Returns
    -------
    return_type
        ``'percentage'`` | ``'logarithmic'`` | ``'absolute'``.

    Data quality
    ------------
    fill_method
        Applied to raw ETF prices after loading to handle missing bars.
        ``'ffill'`` (default) | ``'bfill'`` | ``'time'`` | ``'linear'`` | ``None``.

    Sources & markets
    -----------------
    etf_source / fx_source / fx_forward_source
        Provider names (``'timescale'``, ``'bloomberg'``, …).
    etf_market
        Market code forwarded to market API calls (default ``"ETFP"``).
    etf_fallbacks
        Ordered list of fallback provider configs tried on partial results.
    """

    # --- Time window ---
    start: Union[str, date, datetime] = None
    end:   Union[str, date, datetime] = None

    # --- Frequency & timing ---
    frequency:           str  = "daily"
    snapshot_time:       time = field(default_factory=lambda: time(17, 0))
    fx_snapshot_time:    Optional[time] = None
    intraday_start_time: time = field(default_factory=lambda: time(9,  0))
    intraday_end_time:   time = field(default_factory=lambda: time(17, 30))

    # --- FX ---
    base_currency:           str            = "EUR"
    fx_composition_ref_date: Optional[date] = None
    fx_forward_tenor:        str            = "1M"

    # --- Adjustments ---
    adjust_ter:         bool            = True
    adjust_fx_spot:     bool            = True
    adjust_fx_forward:  bool            = True
    adjust_dividends:   bool            = True
    ter_scale:          float           = 1.0
    outlier_clip_sigma: Optional[float] = None

    # --- Returns ---
    return_type: Literal["percentage", "logarithmic", "absolute"] = "percentage"

    # --- Data quality ---
    fill_method: Optional[Literal["ffill", "bfill", "time", "linear"]] = "ffill"

    # --- Sources ---
    etf_source:        str                  = "timescale"
    etf_market:        str                  = "ETFP"
    fx_source:         str                  = "timescale"
    fx_forward_source: str                  = "bloomberg"
    etf_fallbacks:     Optional[List[dict]] = None

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def effective_fx_snapshot_time(self) -> time:
        """FX snapshot time; falls back to ``snapshot_time`` when not set."""
        return self.fx_snapshot_time or self.snapshot_time

    @property
    def effective_fx_ref_date(self) -> Optional[date]:
        """FX composition reference date; falls back to ``end`` when not set."""
        if self.fx_composition_ref_date is not None:
            return self.fx_composition_ref_date
        return pd.to_datetime(self.end).date() if self.end is not None else None


class EtfDataPipeline:
    """
    Pipeline for loading and cleaning ETF data.

    Self-contained: downloads all required data automatically from BshData API.
    Individual data inputs can be injected via ``set_data()`` / ``override_*()``
    before calling ``load()``.

    Example — basic::

        pipeline = EtfDataPipeline(api, ["IE00B4L5Y983"], "2024-01-01", "2024-06-01")
        pipeline.load()
        returns = pipeline.clean_returns
        print(pipeline.summary())

    Example — inject data and tune parameters::

        config = PipelineConfig(
            ter_scale=0.5,               # half-year period
            outlier_clip_sigma=5.0,      # remove extreme days
            fx_snapshot_time=time(16),   # FX closes one hour earlier
            base_currency="USD",
        )
        (
            EtfDataPipeline(api, instruments, start, end, config=config)
            .set_data(ter={"IE00B4L5Y983": 0.002})  # override a single TER
            .disable_adjustment("fx_forward")
            .load()
        )

    Example — create date-range variant without rebuilding overrides::

        base = EtfDataPipeline(api, instruments, "2023-01-01", "2023-12-31")
        extended = base.with_date_range("2020-01-01", "2023-12-31")
        extended.load()
    """

    def __init__(
        self,
        api,
        instruments: Union[List[str], List],
        start: Union[str, date, datetime],
        end:   Union[str, date, datetime],
        config: Optional[PipelineConfig] = None,
        frequency: Optional[str] = None,
    ):
        if not instruments:
            raise ValueError("instruments list cannot be empty")

        self.api = api

        if hasattr(instruments[0], "id"):
            self.instrument_objects = instruments
            self.instrument_ids = [inst.id for inst in instruments]
        else:
            self.instrument_ids = list(instruments)
            self.instrument_objects = None

        self.config = config or PipelineConfig()
        self.config.start = start
        self.config.end   = end
        if frequency is not None:
            self.config.frequency = frequency

        # Unified override store populated by set_data() / override_*()
        self._overrides: Dict[str, Any] = {}

        self._reset_data()

        logger.info(
            "EtfDataPipeline initialized: %d instruments, %s → %s, freq=%s",
            len(self.instrument_ids), self.config.start, self.config.end,
            self.config.frequency,
        )

    # ================================================================
    # INTERNAL HELPERS
    # ================================================================

    @property
    def _is_daily(self) -> bool:
        return self.config.frequency.lower() in ("daily", "1d")

    def _reset_data(self) -> None:
        """Clear all cached results (called before load() and in with_date_range())."""
        self._etf_prices:              Optional[pd.DataFrame] = None
        self._fx_prices:               Optional[pd.DataFrame] = None
        self._fx_forward_prices:       Optional[pd.DataFrame] = None
        self._fx_composition:          Optional[pd.DataFrame] = None
        self._fx_forward_composition:  Optional[pd.DataFrame] = None
        self._dividends:               Optional[pd.DataFrame] = None
        self._ter:                     Optional[dict]         = None
        self._adjuster:                Optional[Adjuster]     = None
        self._is_loaded = False

    def _ensure_loaded(self) -> None:
        if not self._is_loaded:
            raise RuntimeError("Pipeline not loaded. Call load() first.")

    # ================================================================
    # DATA INJECTION (before load())
    # ================================================================

    def set_data(self, **kwargs) -> "EtfDataPipeline":
        """
        Inject pre-built data to bypass one or more API calls (fluent).

        Accepted keys:
            ``ter``                    – dict {instrument_id: annual_ter}
            ``fx_composition``         – DataFrame (instruments × currencies, spot weights)
            ``fx_forward_composition`` – DataFrame (instruments × currencies, fwd weights)
            ``dividends``              – DataFrame (dates × instruments, dividend amounts)
            ``fx_prices``              – DataFrame (dates × currency pairs, spot rates)
            ``fx_forward_prices``      – DataFrame (dates × currency pairs, fwd points)

        All keys are optional and can be combined::

            pipeline.set_data(
                ter={"IE00B4L5Y983": 0.002},
                fx_prices=my_fx_df,
            )
        """
        unknown = set(kwargs) - _DATA_KEYS
        if unknown:
            raise ValueError(
                f"Unknown keys: {unknown}.  Valid: {sorted(_DATA_KEYS)}"
            )
        self._overrides.update(kwargs)
        logger.info("Data overrides set: %s", list(kwargs))
        return self

    # Individual override methods kept for backward compatibility
    def override_ter(self, ter: dict) -> "EtfDataPipeline":
        """Override TER values. ``ter`` is merged on top of API results."""
        return self.set_data(ter=ter)

    def override_fx_composition(self, fx_composition: pd.DataFrame) -> "EtfDataPipeline":
        return self.set_data(fx_composition=fx_composition)

    def override_fx_forward_composition(self, fx_forward_composition: pd.DataFrame) -> "EtfDataPipeline":
        return self.set_data(fx_forward_composition=fx_forward_composition)

    def override_dividends(self, dividends: pd.DataFrame) -> "EtfDataPipeline":
        return self.set_data(dividends=dividends)

    def override_fx_prices(self, fx_prices: pd.DataFrame) -> "EtfDataPipeline":
        return self.set_data(fx_prices=fx_prices)

    def override_fx_forward_prices(self, fx_forward_prices: pd.DataFrame) -> "EtfDataPipeline":
        return self.set_data(fx_forward_prices=fx_forward_prices)

    def disable_adjustment(self, *adjustments: str) -> "EtfDataPipeline":
        """
        Disable one or more adjustment components (fluent).

        Names: ``'ter'``, ``'fx_spot'``, ``'fx_forward'``, ``'dividends'``.

        Example::

            pipeline.disable_adjustment("fx_forward", "dividends")
        """
        mapping = {
            "ter":        "adjust_ter",
            "fx_spot":    "adjust_fx_spot",
            "fx_forward": "adjust_fx_forward",
            "dividends":  "adjust_dividends",
        }
        for adj in adjustments:
            attr = mapping.get(adj.lower())
            if attr is None:
                logger.warning("Unknown adjustment name: %s  (valid: %s)", adj, list(mapping))
            else:
                setattr(self.config, attr, False)
        logger.info("Disabled adjustments: %s", adjustments)
        return self

    # ================================================================
    # LOADING
    # ================================================================

    def load(self) -> "EtfDataPipeline":
        """
        Download all required data and build the Adjuster.

        Safe to call multiple times; each call re-fetches everything.
        """
        self._reset_data()
        logger.info("EtfDataPipeline: loading…")

        self._load_etf_prices()

        if self.config.adjust_fx_spot or self.config.adjust_fx_forward:
            self._load_fx_data()

        if self.config.adjust_dividends:
            self._load_dividends()

        if self.config.adjust_ter:
            self._load_ter()

        self._build_adjuster()
        self._is_loaded = True
        logger.info("EtfDataPipeline: load complete — %r", self)
        return self

    # ----------------------------------------------------------------
    # Price loading
    # ----------------------------------------------------------------

    def _load_etf_prices(self) -> None:
        if self._is_daily:
            self._etf_prices = self.api.market.get_daily_etf(
                id=self.instrument_ids,
                start=self.config.start,
                end=self.config.end,
                snapshot_time=self.config.snapshot_time,
                market=self.config.etf_market,
                source=self.config.etf_source,
                fallbacks=self.config.etf_fallbacks,
            )
        else:
            self._etf_prices = self._load_intraday_prices()

        self._etf_prices = self._apply_fill(self._etf_prices)
        logger.info("Loaded ETF prices: %s", self._etf_prices.shape)

    def _load_intraday_prices(self) -> pd.DataFrame:
        start_date = pd.to_datetime(self.config.start).date()
        end_date   = pd.to_datetime(self.config.end).date()
        days = pd.date_range(start_date, end_date, freq="B")
        rows = []
        for d in days:
            try:
                df = self.api.market.get_intraday_etf(
                    start=d.date(),
                    end=d.date(),
                    id=self.instrument_ids,
                    frequency=self.config.frequency,
                    start_time=self.config.intraday_start_time,
                    end_time=self.config.intraday_end_time,
                    market=self.config.etf_market,
                    source=self.config.etf_source,
                )
                if df is not None and not df.empty:
                    rows.append(df)
            except Exception as exc:
                logger.warning("Failed to load intraday for %s: %s", d.date(), exc)
        if not rows:
            raise ValueError("No intraday prices loaded for the given range.")
        return pd.concat(rows).sort_index()

    def _apply_fill(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Apply fill_method to prices; returns df unchanged if fill_method is None."""
        if df is None or df.empty or self.config.fill_method is None:
            return df
        method = self.config.fill_method
        if method == "ffill":
            return df.ffill()
        if method == "bfill":
            return df.bfill()
        if method in ("time", "linear"):
            return df.interpolate(method=method)
        return df

    # ----------------------------------------------------------------
    # FX loading
    # ----------------------------------------------------------------

    def _load_fx_data(self) -> None:
        """Load FX compositions and prices (single API call for compositions)."""
        self._fx_composition         = self._overrides.get("fx_composition")
        self._fx_forward_composition = self._overrides.get("fx_forward_composition")

        if self._fx_composition is None or self._fx_forward_composition is None:
            fetched_spot, fetched_fwd = self._fetch_fx_compositions()
            if self._fx_composition is None:
                self._fx_composition = fetched_spot
            if self._fx_forward_composition is None:
                self._fx_forward_composition = fetched_fwd

        # Spot prices
        currencies_spot = self._extract_currencies(self._fx_composition)
        if self.config.adjust_fx_spot and currencies_spot:
            self._fx_prices = (
                self._overrides.get("fx_prices")
                or self._fetch_fx_spot_prices(currencies_spot)
            )

        # Forward prices
        currencies_fwd = self._extract_currencies(self._fx_forward_composition)
        if self.config.adjust_fx_forward and currencies_fwd:
            self._fx_forward_prices = (
                self._overrides.get("fx_forward_prices")
                or self._fetch_fx_forward_prices(currencies_fwd)
            )

    def _fetch_fx_compositions(self) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        try:
            data = self.api.info.get_currency_exposure(
                id=self.instrument_ids,
                reference_date=self.config.effective_fx_ref_date,
            )
            if data is None:
                return None, None
            spot = data.pivot(index="index", columns="CURRENCY", values="WEIGHT").fillna(0)
            fwd  = data.pivot(index="index", columns="CURRENCY", values="WEIGHT_FX_FORWARD").fillna(0)
            logger.info("Fetched FX compositions: spot=%s, fwd=%s", spot.shape, fwd.shape)
            return spot, fwd
        except Exception as exc:
            logger.warning("Could not fetch FX compositions: %s", exc)
            return None, None

    def _extract_currencies(self, composition: Optional[pd.DataFrame]) -> List[str]:
        """Return column names excluding base_currency."""
        if composition is None:
            return []
        return [c for c in composition.columns if c != self.config.base_currency]

    def _fetch_fx_spot_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        pairs = [f"{self.config.base_currency}{ccy}" for ccy in currencies]
        df = self.api.market.get_daily_currency(
            id=pairs,
            start=self.config.start,
            end=self.config.end,
            snapshot_time=self.config.effective_fx_snapshot_time,
            source=self.config.fx_source,
        )
        logger.info("Fetched FX spot prices: %s", df.shape if df is not None else None)
        return df

    def _fetch_fx_forward_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        df = self.api.market.get_daily_fx_forward(
            quoted_currency=currencies,
            start=self.config.start,
            end=self.config.end,
            tenor=self.config.fx_forward_tenor,
            source=self.config.fx_forward_source,
        )
        logger.info("Fetched FX forward prices: %s", df.shape if df is not None else None)
        return df

    # ----------------------------------------------------------------
    # Dividends & TER
    # ----------------------------------------------------------------

    def _load_dividends(self) -> None:
        self._dividends = (
            self._overrides.get("dividends")
            or self.api.info.get_dividends(id=self.instrument_ids)
        )
        n = len(self._dividends) if self._dividends is not None else 0
        logger.info("Loaded dividends for %d instruments", n)

    def _load_ter(self) -> None:
        api_ter = self.api.info.get_ter(id=self.instrument_ids) or {}
        override = self._overrides.get("ter") or {}
        self._ter = {**api_ter, **override}  # override wins on conflict

        if self.config.ter_scale != 1.0 and self._ter:
            self._ter = {k: v * self.config.ter_scale for k, v in self._ter.items()}

        logger.info("Loaded TER for %d instruments (scale=%.3f)", len(self._ter), self.config.ter_scale)

    # ----------------------------------------------------------------
    # Adjuster construction
    # ----------------------------------------------------------------

    def _build_adjuster(self) -> None:
        self._adjuster = Adjuster(
            prices=self._etf_prices,
            is_intraday=not self._is_daily,
            return_type=self.config.return_type,
        )

        if self.config.adjust_ter and self._ter:
            self._adjuster.add(TerComponent(self._ter))

        if (self.config.adjust_fx_spot
                and self._fx_prices is not None
                and self._fx_composition is not None):
            self._adjuster.add(FxSpotComponent(self._fx_composition, self._fx_prices))

        if (self.config.adjust_fx_forward
                and self._fx_forward_prices is not None
                and self._fx_forward_composition is not None
                and self._fx_prices is not None):
            self._adjuster.add(FxForwardCarryComponent(
                self._fx_forward_composition,
                self._fx_forward_prices,
                self.config.fx_forward_tenor,
                self._fx_prices,
            ))

        if self.config.adjust_dividends and self._dividends is not None:
            self._adjuster.add(DividendComponent(self._dividends))

        logger.info(
            "Adjuster built: %d components [%s]",
            len(self._adjuster.components),
            ", ".join(c.__class__.__name__ for c in self._adjuster.components),
        )

    # ================================================================
    # OUTPUT PROPERTIES
    # ================================================================

    @property
    def etf_prices(self) -> pd.DataFrame:
        self._ensure_loaded(); return self._etf_prices

    @property
    def fx_prices(self) -> Optional[pd.DataFrame]:
        self._ensure_loaded(); return self._fx_prices

    @property
    def fx_forward_prices(self) -> Optional[pd.DataFrame]:
        self._ensure_loaded(); return self._fx_forward_prices

    @property
    def fx_composition(self) -> Optional[pd.DataFrame]:
        self._ensure_loaded(); return self._fx_composition

    @property
    def fx_forward_composition(self) -> Optional[pd.DataFrame]:
        self._ensure_loaded(); return self._fx_forward_composition

    @property
    def dividends(self) -> Optional[pd.DataFrame]:
        self._ensure_loaded(); return self._dividends

    @property
    def ter(self) -> Optional[dict]:
        self._ensure_loaded(); return self._ter

    @property
    def adjuster(self) -> Adjuster:
        self._ensure_loaded(); return self._adjuster

    @property
    def clean_returns(self) -> pd.DataFrame:
        """Adjusted returns, optionally winsorised by ``outlier_clip_sigma``."""
        self._ensure_loaded()
        returns = self._adjuster.get_clean_returns()
        return self._clip_outliers(returns)

    @property
    def raw_returns(self) -> pd.DataFrame:
        """Unadjusted returns."""
        self._ensure_loaded()
        return self._adjuster.return_calculator.calculate_returns(self._etf_prices)

    @property
    def clean_prices(self) -> pd.DataFrame:
        """Clean prices reconstructed from initial price × cumulative clean returns."""
        self._ensure_loaded()
        return self._adjuster.return_calculator.returns_to_prices(
            returns=self.clean_returns,
            initial_price=self._etf_prices.iloc[0],
        )

    @property
    def adjustments(self) -> pd.DataFrame:
        """Total adjustments (sum of all components) per date and instrument."""
        self._ensure_loaded()
        return self._adjuster.calculate_adjustment()

    @property
    def adjustment_breakdown(self) -> Dict[str, pd.DataFrame]:
        """Per-component adjustment DataFrames."""
        self._ensure_loaded()
        return self._adjuster.get_breakdown()

    # ================================================================
    # UTILITIES
    # ================================================================

    def _clip_outliers(self, returns: pd.DataFrame) -> pd.DataFrame:
        sigma = self.config.outlier_clip_sigma
        if sigma is None:
            return returns
        std = returns.std()
        return returns.clip(lower=-sigma * std, upper=sigma * std, axis=1)

    def get_returns(
        self,
        instrument_id: Optional[str] = None,
        clean: bool = True,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Return clean or raw returns for one or all instruments.

        Args:
            instrument_id: If given, returns a Series for that instrument.
            clean: ``True`` for adjusted returns, ``False`` for raw.
        """
        r = self.clean_returns if clean else self.raw_returns
        return r[instrument_id] if instrument_id is not None else r

    def resample(self, freq: str) -> pd.DataFrame:
        """
        Resample clean returns to a lower frequency.

        Args:
            freq: Pandas offset string (``'5min'``, ``'1h'``, ``'1d'``, …).
        """
        self._ensure_loaded()
        if self.config.return_type == "logarithmic":
            return self.clean_returns.resample(freq).sum()
        return (1 + self.clean_returns).resample(freq).prod() - 1

    def summary(self) -> pd.DataFrame:
        """
        Key performance statistics per instrument.

        Returns a DataFrame (metrics × instruments)::

            n_obs           number of return observations
            clean_ret_ann   annualised mean clean return (×252)
            clean_vol_ann   annualised clean volatility
            sharpe          clean Sharpe ratio (×252, unscaled)
            raw_ret_ann     annualised mean raw return
            total_adj_bps   total adjustment over the period, in basis points
        """
        self._ensure_loaded()
        cr = self.clean_returns.dropna()
        rr = self.raw_returns.dropna()
        vol = cr.std().replace(0, np.nan)
        stats = {
            "n_obs":         cr.count(),
            "clean_ret_ann": (cr.mean() * 252).round(6),
            "clean_vol_ann": (vol * np.sqrt(252)).round(6),
            "sharpe":        ((cr.mean() / vol) * np.sqrt(252)).round(4),
            "raw_ret_ann":   (rr.mean() * 252).round(6),
            "total_adj_bps": (self.adjustments.sum() * 1e4).round(2),
        }
        return pd.DataFrame(stats).T

    def with_date_range(
        self,
        start: Union[str, date, datetime],
        end:   Union[str, date, datetime],
    ) -> "EtfDataPipeline":
        """
        Return a new *unloaded* pipeline with a different date range.

        Config, overrides, and disabled-adjustment flags are preserved;
        all loaded data is reset.  Call ``.load()`` on the result.

        Example::

            full_history = pipeline.with_date_range("2018-01-01", "2024-01-01")
            full_history.load()
        """
        other = copy.copy(self)
        other.config = copy.copy(self.config)
        other.config.start = start
        other.config.end   = end
        other._overrides = dict(self._overrides)
        other._reset_data()
        return other

    def __repr__(self) -> str:
        if self._is_loaded and self._adjuster:
            components = ", ".join(
                c.__class__.__name__.replace("Component", "")
                for c in self._adjuster.components
            )
            detail = f"  components  = [{components}]\n"
        else:
            detail = ""

        return (
            f"EtfDataPipeline(\n"
            f"  instruments = {len(self.instrument_ids)}\n"
            f"  period      = {self.config.start} → {self.config.end}\n"
            f"  frequency   = {self.config.frequency}\n"
            f"{detail}"
            f"  status      = {'loaded' if self._is_loaded else 'not loaded'}\n"
            f")"
        )
