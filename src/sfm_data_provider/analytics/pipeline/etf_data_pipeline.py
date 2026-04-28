"""
ETF Data Pipeline - Unified data loading and cleaning for ETF analysis.

This module provides a high-level pipeline for loading ETF data,
FX rates, dividends, TER, and producing clean returns ready for analysis.

Typical usage:
    >>> from sfm_data_provider.analytics import EtfDataPipeline
    >>>
    >>> pipeline = EtfDataPipeline(
    ...     api=BshData(config_path="..."),
    ...     instruments=["IE00B4L5Y983", "IE00B5BMR087"],
    ...     start="2024-01-01",
    ...     end="2024-06-01",
    ... )
    >>>
    >>> pipeline.load()
    >>> clean_returns = pipeline.clean_returns
    >>> raw_prices = pipeline.etf_prices

    # With overrides:
    >>> pipeline = EtfDataPipeline(api, instruments, start, end)
    >>> pipeline.override_ter({"IE00B4L5Y983": 0.002})
    >>> pipeline.override_fx_composition(my_fx_df)
    >>> pipeline.load()
"""

from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Optional, Union, List, Literal
import logging

import pandas as pd

from sfm_data_provider.analytics.adjustments import (
    Adjuster)
from sfm_data_provider.analytics.adjustments.dividend import DividendComponent
from sfm_data_provider.analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from sfm_data_provider.analytics.adjustments.fx_spot import FxSpotComponent
from sfm_data_provider.analytics.adjustments.ter import TerComponent

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for ETF Data Pipeline."""

    # Time range
    start: Union[str, date, datetime] = None
    end: Union[str, date, datetime] = None

    # Snapshot time for daily data
    snapshot_time: time = field(default_factory=lambda: time(17, 0))

    # Frequency: 'daily' or intraday like '1m', '5m', '15m', '1h'
    frequency: str = "daily"

    # For intraday: time range within day
    intraday_start_time: time = field(default_factory=lambda: time(9, 0))
    intraday_end_time: time = field(default_factory=lambda: time(17, 30))

    # FX forward tenor for carry adjustment
    fx_forward_tenor: str = "1M"

    # Which adjustments to apply
    adjust_ter: bool = True
    adjust_fx_spot: bool = True
    adjust_fx_forward: bool = True
    adjust_dividends: bool = True

    # Return calculation type
    return_type: Literal["percentage", "logarithmic", "absolute"] = "percentage"

    # Fill method for missing prices
    fill_method: Optional[Literal['ffill', 'bfill', 'time', 'linear']] = 'ffill'

    # Data sources
    etf_source: str = "timescale"
    fx_source: str = "timescale"
    fx_forward_source: str = "bloomberg"

    # Fallbacks
    etf_fallbacks: Optional[List[dict]] = None


class EtfDataPipeline:
    """
    Pipeline for loading and cleaning ETF data.

    Self-contained: downloads all required data automatically from BshData API.
    Supports overriding specific components before load().

    Example:
        # Simple setup
        pipeline = EtfDataPipeline(
            api=api,
            instruments=["IE00B4L5Y983"],
            start="2024-01-01",
            end="2024-06-01",
        )
        pipeline.load()
        returns = pipeline.clean_returns

        # With overrides
        pipeline = EtfDataPipeline(api, instruments, start, end)
        pipeline.override_ter({"IE00B4L5Y983": 0.002})
        pipeline.load()
    """

    def __init__(
            self,
            api,
            instruments: Union[List[str], List],
            start: Union[str, date, datetime],
            end: Union[str, date, datetime],
            config: Optional[PipelineConfig] = None,
            frequency: Optional[str] = None,
    ):
        if not instruments:
            raise ValueError("instruments list cannot be empty")

        self.api = api

        # Handle instruments (IDs or objects)
        if instruments and hasattr(instruments[0], 'id'):
            self.instrument_objects = instruments
            self.instrument_ids = [inst.id for inst in instruments]
        else:
            self.instrument_ids = list(instruments)
            self.instrument_objects = None

        # Configuration
        self.config = config or PipelineConfig()
        self.config.start = start
        self.config.end = end
        if frequency is not None:
            self.config.frequency = frequency

        # Override storage (populated via override_* methods)
        self._override_ter: Optional[dict] = None
        self._override_fx_composition: Optional[pd.DataFrame] = None
        self._override_fx_forward_composition: Optional[pd.DataFrame] = None
        self._override_dividends: Optional[pd.DataFrame] = None
        self._override_fx_prices: Optional[pd.DataFrame] = None
        self._override_fx_forward_prices: Optional[pd.DataFrame] = None

        # Results (populated by load())
        self._etf_prices: Optional[pd.DataFrame] = None
        self._fx_prices: Optional[pd.DataFrame] = None
        self._fx_forward_prices: Optional[pd.DataFrame] = None
        self._fx_composition: Optional[pd.DataFrame] = None
        self._fx_forward_composition: Optional[pd.DataFrame] = None
        self._dividends: Optional[pd.DataFrame] = None
        self._ter: Optional[dict] = None
        self._adjuster: Optional[Adjuster] = None

        # State
        self._is_loaded = False

        logger.info(
            f"EtfDataPipeline initialized: {len(self.instrument_ids)} instruments, "
            f"{self.config.start} to {self.config.end}, freq={self.config.frequency}"
        )

    # ============================================================
    # PROPERTIES
    # ============================================================

    @property
    def _is_daily(self) -> bool:
        return self.config.frequency.lower() in ("daily", "1d")

    # ============================================================
    # OVERRIDE METHODS (call before load())
    # ============================================================

    def override_ter(self, ter: dict) -> "EtfDataPipeline":
        """Override TER data. Returns self for chaining."""
        self._override_ter = ter
        logger.info(f"TER override set: {len(ter)} instruments")
        return self

    def override_fx_composition(self, fx_composition: pd.DataFrame) -> "EtfDataPipeline":
        """
        Override FX spot composition.

        Args:
            fx_composition: DataFrame with index=instrument IDs, columns=currency codes, values=weights.
        """
        self._override_fx_composition = fx_composition
        logger.info(f"FX composition override set: {fx_composition.shape}")
        return self

    def override_fx_forward_composition(self, fx_forward_composition: pd.DataFrame) -> "EtfDataPipeline":
        """
        Override FX forward composition.

        Args:
            fx_forward_composition: DataFrame with index=instrument IDs, columns=currency codes, values=weights.
        """
        self._override_fx_forward_composition = fx_forward_composition
        logger.info(f"FX forward composition override set: {fx_forward_composition.shape}")
        return self

    def override_dividends(self, dividends: pd.DataFrame) -> "EtfDataPipeline":
        """Override dividend data. Returns self for chaining."""
        self._override_dividends = dividends
        logger.info(f"Dividends override set")
        return self

    def override_fx_prices(self, fx_prices: pd.DataFrame) -> "EtfDataPipeline":
        """
        Override FX spot prices.

        Args:
            fx_prices: DataFrame with index=dates, columns=currency pairs (EURUSD, EURGBP, …).
        """
        self._override_fx_prices = fx_prices
        logger.info(f"FX prices override set: {fx_prices.shape}")
        return self

    def override_fx_forward_prices(self, fx_forward_prices: pd.DataFrame) -> "EtfDataPipeline":
        """Override FX forward prices. Returns self for chaining."""
        self._override_fx_forward_prices = fx_forward_prices
        logger.info(f"FX forward prices override set: {fx_forward_prices.shape}")
        return self

    def disable_adjustment(self, *adjustments: str) -> "EtfDataPipeline":
        """
        Disable specific adjustments.

        Args:
            *adjustments: Names to disable: 'ter', 'fx_spot', 'fx_forward', 'dividends'.

        Example:
            pipeline.disable_adjustment('fx_forward', 'dividends')
        """
        for adj in adjustments:
            adj_lower = adj.lower()
            if adj_lower == 'ter':
                self.config.adjust_ter = False
            elif adj_lower == 'fx_spot':
                self.config.adjust_fx_spot = False
            elif adj_lower == 'fx_forward':
                self.config.adjust_fx_forward = False
            elif adj_lower == 'dividends':
                self.config.adjust_dividends = False
            else:
                logger.warning(f"Unknown adjustment: {adj}")

        logger.info(f"Disabled adjustments: {adjustments}")
        return self

    # ============================================================
    # LOADING
    # ============================================================

    def load(self) -> "EtfDataPipeline":
        """
        Load all data and build the adjuster.

        Safe to call multiple times: re-downloads all data on each call.
        """
        self._is_loaded = False
        logger.info("EtfDataPipeline: Loading data...")

        self._load_etf_prices()

        if self.config.adjust_fx_spot or self.config.adjust_fx_forward:
            self._load_fx_data()

        if self.config.adjust_dividends:
            self._load_dividends()

        if self.config.adjust_ter:
            self._load_ter()

        self._build_adjuster()

        self._is_loaded = True
        logger.info("EtfDataPipeline: Load complete")

        return self

    def _load_etf_prices(self):
        if self._is_daily:
            self._etf_prices = self.api.market.get_daily_etf(
                id=self.instrument_ids,
                start=self.config.start,
                end=self.config.end,
                snapshot_time=self.config.snapshot_time,
                source=self.config.etf_source,
                fallbacks=self.config.etf_fallbacks,
            )
        else:
            self._etf_prices = self._load_intraday_prices()

        logger.info(f"Loaded ETF prices: {self._etf_prices.shape}")

    def _load_intraday_prices(self) -> pd.DataFrame:
        start_date = pd.to_datetime(self.config.start).date()
        end_date = pd.to_datetime(self.config.end).date()
        dates = pd.date_range(start_date, end_date, freq='B')

        all_prices = []
        for d in dates:
            try:
                day_prices = self.api.market.get_intraday_etf(
                    date=d.date(),
                    id=self.instrument_ids,
                    frequency=self.config.frequency,
                    start_time=self.config.intraday_start_time,
                    end_time=self.config.intraday_end_time,
                    source=self.config.etf_source,
                )
                if day_prices is not None and not day_prices.empty:
                    all_prices.append(day_prices)
            except Exception as e:
                logger.warning(f"Failed to load intraday for {d.date()}: {e}")

        if not all_prices:
            raise ValueError("No intraday prices loaded")

        return pd.concat(all_prices).sort_index()

    def _load_fx_data(self):
        """Load FX compositions and prices, combining into a single API call where possible."""
        # Apply overrides first; fetch only what's still missing
        self._fx_composition = self._override_fx_composition
        self._fx_forward_composition = self._override_fx_forward_composition

        if self._fx_composition is None or self._fx_forward_composition is None:
            fetched_spot, fetched_fwd = self._fetch_fx_compositions()
            if self._fx_composition is None:
                self._fx_composition = fetched_spot
            if self._fx_forward_composition is None:
                self._fx_forward_composition = fetched_fwd

        # FX Spot Prices
        currencies_spot = self._get_currencies_from_composition(self._fx_composition)
        if self.config.adjust_fx_spot and currencies_spot:
            self._fx_prices = (
                self._override_fx_prices
                if self._override_fx_prices is not None
                else self._fetch_fx_prices(currencies_spot)
            )

        # FX Forward Prices
        currencies_fwd = self._get_currencies_from_composition(self._fx_forward_composition)
        if self.config.adjust_fx_forward and currencies_fwd:
            self._fx_forward_prices = (
                self._override_fx_forward_prices
                if self._override_fx_forward_prices is not None
                else self._fetch_fx_forward_prices(currencies_fwd)
            )

    def _fetch_fx_compositions(self) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Fetch both spot and forward FX compositions in a single API call."""
        try:
            currency_data = self.api.info.get_currency_exposure(id=self.instrument_ids)
            if currency_data is None:
                return None, None
            spot = currency_data.pivot(
                index="index", columns="CURRENCY", values="WEIGHT"
            ).fillna(0)
            fwd = currency_data.pivot(
                index="index", columns="CURRENCY", values="WEIGHT_FX_FORWARD"
            ).fillna(0)
            logger.info(f"Fetched FX compositions: spot={spot.shape}, fwd={fwd.shape}")
            return spot, fwd
        except Exception as e:
            logger.warning(f"Could not fetch FX compositions: {e}")
            return None, None

    def _get_currencies_from_composition(self, composition: Optional[pd.DataFrame]) -> List[str]:
        if composition is None:
            return []
        return [c for c in composition.columns.tolist() if c != "EUR"]

    def _fetch_fx_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        fx_pairs = [f"EUR{ccy}" for ccy in currencies]
        fx_prices = self.api.market.get_daily_currency(
            id=fx_pairs,
            start=self.config.start,
            end=self.config.end,
            source=self.config.fx_source,
        )
        logger.info(f"Fetched FX spot prices: {fx_prices.shape if fx_prices is not None else 'None'}")
        return fx_prices

    def _fetch_fx_forward_prices(self, currencies: List[str]) -> Optional[pd.DataFrame]:
        if not currencies:
            return None
        fx_fwd_prices = self.api.market.get_daily_fx_forward(
            quoted_currency=currencies,
            start=self.config.start,
            end=self.config.end,
            tenor=self.config.fx_forward_tenor,
            source=self.config.fx_forward_source,
        )
        logger.info(f"Fetched FX forward prices: {fx_fwd_prices.shape if fx_fwd_prices is not None else 'None'}")
        return fx_fwd_prices

    def _load_dividends(self):
        if self._override_dividends is not None:
            self._dividends = self._override_dividends
        else:
            self._dividends = self.api.info.get_dividends(id=self.instrument_ids)
            logger.info(f"Loaded dividends for {len(self._dividends) if self._dividends else 0} instruments")

    def _load_ter(self):
        self._ter = self.api.info.get_ter(id=self.instrument_ids)
        if self._override_ter:
            if self._ter is None:
                self._ter = {}
            self._ter.update(self._override_ter)
        logger.info(f"Loaded TER for {len(self._ter) if self._ter else 0} instruments")

    def _build_adjuster(self):
        self._adjuster = Adjuster(
            prices=self._etf_prices,
            is_intraday=not self._is_daily,
            return_type=self.config.return_type,
        )

        if self.config.adjust_ter and self._ter:
            self._adjuster.add(TerComponent(self._ter))
            logger.debug("Added TerComponent")

        if (self.config.adjust_fx_spot and
                self._fx_prices is not None and
                self._fx_composition is not None):
            self._adjuster.add(FxSpotComponent(self._fx_composition, self._fx_prices))
            logger.debug("Added FxSpotComponent")

        if (self.config.adjust_fx_forward and
                self._fx_forward_prices is not None and
                self._fx_forward_composition is not None and
                self._fx_prices is not None):
            self._adjuster.add(FxForwardCarryComponent(
                self._fx_forward_composition,
                self._fx_forward_prices,
                self.config.fx_forward_tenor,
                self._fx_prices,
            ))
            logger.debug("Added FxForwardCarryComponent")

        if self.config.adjust_dividends and self._dividends is not None:
            self._adjuster.add(DividendComponent(self._dividends))
            logger.debug("Added DividendComponent")

        logger.info(f"Adjuster built with {len(self._adjuster.components)} components")

    # ============================================================
    # ACCESSORS
    # ============================================================

    def _ensure_loaded(self):
        if not self._is_loaded:
            raise RuntimeError("Pipeline not loaded. Call load() first.")

    @property
    def etf_prices(self) -> pd.DataFrame:
        """Raw ETF prices."""
        self._ensure_loaded()
        return self._etf_prices

    @property
    def fx_prices(self) -> Optional[pd.DataFrame]:
        """FX spot prices."""
        self._ensure_loaded()
        return self._fx_prices

    @property
    def fx_forward_prices(self) -> Optional[pd.DataFrame]:
        """FX forward prices."""
        self._ensure_loaded()
        return self._fx_forward_prices

    @property
    def fx_composition(self) -> Optional[pd.DataFrame]:
        """FX spot composition."""
        self._ensure_loaded()
        return self._fx_composition

    @property
    def fx_forward_composition(self) -> Optional[pd.DataFrame]:
        """FX forward composition."""
        self._ensure_loaded()
        return self._fx_forward_composition

    @property
    def dividends(self) -> Optional[pd.DataFrame]:
        """Dividend data."""
        self._ensure_loaded()
        return self._dividends

    @property
    def ter(self) -> Optional[dict]:
        """TER data."""
        self._ensure_loaded()
        return self._ter

    @property
    def adjuster(self) -> Adjuster:
        """The Adjuster instance."""
        self._ensure_loaded()
        return self._adjuster

    @property
    def clean_returns(self) -> pd.DataFrame:
        """Clean returns (adjusted for TER, FX, dividends)."""
        self._ensure_loaded()
        return self._adjuster.get_clean_returns()

    @property
    def raw_returns(self) -> pd.DataFrame:
        """Raw returns (unadjusted)."""
        self._ensure_loaded()
        return self._adjuster.return_calculator.calculate_returns(self._etf_prices)

    @property
    def clean_prices(self) -> pd.DataFrame:
        """Clean prices reconstructed from base price × cumulative clean returns."""
        self._ensure_loaded()
        initial = self._etf_prices.iloc[0]
        cum_returns = (1 + self.clean_returns).cumprod()
        return cum_returns.multiply(initial, axis=1)

    @property
    def adjustments(self) -> pd.DataFrame:
        """Total adjustments applied."""
        self._ensure_loaded()
        return self._adjuster.calculate_adjustment()

    @property
    def adjustment_breakdown(self) -> dict:
        """Breakdown of adjustments by component."""
        self._ensure_loaded()
        return self._adjuster.get_breakdown()

    # ============================================================
    # UTILITIES
    # ============================================================

    def get_returns(
            self,
            instrument_id: Optional[str] = None,
            clean: bool = True,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Get returns for one or all instruments.

        Args:
            instrument_id: If provided, return Series for that instrument; otherwise all.
            clean: If True, return clean returns; if False, raw returns.
        """
        returns = self.clean_returns if clean else self.raw_returns
        if instrument_id is not None:
            return returns[instrument_id]
        return returns

    def resample(self, freq: str) -> pd.DataFrame:
        """
        Resample clean returns to a different frequency.

        Args:
            freq: Target frequency ('5min', '15min', '1h', '1d', …).
        """
        self._ensure_loaded()
        if self.config.return_type == "logarithmic":
            return self.clean_returns.resample(freq).sum()
        return (1 + self.clean_returns).resample(freq).prod() - 1

    def __repr__(self) -> str:
        status = "loaded" if self._is_loaded else "not loaded"
        return (
            f"EtfDataPipeline("
            f"instruments={len(self.instrument_ids)}, "
            f"freq={self.config.frequency}, "
            f"status={status})"
        )
