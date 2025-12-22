"""
Adjuster orchestrator for return adjustments.
"""
from datetime import date, datetime
import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional, Union, List

from analytics.adjustments.component import Component
from analytics.adjustments.protocols import InstrumentProtocol
from core.instruments.instrument_factory import InstrumentFactory

logger = logging.getLogger(__name__)


class Adjuster:
    """
    Orchestrates adjustment calculations across multiple instruments.

    Usage:
        # Prepare data
        prices = pd.DataFrame(...)      # dates × instruments
        fx_prices = pd.DataFrame(...)   # dates × currencies

        # Option 1: Explicit instruments
        instruments = factory.get_many(['IWDA LN', 'VWRL LN'])
        adjuster = Adjuster(prices, fx_prices, instruments)

        # Option 2: Auto-fetch from factory (lazy)
        adjuster = Adjuster(prices, fx_prices)  # Uses InstrumentFactory

        # Build pipeline
        adjuster.add(TerComponent(ters)).add(YtmComponent(ytms))

        # Calculate
        adjustments = adjuster.calculate()  # DataFrame(dates × instruments)
    """

    def __init__(
        self,
        prices: pd.DataFrame,
        fx_prices: pd.DataFrame,
        instruments: Optional[Dict[str, InstrumentProtocol]] = None,
        intraday: bool = False,
        settlement_days: Union[int, pd.Series, Dict[str, int]] = 2,
    ):
        """
        Initialize adjuster.

        Args:
            prices: DataFrame with instrument prices
                    - Index: dates (DatetimeIndex or convertible)
                    - Columns: instrument IDs
            fx_prices: DataFrame with FX spot rates (EUR base)
                       - Index: dates (DatetimeIndex or convertible)
                       - Columns: currency codes or FX tickers (EURUSD, EURGBP, etc.)
            instruments: Optional dict[instrument_id → Instrument object]
                        If None, fetches from InstrumentFactory
            intraday: If True, preserve intraday timestamps; if False, normalize to dates
            settlement_days: Settlement lag for all instruments or per-instrument
                           - int: Same for all (e.g., 2 for T+2)
                           - pd.Series: Per-instrument (index=instrument_id, values=days)
                           - Dict: Per-instrument mapping {instrument_id: days}

        Raises:
            ValueError: If DataFrames malformed or incompatible
        """
        # Store intraday flag
        self.intraday = intraday
        
        # Parse settlement_days
        self.settlement_days = self._parse_settlement_days(
            settlement_days, 
            prices.columns.tolist()
        )
        
        # Validate and normalize DataFrames
        self.prices = self._validate_and_transpose(prices, "prices")
        self.fx_prices = self._validate_and_transpose(fx_prices, "fx_prices")
        
        # Normalize FX prices columns (EURUSD → USD)
        self.fx_prices = self._normalize_fx_columns(self.fx_prices)
        
        # Normalize dates if not intraday
        if not self.intraday:
            self.prices.index = self.prices.index.normalize()
            self.fx_prices.index = self.fx_prices.index.normalize()

        # Extract instrument IDs from columns
        self.instrument_ids = self.prices.columns.tolist()

        # Get instruments (explicit or from factory)
        if instruments is None:
            self.instruments = self._fetch_instruments_from_factory()
        else:
            self.instruments = instruments

        # Validate instruments
        self._validate_instruments()

        # Component registry
        self.components: list[Component] = []
        
        # Cache for static vs dynamic adjustments
        self._static_components_cache: Optional[pd.DataFrame] = None
        self._fx_dependent_components: list[Component] = []

        logger.info(
            f"Adjuster initialized: {len(self.instrument_ids)} instruments, "
            f"{len(self.prices.index)} {'timestamps' if self.intraday else 'dates'}"
        )
    
    @staticmethod
    def _parse_settlement_days(
            settlement_days: Union[int, pd.Series, Dict[str, int]],
        instrument_ids: List[str]
    ) -> pd.Series:
        """
        Parse settlement_days parameter into per-instrument Series.
        
        Args:
            settlement_days: int, Series, or Dict
            instrument_ids: List of instrument IDs
        
        Returns:
            pd.Series with settlement days per instrument
        """
        # Case 1: int (same for all)
        if isinstance(settlement_days, int):
            if settlement_days < 0 or settlement_days > 5:
                raise ValueError(f"settlement_days must be 0-5, got {settlement_days}")
            return pd.Series(settlement_days, index=instrument_ids)
        
        # Case 2: Dict
        if isinstance(settlement_days, dict):
            settlement_days = pd.Series(settlement_days)
        
        # Case 3: Series
        if isinstance(settlement_days, pd.Series):
            # Validate values
            if (settlement_days < 0).any() or (settlement_days > 5).any():
                raise ValueError("settlement_days values must be 0-5")
            
            # Fill missing instruments with default (2)
            settlement_series = pd.Series(2, index=instrument_ids)
            settlement_series.update(settlement_days)
            return settlement_series
        
        raise TypeError(
            f"settlement_days must be int, pd.Series, or Dict, got {type(settlement_days)}"
        )

    @staticmethod
    def _normalize_fx_columns(fx_prices: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize FX price columns from tickers to currency codes.
        
        Converts:
            EURUSD      → USD
            EURUSD 1M   → USD
            EURUSD 3M   → USD
            EURGBP      → GBP
            EURGBP 1W   → GBP
            USD         → USD (unchanged, warning)
            USDEUR      → USD (inverted)
            USDEUR 1M   → USD (inverted with tenor)
        
        Args:
            fx_prices: DataFrame with FX prices (supports tenor suffixes)
        
        Returns:
            DataFrame with normalized column names and inverted prices where needed
        """
        normalized_columns = {}
        columns_to_invert = []  # Track which columns need 1/price
        
        for col in fx_prices.columns:
            col_str = str(col).upper().strip()
            
            # Extract base ticker (remove tenor suffix if present)
            # "EURUSD 1M" → "EURUSD", "EURGBP 3M" → "EURGBP"
            parts = col_str.split()
            base_ticker = parts[0]
            tenor = parts[1] if len(parts) > 1 else None
            
            # Case 1: 6-char EUR-based ticker (EURUSD, EURGBP, etc.)
            if len(base_ticker) == 6 and base_ticker.startswith('EUR'):
                # Extract quote currency (last 3 chars)
                currency = base_ticker[-3:]
                normalized_columns[col] = currency
                
                if tenor:
                    logger.debug(
                        f"Normalized FX column with tenor: {col} → {currency} "
                        f"(tenor: {tenor})"
                    )
                else:
                    logger.debug(f"Normalized FX column: {col} → {currency}")
            
            # Case 2: 6-char inverted ticker (USDEUR, GBPEUR, etc.)
            elif len(base_ticker) == 6 and base_ticker.endswith('EUR'):
                # Extract base currency (first 3 chars)
                currency = base_ticker[:3]
                normalized_columns[col] = currency
                columns_to_invert.append(col)
                
                if tenor:
                    logger.warning(
                        f"FX column '{col}' is inverted (base currency is not EUR). "
                        f"Inverting prices: 1/{col} → {currency} (tenor: {tenor})"
                    )
                else:
                    logger.warning(
                        f"FX column '{col}' is inverted (base currency is not EUR). "
                        f"Inverting prices: 1/{col} → {currency}"
                    )
            
            # Case 3: 3-char currency code (USD, GBP, etc.)
            elif len(base_ticker) == 3:
                normalized_columns[col] = base_ticker
                logger.warning(
                    f"FX column '{col}' is a currency code without EUR base indication. "
                    f"Assuming it represents EUR{base_ticker} (e.g., EUR/{base_ticker} rate)."
                )
            
            # Case 4: Other formats - warn and keep as-is
            else:
                logger.warning(
                    f"FX column '{col}' doesn't match expected format "
                    f"(EURCCY, CCYEUR, CCY, or EURCCY TENOR). "
                    "Keeping as-is."
                )
                normalized_columns[col] = col
        
        # Create normalized DataFrame
        fx_normalized = fx_prices.copy()
        
        # Invert prices for inverted tickers (USDEUR → 1/USDEUR)
        for col in columns_to_invert:
            logger.info(f"Inverting FX prices for {col}: new values = 1 / old values")
            fx_normalized[col] = 1.0 / fx_normalized[col]
            # Replace inf with NaN (in case of zero prices)
            fx_normalized[col].replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Rename columns
        fx_normalized = fx_normalized.rename(columns=normalized_columns)
        
        # Log duplicates if any
        duplicates = fx_normalized.columns[fx_normalized.columns.duplicated()].tolist()
        if duplicates:
            logger.warning(
                f"Duplicate currency codes after normalization: {duplicates}. "
                "Keeping first occurrence."
            )
            fx_normalized = fx_normalized.loc[:, ~fx_normalized.columns.duplicated()]
        
        logger.info(
            f"FX columns normalized: {list(fx_prices.columns)} → {list(fx_normalized.columns)}"
        )
        
        return fx_normalized

    @staticmethod
    def _validate_and_transpose(df: pd.DataFrame, name: str) -> pd.DataFrame:
        """
        Validate DataFrame structure and transpose if needed.

        Expected format:
            - Index: dates (DatetimeIndex preferred)
            - Columns: instruments/currencies (strings)

        Auto-transposes if:
            - Columns look like dates
            - Index looks like instruments/currencies

        Args:
            df: Input DataFrame
            name: Name for error messages

        Returns:
            Validated DataFrame (dates × items)

        Raises:
            ValueError: If DataFrame malformed
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{name} must be DataFrame, got {type(df)}")

        if df.empty:
            raise ValueError(f"{name} is empty")

        # Check if needs transposing
        needs_transpose = False

        # Heuristic 1: Columns are dates
        if isinstance(df.columns, pd.DatetimeIndex):
            needs_transpose = True
            logger.debug(f"{name}: Transposing (columns are DatetimeIndex)")

        # Heuristic 2: Index looks like strings, columns like dates
        elif (isinstance(df.index[0], str) and
              isinstance(df.columns[0], (date, pd.Timestamp))):
            needs_transpose = True
            logger.debug(f"{name}: Transposing (index=str, columns=dates)")

        # Transpose if needed
        if needs_transpose:
            df = df.T
            logger.info(f"{name}: Transposed to {df.shape} (dates × items)")

        # Convert index to DatetimeIndex if needed
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
                logger.debug(f"{name}: Converted index to DatetimeIndex")
            except Exception as e:
                raise ValueError(
                    f"{name} index must be dates or convertible to DatetimeIndex. "
                    f"Got: {type(df.index[0])}. Error: {e}"
                )

        # Validate structure
        if len(df.index) == 0:
            raise ValueError(f"{name} has no dates in index")

        if len(df.columns) == 0:
            raise ValueError(f"{name} has no items in columns")

        return df

    def _fetch_instruments_from_factory(self) -> Dict[str, InstrumentProtocol]:
        """
        Fetch instruments from InstrumentFactory.

        Uses factory's internal registry (shared with API calls).

        Returns:
            Dict[instrument_id → Instrument object]

        Raises:
            ImportError: If InstrumentFactory not available
        """

        logger.debug(f"Fetching {len(self.instrument_ids)} instruments from factory...")

        factory = InstrumentFactory()
        instruments = factory.get_many(self.instrument_ids)

        logger.info(f"Fetched {len(instruments)} instruments from InstrumentFactory")

        return instruments

    def _validate_instruments(self):
        """Validate that all instruments in prices have metadata"""
        missing = set(self.instrument_ids) - set(self.instruments.keys())
        if missing:
            raise ValueError(
                f"Missing instruments in metadata: {missing}. "
                f"Provide instruments for all columns in prices."
            )

    def add(self, component: Component) -> 'Adjuster':
        """
        Add component to calculation pipeline (fluent API).

        Args:
            component: Component instance with data

        Returns:
            Self for chaining

        Example:
            adjuster.add(TerComponent(ters)).add(YtmComponent(ytms))
        """
        self.components.append(component)
        
        # Track FX-dependent components (invalidate cache on FX update)
        from analytics.adjustments.fx_spot import FxSpotComponent
        from analytics.adjustments.fx_forward_carry import FxForwardComponent
        
        if isinstance(component, (FxSpotComponent, FxForwardComponent)):
            self._fx_dependent_components.append(component)
        
        logger.debug(f"Added component: {component.__class__.__name__}")
        return self

    def calculate(self, dates: Union[list[date], list[datetime], None] = None) -> pd.DataFrame:
        """
        Calculate adjustments for all instruments.

        Args:
            dates: Optional subset of dates/datetimes (default: all from prices.index)
                  - If intraday=True: expects datetime objects
                  - If intraday=False: expects date objects or will normalize datetimes

        Returns:
            DataFrame(dates × instruments) with total adjustments
        """
        # Get dates from index if not provided
        if dates is None:
            calc_dates = self.prices.index.tolist()
        else:
            calc_dates = dates
            
            # Normalize to dates if not intraday
            if not self.intraday:
                calc_dates = [
                    d.date() if isinstance(d, (datetime, pd.Timestamp)) else d
                    for d in calc_dates
                ]

        # Initialize result
        adjustments = pd.DataFrame(0.0, index=calc_dates, columns=self.instrument_ids)

        # Sum contributions from each component
        for component in self.components:
            comp_name = component.__class__.__name__
            logger.debug(f"Calculating {comp_name}...")

            try:
                component_adj = component.calculate_adjustment(instruments=self.instruments, dates=calc_dates,
                                                               prices=self.prices, fx_prices=self.fx_prices)

                # Add to total
                adjustments += component_adj

                # Log stats
                non_zero = (component_adj != 0).sum().sum()
                logger.info(
                    f"{comp_name}: {non_zero} non-zero adjustments "
                    f"(mean={component_adj.mean().mean():.6f})"
                )

            except Exception as e:
                logger.error(
                    f"{comp_name} failed completely: {e}",
                    exc_info=True
                )
                # Continue with other components

        return adjustments
    
    def get_adjustments_cumulative(self, dates: Union[list[date], list[datetime], None] = None) -> pd.DataFrame:
        """
        Get cumulative adjustments (sum from end to each date).
        
        Useful for applying adjustments to cumulative returns.
        
        Formula:
            cumulative_adj[t] = sum(adjustments[t:end])
        
        Args:
            dates: Optional subset of dates/datetimes
        
        Returns:
            DataFrame(dates × instruments) with cumulative adjustments
        
        Example:
            # Period adjustments
            adjustments = adjuster.calculate()
            # [0.001, 0.002, 0.001]
            
            # Cumulative adjustments
            cumulative_adj = adjuster.get_adjustments_cumulative()
            # [0.004, 0.003, 0.001]  (reverse cumsum)
        """
        # Get regular adjustments
        adjustments = self.calculate(dates)
        
        # Reverse cumulative sum: [::-1].cumsum()[::-1]
        cumulative = adjustments[::-1].cumsum()[::-1]
        
        return cumulative
    
    def update_fx_prices(self, new_fx_prices: Union[pd.Series, pd.DataFrame], timestamp: Optional[Union[date, datetime]] = None):
        """
        Update FX prices for live data (invalidates FX-dependent component cache).
        
        Use this when receiving live FX price updates to avoid recalculating
        static adjustments (TER, YTM, dividends).
        
        Args:
            new_fx_prices: Updated FX prices
                          - pd.Series: {currency: price} (updates last row)
                          - pd.DataFrame: Full update (replaces fx_prices)
            timestamp: Optional timestamp to update (default: last row)
        
        Example:
            # Initial setup
            adj = Adjuster(prices, fx_prices, intraday=True)
            adj.add(FxSpotComponent(fx_comp))  # FX-dependent
            adj.add(TerComponent(ters))         # Static
            
            # First calculation (calculates everything)
            adjustments_9am = adj.calculate()
            
            # --- Live FX update at 10:30 ---
            new_fx = pd.Series({'USD': 1.12, 'GBP': 0.86})
            adj.update_fx_prices(new_fx)
            
            # Recalculation (only FX components recalculated, TER cached)
            adjustments_10_30 = adj.calculate()
        """
        if isinstance(new_fx_prices, pd.Series):
            # Update last row (or specified timestamp)
            if timestamp is None:
                timestamp = self.fx_prices.index[-1]
            
            # Normalize FX columns if needed
            normalized_fx = self._normalize_fx_series(new_fx_prices)
            
            # Update prices
            for currency, price in normalized_fx.items():
                if currency in self.fx_prices.columns:
                    self.fx_prices.loc[timestamp, currency] = price
                else:
                    logger.warning(f"Currency '{currency}' not in fx_prices columns, skipping")
        
        elif isinstance(new_fx_prices, pd.DataFrame):
            # Full replacement
            self.fx_prices = self._normalize_fx_columns(new_fx_prices)
        
        else:
            raise TypeError(f"new_fx_prices must be pd.Series or pd.DataFrame, got {type(new_fx_prices)}")
        
        # Invalidate cache for FX-dependent components
        # (Static components like TER, YTM remain cached)
        logger.debug(f"FX prices updated, invalidated {len(self._fx_dependent_components)} FX-dependent components")
    
    @staticmethod
    def _normalize_fx_series(fx_series: pd.Series) -> pd.Series:
        """
        Normalize FX series (similar to _normalize_fx_columns but for Series).
        
        Args:
            fx_series: Series with currency codes or tickers as index
        
        Returns:
            Normalized series with currency codes
        """
        normalized = {}
        
        for key, value in fx_series.items():
            key_str = str(key).upper()
            
            # EURUSD → USD
            if len(key_str) == 6 and key_str.startswith('EUR'):
                currency = key_str[-3:]
                normalized[currency] = value
            
            # USDEUR → USD (inverted)
            elif len(key_str) == 6 and key_str.endswith('EUR'):
                currency = key_str[:3]
                normalized[currency] = 1.0 / value if value != 0 else np.nan
            
            # USD → USD
            elif len(key_str) == 3:
                normalized[key_str] = value
            
            else:
                normalized[key] = value
        
        return pd.Series(normalized)

    def get_breakdown(self, dates: Union[list[date], list[datetime], None] = None) -> dict[str, pd.DataFrame]:
        """
        Get adjustments broken down by component.

        Args:
            dates: Optional subset of dates/datetimes (default: all from prices.index)
                  - If intraday=True: expects datetime objects
                  - If intraday=False: expects date objects or will normalize datetimes

        Returns:
            Dict[component_name → DataFrame(dates × instruments)]

        Example:
            breakdown = adjuster.get_breakdown()
            ter_adj = breakdown['TerComponent']
            ytm_adj = breakdown['YtmComponent']
        """
        # Get dates from index if not provided
        if dates is None:
            calc_dates = self.prices.index.tolist()
        else:
            calc_dates = dates
            
            # Normalize to dates if not intraday
            if not self.intraday:
                calc_dates = [
                    d.date() if isinstance(d, (datetime, pd.Timestamp)) else d
                    for d in calc_dates
                ]
        breakdown = {}

        for component in self.components:
            comp_name = component.__class__.__name__

            try:
                breakdown[comp_name] = component.calculate_adjustment(instruments=self.instruments, dates=calc_dates,
                                                                      prices=self.prices, fx_prices=self.fx_prices)
            except Exception as e:
                logger.error(f"{comp_name} failed: {e}")
                breakdown[comp_name] = pd.DataFrame(
                    0.0, index=calc_dates, columns=self.instrument_ids
                )

        return breakdown

    def clean_returns(
        self,
        raw_returns: pd.DataFrame,
        dates: Union[list[date], list[datetime], None] = None,
    ) -> pd.DataFrame:
        """
        Clean raw returns by applying adjustments.

        Args:
            raw_returns: DataFrame(dates × instruments) with raw returns
            dates: Optional subset of dates/datetimes
                  - If intraday=True: expects datetime objects
                  - If intraday=False: expects date objects or will normalize datetimes

        Returns:
            DataFrame(dates × instruments) with cleaned returns

        Formula:
            clean_return = raw_return + adjustment
        """
        adjustments = self.calculate(dates)

        # Align and add
        cleaned = raw_returns.add(
            adjustments.reindex(raw_returns.index, columns=raw_returns.columns),
            fill_value=0.0
        )

        return cleaned

    def __repr__(self) -> str:
        return (
            f"Adjuster("
            f"instruments={len(self.instrument_ids)}, "
            f"dates={len(self.prices.index)}, "
            f"components={len(self.components)})"
        )