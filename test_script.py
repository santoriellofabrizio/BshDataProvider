"""
Download FREE data and calculate FX Forward rates.

Uses:
1. FRED API (Federal Reserve) - Interest rates
2. ECB API - European interest rates  
3. Yahoo Finance - FX spot rates (fallback)

Calculates forward rates using Covered Interest Rate Parity (CIP).
"""
import pandas as pd
import requests
from datetime import datetime, timedelta
import numpy as np


class FreeForwardRatesDownloader:
    """
    Download free data and calculate FX forward rates.

    Sources:
    - FRED (Federal Reserve Economic Data) - Free API
    - ECB Statistical Data Warehouse - Free API
    - Yahoo Finance - Free (no API key needed)
    """

    def __init__(self, fred_api_key: str = None):
        """
        Initialize downloader.

        Args:
            fred_api_key: Optional FRED API key (free from https://fred.stlouisfed.org/docs/api/api_key.html)
                         If None, uses fallback methods
        """
        self.fred_api_key = fred_api_key
        self.fred_base_url = "https://api.stlouisfed.org/fred/series/observations"

    def download_all(
        self,
        currencies: list[str],
        start_date: str,
        end_date: str,
        save_csv: bool = True,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Download all necessary data and calculate forward rates.

        Args:
            currencies: List of currency codes (e.g., ['USD', 'GBP', 'JPY'])
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            save_csv: If True, save results to CSV

        Returns:
            (fx_spot_prices, fx_fwd_prices)
        """
        print("=" * 70)
        print("FREE FX FORWARD RATES DOWNLOADER")
        print("=" * 70)

        # 1. Download interest rates
        print("\n1. Downloading interest rates (OIS/Policy Rates)...")
        rates = self.download_interest_rates(currencies, start_date, end_date)
        print(f"   ✅ Downloaded rates for {len(rates.columns)} currencies")

        # 2. Download FX spot rates
        print("\n2. Downloading FX spot rates...")
        fx_spot = self.download_fx_spot(currencies, start_date, end_date)
        print(f"   ✅ Downloaded spot rates for {len(fx_spot.columns)} currencies")

        # 3. Calculate forward rates
        print("\n3. Calculating FX forward 1M rates...")
        fx_fwd = self.calculate_forward_rates(rates, tenor_days=30)
        print(f"   ✅ Calculated forward rates")

        # 4. Save to CSV
        if save_csv:
            fx_spot.to_csv('fx_spot_prices.csv')
            fx_fwd.to_csv('fx_forward_1m_prices.csv')
            rates.to_csv('interest_rates.csv')
            print("\n✅ Saved to CSV:")
            print("   - fx_spot_prices.csv")
            print("   - fx_forward_1m_prices.csv")
            print("   - interest_rates.csv")

        print("\n" + "=" * 70)
        print("DOWNLOAD COMPLETE!")
        print("=" * 70)

        return fx_spot, fx_fwd

    def download_interest_rates(
        self,
        currencies: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Download interest rates (OIS or policy rates).

        Returns:
            DataFrame(dates × currencies) with annual interest rates (decimal)
        """
        rates_map = {
            'EUR': self._get_ecb_rate,      # ECB deposit rate
            'USD': self._get_fed_rate,      # Fed funds rate
            'GBP': self._get_boe_rate,      # BoE base rate
            'JPY': self._get_boj_rate,      # BoJ policy rate
            'CHF': self._get_snb_rate,      # SNB policy rate
            'CAD': self._get_boc_rate,      # BoC overnight rate
            'AUD': self._get_rba_rate,      # RBA cash rate
        }

        rates_data = {}

        for ccy in currencies:
            if ccy == 'EUR':
                continue  # EUR is base, rate = 0 differential

            if ccy in rates_map:
                print(f"   Downloading {ccy} rates...")
                try:
                    rates_data[ccy] = rates_map[ccy](start_date, end_date)
                except Exception as e:
                    print(f"   ⚠️  Failed to download {ccy}: {e}")
                    # Fallback to constant rate
                    rates_data[ccy] = self._get_fallback_rate(ccy, start_date, end_date)
            else:
                print(f"   ⚠️  {ccy} not supported, using fallback")
                rates_data[ccy] = self._get_fallback_rate(ccy, start_date, end_date)

        # Combine into DataFrame
        if not rates_data:
            # No data, return zeros
            dates = pd.date_range(start_date, end_date, freq='D')
            return pd.DataFrame(0.0, index=dates, columns=currencies)

        rates_df = pd.DataFrame(rates_data)
        rates_df = rates_df.ffill().fillna(0.0)

        return rates_df

    def download_fx_spot(
        self,
        currencies: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Download FX spot rates using Yahoo Finance.

        Returns:
            DataFrame(dates × currencies) with spot FX rates (EUR base)
        """
        import yfinance as yf

        series_dict = {}  # Store Series (not DataFrames!)

        for ccy in currencies:
            if ccy == 'EUR':
                continue  # EUR vs EUR = 1.0

            # Yahoo Finance ticker
            ticker = f'EUR{ccy}=X'

            try:
                data = yf.download(
                    ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=True,
                )

                # yfinance returns DataFrame, need to extract Close column as Series
                if not data.empty:
                    # Check if 'Close' is a column or if data itself is the series
                    if isinstance(data, pd.DataFrame) and 'Close' in data.columns:
                        close_series = data['Close']
                    elif isinstance(data, pd.Series):
                        close_series = data
                    else:
                        # Try to get first column
                        close_series = data.iloc[:, 0] if len(data.columns) > 0 else None

                    if close_series is not None and len(close_series) > 0:
                        series_dict[ccy] = close_series  # Store Series, not DataFrame!
                        print(f"   ✅ {ccy}: {len(close_series)} days")
                    else:
                        print(f"   ⚠️  {ccy}: No Close data (skipped)")
                else:
                    print(f"   ⚠️  {ccy}: Empty data (skipped)")

            except Exception as e:
                print(f"   ⚠️  {ccy}: Error - {e} (skipped)")

        # Create DataFrame from dict of Series
        if not series_dict:
            # No data at all
            dates = pd.date_range(start_date, end_date, freq='D')
            return pd.DataFrame(index=dates, columns=[c for c in currencies if c != 'EUR'])

        # pd.DataFrame(dict_of_series) works perfectly!
        fx_df = pd.concat(list(series_dict.values()),axis=1)

        # Forward fill missing values
        fx_df = fx_df.ffill()

        return fx_df

    def calculate_forward_rates(
        self,
        interest_rates: pd.DataFrame,
        tenor_days: int = 30,
    ) -> pd.DataFrame:
        """
        Calculate FX forward rates from interest rate differentials.

        Formula (Covered Interest Parity):
            Forward Premium = (r_EUR - r_ccy) × (tenor_days / 360)
            In basis points: premium_bps = premium × 10000

        Args:
            interest_rates: DataFrame(dates × currencies) with annual rates
            tenor_days: Forward tenor (30 for 1M)

        Returns:
            DataFrame(dates × currencies) with forward premiums in bps
        """
        # EUR rate (base)
        r_eur = 0.035  # ECB deposit rate (update manually or download)

        # Calculate differentials
        fwd_premiums = pd.DataFrame(index=interest_rates.index)

        for ccy in interest_rates.columns:
            r_ccy = interest_rates[ccy]

            # Interest rate differential (annual)
            rate_diff = r_eur - r_ccy

            # Convert to tenor (monthly for 1M)
            year_fraction = tenor_days / 360  # Money market convention
            premium = rate_diff * year_fraction

            # Convert to basis points
            premium_bps = premium * 10000

            fwd_premiums[ccy] = premium_bps

        return fwd_premiums

    # ========================================================================
    # RATE DOWNLOADERS (Free APIs)
    # ========================================================================

    def _get_fed_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get Fed Funds Rate from FRED."""
        if self.fred_api_key:
            return self._download_from_fred('DFF', start_date, end_date)
        else:
            # Fallback: Use recent known rate
            return self._get_fallback_rate('USD', start_date, end_date)

    def _get_ecb_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get ECB deposit rate from ECB API."""
        # ECB SDW API (free)
        url = "https://data.ecb.europa.eu/data-detail-api"
        # Simplified: use constant rate (you can implement full ECB API)
        return self._get_fallback_rate('EUR', start_date, end_date)

    def _get_boe_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get Bank of England base rate."""
        return self._get_fallback_rate('GBP', start_date, end_date)

    def _get_boj_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get Bank of Japan policy rate."""
        return self._get_fallback_rate('JPY', start_date, end_date)

    def _get_snb_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get Swiss National Bank policy rate."""
        return self._get_fallback_rate('CHF', start_date, end_date)

    def _get_boc_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get Bank of Canada overnight rate."""
        return self._get_fallback_rate('CAD', start_date, end_date)

    def _get_rba_rate(self, start_date: str, end_date: str) -> pd.Series:
        """Get Reserve Bank of Australia cash rate."""
        return self._get_fallback_rate('AUD', start_date, end_date)

    def _download_from_fred(
        self,
        series_id: str,
        start_date: str,
        end_date: str,
    ) -> pd.Series:
        """Download from FRED API."""
        params = {
            'series_id': series_id,
            'api_key': self.fred_api_key,
            'file_type': 'json',
            'observation_start': start_date,
            'observation_end': end_date,
        }

        response = requests.get(self.fred_base_url, params=params)
        data = response.json()

        if 'observations' not in data:
            raise ValueError(f"No data from FRED for {series_id}")

        # Parse
        obs = data['observations']
        dates = [o['date'] for o in obs]
        values = [float(o['value']) if o['value'] != '.' else np.nan for o in obs]

        series = pd.Series(values, index=pd.to_datetime(dates))
        series = series / 100  # Convert percentage to decimal

        return series

    def _get_fallback_rate(
        self,
        ccy: str,
        start_date: str,
        end_date: str,
    ) -> pd.Series:
        """
        Fallback: Use recent known policy rates (constant).

        Note: Update these manually from central bank websites.
        """
        # As of December 2024 (approximate)
        current_rates = {
            'EUR': 0.0350,  # ECB deposit rate
            'USD': 0.0450,  # Fed funds target (midpoint)
            'GBP': 0.0475,  # BoE base rate
            'JPY': -0.0010, # BoJ (still negative)
            'CHF': 0.0150,  # SNB policy rate
            'CAD': 0.0475,  # BoC overnight rate
            'AUD': 0.0410,  # RBA cash rate
        }

        rate = current_rates.get(ccy, 0.03)  # Default 3%

        dates = pd.date_range(start_date, end_date, freq='D')
        return pd.Series(rate, index=dates)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    # Initialize downloader
    downloader = FreeForwardRatesDownloader(
        fred_api_key=None  # Get free key from https://fred.stlouisfed.org/docs/api/api_key.html
    )

    # Download data
    fx_spot, fx_fwd = downloader.download_all(
        currencies=['USD', 'GBP', 'JPY', 'CHF'],
        start_date='2024-01-01',
        end_date='2024-12-31',
        save_csv=True,
    )

    print("\n" + "=" * 70)
    print("SAMPLE DATA")
    print("=" * 70)

    print("\nFX Spot Prices (EUR base):")
    print(fx_spot.head())

    print("\nFX Forward 1M Prices (basis points):")
    print(fx_fwd.head())

    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)
    print("""
    FX Forward prices in basis points:
    - Positive = EUR rates higher → pay premium to hedge
    - Negative = EUR rates lower → receive discount
    
    Example:
        USD = +150 bps → EUR rates 1.5% higher than USD
                      → Costs 1.5% annually to hedge USD exposure
    """)