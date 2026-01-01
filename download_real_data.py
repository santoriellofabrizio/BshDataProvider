"""
Download real intraday data for IUSA.MI and IUSE.MI ETFs.

Downloads:
- Intraday prices (15min frequency, 1 month)
- EURUSD FX prices
- Dividends data
- TER data
- FX forward points (assumed 21 for all dates)
- FX composition

Saves all data to parquet format in ./real_data/ folder.
"""
import sys
sys.path.insert(0, 'src')

import yfinance as yf
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Data directory
DATA_DIR = Path("real_data")
DATA_DIR.mkdir(exist_ok=True)

print("="*80)
print("DOWNLOADING REAL INTRADAY DATA")
print("="*80)

# Date range: 1 month of intraday data
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

print(f"\nDate range: {start_date.date()} to {end_date.date()}")
print(f"Frequency: 15 minutes")

# ============================================================================
# 1. Download ETF Prices (IUSA.MI, IUSE.MI)
# ============================================================================
print("\n" + "="*80)
print("1. Downloading ETF intraday prices (15min)")
print("="*80)

tickers = ["IUSA.MI", "IUSE.MI"]
etf_data = {}

for ticker in tickers:
    print(f"\n  Downloading {ticker}...")
    try:
        data = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval="15m",
            progress=False,
            auto_adjust=False  # Keep unadjusted prices
        )

        if not data.empty:
            close_series = data['Close']
            # Map ticker to instrument name
            if ticker == 'IUSA.MI':
                inst_name = 'IUSA'
            elif ticker == 'IUSE.MI':
                inst_name = 'IUSE'
            else:
                inst_name = ticker

            close_series.name = inst_name
            etf_data[inst_name] = close_series
            print(f"  [OK] {ticker}: {len(data)} rows, {data.index[0]} to {data.index[-1]}")
        else:
            print(f"  [FAIL] {ticker}: No data available")

    except Exception as e:
        print(f"  [FAIL] {ticker}: Error - {e}")

# Combine into single DataFrame
if etf_data:
    # Create DataFrame from series (names are already set)
    prices = pd.concat(list(etf_data.values()), axis=1)

    # Ensure column names are correct (not tickers)
    prices.columns = list(etf_data.keys())

    # Remove timezone info for consistency
    if prices.index.tz is not None:
        prices.index = prices.index.tz_localize(None)

    # Forward fill missing values (different trading hours)
    prices = prices.ffill()

    print(f"\n  Combined prices shape: {prices.shape}")
    print(f"  Date range: {prices.index[0]} to {prices.index[-1]}")
    print(f"  Missing values: {prices.isna().sum().sum()}")

    # Save to parquet
    prices_file = DATA_DIR / "prices.parquet"
    prices.to_parquet(prices_file)
    print(f"  [OK] Saved to {prices_file}")
else:
    print("  [FAIL] No ETF data downloaded")
    sys.exit(1)

# ============================================================================
# 2. Download EURUSD FX Prices
# ============================================================================
print("\n" + "="*80)
print("2. Downloading EURUSD intraday prices (15min)")
print("="*80)

try:
    print(f"\n  Downloading EURUSD=X...")
    fx_data = yf.download(
        "EURUSD=X",
        start=start_date,
        end=end_date,
        interval="15m",
        progress=False
    )

    if not fx_data.empty:
        fx_prices = pd.DataFrame({
            'USD': fx_data['Close'].iloc[:,0].values
        }, index=fx_data.index)

        # Remove timezone info
        if fx_prices.index.tz is not None:
            fx_prices.index = fx_prices.index.tz_localize(None)

        # Align with ETF prices dates (forward fill for missing FX data)
        fx_prices = fx_prices.reindex(prices.index, method='ffill')

        print(f"  [OK] EURUSD: {len(fx_prices)} rows, {fx_prices.index[0]} to {fx_prices.index[-1]}")
        print(f"  Missing values: {fx_prices.isna().sum().sum()}")

        # Save to parquet
        fx_file = DATA_DIR / "fx_prices.parquet"
        fx_prices.to_parquet(fx_file)
        print(f"  [OK] Saved to {fx_file}")
    else:
        print(f"  [FAIL] EURUSD: No data available")
        # Create dummy FX prices (constant 1.10)
        fx_prices = pd.DataFrame(
            data={'USD': [1.10] * len(prices.index)},
            index=prices.index
        )
        fx_file = DATA_DIR / "fx_prices.parquet"
        fx_prices.to_parquet(fx_file)
        print(f"  [WARN] Created dummy FX prices (1.10)")

except Exception as e:
    print(f"  [FAIL] EURUSD: Error - {e}")
    # Create dummy FX prices
    fx_prices = pd.DataFrame(
        data={'USD': [1.10] * len(prices.index)},
        index=prices.index
    )
    fx_file = DATA_DIR / "fx_prices.parquet"
    fx_prices.to_parquet(fx_file)
    print(f"  [WARN] Created dummy FX prices (1.10)")

# ============================================================================
# 3. Download Dividend Data
# ============================================================================
print("\n" + "="*80)
print("3. Downloading dividend data")
print("="*80)

dividends_data = {}

for ticker_name, ticker_symbol in [('IUSA', 'IUSA.MI'), ('IUSE', 'IUSE.MI')]:
    print(f"\n  Fetching dividends for {ticker_symbol}...")
    try:
        ticker = yf.Ticker(ticker_symbol)
        divs = ticker.dividends

        if not divs.empty:
            # Remove timezone info for comparison
            if divs.index.tz is not None:
                divs.index = divs.index.tz_localize(None)

            # Filter to our date range
            divs = divs[(divs.index >= start_date) & (divs.index <= end_date)]

            if not divs.empty:
                dividends_data[ticker_name] = divs
                print(f"  [OK] {ticker_name}: {len(divs)} dividends")
                for date, amount in divs.items():
                    print(f"    - {date.date()}: [X]{amount:.4f}")
            else:
                print(f"  [WARN] {ticker_name}: No dividends in date range")
                dividends_data[ticker_name] = pd.Series([], dtype=float)
        else:
            print(f"  [WARN] {ticker_name}: No dividend history")
            dividends_data[ticker_name] = pd.Series([], dtype=float)

    except Exception as e:
        print(f"  [FAIL] {ticker_name}: Error - {e}")
        dividends_data[ticker_name] = pd.Series([], dtype=float)

# Create dividend DataFrame aligned with intraday prices
dividends_df = pd.DataFrame(0.0, index=prices.index, columns=['IUSA', 'IUSE'])

# Map dividends to the intraday index (use first timestamp of the day)
for ticker_name, divs in dividends_data.items():
    for div_date, div_amount in divs.items():
        # Find the first intraday timestamp on this date
        div_date_normalized = pd.Timestamp(div_date).normalize()
        mask = dividends_df.index.normalize() == div_date_normalized
        if mask.any():
            first_timestamp = dividends_df.index[mask][0]
            dividends_df.loc[first_timestamp, ticker_name] = div_amount

# Save dividends
dividends_file = DATA_DIR / "dividends.parquet"
dividends_df.to_parquet(dividends_file)
print(f"\n  [OK] Saved to {dividends_file}")
print(f"  Non-zero dividends: {(dividends_df != 0).sum().sum()}")

# ============================================================================
# 4. Fetch TER Data
# ============================================================================
print("\n" + "="*80)
print("4. Fetching TER (Total Expense Ratio)")
print("="*80)

ters = {}

for ticker_name, ticker_symbol in [('IUSA', 'IUSA.MI'), ('IUSE', 'IUSE.MI')]:
    print(f"\n  Fetching TER for {ticker_symbol}...")
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info

        # Try to get expense ratio (annual fee)
        ter = None
        if 'annualReportExpenseRatio' in info and info['annualReportExpenseRatio']:
            ter = info['annualReportExpenseRatio']
        elif 'totalAssets' in info:
            # Use specified TER values
            # IUSA is iShares Core S&P 500 UCITS ETF - 0.07%
            # IUSE is iShares MSCI USA ESG Screened UCITS ETF - 0.20%
            if 'IUSA' in ticker_name:
                ter = 0.0007  # 0.07%
            else:
                ter = 0.0020  # 0.20%
            print(f"  [WARN] Using specified value for {ticker_name}: {ter*100:.2f}%")

        if ter is not None:
            ters[ticker_name] = ter
            print(f"  [OK] {ticker_name}: {ter*100:.4f}%")
        else:
            # Default
            ters[ticker_name] = 0.0007
            print(f"  [WARN] {ticker_name}: Using default 0.07%")

    except Exception as e:
        print(f"  [FAIL] {ticker_name}: Error - {e}, using default 0.07%")
        ters[ticker_name] = 0.0007

# Save TER
ters_df = pd.Series(ters)
ters_file = DATA_DIR / "ters.parquet"
ters_df.to_frame('TER').to_parquet(ters_file)
print(f"\n  [OK] Saved to {ters_file}")

# ============================================================================
# 5. Create FX Forward Points
# ============================================================================
print("\n" + "="*80)
print("5. Creating FX forward points (assumed 21 for all dates)")
print("="*80)

# FX forward points: 21 points (0.0021 in decimal)
fx_forward_points = pd.DataFrame({
    'USD': 21.0  # 21 points
}, index=prices.index)

fx_forward_file = DATA_DIR / "fx_forward_points.parquet"
fx_forward_points.to_parquet(fx_forward_file)
print(f"  [OK] Saved to {fx_forward_file}")
print(f"  Forward points: 21 for all {len(fx_forward_points)} timestamps")

# ============================================================================
# 6. Create FX Composition
# ============================================================================
print("\n" + "="*80)
print("6. Creating FX composition")
print("="*80)

# IUSA: iShares Core S&P 500 (100% USD exposure, EUR trading)
# IUSE: iShares MSCI USA ESG (0% USD exposure - base currency EUR)
fx_composition = pd.DataFrame({
    'USD': [1.0, 0.0],
    'EUR': [0.0, 1.0]
}, index=['IUSA', 'IUSE'])

fx_comp_file = DATA_DIR / "fx_composition.parquet"
fx_composition.to_parquet(fx_comp_file)
print(f"  [OK] Saved to {fx_comp_file}")
print("\n  FX Composition:")
print(fx_composition)

# ============================================================================
# 7. Create FX Forward Composition
# ============================================================================
print("\n" + "="*80)
print("7. Creating FX forward composition")
print("="*80)

# IUSA: No forward (0%) - uses spot FX
# IUSE: Full forward (100%) - uses FX forward
fx_forward_composition = pd.DataFrame({
    'USD': [0.0, 1.0],  # IUSA: 0% forward, IUSE: 100% forward
    'EUR': [0.0, 0.0]   # No EUR forward for either
}, index=['IUSA', 'IUSE'])

fx_fwd_comp_file = DATA_DIR / "fx_forward_composition.parquet"
fx_forward_composition.to_parquet(fx_fwd_comp_file)
print(f"  [OK] Saved to {fx_fwd_comp_file}")
print("\n  FX Forward Composition:")
print(fx_forward_composition)

# ============================================================================
# Summary
# ============================================================================
print("\n" + "="*80)
print("DOWNLOAD COMPLETE - SUMMARY")
print("="*80)

print(f"\n[X] Data directory: {DATA_DIR.absolute()}")
print(f"\n[X] Files created:")
print(f"  1. prices.parquet         - {prices.shape[0]} rows [X] {prices.shape[1]} instruments")
print(f"  2. fx_prices.parquet      - {fx_prices.shape[0]} rows [X] {fx_prices.shape[1]} currencies")
print(f"  3. dividends.parquet      - {dividends_df.shape[0]} rows [X] {dividends_df.shape[1]} instruments")
print(f"  4. ters.parquet           - {len(ters)} instruments")
print(f"  5. fx_forward_points.parquet - {fx_forward_points.shape[0]} rows")
print(f"  6. fx_composition.parquet - {fx_composition.shape[0]} instruments")
print(f"  7. fx_forward_composition.parquet - {fx_forward_composition.shape[0]} instruments")

print(f"\n[X] Date range: {prices.index[0]} to {prices.index[-1]}")
print(f"[X] Total timestamps: {len(prices)}")
print(f"[TIME]  Frequency: 15 minutes")

# Display sample data
print("\n[X] Sample prices (first 5 rows):")
print(prices.head())

print("\n[X] Dividends summary:")
div_summary = dividends_df[dividends_df > 0].stack()
if len(div_summary) > 0:
    print(div_summary)
else:
    print("  No dividends in this period")

print("\n[X] TER (annual):")
for ticker, ter in ters.items():
    print(f"  {ticker}: {ter*100:.4f}%")

print("\n[X] FX prices sample (first 5 rows):")
print(fx_prices.head())

print("\n" + "="*80)
print("[OK] All data downloaded and saved successfully!")
print("="*80)
