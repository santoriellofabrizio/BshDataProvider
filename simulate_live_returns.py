"""
Live Simulation per Clean Returns.
Replaya dati storici FX ed ETF per simulare un ambiente live.
"""
import cProfile
import io
import pstats
import sys
import random
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib


matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent / "src"))

from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.ter import TerComponent
from core.enums.instrument_types import InstrumentType


# =============================================================================
# Config
# =============================================================================

DATA_DIR = Path(r"C:\Users\GBS08935\Desktop\dataEquity")
EXCLUDED_DATES = ["2025-12-19"]
SUBSET_TICKERS = ["IUSA", "IUSE"]
INTERVAL = 0.5


# =============================================================================
# Mock Instrument
# =============================================================================

class EtfInstrument:
    def __init__(self, isin: str):
        self.id = isin
        self.isin = isin
        self.type = InstrumentType.ETP
        self.currency = "EUR"
        self.underlying_type = "EQUITY"
        self.payment_policy = "DIST"
        self.fund_currency = "EUR"
        self.currency_hedged = False


# =============================================================================
# Data Loading
# =============================================================================

def load_data() -> dict:
    """Carica tutti i dati."""
    print("Caricamento dati...")

    data = {
        "etf_prices": pd.read_parquet(DATA_DIR / "ETF_prices.parquet"),
        "fx_prices": pd.read_parquet(DATA_DIR / "FX_prices.parquet"),
        "fx_composition": pd.read_parquet(DATA_DIR / "FX_composition.parquet"),
        "fx_forward_composition": pd.read_parquet(DATA_DIR / "FX_forward.parquet"),
        "fx_forward_prices": pd.read_parquet(DATA_DIR / "FX_forward_prices.parquet"),
        "ter": pd.read_csv("ter.csv", index_col=0).iloc[:, 0] / 100,
        "dividends": pd.read_csv("dividends.csv", index_col="Date", parse_dates=True),
        "tickers": pd.read_csv("tickers.csv", index_col=0)["TICKER"].to_dict(),
    }

    for date_str in EXCLUDED_DATES:
        if date_str in data["etf_prices"].index:
            data["etf_prices"].drop(date_str, inplace=True)
        if date_str in data["fx_prices"].index:
            data["fx_prices"].drop(date_str, inplace=True)

    data["etf_prices"] = data["etf_prices"].interpolate(method="time")
    data["fx_prices"] = data["fx_prices"].interpolate(method="time").dropna(axis=1)

    print(f"  ETF prices: {data['etf_prices'].shape}")
    print(f"  FX prices: {data['fx_prices'].shape}")

    return data


# =============================================================================
# Main
# =============================================================================

def main(append_mode=False):
    # Load
    data = load_data()
    tickers = data["tickers"]
    ticker_to_isin = {v: k for k, v in tickers.items()}
    subset_isins = [ticker_to_isin[t] for t in SUBSET_TICKERS]

    # Filter to subset
    etf_prices = data["etf_prices"][subset_isins]
    fx_prices = data["fx_prices"]
    instruments = {isin: EtfInstrument(isin) for isin in subset_isins}

    # Create adjuster
    print("Creating adjuster...")
    adjuster = (
        Adjuster(etf_prices, instruments=instruments, is_intraday=True, return_type='logarithmic')
        .add(TerComponent(data["ter"]))
        .add(FxSpotComponent(data["fx_composition"], fx_prices))
        .add(FxForwardCarryComponent(
            data["fx_forward_composition"],
            data["fx_forward_prices"],
            "1M",
            fx_prices,
        ))
        .add(DividendComponent(data["dividends"],etf_prices, fx_prices))
    )

    # Simulator state
    etf_returns = etf_prices.pct_change().dropna()
    fx_returns = fx_prices.pct_change().dropna()
    current_etf = etf_prices.iloc[-1].copy()
    current_fx = fx_prices.iloc[-1].copy()
    n_returns = len(etf_returns)

    # Setup plot
    plt.ion()
    fig, ax = plt.subplots(figsize=(14, 6))
    fig.show()

    # Run simulation
    print(f"Running simulation... Press Ctrl+C to stop.")
    i = 0

    try:
        while True:

            # Update prices (random index from history)
            idx = random.randint(0, n_returns - 1)
            # Calculate clean returns (incrementale: solo ultima riga ricalcolata)
            if etf_returns.index[idx].date() == date(2025, 12, 11):
                pass

            profiler = cProfile.Profile()
            profiler.enable()

            start_time = time.perf_counter()
            
            if append_mode:
                cumulative = adjuster.append_update(
                    prices=current_etf * (1 + etf_returns.iloc[idx]),
                    fx_prices=current_fx * (1 + fx_returns.iloc[idx]),
                ).get_clean_returns()
                calc_time = (time.perf_counter() - start_time) * 1000
            else:
                with adjuster.live_update(
                    prices=current_etf * (1 + etf_returns.iloc[idx]),
                    fx_prices=current_fx * (1 + fx_returns.iloc[idx]),
                ):
                    cumulative = adjuster.get_clean_returns()
                    calc_time = (time.perf_counter() - start_time) * 1000

            print(f"\nTotal time: {calc_time:.2f}ms")
            time.sleep(1)
            # Get all rows for plot
            cumulative = cumulative.rename(columns=tickers)
            cumulative_bps = cumulative[SUBSET_TICKERS] * 10000

            # Print last row
            last_ret = cumulative_bps.iloc[-1]
            ret_str = " | ".join(f"{t}: {last_ret[t]:+.1f}" for t in SUBSET_TICKERS)
            print(f"[{i+1:03d}] idx={idx} | {ret_str} | {calc_time:.1f}ms")

            # Update plot - grouped bar chart with all returns
            ax.clear()

            x = np.arange(len(cumulative_bps))
            width = 0.8 / len(SUBSET_TICKERS)

            for j, ticker in enumerate(SUBSET_TICKERS):
                offset = (j - len(SUBSET_TICKERS) / 2 + 0.5) * width
                ax.bar(x + offset, cumulative_bps[ticker].values, width, label=ticker)

            ax.set_title(f"Iteration {i+1} - Cumulative Returns (bps) - {calc_time:.1f}ms")
            ax.set_xlabel("Date Index")
            ax.set_ylabel("bps")
            ax.axhline(0, color='black', linewidth=0.5)
            ax.grid(True, alpha=0.3, axis='y')
            ax.legend()

            fig.canvas.draw()
            fig.canvas.flush_events()
            plt.pause(INTERVAL)

            i += 1

    except KeyboardInterrupt:
        print(f"\nStopped after {i} iterations.")

    plt.ioff()
    plt.show()


if __name__ == "__main__":
    main()
