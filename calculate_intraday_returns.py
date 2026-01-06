"""Intraday live test with returns histogram - Ctrl+C to stop"""
import matplotlib
matplotlib.use('TkAgg')  # Force interactive backend
import asyncio, sys, warnings, pandas as pd, numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.utils import today
from xbbg import blp
from analytics.adjustments import Adjuster
from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.ter import TerComponent
from core.enums.currencies import CurrencyEnum
from core.holidays.holiday_manager import HolidayManager
from interface.bshdata import BshData

warnings.filterwarnings("ignore")


async def main(tickers):
    api = BshData()
    yesterday = HolidayManager().add_business_days(today(), -1).date()
    currencies = [c for c in CurrencyEnum.__members__ if c != "EUR"]

    # Load data
    etf = api.market.get_intraday_etf(date=yesterday, ticker=tickers, frequency="15m", start_time="10:00")
    fx = api.market.get_intraday_fx(date=yesterday, id=[f"EUR{c}" for c in currencies], frequency="15m", start_time="10:00")

    # Setup adjuster
    adj = (Adjuster(prices=etf, intraday=True)
           .add(TerComponent(api.info.get_ter(ticker=tickers)))
           .add(DividendComponent(api.info.get_dividends(start=yesterday, ticker=tickers), fx_prices=fx))
           .add(FxSpotComponent(api.info.get_fx_composition(ticker=tickers, fx_fxfwrd="fx"), fx_prices=fx)))

    col1, col2 = etf.columns[0], etf.columns[1]
    bbg_map = {f"{t} IM Equity": c for t, c in zip(tickers, [col1, col2])}
    bbg_map.update({f"EUR{c} Curncy": f"EUR{c}" for c in currencies})

    live_etf, live_fx, n = pd.Series(dtype=float), pd.Series(dtype=float), 0
    ref_prices = etf.copy()
    n_periods = len(ref_prices)

    # Setup plot ONCE
    plt.ion()
    fig, ax = plt.subplots(figsize=(14, 6))

    x = np.arange(n_periods)
    width = 0.35
    bars1 = ax.bar(x - width/2, np.zeros(n_periods), width, label=col1, color='steelblue')
    bars2 = ax.bar(x + width/2, np.zeros(n_periods), width, label=col2, color='darkorange')

    ax.set_xlabel("From Time")
    ax.set_ylabel("Cumulative Return (BP)")
    ax.set_xticks(x)
    ax.set_xticklabels([t.strftime("%H:%M") for t in ref_prices.index], rotation=45, ha='right')
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.legend()
    title = ax.set_title("Waiting for data...")

    plt.tight_layout()
    plt.show(block=False)
    fig.canvas.draw()
    fig.canvas.flush_events()

    print(f"Live: {tickers} | Data: {etf.shape} | Ctrl+C to stop\n")

    try:
        async for u in blp.live(list(bbg_map.keys()), flds=["MID"]):
            if (p := u.get("MID")) is None: continue
            if (col := bbg_map.get(u.get("TICKER", ""))) is None: continue

            (live_etf if "Equity" in u.get("TICKER", "") else live_fx).__setitem__(col, p)
            n += 1

            if col1 not in live_etf or col2 not in live_etf: continue

            # Calculate returns from each historical timestamp to NOW (live price)
            rets = adj.clean_returns(live_prices=live_etf, fx_prices=live_fx, cumulative=True)*1e4
            adj.get_breakdown()

            # Extract and filter outliers (> 3 std)
            rets1 = rets[col1]
            rets1 = rets1[abs(rets1 - rets1.mean()) <= 3*rets1.std()]
            rets2 = rets[col2]
            rets2 = rets2[abs(rets2 - rets2.mean()) <= 3*rets2.std()]

            # Update bar heights using numpy arrays
            for i, (bar, h) in enumerate(zip(bars1, rets1.reindex(ref_prices.index).fillna(0).values)):
                bar.set_height(h)
            for i, (bar, h) in enumerate(zip(bars2, rets2.reindex(ref_prices.index).fillna(0).values)):
                bar.set_height(h)

            # Adjust y limits
            all_vals = np.concatenate([rets1.values, rets2.values])
            margin = max(abs(all_vals.max()), abs(all_vals.min())) * 0.1 + 1
            ax.set_ylim(all_vals.min() - margin, all_vals.max() + margin)

            # Update title
            title.set_text(f"Returns from t to NOW | {datetime.now():%H:%M:%S} | #{n}")

            # Force redraw
            fig.canvas.draw()
            fig.canvas.flush_events()
            plt.pause(0.001)  # Force GUI update

            # Console
            spread = (rets1.iloc[0] - rets2.iloc[0]) if len(rets1) > 0 and len(rets2) > 0 else 0
            sys.stdout.write(f"\r{datetime.now():%H:%M:%S} | {col1}: {live_etf[col1]:.4f} | {col2}: {live_etf[col2]:.4f} | Spread: {spread:+.2f} bp | #{n}")
            sys.stdout.flush()

    except KeyboardInterrupt:
        print(f"\nStopped. Updates: {n}")
        plt.savefig("returns_histogram.png", dpi=100)
        print("Saved to returns_histogram.png")
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    tickers = ["IUSA","IUSE"]
    asyncio.run(main(tickers))
