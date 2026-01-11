"""
Test script showing return difference between two tickers in basis points.
Compares raw vs clean return spreads across various time horizons.
"""
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

from analytics.adjustments import Adjuster
from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.ter import TerComponent
from interface.bshdata import BshData


class DataLoader:
    """Loads all data from parquet files."""

    def __init__(self):
        self.api = BshData(r"C:\AFMachineLearning\Libraries\BshDataProvider\config\bshdata_config.yaml")

        self.prices = pd.read_parquet("data/etf_prices.parquet")
        self.fx = pd.read_parquet("data/fx_prices.parquet")
        self.fx_forward_prices = pd.read_parquet("data/fx_forward_prices.parquet")
        self.dividends = pd.read_parquet("data/dividends.parquet")

        self.fx_composition = pd.read_parquet("data/fx_composition.parquet")
        self.fx_forward_composition = pd.read_parquet("data/fx_forward.parquet")
        self.ter = pd.read_parquet("data/ter.parquet")

        tickers = self.api.info.get_etp_fields(
            fields=["TICKER"],
            isin=self.prices.columns.tolist()
        )
        self.tickers_map = tickers["TICKER"].to_dict()


def setup_adjuster(tickers: list[str]) -> tuple[Adjuster, dict, pd.DataFrame]:
    """Setup adjuster with all data."""

    data = DataLoader()

    ticker_to_isin = {v: k for k, v in data.tickers_map.items()}
    isins = [ticker_to_isin[t] for t in tickers if t in ticker_to_isin]
    tickers_map = {isin: data.tickers_map[isin] for isin in isins}

    prices = data.prices[isins]
    fx = data.fx
    fx_fwd = data.fx_forward_prices
    divs = data.dividends[[c for c in isins if c in data.dividends.columns]] if hasattr(data.dividends, 'columns') else data.dividends

    ter = data.ter.loc[[i for i in isins if i in data.ter.index]]
    fx_comp = data.fx_composition.loc[[i for i in isins if i in data.fx_composition.index]]
    fx_fwd_comp = data.fx_forward_composition.loc[[i for i in isins if i in data.fx_forward_composition.index]]

    adjuster = (
        Adjuster(prices=prices, is_intraday=False)
        .add(TerComponent(ter))
        .add(DividendComponent(divs, fx_prices=fx))
        .add(FxForwardCarryComponent(fx_fwd_comp, fx_fwd, "1M", fx))
        .add(FxSpotComponent(fx_comp, fx_prices=fx))
    )

    return adjuster, tickers_map, prices


def calculate_return_spread_bp(prices: pd.DataFrame, ticker1: str, ticker2: str, periods: dict[str, int]) -> pd.DataFrame:
    """
    Calculate return spread (ticker1 - ticker2) in basis points for various periods.

    Args:
        prices: DataFrame with prices
        ticker1, ticker2: Ticker names
        periods: Dict of period_name -> days

    Returns:
        Series with spread in BP for each period
    """
    results = {}

    for period_name, days in periods.items():
        if len(prices) >= days:
            ret1 = (prices[ticker1].iloc[-1] / prices[ticker1].iloc[-days] - 1)
            ret2 = (prices[ticker2].iloc[-1] / prices[ticker2].iloc[-days] - 1)
            spread_bp = (ret1 - ret2) * 10000  # Convert to basis points
            results[period_name] = spread_bp

    return pd.Series(results)


def run_spread_test(ticker1: str, ticker2: str):
    """
    Show return spread between two tickers in basis points.
    """
    print("=" * 60)
    print(f"Return Spread: {ticker1} vs {ticker2}")
    print("=" * 60)

    adjuster, tickers_map, raw_prices = setup_adjuster([ticker1, ticker2])

    # Get clean prices
    clean_prices = adjuster.clean_prices(rebase=False)

    # Rename to tickers
    raw_prices = raw_prices.rename(columns=tickers_map)
    clean_prices = clean_prices.rename(columns=tickers_map)

    print(f"Date range: {raw_prices.index[0].date()} to {raw_prices.index[-1].date()}")
    print(f"Total days: {len(raw_prices)}")

    # Define periods
    periods = {
        '1D': 1,
        '2D': 2,
        '3D': 3,
        '1W': 5,
        '2W': 10,
        'Full': len(raw_prices)
    }

    # Calculate spreads
    raw_spread = calculate_return_spread_bp(raw_prices, ticker1, ticker2, periods)
    clean_spread = calculate_return_spread_bp(clean_prices, ticker1, ticker2, periods)
    adjustment_impact = clean_spread - raw_spread

    # Print table
    print(f"\nReturn Spread ({ticker1} - {ticker2}) in Basis Points:")
    print("-" * 55)
    print(f"{'Period':<10} {'Raw (BP)':>12} {'Clean (BP)':>12} {'Adj Impact':>12}")
    print("-" * 55)

    for period in periods.keys():
        raw_bp = raw_spread[period]
        clean_bp = clean_spread[period]
        impact = adjustment_impact[period]
        print(f"{period:<10} {raw_bp:>12.2f} {clean_bp:>12.2f} {impact:>12.2f}")

    print("-" * 55)

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Bar chart comparison
    x = np.arange(len(periods))
    width = 0.35

    axes[0].bar(x - width/2, raw_spread.values, width, label='Raw', color='steelblue', alpha=0.8)
    axes[0].bar(x + width/2, clean_spread.values, width, label='Clean', color='darkorange', alpha=0.8)

    axes[0].set_xlabel('Horizon')
    axes[0].set_ylabel('Spread (BP)')
    axes[0].set_title(f'Return Spread: {ticker1} - {ticker2}')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(periods.keys())
    axes[0].legend()
    axes[0].grid(True, alpha=0.3, axis='y')
    axes[0].axhline(y=0, color='black', linestyle='-', linewidth=0.5)

    # Add values on bars
    for i, (raw, clean) in enumerate(zip(raw_spread.values, clean_spread.values)):
        axes[0].text(i - width/2, raw + 2, f'{raw:.0f}', ha='center', va='bottom', fontsize=8)
        axes[0].text(i + width/2, clean + 2, f'{clean:.0f}', ha='center', va='bottom', fontsize=8)

    # Adjustment impact
    colors = ['green' if v >= 0 else 'red' for v in adjustment_impact.values]
    axes[1].bar(x, adjustment_impact.values, width=0.6, color=colors, alpha=0.8)

    axes[1].set_xlabel('Horizon')
    axes[1].set_ylabel('Impact (BP)')
    axes[1].set_title(f'Adjustment Impact on Spread ({ticker1} - {ticker2})')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(periods.keys())
    axes[1].grid(True, alpha=0.3, axis='y')
    axes[1].axhline(y=0, color='black', linestyle='-', linewidth=0.5)

    # Add values on bars
    for i, impact in enumerate(adjustment_impact.values):
        va = 'bottom' if impact >= 0 else 'top'
        offset = 1 if impact >= 0 else -1
        axes[1].text(i, impact + offset, f'{impact:.1f}', ha='center', va=va, fontsize=9)

    plt.tight_layout()
    plt.savefig("return_spread_bp.png", dpi=100)
    print(f"\nSaved to return_spread_bp.png")
    plt.show()

    # Breakdown by component
    print(f"\n{'='*60}")
    print("Spread Impact by Component")
    print("=" * 60)

    breakdown = adjuster.get_breakdown()

    print(f"\nCumulative Adjustment Impact on Spread (Full Period):")
    print("-" * 45)
    print(f"{'Component':<30} {'Impact (BP)':>12}")
    print("-" * 45)

    total_impact = 0
    isin1 = [k for k, v in tickers_map.items() if v == ticker1][0]
    isin2 = [k for k, v in tickers_map.items() if v == ticker2][0]

    for comp_name, adj_df in breakdown.items():
        # Cumulative adjustment difference
        cum_adj1 = adj_df[isin1].sum()
        cum_adj2 = adj_df[isin2].sum()
        impact_bp = (cum_adj1 - cum_adj2) * 10000
        total_impact += impact_bp
        print(f"{comp_name:<30} {impact_bp:>12.2f}")

    print("-" * 45)
    print(f"{'TOTAL':<30} {total_impact:>12.2f}")

    return raw_spread, clean_spread, adjustment_impact


if __name__ == "__main__":
    raw_spread, clean_spread, adjustment_impact = run_spread_test("HWDE", "HMWA")