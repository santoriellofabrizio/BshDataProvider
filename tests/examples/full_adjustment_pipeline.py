"""
Example: full adjustment pipeline for two hedged/unhedged ETF pairs.

Demonstrates the manual Adjuster workflow (TER + FX spot + FX forward carry +
dividends) and a regression analysis (alpha, beta, R²) between IUSA and IUSE.

Prerequisites:
  - Valid bshdata_config.yaml pointing to a live Bloomberg / Timescale instance
  - matplotlib installed

Run directly:
    python tests/examples/full_adjustment_pipeline.py
"""

import numpy as np
import pandas as pd
from datetime import time, date
from matplotlib import pyplot as plt

from sfm_data_provider.analytics.adjustments.dividend import DividendComponent
from sfm_data_provider.analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from sfm_data_provider.analytics.adjustments.fx_spot import FxSpotComponent
from sfm_data_provider.analytics.adjustments.ter import TerComponent
from sfm_data_provider.analytics.adjustments.adjuster import Adjuster
from sfm_data_provider.interface.bshdata import BshData


def run():
    # --- 1. SETUP ---
    ticker = ['IUSA', 'IUSE']
    currencies = ['USD']
    start_date = '2025-09-10'
    end_date = '2026-03-01'
    ref_date = date(2026, 3, 2)

    config_path = r'C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml'
    api = BshData(config_path)

    # --- 2. STATIC DATA ---
    ter = api.info.get_ter(ticker=ticker) / 100

    fx_composition = api.info.get_fx_composition(
        ticker=ticker, fx_fxfwrd='fx', reference_date=ref_date
    )
    fx_forward_info = api.info.get_fx_composition(
        ticker=ticker, fx_fxfwrd="fxfwrd", reference_date=ref_date
    )

    # --- 3. MARKET DATA ---
    snap = time(16)

    prices = api.market.get_daily_etf(
        ticker=ticker, start=start_date, end=end_date, snapshot_time=snap
    )
    fx_prices = api.market.get_daily_currency(
        ticker=[f"EUR{c}" for c in currencies], start=start_date, end=end_date, snapshot_time=snap
    ).to_frame(name="EURUSD")
    fx_forward_prices = api.market.get_daily_fx_forward(
        base_currency='EUR',
        quoted_currency=fx_forward_info.columns.tolist(),
        start=start_date,
        end=end_date,
    )
    dividends = api.info.get_dividends(ticker=ticker, start=start_date)

    # --- 4. ADJUSTER ---
    adjuster = (
        Adjuster(prices)
        .add(TerComponent(ter))
        .add(FxSpotComponent(fx_composition, fx_prices))
        .add(FxForwardCarryComponent(fx_forward_info, fx_forward_prices, "1M", fx_prices))
        .add(DividendComponent(dividends, prices, fx_prices=fx_prices))
    )

    clean_returns = adjuster.get_clean_returns().dropna() * 10_000

    for name, val in adjuster.get_breakdown().items():
        val.plot(kind='bar', title=name)
        plt.show()

    # --- 5. REGRESSION IUSA vs IUSE ---
    x = clean_returns['IUSA'].values
    y = clean_returns['IUSE'].values

    cov = np.cov(x, y)
    beta = cov[0, 1] / cov[0, 0]
    alpha_daily = np.mean(y) - beta * np.mean(x)
    alpha_annualized = alpha_daily * 252
    r_squared = np.corrcoef(x, y)[0, 1] ** 2

    # --- 6. OUTPUT ---
    print("\n" + "=" * 40)
    print("RESULTS: IUSA vs IUSE (Adjusted)")
    print("=" * 40)
    print(f"BETA:         {beta:.4f}  (expected: ~1.0)")
    print(f"ALPHA (Ann.): {alpha_annualized:.6f}  (expected: ~0.0)")
    print(f"R-SQUARED:    {r_squared:.4f}  (expected: >0.98)")
    print("=" * 40)

    # --- 7. CHARTS ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    (1 + clean_returns).cumprod().plot(ax=ax1, title='Cumulative Adjusted Performance')
    ax1.set_ylabel('Rebased (1.0)')
    ax1.grid(True, linestyle='--', alpha=0.7)

    ax2.scatter(x, y, alpha=0.5, color='blue', label='Daily returns')
    ax2.plot(x, alpha_daily + beta * x, color='red', lw=2, label=f'Fit: Beta={beta:.3f}')
    ax2.set_title('Regression: IUSA vs IUSE')
    ax2.set_xlabel('IUSA (Clean)')
    ax2.set_ylabel('IUSE (Clean)')
    ax2.legend()
    ax2.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    run()
