from datetime import timezone

import pandas as pd
import matplotlib.pyplot as plt
from analytics.adjustments.adjuster import Adjuster
from analytics.adjustments.fx_forward_carry import FxForwardComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.ter import TerComponent
from interface.bshdata import BshData


def main():
    # ============================================================
    # LOAD DATA
    # ============================================================
    fx = pd.read_parquet("EURUSD.parquet").rename({"EURUSD": "USD"}, axis=1)
    fx.index = fx.index.normalize()

    csspx = pd.read_parquet("CSSPX.parquet")
    iuse = pd.read_parquet("IUSE.parquet")

    fx_forward_prices = pd.read_parquet("fx_forward_prices.parquet")
    fx_forward_prices.index = fx_forward_prices.index.tz_localize('Europe/Rome')

    prices = pd.concat([csspx, iuse], axis=1)
    prices.index = prices.index.normalize()

    fx_forward_prices = fx_forward_prices.iloc[:len(prices.index)]
    fx_forward_prices.index = prices.index
    #     # ============================================================
    # GET FX COMPOSITION
    # ============================================================
    API = BshData(autocomplete=True)
    fx_spot_composition = API.info.get_fx_composition(
        ticker=["CSSPX", "IUSE"],
        fx_fxfwrd="fx"
    )

    fx_fwrd_composition = API.info.get_fx_composition(ticker=["CSSPX", "IUSE"], fx_fxfwrd="fxfwrd")

    # ============================================================
    # CALCULATE ADJUSTMENTS
    # ============================================================
    adjuster = Adjuster(prices=prices, fx_prices=fx)
    adjuster.add(FxSpotComponent(fx_spot_composition))
    adjuster.add(TerComponent(ters={"IUSE": 0.002, "CSSPX": 0.0007}))
    adjuster.add(FxForwardComponent(fxfwd_composition=fx_fwrd_composition, fx_fwd_prices=fx_forward_prices))

    breakdown = adjuster.get_breakdown()
    fx_spot_adj = breakdown['FxSpotComponent']
    ter_adj = breakdown['TerComponent']
    fx_fwd_adj = breakdown['FxForwardComponent']

    # ============================================================
    # CALCULATE RETURNS
    # ============================================================
    raw_returns = prices.pct_change().fillna(0.0)
    fx_returns = fx.pct_change().fillna(0.0)
    clean_returns = adjuster.clean_returns(raw_returns)

    # ============================================================
    # EXPORT TO EXCEL
    # ============================================================
    results = pd.DataFrame({
        'Date': raw_returns.index,
        'FX_Return_%': fx_returns['USD'] * 100,
        'CSSPX_Raw_%': raw_returns['CSSPX'] * 100,
        'CSSPX_FX_Spot_%': fx_spot_adj['CSSPX'] * 100,
        'CSSPX_FX_Fwd_%': fx_fwd_adj['CSSPX'] * 100,
        'CSSPX_TER_%': ter_adj['CSSPX'] * 100,
        'CSSPX_Clean_%': clean_returns['CSSPX'] * 100,
        'IUSE_Raw_%': raw_returns['IUSE'] * 100,
        'IUSE_FX_Spot_%': fx_spot_adj['IUSE'] * 100,
        'IUSE_FX_Fwd_%': fx_fwd_adj['IUSE'] * 100,
        'IUSE_TER_%': ter_adj['IUSE'] * 100,
        'IUSE_Clean_%': clean_returns['IUSE'] * 100,
    })

    results['Diff_%'] = (clean_returns['CSSPX'] - clean_returns['IUSE']) * 100
    results['Error_%'] = results['Diff_%'] - results['FX_Return_%']
    results['Date'] = results['Date'].dt.tz_localize(None)

    results.to_excel('fx_analysis.xlsx', index=False)
    print("✅ Saved: fx_analysis.xlsx")

    # ============================================================
    # CALCULATE CUMULATIVE ADJUSTMENTS
    # ============================================================
    cum_clean = (1 + clean_returns).cumprod() * 100
    cum_fx = (1 + fx_returns['USD']).cumprod() * 100 - 100
    cum_fx_spot = (1 + fx_spot_adj).cumprod() * 100 - 100
    cum_fx_fwd = (1 + fx_fwd_adj).cumprod() * 100 - 100
    cum_ter = (1 + ter_adj).cumprod() * 100 - 100

    # ============================================================
    # PLOT - FIGURE 1: Overview
    # ============================================================
    fig1 = plt.figure(figsize=(16, 10))

    # Main plot: Clean Returns
    ax1 = plt.subplot(2, 3, (1, 2))
    ax1.plot(cum_clean.index, cum_clean['CSSPX'], label='CSSPX', linewidth=2)
    ax1.plot(cum_clean.index, cum_clean['IUSE'], label='IUSE', linewidth=2)
    ax1.axhline(100, color='gray', linestyle='--', alpha=0.5)
    ax1.set_ylabel('Rebased Price', fontsize=10)
    ax1.set_title('Clean Returns (Adjusted)', fontsize=12, fontweight='bold')
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)

    # FX Spot Adjustment
    ax2 = plt.subplot(2, 3, 4)
    ax2.plot(cum_fx_spot.index, cum_fx_spot['CSSPX'], label='CSSPX', linewidth=1.5)
    ax2.plot(cum_fx_spot.index, cum_fx_spot['IUSE'], label='IUSE', linewidth=1.5)
    ax2.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax2.set_ylabel('Impact (%)', fontsize=9)
    ax2.set_xlabel('Date', fontsize=9)
    ax2.set_title('FX Spot Correction', fontsize=10, fontweight='bold')
    ax2.legend(fontsize=7)
    ax2.grid(True, alpha=0.3)

    # FX Forward Adjustment
    ax3 = plt.subplot(2, 3, 5)
    ax3.plot(cum_fx_fwd.index, cum_fx_fwd['CSSPX'], label='CSSPX', linewidth=1.5)
    ax3.plot(cum_fx_fwd.index, cum_fx_fwd['IUSE'], label='IUSE', linewidth=1.5)
    ax3.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax3.set_ylabel('Impact (%)', fontsize=9)
    ax3.set_xlabel('Date', fontsize=9)
    ax3.set_title('FX Forward Carry', fontsize=10, fontweight='bold')
    ax3.legend(fontsize=7)
    ax3.grid(True, alpha=0.3)

    # TER Impact
    ax4 = plt.subplot(2, 3, 6)
    ax4.plot(cum_ter.index, cum_ter['CSSPX'], label='CSSPX', linewidth=1.5)
    ax4.plot(cum_ter.index, cum_ter['IUSE'], label='IUSE', linewidth=1.5)
    ax4.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax4.set_ylabel('Impact (%)', fontsize=9)
    ax4.set_xlabel('Date', fontsize=9)
    ax4.set_title('TER', fontsize=10, fontweight='bold')
    ax4.legend(fontsize=7)
    ax4.grid(True, alpha=0.3)

    # Summary box
    ax5 = plt.subplot(2, 3, 3)
    ax5.axis('off')

    summary_text = "Final Impact Summary\n" + "=" * 35 + "\n\n"
    for ticker in ['CSSPX', 'IUSE']:
        summary_text += f"{ticker}:\n"
        summary_text += f"  Clean Return:  {cum_clean[ticker].iloc[-1] - 100:+.2f}%\n"
        summary_text += f"  ├─ FX Spot:    {cum_fx_spot[ticker].iloc[-1]:+.2f}%\n"
        summary_text += f"  ├─ FX Forward: {cum_fx_fwd[ticker].iloc[-1]:+.2f}%\n"
        summary_text += f"  └─ TER:        {cum_ter[ticker].iloc[-1]:+.2f}%\n\n"

    hedging_cost = cum_clean['CSSPX'].iloc[-1] - cum_clean['IUSE'].iloc[-1]
    summary_text += f"Hedging Cost:\n"
    summary_text += f"  Total:         {hedging_cost:.2f}%\n"
    summary_text += f"  vs FX Return:  {cum_fx.iloc[-1]:.2f}%\n"

    ax5.text(0.05, 0.5, summary_text, fontsize=9, family='monospace',
             verticalalignment='center',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

    plt.tight_layout()
    plt.savefig('fx_analysis_overview.png', dpi=150, bbox_inches='tight')
    print("✅ Saved: fx_analysis_overview.png")

    # ============================================================
    # PLOT - FIGURE 2: Detailed Breakdown
    # ============================================================
    fig2, axes = plt.subplots(2, 3, figsize=(16, 10))

    # CSSPX - All corrections stacked
    ax = axes[0, 0]
    ax.plot(cum_clean.index, cum_clean['CSSPX'] - 100, label='Total Clean', linewidth=2, color='black')
    ax.plot(cum_fx_spot.index, cum_fx_spot['CSSPX'], label='FX Spot', linewidth=1.5, alpha=0.7)
    ax.plot(cum_fx_fwd.index, cum_fx_fwd['CSSPX'], label='FX Forward', linewidth=1.5, alpha=0.7)
    ax.plot(cum_ter.index, cum_ter['CSSPX'], label='TER', linewidth=1.5, alpha=0.7)
    ax.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax.set_ylabel('Impact (%)', fontsize=9)
    ax.set_title('CSSPX - All Corrections', fontsize=10, fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # IUSE - All corrections stacked
    ax = axes[0, 1]
    ax.plot(cum_clean.index, cum_clean['IUSE'] - 100, label='Total Clean', linewidth=2, color='black')
    ax.plot(cum_fx_spot.index, cum_fx_spot['IUSE'], label='FX Spot', linewidth=1.5, alpha=0.7)
    ax.plot(cum_fx_fwd.index, cum_fx_fwd['IUSE'], label='FX Forward', linewidth=1.5, alpha=0.7)
    ax.plot(cum_ter.index, cum_ter['IUSE'], label='TER', linewidth=1.5, alpha=0.7)
    ax.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax.set_ylabel('Impact (%)', fontsize=9)
    ax.set_title('IUSE - All Corrections', fontsize=10, fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # FX Return Reference
    ax = axes[0, 2]
    ax.plot(cum_fx.index, cum_fx, label='EUR/USD', color='purple', linewidth=2)
    ax.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax.set_ylabel('Return (%)', fontsize=9)
    ax.set_title('FX Return (Reference)', fontsize=10, fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # Daily FX Spot corrections
    ax = axes[1, 0]
    ax.plot(fx_spot_adj.index, fx_spot_adj['CSSPX'] * 100, label='CSSPX', linewidth=1, alpha=0.7)
    ax.plot(fx_spot_adj.index, fx_spot_adj['IUSE'] * 100, label='IUSE', linewidth=1, alpha=0.7)
    ax.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax.set_ylabel('Daily Impact (%)', fontsize=9)
    ax.set_xlabel('Date', fontsize=9)
    ax.set_title('Daily FX Spot Corrections', fontsize=10, fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # Daily FX Forward corrections
    ax = axes[1, 1]
    ax.plot(fx_fwd_adj.index, fx_fwd_adj['CSSPX'] * 100, label='CSSPX', linewidth=1, alpha=0.7)
    ax.plot(fx_fwd_adj.index, fx_fwd_adj['IUSE'] * 100, label='IUSE', linewidth=1, alpha=0.7)
    ax.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax.set_ylabel('Daily Impact (%)', fontsize=9)
    ax.set_xlabel('Date', fontsize=9)
    ax.set_title('Daily FX Forward Carry', fontsize=10, fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # Hedging Cost vs FX
    ax = axes[1, 2]
    diff = cum_clean['CSSPX'] - cum_clean['IUSE'] - 100
    ax.plot(diff.index, diff, label='CSSPX - IUSE (Hedging Cost)', color='red', linewidth=2)
    ax.plot(cum_fx.index, cum_fx, label='FX Return', color='purple', linewidth=1.5, alpha=0.7)
    ax.fill_between(diff.index, diff, cum_fx, alpha=0.2, color='orange', label='Unexplained')
    ax.axhline(0, color='gray', linestyle='--', alpha=0.5)
    ax.set_ylabel('Return Difference (%)', fontsize=9)
    ax.set_xlabel('Date', fontsize=9)
    ax.set_title('Hedging Cost Analysis', fontsize=10, fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('fx_analysis_detailed.png', dpi=150, bbox_inches='tight')
    print("✅ Saved: fx_analysis_detailed.png")

    plt.show()

    # ============================================================
    # SUMMARY
    # ============================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"FX Return:          {cum_fx.iloc[-1]:+.2f}%")
    print(f"\nCSSPX:")
    print(f"  Clean Return:     {cum_clean['CSSPX'].iloc[-1] - 100:+.2f}%")
    print(f"  FX Spot:          {cum_fx_spot['CSSPX'].iloc[-1]:+.2f}%")
    print(f"  FX Forward:       {cum_fx_fwd['CSSPX'].iloc[-1]:+.2f}%")
    print(f"  TER:              {cum_ter['CSSPX'].iloc[-1]:+.2f}%")
    print(f"\nIUSE:")
    print(f"  Clean Return:     {cum_clean['IUSE'].iloc[-1] - 100:+.2f}%")
    print(f"  FX Spot:          {cum_fx_spot['IUSE'].iloc[-1]:+.2f}%")
    print(f"  FX Forward:       {cum_fx_fwd['IUSE'].iloc[-1]:+.2f}%")
    print(f"  TER:              {cum_ter['IUSE'].iloc[-1]:+.2f}%")
    print(f"\nHedging Cost:       {hedging_cost:+.2f}%")
    print(f"Unexplained:        {hedging_cost - cum_fx.iloc[-1]:+.2f}%")


if __name__ == "__main__":
    main()