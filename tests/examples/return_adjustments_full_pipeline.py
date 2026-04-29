"""Full adjustments pipeline: pairwise returns, correlation, alpha estimation."""

from datetime import time
from itertools import combinations
import numpy as np
import pandas as pd
import pytest
import logging
from matplotlib import pyplot as plt

logging.getLogger('matplotlib').setLevel(logging.WARNING)

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

from sfm_data_provider.analytics.adjustments import (
    Adjuster, DividendComponent, FxForwardCarryComponent,
    FxSpotComponent, RepoComponent, TerComponent, YtmComponent,
)
from sfm_data_provider.analytics.pipeline import DataPipeline
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.interface.bshdata import BshData

# --- Config ---
IDS = ["IITB", "FBTP"]
START, END = "2026-01-01", "2026-03-01"
SNAPSHOT_TIME = time(15)
FREQUENCY = "1D"
ALPHA_SIGNIFICANCE = 0.05

# --- Fixtures ---
@pytest.fixture(scope="module")
def api():
    return BshData(cache=False, log_level="DEBUG")

@pytest.fixture
def sample_isins():
    with open(r"tests\sample_isin.txt") as f:
        return [l.strip() for l in f]

# --- Pipeline ---
def build_pipeline(api: BshData) -> DataPipeline:
    return DataPipeline(api, IDS, START, END, FREQUENCY, SNAPSHOT_TIME)

def build_adjuster(data: DataPipeline) -> Adjuster:
    instruments = data.get_instruments()
    adj = (
        Adjuster(data.prices, instruments)
        .add(TerComponent(data.ter))
        .add(FxSpotComponent(data.fx_composition, data.fx_prices))
        .add(DividendComponent(data.dividends, data.prices, data.fx_prices))
    )
    if (ytm := data.ytm) is not None:
        adj = adj.add(YtmComponent(ytm))
    if futures := [i for i in instruments.values() if i.type == InstrumentType.FUTURE]:
        adj = adj.add(RepoComponent(data.repo, "currency", {f.id: f.currency.value for f in futures}))
    return adj

# --- Analytics ---
def compute_cumulative_returns(r: pd.DataFrame) -> pd.DataFrame:
    return (1 + r).cumprod() - 1

def compute_correlation(r: pd.DataFrame) -> pd.DataFrame:
    return r.corr()

def compute_alpha(rx: pd.Series, ry: pd.Series, lx: str, ly: str) -> dict:
    pair = f"{ly} ~ {lx}"
    base = dict(pair=pair, alpha_daily=np.nan, alpha_annualised=np.nan,
                beta=np.nan, r_squared=np.nan, p_value_alpha=np.nan, significant=False)
    if not SCIPY_AVAILABLE:
        return {**base, "error": "scipy not installed"}
    try:
        df = pd.concat([rx, ry], axis=1).dropna()
        if len(df) < 3:
            raise ValueError(f"Insufficient observations: {len(df)}")
        slope, intercept, r_value, p_value, _ = stats.linregress(df.iloc[:, 0], df.iloc[:, 1])
        return dict(pair=pair, alpha_daily=round(intercept, 6),
                    alpha_annualised=round(intercept * 252, 4), beta=round(slope, 4),
                    r_squared=round(r_value**2, 4), p_value_alpha=round(p_value, 4),
                    significant=p_value < ALPHA_SIGNIFICANCE, error=None)
    except Exception as e:
        return {**base, "error": str(e)}

def pairwise_alpha(r: pd.DataFrame) -> pd.DataFrame:
    cols = r.columns.tolist()
    results = [
        compute_alpha(r[a], r[b], a, b)
        for a, b in combinations(cols, 2)
        for a, b in [(a, b), (b, a)]
    ]
    return pd.DataFrame(results).set_index("pair")

# --- Plots ---
def plot_breakdown(debug: dict):
    for name, val in debug.items():
        val.plot(kind="bar", title=name); plt.tight_layout(); plt.show()

def plot_clean_returns(r: pd.DataFrame):
    r.plot(kind="bar", title="Clean Simple Returns"); plt.tight_layout(); plt.show()

def plot_cumulative_returns(cum: pd.DataFrame):
    cum.plot(title="Cumulative Returns")
    plt.ylabel("Cumulative Return"); plt.xlabel("Date"); plt.tight_layout(); plt.show()

def plot_correlation_heatmap(corr: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(6, 5))
    cax = ax.matshow(corr, cmap="RdYlGn", vmin=-1, vmax=1)
    fig.colorbar(cax)
    ax.set_xticks(range(len(corr.columns))); ax.set_yticks(range(len(corr.index)))
    ax.set_xticklabels(corr.columns, rotation=45, ha="left"); ax.set_yticklabels(corr.index)
    ax.set_title("Return Correlation Matrix", pad=20)
    for i in range(len(corr.index)):
        for j in range(len(corr.columns)):
            ax.text(j, i, f"{corr.iloc[i,j]:.2f}", ha="center", va="center", fontsize=10)
    plt.tight_layout(); plt.show()

def plot_return_comparison(r: pd.DataFrame):
    avg, std = r.mean(), r.std()
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(avg))
    bars = ax.bar(x, avg * 100, yerr=std * 100, capsize=5, color=["steelblue", "coral"])
    ax.set_xticks(x); ax.set_xticklabels(avg.index)
    ax.set_ylabel("Average Daily Return (%)"); ax.set_title("Return Comparison (mean ± 1σ)")
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    for bar, v in zip(bars, avg):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.001,
                f"{v*100:.3f}%", ha="center", va="bottom", fontsize=9)
    plt.tight_layout(); plt.show()

def print_alpha_report(alpha_df: pd.DataFrame):
    if not SCIPY_AVAILABLE:
        print("\n⚠  Alpha report non disponibile: scipy non installato."); return
    cols = ["alpha_daily", "alpha_annualised", "beta", "r_squared", "p_value_alpha", "significant"]
    print("\n" + "=" * 60 + "\nPAIRWISE ALPHA REPORT\n" + "=" * 60)
    print(alpha_df[cols].to_string())
    if (failed := alpha_df[alpha_df.get("error", pd.Series()).notna()]).shape[0]:
        print("\n✗  Coppie con errori:")
        for pair, row in failed.iterrows(): print(f"  {pair}: {row['error']}")
    if (sig := alpha_df[alpha_df["significant"]]).empty:
        print("\n✓  No statistically significant alpha found.")
    else:
        print(f"\n⚠  Significant alphas (p < {ALPHA_SIGNIFICANCE:.0%}):")
        for pair, row in sig.iterrows():
            print(f"  {pair}: α={row['alpha_annualised']:.4f} p.a. "
                  f"({'pos' if row['alpha_annualised']>0 else 'neg'}), β={row['beta']:.4f}, R²={row['r_squared']:.4f}")
    print("=" * 60)

# --- Main ---
def full_adjustments_pipeline(api: BshData):
    data = build_pipeline(api)
    adj = build_adjuster(data)
    plot_breakdown(adj.get_breakdown())
    clean_returns = adj.get_clean_returns()
    plot_clean_returns(clean_returns)
    plot_cumulative_returns(compute_cumulative_returns(clean_returns))
    plot_return_comparison(clean_returns)
    corr = compute_correlation(clean_returns)
    plot_correlation_heatmap(corr)
    print("\nCorrelation Matrix:\n", corr.round(4))
    print_alpha_report(pairwise_alpha(clean_returns))

if __name__ == "__main__":
    api = BshData(
        config_path=r"C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml",
        cache=False, log_level="DEBUG",
    )
    full_adjustments_pipeline(api)