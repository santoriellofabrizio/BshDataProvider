"""Correlation, mean return difference, and cumulative returns between instruments."""

from datetime import time
import pandas as pd
from matplotlib import pyplot as plt
import logging

logging.getLogger("matplotlib").setLevel(logging.WARNING)
from sfm_data_provider.analytics.adjustments import (
    Adjuster, DividendComponent, FxForwardCarryComponent,
    FxSpotComponent, RepoComponent, TerComponent, YtmComponent,
)
from sfm_data_provider.analytics.pipeline.etf_data_loading import DataPipeline
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.interface.bshdata import BshData

# --- Config ---
IDS = ["INFU", "INFH"]
START, END = "2026-03-12", "2026-04-27"
SNAPSHOT_TIME = time(16)
FREQUENCY = "1D"


def build_pipeline(api: BshData) -> DataPipeline:
    return DataPipeline(api, IDS, START, END, FREQUENCY, SNAPSHOT_TIME)


def build_adjuster(data: DataPipeline) -> Adjuster:

    instruments = data.get_instruments()

    data.override_ytm(data.ytm.copy().assign(INFH=data.ytm["INFU"]))
    data.override_fx_forward_composition(pd.DataFrame({"INFH": {"USD": -1}, "INFU": {"USD": 0}}).T)

    adj = (
        Adjuster(data.prices, instruments)
        .add(TerComponent(data.ter))
        .add(FxSpotComponent(data.fx_composition, data.fx_prices))
        .add(FxForwardCarryComponent(data.fx_forward_composition, data.fx_forward_prices, "1M", data.fx_prices))
        .add(DividendComponent(data.dividends, data.prices, data.fx_prices))
    )
    if (ytm := data.ytm) is not None:
        adj = adj.add(YtmComponent(ytm))
    if futures := [i for i in instruments.values() if i.type == InstrumentType.FUTURE]:
        adj = adj.add(RepoComponent(data.repo, "currency", {f.id: f.currency.value for f in futures}))
    return adj


def report(api: BshData):
    adj = build_adjuster(build_pipeline(api))
    r = adj.get_clean_returns()

    for name, comp in adj.get_breakdown().items():
        comp.mul(10000).plot(title=f"{name} (BP)", kind="bar")

    print("\nCorrelation Matrix:")
    print(r.corr().round(4))

    print("\nMean Daily Return Difference:")
    diff = r.mean().diff().dropna()
    for pair, val in diff.items():
        print(f"  {r.columns[0]} vs {pair}: {val * 100:.4f}%  ({val * 252 * 100:.2f}% p.a.)")

    cum = ((1 + r).cumprod() - 1) * 10000
    cum.plot(title="Cumulative Returns")
    plt.ylabel("Cumulative Return")
    plt.xlabel("Date")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    api = BshData(
        config_path=r"C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml", log_level="DEBUG")
    report(api)
