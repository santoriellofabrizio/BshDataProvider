"""
Benchmark: Adjuster.get_clean_returns()

Misura il tempo di esecuzione in due modalità:
  - Cold: adjuster appena creato, cache vuota (prima chiamata)
  - Warm: adjuster con cache già popolata (chiamate successive)

Scenari:
  - Daily:    250/1000/3000 giorni × 5/20/50 strumenti
  - Intraday: 5/20/60 giorni di barre 5-min × 5/10/20 strumenti

Uso:
    cd /home/user/BshDataProvider
    python benchmarks/benchmark_get_clean_returns.py
"""
import time
import sys
import types
import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────
# Stub dipendenze esterne non disponibili in questo ambiente
# (blpapi, BSHDataClient, InstrumentFactory, ecc.)
# ─────────────────────────────────────────────────────────────
def _stub(name: str):
    m = types.ModuleType(name)
    sys.modules[name] = m
    return m

for _n in ["blpapi", "dotenv", "ruamel", "ruamel.yaml", "tqdm", "joblib",
           "sfm_datalibrary"]:
    _stub(_n)

# stub sfm_data_provider top-level (bypassa __init__.py)
_pkg = types.ModuleType("sfm_data_provider")
_pkg.__path__ = ["src/sfm_data_provider"]
_pkg.__package__ = "sfm_data_provider"
sys.modules["sfm_data_provider"] = _pkg
sys.path.insert(0, "src")

# stub InstrumentFactory (importata da adjuster.py, non serve nel benchmark)
_factory_mod = _stub("sfm_data_provider.core.instruments.instrument_factory")
class _FakeFactory:
    def get_many(self, ids):
        raise RuntimeError("InstrumentFactory non disponibile in benchmark")
_factory_mod.InstrumentFactory = _FakeFactory

# stub client e provider Bloomberg
for _n in [
    "sfm_data_provider.client",
    "sfm_data_provider.providers",
    "sfm_data_provider.providers.bloomberg",
    "sfm_data_provider.providers.bloomberg.bloomberg",
]:
    _stub(_n)

from sfm_data_provider.analytics.adjustments.adjuster import Adjuster          # noqa: E402
from sfm_data_provider.analytics.adjustments.ter import TerComponent            # noqa: E402
from sfm_data_provider.analytics.adjustments.fx_spot import FxSpotComponent    # noqa: E402
from sfm_data_provider.analytics.adjustments.dividend import DividendComponent  # noqa: E402
from sfm_data_provider.core.instruments.instruments import EtfInstrument        # noqa: E402
from sfm_data_provider.core.enums.currencies import CurrencyEnum                # noqa: E402


# ─────────────────────────────────────────────────────────────
# FACTORY HELPERS
# ─────────────────────────────────────────────────────────────

def make_instruments(n: int) -> dict:
    """Crea n EtfInstrument con valuta EUR e payment_policy DIST."""
    instruments = {}
    for i in range(n):
        inst_id = f"ETF{i:03d}"
        inst = EtfInstrument(
            id=inst_id,
            ticker=inst_id,
            currency=CurrencyEnum.EUR,
            payment_policy="DIST",
            fund_currency=CurrencyEnum.EUR,
        )
        instruments[inst_id] = inst
    return instruments


def make_daily_prices(n_days: int, inst_ids: list, rng) -> pd.DataFrame:
    dates = pd.date_range("2020-01-02", periods=n_days, freq="B")
    ret = rng.normal(0.0003, 0.008, (n_days, len(inst_ids)))
    prices = 100.0 * np.exp(np.cumsum(ret, axis=0))
    return pd.DataFrame(prices, index=dates, columns=inst_ids)


def make_fx_prices(index: pd.DatetimeIndex, rng) -> pd.DataFrame:
    n = len(index)
    return pd.DataFrame({
        "EURUSD": 1.10 + rng.normal(0, 0.004, n),
        "EURGBP": 0.86 + rng.normal(0, 0.002, n),
    }, index=index)


def make_fx_composition(inst_ids: list) -> pd.DataFrame:
    """50 % USD, 30 % GBP, 20 % EUR (implicito)."""
    return pd.DataFrame({
        "USD": [0.50] * len(inst_ids),
        "GBP": [0.30] * len(inst_ids),
    }, index=inst_ids)


def make_dividends(index: pd.DatetimeIndex, inst_ids: list, rng) -> pd.DataFrame:
    """Dividendi trimestrali su 1/3 degli strumenti."""
    divs = pd.DataFrame(0.0, index=index, columns=inst_ids)
    div_dates = index[::63]  # circa trimestrali
    n_paying = max(1, len(inst_ids) // 3)
    for d in div_dates:
        for inst_id in inst_ids[:n_paying]:
            divs.loc[d, inst_id] = 0.50
    return divs


def make_intraday_prices(n_days: int, inst_ids: list, freq_min: int, rng) -> pd.DataFrame:
    """Barre intraday 09:00-17:00."""
    bars_per_day = (8 * 60) // freq_min
    dates = pd.date_range("2024-01-02 09:00", periods=n_days * bars_per_day,
                          freq=f"{freq_min}min")
    dates = dates[(dates.time >= pd.Timestamp("09:00").time()) &
                  (dates.time <= pd.Timestamp("17:00").time())]
    ret = rng.normal(0, 0.0008, (len(dates), len(inst_ids)))
    prices = 100.0 * np.exp(np.cumsum(ret, axis=0))
    return pd.DataFrame(prices, index=dates, columns=inst_ids)


# ─────────────────────────────────────────────────────────────
# ADJUSTER FACTORIES
# ─────────────────────────────────────────────────────────────

def daily_adjuster_factory(n_days: int, n_inst: int, seed: int = 42):
    """Restituisce una funzione che crea un Adjuster daily fresco."""
    rng = np.random.default_rng(seed)
    instruments = make_instruments(n_inst)
    inst_ids = list(instruments.keys())

    prices_df    = make_daily_prices(n_days, inst_ids, rng)
    fx_prices    = make_fx_prices(prices_df.index, rng)
    fx_comp      = make_fx_composition(inst_ids)
    ters         = {iid: 0.002 for iid in inst_ids}
    dividends    = make_dividends(prices_df.index, inst_ids, rng)

    def factory():
        adj = Adjuster(prices_df, instruments=instruments, is_intraday=False)
        adj.add(TerComponent(ters))
        adj.add(FxSpotComponent(fx_comp, fx_prices))
        adj.add(DividendComponent(dividends, prices_df, fx_prices))
        return adj

    return factory


def intraday_adjuster_factory(n_days: int, n_inst: int, freq_min: int = 5, seed: int = 42):
    """Restituisce una funzione che crea un Adjuster intraday fresco."""
    rng = np.random.default_rng(seed)
    instruments = make_instruments(n_inst)
    inst_ids = list(instruments.keys())

    prices_df = make_intraday_prices(n_days, inst_ids, freq_min, rng)

    # FX intraday per FxSpot
    fx_intraday = make_fx_prices(prices_df.index, rng)

    # FX e dividendi giornalieri per DividendComponent
    daily_idx = pd.DatetimeIndex(sorted({d.normalize() for d in prices_df.index}))
    fx_daily  = make_fx_prices(daily_idx, rng)
    dividends = make_dividends(daily_idx, inst_ids, rng)

    fx_comp = make_fx_composition(inst_ids)
    ters    = {iid: 0.002 for iid in inst_ids}

    def factory():
        adj = Adjuster(prices_df, instruments=instruments, is_intraday=True)
        adj.add(TerComponent(ters))
        adj.add(FxSpotComponent(fx_comp, fx_intraday))
        adj.add(DividendComponent(dividends, prices_df, fx_daily))
        return adj

    return factory


# ─────────────────────────────────────────────────────────────
# TIMING
# ─────────────────────────────────────────────────────────────

def measure(factory, n_cold: int = 3, n_warm: int = 5):
    """
    Misura cold e warm timing.

    Cold: crea un Adjuster fresco prima di ogni chiamata (cache vuota).
    Warm: usa lo stesso Adjuster dopo la prima chiamata (cache popolata).
    """
    # --- Cold ---
    cold_ms = []
    for _ in range(n_cold):
        adj = factory()
        t0 = time.perf_counter()
        adj.get_clean_returns()
        cold_ms.append((time.perf_counter() - t0) * 1000)

    # --- Warm ---
    adj = factory()
    adj.get_clean_returns()          # popola la cache
    warm_ms = []
    for _ in range(n_warm):
        t0 = time.perf_counter()
        adj.get_clean_returns()
        warm_ms.append((time.perf_counter() - t0) * 1000)

    return cold_ms, warm_ms


def stats(values: list) -> tuple:
    """Restituisce (mean, min, max) in ms."""
    a = np.array(values)
    return a.mean(), a.min(), a.max()


# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────

HDR = f"{'Scenario':<32} {'Size':>10}   " \
      f"{'Cold mean':>10} {'Cold min':>9} {'Cold max':>9}   " \
      f"{'Warm mean':>10} {'Warm min':>9} {'Warm max':>9}"
SEP = "─" * len(HDR)


def print_row(label: str, size_str: str, cold: list, warm: list):
    cm, cmin, cmax = stats(cold)
    wm, wmin, wmax = stats(warm)
    print(f"{label:<32} {size_str:>10}   "
          f"{cm:>9.1f}ms {cmin:>8.1f}ms {cmax:>8.1f}ms   "
          f"{wm:>9.1f}ms {wmin:>8.1f}ms {wmax:>8.1f}ms")


def run():
    print()
    print("═" * len(HDR))
    print("  BENCHMARK — Adjuster.get_clean_returns()")
    print("═" * len(HDR))

    # ── DAILY ─────────────────────────────────────────────────
    print("\n[DAILY mode]  (componenti: TER + FxSpot + Dividend)\n")
    print(HDR)
    print(SEP)

    daily_scenarios = [
        ("Small  — 250d × 5 inst",    250,   5),
        ("Medium — 1000d × 20 inst",  1000,  20),
        ("Large  — 3000d × 50 inst",  3000,  50),
    ]
    for label, n_days, n_inst in daily_scenarios:
        factory = daily_adjuster_factory(n_days, n_inst)
        cold, warm = measure(factory, n_cold=3, n_warm=5)
        size_str = f"{n_days}d×{n_inst}i"
        print_row(label, size_str, cold, warm)

    # ── INTRADAY ──────────────────────────────────────────────
    print(f"\n[INTRADAY mode — 5-min bars]  (componenti: TER + FxSpot + Dividend)\n")
    print(HDR)
    print(SEP)

    intraday_scenarios = [
        ("Small  — 5d × 5 inst",    5,   5),
        ("Medium — 20d × 10 inst",  20,  10),
        ("Large  — 60d × 20 inst",  60,  20),
    ]
    for label, n_days, n_inst in intraday_scenarios:
        factory = intraday_adjuster_factory(n_days, n_inst, freq_min=5)
        adj_sample = factory()
        n_bars = len(adj_sample.prices)
        cold, warm = measure(factory, n_cold=3, n_warm=5)
        size_str = f"{n_bars}bars×{n_inst}i"
        print_row(label, size_str, cold, warm)

    print()


if __name__ == "__main__":
    run()
