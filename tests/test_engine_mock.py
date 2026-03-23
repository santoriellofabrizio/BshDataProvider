"""
Integration-style performance tests that mirror EtfEquityPriceEngine.

Instantiates a MockEtfEquityPriceEngine that replicates the production engine's
adjuster setup and core runtime loops using purely random data:

  adjuster          (daily)   – Ter + FxSpot + FxForwardCarry + Dividend
  intraday_adjuster (15-min)  – FxSpot + Dividend
  get_mid()                   – live_update on both adjusters (temporary tick)
  update_lf()                 – append_update on the intraday adjuster (permanent)

Also covers unit tests for _calculate_cluster_correction() and
_prepare_beta_matrix().

NOTE: get_mid() routes through the *intraday* path of TerComponent and
FxForwardCarryComponent (because live_update adds a non-midnight timestamp,
making the dates list mixed daily+intraday).  That path uses pandas .loc
per cell and is a known next optimization target – budgets reflect current
unoptimized performance.
"""
import time
import numpy as np
import pandas as pd
import pytest
from datetime import datetime

from sfm_data_provider.analytics.adjustments.ter import TerComponent
from sfm_data_provider.analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from sfm_data_provider.analytics.adjustments.fx_spot import FxSpotComponent
from sfm_data_provider.analytics.adjustments.dividend import DividendComponent
from sfm_data_provider.analytics.adjustments.adjuster import Adjuster
from sfm_data_provider.core.instruments.instruments import EtfInstrument


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

N_INSTRUMENTS = 800
N_DAILY_DATES = 11          # ~2 business weeks of daily prices
N_INTRADAY_DAYS = 3         # 3 business days of 15-min prices
SLOTS_PER_DAY = 29          # 10:00–17:00 inclusive @ 15 min  (29 slots)
N_INTRADAY_DATES = N_INTRADAY_DAYS * SLOTS_PER_DAY   # 87

N_DIVIDEND_INSTRUMENTS = 100  # subset that pay a dividend

# EUR-prefixed ticker format used in production for Bloomberg subscriptions
CURRENCIES_EUR = [
    "EURUSD", "EURGBP", "EURCHF", "EURAUD", "EURDKK",
    "EURHKD", "EURNOK", "EURPLN", "EURSEK", "EURCNY",
    "EURJPY", "EURCNH", "EURCAD", "EURINR", "EURBRL",
]
# Normalised (EUR-stripped) names used inside composition matrices
CURRENCIES = [
    "USD", "GBP", "CHF", "AUD", "DKK", "HKD", "NOK",
    "PLN", "SEK", "CNY", "JPY", "CNH", "CAD", "INR", "BRL",
]


# ---------------------------------------------------------------------------
# Data factories
# ---------------------------------------------------------------------------

def _make_instruments(n: int) -> dict[str, EtfInstrument]:
    return {f"IE{i:010d}": EtfInstrument(id=f"IE{i:010d}", currency="EUR")
            for i in range(n)}


def _make_daily_dates(n: int) -> list[datetime]:
    return [d.to_pydatetime()
            for d in pd.bdate_range("2026-03-02", periods=n, freq="B")]


def _make_intraday_dates() -> list:
    """15-min timestamps 10:00–17:00 for N_INTRADAY_DAYS business days."""
    days = pd.bdate_range("2026-03-02", periods=N_INTRADAY_DAYS, freq="B")
    result = []
    for d in days:
        result.extend(
            pd.date_range(d.replace(hour=10), d.replace(hour=17), freq="15min").tolist()
        )
    assert len(result) == N_INTRADAY_DATES
    return result


def _make_prices(dates, instruments, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        rng.uniform(50, 200, size=(len(dates), len(instruments))),
        index=pd.DatetimeIndex(dates),
        columns=list(instruments.keys()),
    )


def _make_fx_prices(dates, eur_format: bool, seed: int) -> pd.DataFrame:
    """
    FX prices.  eur_format=True uses EURUSD column names (production style,
    FxSpotComponent normalises internally); False uses bare USD names.
    """
    rng = np.random.default_rng(seed)
    cols = CURRENCIES_EUR if eur_format else CURRENCIES
    return pd.DataFrame(
        rng.uniform(0.8, 1.5, size=(len(dates), len(cols))),
        index=pd.DatetimeIndex(dates),
        columns=cols,
    )


def _make_fx_forward_prices(dates, seed: int) -> pd.DataFrame:
    """Forward points in basis points with EURUSD column names."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        rng.uniform(-50, 50, size=(len(dates), len(CURRENCIES_EUR))),
        index=pd.DatetimeIndex(dates),
        columns=CURRENCIES_EUR,
    )


def _make_composition(instruments, seed: int) -> pd.DataFrame:
    """Dirichlet-sampled (n_instruments × n_currencies), cols = CURRENCIES."""
    rng = np.random.default_rng(seed)
    raw = rng.dirichlet(np.ones(len(CURRENCIES)), size=len(instruments))
    return pd.DataFrame(raw, index=list(instruments.keys()), columns=CURRENCIES)


def _make_ter(instruments, seed: int) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(
        {isin: rng.uniform(0.0001, 0.005) for isin in instruments}
    )


def _make_dividends(dates, instruments, div_date_idx: int, seed: int) -> pd.DataFrame:
    """
    Sparse dividends: N_DIVIDEND_INSTRUMENTS instruments have a non-zero
    dividend on dates[div_date_idx], all other cells are 0.
    """
    rng = np.random.default_rng(seed)
    div_ids = list(instruments.keys())[:N_DIVIDEND_INSTRUMENTS]
    data = np.zeros((len(dates), N_DIVIDEND_INSTRUMENTS))
    data[div_date_idx, :] = rng.uniform(0.5, 2.0, size=N_DIVIDEND_INSTRUMENTS)
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=div_ids)


def _make_last_mid(instruments, seed: int) -> pd.Series:
    """Simulated Bloomberg snapshot: ETF prices + FX in EURUSD format."""
    rng = np.random.default_rng(seed)
    return pd.Series(
        np.concatenate([
            rng.uniform(50, 200, size=len(instruments)),
            rng.uniform(0.8, 1.5, size=len(CURRENCIES_EUR)),
        ]),
        index=list(instruments.keys()) + CURRENCIES_EUR,
    )


def _make_beta_matrix(instruments, density: float = 0.3, seed: int = 70) -> pd.DataFrame:
    """Sparse random beta matrix (n × n); no self-betas; rows with drivers sum to 1."""
    rng = np.random.default_rng(seed)
    isins = list(instruments.keys())
    n = len(isins)
    betas = np.zeros((n, n))
    for i in range(n):
        pool = [j for j in range(n) if j != i]
        k = max(1, int(n * density))
        active = rng.choice(pool, size=min(k, len(pool)), replace=False)
        betas[i, active] = rng.dirichlet(np.ones(len(active)))
    return pd.DataFrame(betas, index=isins, columns=isins)


# ---------------------------------------------------------------------------
# MockEtfEquityPriceEngine
# ---------------------------------------------------------------------------

class MockEtfEquityPriceEngine:
    """
    Minimal replica of EtfEquityPriceEngine's adjuster wiring and runtime
    loops, using only the adjuster/component layer (no Bloomberg, no Redis,
    no StrategyUI).

    Mirrors:
      _init_historical_data  → __init__
      get_mid()              → live_update on both adjusters
      update_LF()            → append_update on intraday_adjuster
      _calculate_cluster_correction() → staticmethod
      _prepare_beta_matrix() → instance method (simplified)
    """

    def __init__(
        self,
        instruments: dict[str, EtfInstrument],
        etf_prices_daily: pd.DataFrame,
        etf_prices_intraday: pd.DataFrame,
        fx_prices_daily: pd.DataFrame,
        fx_prices_intraday: pd.DataFrame,
        fx_composition: pd.DataFrame,
        fx_forward_composition: pd.DataFrame,
        fx_forward_prices: pd.DataFrame,
        dividends_daily: pd.DataFrame,
        dividends_intraday: pd.DataFrame,
        ter: pd.Series,
    ):
        self.etfs = list(instruments.keys())
        self.currencies = CURRENCIES_EUR

        # ── Mirrors _init_historical_data() ──────────────────────────────
        # Production creates Adjuster without is_intraday keyword → default True
        self.adjuster = (
            Adjuster(etf_prices_daily, instruments=instruments)
            .add(TerComponent(ter.to_dict()))
            .add(FxSpotComponent(fx_composition, fx_prices_daily))
            .add(FxForwardCarryComponent(
                fx_forward_composition, fx_forward_prices, "1M", fx_prices_daily
            ))
            .add(DividendComponent(
                dividends_daily, etf_prices_daily, fx_prices=fx_prices_daily
            ))
        )
        self.intraday_adjuster = (
            Adjuster(etf_prices_intraday, instruments=instruments)
            .add(FxSpotComponent(fx_composition, fx_prices_intraday))
            .add(DividendComponent(
                dividends_intraday, etf_prices_intraday, fx_prices=fx_prices_intraday
            ))
        )

        self.corrected_return: pd.DataFrame | None = None
        self.corrected_return_intraday: pd.DataFrame | None = None

    # ── Core loops ────────────────────────────────────────────────────────

    def get_mid(self, last_mid: pd.Series) -> tuple[pd.Series, pd.Series]:
        """
        Mirror of EtfEquityPriceEngine.get_mid() adjuster section.

        live_update adds a real-time (non-midnight) tick to both adjusters.
        This causes TerComponent and FxForwardCarryComponent to fall through
        to their *intraday* code path (mixed daily+live timestamps), which is
        the current performance bottleneck.
        """
        # Daily adjuster: temporary snapshot
        with self.adjuster.live_update(
            fx_prices=last_mid[self.currencies],
            prices=last_mid,
        ):
            self.corrected_return = self.adjuster.get_clean_returns(cumulative=True).T
            last_return = self.corrected_return.iloc[:, -1]

        # Intraday adjuster: temporary snapshot
        with self.intraday_adjuster.live_update(
            fx_prices=last_mid[self.currencies],
            prices=last_mid,
        ):
            self.corrected_return_intraday = (
                self.intraday_adjuster.get_clean_returns(cumulative=True).T
            )
            last_return_intraday = self.corrected_return_intraday.iloc[:, -1]

        return last_return, last_return_intraday

    def update_lf(self, last_mid: pd.Series) -> None:
        """Mirror of EtfEquityPriceEngine.update_LF(): permanent intraday append."""
        self.intraday_adjuster.append_update(
            prices=last_mid[self.etfs],
            fx_prices=last_mid[self.currencies],
        )

    # ── Utilities (ported from EtfEquityPriceEngine) ─────────────────────

    @staticmethod
    def calculate_cluster_correction(
        cluster_betas: pd.DataFrame,
        threshold: float = 0.5,
    ) -> pd.Series:
        """Direct copy of EtfEquityPriceEngine._calculate_cluster_correction()."""
        if cluster_betas.empty:
            return pd.Series(dtype=float)

        cluster_betas = cluster_betas.sort_index(axis=1).sort_index(axis=0).copy()

        for etf in cluster_betas.index:
            if etf in cluster_betas.columns:
                cluster_betas.loc[etf, etf] = 0

        non_zero_counts = (cluster_betas != 0).sum(axis=1)

        cluster_threshold = pd.Series(index=cluster_betas.index, dtype=float)
        cluster_threshold[non_zero_counts > 0] = (
            threshold / non_zero_counts[non_zero_counts > 0]
        )
        cluster_threshold[non_zero_counts == 0] = 0

        cluster_sizes = cluster_betas.gt(cluster_threshold, axis=0).sum(axis=1) + 1
        return cluster_sizes.where(
            cluster_sizes == 1, (cluster_sizes - 1) / cluster_sizes
        )

    def prepare_beta_matrix(
        self,
        beta_df: pd.DataFrame,
        isin_universe: list[str],
    ) -> pd.DataFrame:
        """Simplified copy of EtfEquityPriceEngine._prepare_beta_matrix()."""
        filtered = beta_df.loc[
            beta_df.index.intersection(isin_universe),
            beta_df.columns.intersection(isin_universe),
        ]
        if filtered.empty:
            return pd.DataFrame()

        filtered = (
            filtered
            .dropna(how="all", axis=0)
            .dropna(how="all", axis=1)
        )
        row_sums = filtered.sum(axis=1)
        invalid = (row_sums == 0) | row_sums.isna()
        filtered = filtered[~invalid]
        row_sums = row_sums[~invalid]
        return filtered.div(row_sums, axis=0).fillna(0)


# ---------------------------------------------------------------------------
# Module-scoped data fixtures (expensive random data built once per module)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def instruments():
    return _make_instruments(N_INSTRUMENTS)


@pytest.fixture(scope="module")
def daily_dates():
    return _make_daily_dates(N_DAILY_DATES)


@pytest.fixture(scope="module")
def intraday_dates():
    return _make_intraday_dates()


@pytest.fixture(scope="module")
def etf_prices_daily(daily_dates, instruments):
    return _make_prices(daily_dates, instruments, seed=41)


@pytest.fixture(scope="module")
def etf_prices_intraday(intraday_dates, instruments):
    return _make_prices(intraday_dates, instruments, seed=42)


@pytest.fixture(scope="module")
def fx_prices_daily(daily_dates):
    # EURUSD column format to match production; FxSpotComponent normalises internally
    return _make_fx_prices(daily_dates, eur_format=True, seed=43)


@pytest.fixture(scope="module")
def fx_prices_intraday(intraday_dates):
    return _make_fx_prices(intraday_dates, eur_format=True, seed=44)


@pytest.fixture(scope="module")
def fx_forward_prices(daily_dates):
    return _make_fx_forward_prices(daily_dates, seed=45)


@pytest.fixture(scope="module")
def fx_composition(instruments):
    return _make_composition(instruments, seed=46)


@pytest.fixture(scope="module")
def ter(instruments):
    return _make_ter(instruments, seed=47)


@pytest.fixture(scope="module")
def dividends_daily(daily_dates, instruments):
    # Dividend on date index 3 (4th business day); prices at [0-2] exist before it
    return _make_dividends(daily_dates, instruments, div_date_idx=3, seed=48)


@pytest.fixture(scope="module")
def dividends_intraday(intraday_dates, instruments):
    # Dividend on slot index SLOTS_PER_DAY (first slot of day 2);
    # all of day-1 timestamps lie before it → DividendComponent finds prices
    return _make_dividends(intraday_dates, instruments, div_date_idx=SLOTS_PER_DAY, seed=49)


@pytest.fixture(scope="module")
def last_mid(instruments):
    return _make_last_mid(instruments, seed=60)


@pytest.fixture(scope="module")
def beta_matrix(instruments):
    return _make_beta_matrix(instruments, density=0.3, seed=70)


# ---------------------------------------------------------------------------
# Engine fixtures (function-scoped so each test gets fresh internal state)
# ---------------------------------------------------------------------------

@pytest.fixture
def engine(
    instruments,
    etf_prices_daily, etf_prices_intraday,
    fx_prices_daily, fx_prices_intraday,
    fx_composition, fx_forward_prices,
    dividends_daily, dividends_intraday, ter,
):
    return MockEtfEquityPriceEngine(
        instruments=instruments,
        etf_prices_daily=etf_prices_daily,
        etf_prices_intraday=etf_prices_intraday,
        fx_prices_daily=fx_prices_daily,
        fx_prices_intraday=fx_prices_intraday,
        fx_composition=fx_composition,
        fx_forward_composition=fx_composition,   # same weights for carry in test
        fx_forward_prices=fx_forward_prices,
        dividends_daily=dividends_daily,
        dividends_intraday=dividends_intraday,
        ter=ter,
    )


@pytest.fixture
def warmed_engine(engine, last_mid):
    """Engine with all caches pre-populated by one get_mid() call."""
    engine.get_mid(last_mid)
    return engine


# ---------------------------------------------------------------------------
# TestMockEngineSetup – wiring & correctness
# ---------------------------------------------------------------------------

class TestMockEngineSetup:
    """Verify the engine is wired correctly before measuring performance."""

    def test_daily_adjuster_has_four_components(self, engine):
        assert len(engine.adjuster.components) == 4

    def test_intraday_adjuster_has_two_components(self, engine):
        assert len(engine.intraday_adjuster.components) == 2

    def test_daily_prices_shape(self, engine):
        assert engine.adjuster.prices.shape == (N_DAILY_DATES, N_INSTRUMENTS)

    def test_intraday_prices_shape(self, engine):
        assert engine.intraday_adjuster.prices.shape == (N_INTRADAY_DATES, N_INSTRUMENTS)

    def test_get_mid_returns_two_series(self, engine, last_mid):
        last_ret, last_ret_intra = engine.get_mid(last_mid)
        assert isinstance(last_ret, pd.Series)
        assert isinstance(last_ret_intra, pd.Series)
        assert len(last_ret) == N_INSTRUMENTS
        assert len(last_ret_intra) == N_INSTRUMENTS

    def test_corrected_return_shape(self, engine, last_mid):
        engine.get_mid(last_mid)
        # .T transposes dates×inst → inst×dates; first axis = instruments
        assert engine.corrected_return.shape[0] == N_INSTRUMENTS

    def test_corrected_return_intraday_shape(self, engine, last_mid):
        engine.get_mid(last_mid)
        assert engine.corrected_return_intraday.shape[0] == N_INSTRUMENTS

    def test_live_update_does_not_persist_daily(self, engine, last_mid):
        """get_mid() must not permanently grow the daily price history."""
        before = len(engine.adjuster.prices)
        engine.get_mid(last_mid)
        assert len(engine.adjuster.prices) == before

    def test_live_update_does_not_persist_intraday(self, engine, last_mid):
        """get_mid() must not permanently grow the intraday price history."""
        before = len(engine.intraday_adjuster.prices)
        engine.get_mid(last_mid)
        assert len(engine.intraday_adjuster.prices) == before

    def test_update_lf_persists_one_row(self, engine, last_mid):
        """update_lf() (append_update) must permanently add exactly one row."""
        before = len(engine.intraday_adjuster.prices)
        engine.update_lf(last_mid)
        assert len(engine.intraday_adjuster.prices) == before + 1

    def test_repeated_get_mid_is_idempotent(self, warmed_engine, last_mid):
        """Calling get_mid() repeatedly must leave both price lengths unchanged."""
        daily_len = len(warmed_engine.adjuster.prices)
        intra_len = len(warmed_engine.intraday_adjuster.prices)
        for _ in range(3):
            warmed_engine.get_mid(last_mid)
        assert len(warmed_engine.adjuster.prices) == daily_len
        assert len(warmed_engine.intraday_adjuster.prices) == intra_len


# ---------------------------------------------------------------------------
# TestGetMidPerformance
# ---------------------------------------------------------------------------

class TestGetMidPerformance:
    """
    End-to-end get_mid() timing: two live_update contexts (daily + intraday),
    each calling get_clean_returns(cumulative=True).

    Budget is intentionally generous: live_update adds a non-midnight timestamp,
    causing TerComponent and FxForwardCarryComponent to use the intraday code
    path (pandas .loc per cell across 800 instruments × 11 midnight-dates).
    This path is the next known optimization target.
    """

    TIME_BUDGET_COLD_MS = 2_000   # first call: cache miss + intraday path
    TIME_BUDGET_WARM_MS = 2_000   # subsequent calls: caches warm, intraday path still slow

    def test_cold_under_budget(self, engine, last_mid):
        """First get_mid() call (no warm-up) must complete within budget."""
        t0 = time.perf_counter()
        engine.get_mid(last_mid)
        elapsed_ms = (time.perf_counter() - t0) * 1e3

        print(
            f"\n  get_mid() cold [{N_DAILY_DATES}d daily + "
            f"{N_INTRADAY_DATES} intraday, {N_INSTRUMENTS}i]: "
            f"{elapsed_ms:.1f} ms  (budget {self.TIME_BUDGET_COLD_MS} ms)"
        )
        assert elapsed_ms < self.TIME_BUDGET_COLD_MS, (
            f"get_mid() cold too slow: {elapsed_ms:.1f} ms"
        )

    def test_warm_median_under_budget(self, warmed_engine, last_mid):
        """Median of 5 consecutive ticks (caches warm) must stay under budget."""
        timings = []
        for _ in range(5):
            t0 = time.perf_counter()
            warmed_engine.get_mid(last_mid)
            timings.append((time.perf_counter() - t0) * 1e3)

        median_ms = sorted(timings)[len(timings) // 2]
        print(
            f"\n  get_mid() warm median [{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.TIME_BUDGET_WARM_MS} ms)  "
            f"runs: {', '.join(f'{t:.0f}' for t in timings)}"
        )
        assert median_ms < self.TIME_BUDGET_WARM_MS, (
            f"get_mid() warm too slow: {median_ms:.1f} ms"
        )

    def test_daily_adjuster_component_breakdown(self, engine, last_mid):
        """
        Sanity-check that the corrected_return columns match instruments after
        get_mid().  (The .T transpose in get_mid makes rows=instruments.)
        """
        engine.get_mid(last_mid)
        assert set(engine.corrected_return.index) == set(engine.etfs)

    def test_intraday_adjuster_component_breakdown(self, engine, last_mid):
        engine.get_mid(last_mid)
        assert set(engine.corrected_return_intraday.index) == set(engine.etfs)


# ---------------------------------------------------------------------------
# TestUpdateLfPerformance
# ---------------------------------------------------------------------------

class TestUpdateLfPerformance:
    """
    update_lf() permanently appends one 15-min tick to the intraday adjuster
    (mirrors EtfEquityPriceEngine.update_LF).
    """

    TIME_BUDGET_APPEND_MS = 500
    TIME_BUDGET_GET_MID_AFTER_MS = 2_000

    def test_append_under_budget(self, warmed_engine, last_mid):
        t0 = time.perf_counter()
        warmed_engine.update_lf(last_mid)
        elapsed_ms = (time.perf_counter() - t0) * 1e3

        print(
            f"\n  update_lf() append_update [{N_INTRADAY_DATES}→"
            f"{N_INTRADAY_DATES + 1} rows]: "
            f"{elapsed_ms:.1f} ms  (budget {self.TIME_BUDGET_APPEND_MS} ms)"
        )
        assert elapsed_ms < self.TIME_BUDGET_APPEND_MS

    def test_get_mid_after_append_under_budget(self, warmed_engine, last_mid):
        """get_mid() on the grown dataset must stay within budget."""
        warmed_engine.update_lf(last_mid)

        t0 = time.perf_counter()
        warmed_engine.get_mid(last_mid)
        elapsed_ms = (time.perf_counter() - t0) * 1e3

        print(
            f"\n  get_mid() after update_lf: "
            f"{elapsed_ms:.1f} ms  (budget {self.TIME_BUDGET_GET_MID_AFTER_MS} ms)"
        )
        assert elapsed_ms < self.TIME_BUDGET_GET_MID_AFTER_MS

    def test_multiple_appends_accumulate(self, warmed_engine, instruments):
        """N update_lf() calls with distinct prices each add exactly one row."""
        base = len(warmed_engine.intraday_adjuster.prices)
        n = 5
        for i in range(n):
            # Different seed → different price values → no deduplication in append_update
            warmed_engine.update_lf(_make_last_mid(instruments, seed=80 + i))
        assert len(warmed_engine.intraday_adjuster.prices) == base + n

    def test_get_mid_still_idempotent_after_appends(self, warmed_engine, last_mid):
        """get_mid() (live_update) must not further grow prices even after appends."""
        warmed_engine.update_lf(last_mid)
        warmed_engine.update_lf(last_mid)
        grown_len = len(warmed_engine.intraday_adjuster.prices)
        warmed_engine.get_mid(last_mid)
        warmed_engine.get_mid(last_mid)
        assert len(warmed_engine.intraday_adjuster.prices) == grown_len


# ---------------------------------------------------------------------------
# TestClusterCorrection
# ---------------------------------------------------------------------------

class TestClusterCorrection:
    """Unit tests for _calculate_cluster_correction()."""

    def test_empty_matrix_returns_empty_series(self):
        result = MockEtfEquityPriceEngine.calculate_cluster_correction(pd.DataFrame())
        assert isinstance(result, pd.Series)
        assert result.empty

    def test_self_beta_excluded(self):
        """Diagonal must be zeroed before the threshold comparison."""
        isins = ["A", "B", "C"]
        # A has self-beta = 0.8 and only 0.2 to B → without zeroing diagonal,
        # non_zero = 2; with zeroing, non_zero = 1
        beta = pd.DataFrame(
            [[0.8, 0.2, 0.0],
             [0.5, 0.0, 0.5],
             [0.3, 0.7, 0.0]],
            index=isins, columns=isins,
        )
        result = MockEtfEquityPriceEngine.calculate_cluster_correction(beta, threshold=0.5)
        # A: off-diag non-zero = {0.2} only → non_zero_count=1
        # threshold_per = 0.5/1 = 0.5; 0.2 > 0.5? No → cluster_size=1 → correction=1.0
        assert result["A"] == pytest.approx(1.0)

    def test_zero_beta_row_gives_one(self):
        """Row with no outgoing betas → single-node cluster → correction = 1."""
        isins = ["A", "B", "C"]
        beta = pd.DataFrame(
            [[0.0, 0.0, 0.0],
             [0.5, 0.0, 0.5],
             [0.3, 0.7, 0.0]],
            index=isins, columns=isins,
        )
        result = MockEtfEquityPriceEngine.calculate_cluster_correction(beta, threshold=0.5)
        assert result["A"] == pytest.approx(1.0)

    def test_single_strong_driver(self):
        """One driver above threshold → cluster_size=2 → correction=0.5."""
        isins = ["A", "B", "C"]
        beta = pd.DataFrame(
            [[0.0, 1.0, 0.0],
             [0.0, 0.0, 1.0],
             [1.0, 0.0, 0.0]],
            index=isins, columns=isins,
        )
        result = MockEtfEquityPriceEngine.calculate_cluster_correction(beta, threshold=0.5)
        # A → B only; threshold=0.5/1=0.5; 1.0>0.5 → cluster_size=2 → (2-1)/2=0.5
        assert result["A"] == pytest.approx(0.5)

    def test_all_corrections_in_unit_interval(self, beta_matrix):
        """Corrections must be in [0, 1] for any valid beta matrix."""
        result = MockEtfEquityPriceEngine.calculate_cluster_correction(beta_matrix)
        assert (result >= 0).all() and (result <= 1).all()

    def test_output_indexed_by_beta_rows(self, beta_matrix):
        result = MockEtfEquityPriceEngine.calculate_cluster_correction(beta_matrix)
        assert set(result.index) == set(beta_matrix.index)

    def test_threshold_zero_all_drivers_count(self):
        """threshold=0 → every non-zero beta is a driver → large cluster sizes."""
        isins = ["A", "B", "C", "D"]
        beta = pd.DataFrame(
            [[0.0, 0.4, 0.3, 0.3],
             [0.5, 0.0, 0.5, 0.0],
             [0.2, 0.3, 0.0, 0.5],
             [0.6, 0.4, 0.0, 0.0]],
            index=isins, columns=isins,
        )
        result_zero = MockEtfEquityPriceEngine.calculate_cluster_correction(beta, threshold=0.0)
        result_half = MockEtfEquityPriceEngine.calculate_cluster_correction(beta, threshold=0.5)
        # With threshold=0, any non-zero beta is a driver → more drivers → lower correction
        assert (result_zero <= result_half).all()


# ---------------------------------------------------------------------------
# TestPrepareBetaMatrix
# ---------------------------------------------------------------------------

class TestPrepareBetaMatrix:
    """Unit tests for _prepare_beta_matrix() (filter + row-normalise)."""

    @pytest.fixture
    def eng(self, engine):
        return engine

    def test_rows_sum_to_one(self, eng, beta_matrix, instruments):
        result = eng.prepare_beta_matrix(beta_matrix, list(instruments.keys()))
        np.testing.assert_allclose(
            result.sum(axis=1).values, 1.0, rtol=1e-9,
            err_msg="All rows must sum to 1 after normalisation",
        )

    def test_unknown_instruments_filtered(self, eng, instruments):
        """Rows/columns not in isin_universe must be dropped."""
        isins = list(instruments.keys())[:10]
        all_isins = isins + ["GHOST_A", "GHOST_B"]
        rng = np.random.default_rng(99)
        beta = pd.DataFrame(
            rng.dirichlet(np.ones(len(all_isins)), size=len(all_isins)),
            index=all_isins, columns=all_isins,
        )
        result = eng.prepare_beta_matrix(beta, isins)
        assert "GHOST_A" not in result.index
        assert "GHOST_B" not in result.columns

    def test_empty_input_returns_empty(self, eng, instruments):
        result = eng.prepare_beta_matrix(pd.DataFrame(), list(instruments.keys()))
        assert result.empty

    def test_output_shape_full_universe(self, eng, beta_matrix, instruments):
        """All instruments are in the beta matrix → shape = (N × N)."""
        result = eng.prepare_beta_matrix(beta_matrix, list(instruments.keys()))
        assert result.shape == (N_INSTRUMENTS, N_INSTRUMENTS)

    def test_zero_sum_rows_dropped(self, eng):
        """Rows whose beta values sum to zero must be removed."""
        isins = ["A", "B", "C"]
        beta = pd.DataFrame(
            [[0.0, 0.0, 0.0],   # zero-sum row → should be dropped
             [0.5, 0.0, 0.5],
             [0.3, 0.7, 0.0]],
            index=isins, columns=isins,
        )
        result = eng.prepare_beta_matrix(beta, isins)
        assert "A" not in result.index

    def test_no_nan_in_output(self, eng, beta_matrix, instruments):
        result = eng.prepare_beta_matrix(beta_matrix, list(instruments.keys()))
        assert not result.isna().any().any()
