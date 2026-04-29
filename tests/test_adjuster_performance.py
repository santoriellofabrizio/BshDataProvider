"""
Comprehensive performance benchmarks for all adjustment components and Adjuster pipelines.

Scale: 800 instruments × 11 daily dates (production scale).

Coverage:
  - TerComponent               daily path
  - FxSpotComponent            cold (cache miss) and warm (cache hit) paths
  - FxForwardCarryComponent    daily path
  - DividendComponent          daily path with 100 dividend-paying instruments
  - Adjuster (all 4 components) calculate_adjustment and get_clean_returns
  - Adjuster live_update       temporary snapshot, fully restored after context
  - Adjuster append_update     permanent price/FX data extension
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
from sfm_data_provider.analytics.adjustments.return_calculations import ReturnCalculator
from sfm_data_provider.core.instruments.instruments import EtfInstrument


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

N_INSTRUMENTS = 800
N_DATES = 11
N_DIVIDEND_INSTRUMENTS = 100  # subset that pay dividends

CURRENCIES = [
    "USD", "GBP", "CHF", "AUD", "DKK", "HKD", "NOK",
    "PLN", "SEK", "CNY", "JPY", "CNH", "CAD", "INR", "BRL",
]


# ---------------------------------------------------------------------------
# Data factories (deterministic, seeded)
# ---------------------------------------------------------------------------

def _make_instruments(n: int) -> dict[str, EtfInstrument]:
    """Create n EtfInstrument objects with EUR trading currency."""
    instruments: dict[str, EtfInstrument] = {}
    for i in range(n):
        isin = f"IE{i:010d}"
        instruments[isin] = EtfInstrument(id=isin, currency="EUR")
    return instruments


def _make_daily_dates(n_dates: int) -> list[datetime]:
    """Return n_dates consecutive business-day midnights."""
    idx = pd.bdate_range("2026-03-01", periods=n_dates, freq="B")
    return [d.to_pydatetime() for d in idx]


def _make_prices(
    dates: list[datetime],
    instruments: dict[str, EtfInstrument],
    seed: int = 41,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    data = rng.uniform(50, 200, size=(len(dates), len(instruments)))
    return pd.DataFrame(
        data,
        index=pd.DatetimeIndex(dates),
        columns=list(instruments.keys()),
    )


def _make_fx_spot_prices(dates: list[datetime], seed: int = 42) -> pd.DataFrame:
    """Random EUR-based FX spot rates for each currency in CURRENCIES."""
    rng = np.random.default_rng(seed)
    data = rng.uniform(0.8, 1.5, size=(len(dates), len(CURRENCIES)))
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=CURRENCIES)


def _make_fx_forward_prices(dates: list[datetime], seed: int = 43) -> pd.DataFrame:
    """Random FX forward points (basis points) for CURRENCIES."""
    rng = np.random.default_rng(seed)
    data = rng.uniform(-50, 50, size=(len(dates), len(CURRENCIES)))
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=CURRENCIES)


def _make_composition(
    instruments: dict[str, EtfInstrument],
    seed: int = 44,
) -> pd.DataFrame:
    """Dirichlet-sampled FX composition matrix (instruments × currencies)."""
    rng = np.random.default_rng(seed)
    n = len(instruments)
    raw = rng.dirichlet(np.ones(len(CURRENCIES)), size=n)
    return pd.DataFrame(raw, index=list(instruments.keys()), columns=CURRENCIES)


def _make_dividends(
    dates: list[datetime],
    instruments: dict[str, EtfInstrument],
    seed: int = 46,
) -> pd.DataFrame:
    """
    Sparse dividend DataFrame: N_DIVIDEND_INSTRUMENTS instruments each pay
    a dividend on the 6th date (index 5).  Other cells are 0.
    """
    rng = np.random.default_rng(seed)
    div_inst_ids = list(instruments.keys())[:N_DIVIDEND_INSTRUMENTS]
    data = np.zeros((len(dates), N_DIVIDEND_INSTRUMENTS))
    data[5, :] = rng.uniform(0.5, 2.0, size=N_DIVIDEND_INSTRUMENTS)
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=div_inst_ids)


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

def _median_ms(fn, *, warmup: int = 1, runs: int = 5):
    """
    Run fn() warmup times (discarded), then runs times (timed).
    Returns (median_ms, last_result).
    """
    for _ in range(warmup):
        fn()
    timings = []
    result = None
    for _ in range(runs):
        t0 = time.perf_counter()
        result = fn()
        timings.append((time.perf_counter() - t0) * 1e3)
    return sorted(timings)[len(timings) // 2], result


# ---------------------------------------------------------------------------
# Module-scoped shared data fixtures
# (heavy random data is generated once per test module)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def instruments():
    return _make_instruments(N_INSTRUMENTS)


@pytest.fixture(scope="module")
def daily_dates():
    return _make_daily_dates(N_DATES)


@pytest.fixture(scope="module")
def prices(daily_dates, instruments):
    return _make_prices(daily_dates, instruments)


@pytest.fixture(scope="module")
def fx_spot(daily_dates):
    return _make_fx_spot_prices(daily_dates)


@pytest.fixture(scope="module")
def fx_fwd(daily_dates):
    return _make_fx_forward_prices(daily_dates)


@pytest.fixture(scope="module")
def composition(instruments):
    return _make_composition(instruments)


@pytest.fixture(scope="module")
def dividends(daily_dates, instruments):
    return _make_dividends(daily_dates, instruments)


@pytest.fixture(scope="module")
def return_calculator():
    return ReturnCalculator("percentage")


# ---------------------------------------------------------------------------
# TerComponent
# ---------------------------------------------------------------------------

class TestTerComponentPerformance:
    """TerComponent daily path: 800 instruments × 11 dates."""

    TIME_BUDGET_MS = 100  # generous ceiling; optimized target < 10 ms

    @pytest.fixture
    def component(self, instruments, return_calculator):
        rng = np.random.default_rng(45)
        ters = {isin: rng.uniform(0.0001, 0.005) for isin in instruments}
        c = TerComponent(ters)
        c.set_return_calculator(return_calculator)
        return c

    def test_daily_under_budget(self, component, instruments, daily_dates):
        median_ms, result = _median_ms(
            lambda: component.calculate_adjustment(
                instruments=instruments, dates=daily_dates
            )
        )
        print(
            f"\n  TerComponent daily [{N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.TIME_BUDGET_MS} ms)"
        )
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert median_ms < self.TIME_BUDGET_MS, (
            f"TerComponent too slow: {median_ms:.1f} ms (budget {self.TIME_BUDGET_MS} ms)"
        )

    def test_values_negative(self, component, instruments, daily_dates):
        """TER adjustments must be non-positive (expense deducted from returns)."""
        result = component.calculate_adjustment(instruments=instruments, dates=daily_dates)
        assert (result <= 1e-12).all().all(), "TER adjustments should be ≤ 0"

    def test_values_non_zero_after_first_date(self, component, instruments, daily_dates):
        """Year-fraction is 0 on the first date; all others should carry TER."""
        result = component.calculate_adjustment(instruments=instruments, dates=daily_dates)
        assert (result.iloc[1:] != 0).sum().sum() > 0, "Expected non-zero TER after day 0"


# ---------------------------------------------------------------------------
# FxSpotComponent
# ---------------------------------------------------------------------------

class TestFxSpotComponentPerformance:
    """FxSpotComponent: cold (cache miss) and warm (cache hit) paths."""

    BUDGET_COLD_MS = 100
    BUDGET_WARM_MS = 20  # cache hit should be much faster

    @pytest.fixture
    def component(self, composition, fx_spot, return_calculator):
        """Fresh component (no cache populated yet)."""
        c = FxSpotComponent(fx_composition=composition, fx_prices=fx_spot)
        c.set_return_calculator(return_calculator)
        return c

    def test_cache_miss_under_budget(self, composition, fx_spot, return_calculator, instruments, daily_dates):
        """First call (full computation, no cache) must finish under budget."""
        c = FxSpotComponent(fx_composition=composition, fx_prices=fx_spot)
        c.set_return_calculator(return_calculator)

        t0 = time.perf_counter()
        result = c.calculate_adjustment(instruments=instruments, dates=daily_dates)
        elapsed_ms = (time.perf_counter() - t0) * 1e3

        print(
            f"\n  FxSpotComponent cache-miss [{N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{elapsed_ms:.1f} ms  (budget {self.BUDGET_COLD_MS} ms)"
        )
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert elapsed_ms < self.BUDGET_COLD_MS, (
            f"FxSpotComponent (cold) too slow: {elapsed_ms:.1f} ms"
        )

    def test_cache_hit_under_budget(self, component, instruments, daily_dates):
        """
        Subsequent calls (adjustments_cache populated) must be under warm budget.
        _median_ms issues one warmup call, then 5 timed cache-hit calls.
        """
        median_ms, result = _median_ms(
            lambda: component.calculate_adjustment(
                instruments=instruments, dates=daily_dates
            )
        )
        print(
            f"\n  FxSpotComponent cache-hit  [{N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.BUDGET_WARM_MS} ms)"
        )
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert median_ms < self.BUDGET_WARM_MS, (
            f"FxSpotComponent (warm) too slow: {median_ms:.1f} ms"
        )

    def test_values_non_zero(self, component, instruments, daily_dates):
        result = component.calculate_adjustment(instruments=instruments, dates=daily_dates)
        assert (result != 0).sum().sum() > 0, "All FX-spot adjustments are zero"


# ---------------------------------------------------------------------------
# FxForwardCarryComponent
# ---------------------------------------------------------------------------

class TestFxForwardCarryPerformance:
    """FxForwardCarryComponent daily path: 800 instruments × 11 dates."""

    TIME_BUDGET_MS = 100

    @pytest.fixture
    def component(self, composition, fx_fwd, fx_spot, return_calculator):
        c = FxForwardCarryComponent(fwd_composition=composition, fx_forward_points=fx_fwd, tenor="1M",
                                    fx_spot_prices=fx_spot)
        c.set_return_calculator(return_calculator)
        return c

    def test_daily_under_budget(self, component, instruments, daily_dates):
        median_ms, result = _median_ms(
            lambda: component.calculate_adjustment(
                instruments=instruments, dates=daily_dates
            )
        )
        print(
            f"\n  FxForwardCarryComponent daily [{N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.TIME_BUDGET_MS} ms)"
        )
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert median_ms < self.TIME_BUDGET_MS, (
            f"FxForwardCarryComponent too slow: {median_ms:.1f} ms"
        )

    def test_values_non_zero(self, component, instruments, daily_dates):
        result = component.calculate_adjustment(instruments=instruments, dates=daily_dates)
        assert (result != 0).sum().sum() > 0, "All carry adjustments are zero"


# ---------------------------------------------------------------------------
# DividendComponent
# ---------------------------------------------------------------------------

class TestDividendComponentPerformance:
    """
    DividendComponent daily path.
    N_DIVIDEND_INSTRUMENTS (100) instruments pay a dividend on day 6.
    """

    TIME_BUDGET_MS = 100

    @pytest.fixture
    def component(self, dividends, prices, fx_spot, return_calculator):
        c = DividendComponent(
            dividends=dividends,
            instrument_prices=prices,
            fx_prices=fx_spot,
        )
        c.set_return_calculator(return_calculator)
        return c

    def test_daily_under_budget(self, component, instruments, daily_dates):
        median_ms, result = _median_ms(
            lambda: component.calculate_adjustment(
                instruments=instruments, dates=daily_dates
            )
        )
        print(
            f"\n  DividendComponent daily [{N_DATES}d×{N_INSTRUMENTS}i, "
            f"{N_DIVIDEND_INSTRUMENTS} paying]: "
            f"{median_ms:.1f} ms  (budget {self.TIME_BUDGET_MS} ms)"
        )
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert median_ms < self.TIME_BUDGET_MS, (
            f"DividendComponent too slow: {median_ms:.1f} ms"
        )

    def test_dividends_applied_on_correct_date(self, component, instruments, daily_dates):
        """Dividend adjustments should appear on date index 5 (the payment date)."""
        result = component.calculate_adjustment(instruments=instruments, dates=daily_dates)
        div_inst_ids = list(instruments.keys())[:N_DIVIDEND_INSTRUMENTS]
        non_zero_on_div_date = (result.iloc[5][div_inst_ids] != 0).sum()
        assert non_zero_on_div_date > 0, "Expected dividend adjustments on payment date"

    def test_zero_on_non_dividend_dates(self, component, instruments, daily_dates):
        """Non-paying instruments must always have zero adjustment."""
        result = component.calculate_adjustment(instruments=instruments, dates=daily_dates)
        non_div_ids = list(instruments.keys())[N_DIVIDEND_INSTRUMENTS:]
        assert (result[non_div_ids] == 0).all().all()


# ---------------------------------------------------------------------------
# Adjuster – all four components combined (daily path)
# ---------------------------------------------------------------------------

class TestAdjusterDailyPerformance:
    """Full daily Adjuster with all 4 components."""

    BUDGET_CALC_MS = 200
    BUDGET_RETURNS_MS = 200

    @pytest.fixture
    def adjuster(self, prices, instruments, composition, fx_fwd, fx_spot, dividends):
        adj = Adjuster(
            prices=prices,
            instruments=instruments,
            is_intraday=False,
            return_type="percentage",
        )
        rng = np.random.default_rng(45)
        ters = {isin: rng.uniform(0.0001, 0.005) for isin in instruments}
        adj.add(TerComponent(ters))
        adj.add(FxSpotComponent(fx_composition=composition, fx_prices=fx_spot))
        adj.add(FxForwardCarryComponent(fwd_composition=composition, fx_forward_points=fx_fwd, tenor="1M",
                                        fx_spot_prices=fx_spot))
        adj.add(DividendComponent(
            dividends=dividends,
            instrument_prices=prices,
            fx_prices=fx_spot,
        ))
        return adj

    def test_calculate_adjustment_under_budget(self, adjuster):
        """calculate_adjustment across all 4 components must stay under budget."""
        median_ms, result = _median_ms(lambda: adjuster.calculate_adjustment())
        print(
            f"\n  Adjuster.calculate_adjustment [4 components, {N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.BUDGET_CALC_MS} ms)"
        )
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert median_ms < self.BUDGET_CALC_MS, (
            f"Adjuster.calculate_adjustment too slow: {median_ms:.1f} ms"
        )

    def test_get_clean_returns_under_budget(self, adjuster):
        """get_clean_returns (returns + adjustment) must stay under budget."""
        median_ms, result = _median_ms(lambda: adjuster.get_clean_returns())
        print(
            f"\n  Adjuster.get_clean_returns [4 components, {N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.BUDGET_RETURNS_MS} ms)"
        )
        assert result.shape[1] == N_INSTRUMENTS
        assert median_ms < self.BUDGET_RETURNS_MS, (
            f"Adjuster.get_clean_returns too slow: {median_ms:.1f} ms"
        )


# ---------------------------------------------------------------------------
# live_update path
# ---------------------------------------------------------------------------

class TestLiveUpdatePerformance:
    """
    Adjuster.live_update context manager: temporary price/FX tick without
    mutating persistent state.  Measures end-to-end: context entry + inner
    get_clean_returns + context exit (state restore).
    """

    TIME_BUDGET_MS = 300

    @pytest.fixture
    def adjuster(self, prices, instruments, composition, fx_fwd, fx_spot):
        adj = Adjuster(
            prices=prices,
            instruments=instruments,
            is_intraday=False,
            return_type="percentage",
        )
        rng = np.random.default_rng(45)
        ters = {isin: rng.uniform(0.0001, 0.005) for isin in instruments}
        adj.add(TerComponent(ters))
        adj.add(FxSpotComponent(fx_composition=composition, fx_prices=fx_spot))
        adj.add(FxForwardCarryComponent(fwd_composition=composition, fx_forward_points=fx_fwd, tenor="1M",
                                        fx_spot_prices=fx_spot))
        # Warm up component caches with historical data before live ticks
        adj.get_clean_returns()
        return adj

    @pytest.fixture(scope="module")
    def live_prices(self, instruments):
        rng = np.random.default_rng(50)
        return pd.Series(
            rng.uniform(50, 200, size=N_INSTRUMENTS),
            index=list(instruments.keys()),
        )

    @pytest.fixture(scope="module")
    def live_fx(self):
        rng = np.random.default_rng(51)
        return pd.Series(rng.uniform(0.8, 1.5, size=len(CURRENCIES)), index=CURRENCIES)

    def test_live_update_under_budget(self, adjuster, live_prices, live_fx):
        """End-to-end live_update (enter + get_clean_returns + exit) under budget."""

        def run():
            with adjuster.live_update(prices=live_prices, fx_prices=live_fx):
                return adjuster.get_clean_returns()

        median_ms, result = _median_ms(run)
        print(
            f"\n  live_update + get_clean_returns [{N_DATES}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.TIME_BUDGET_MS} ms)"
        )
        assert result.shape[1] == N_INSTRUMENTS
        assert median_ms < self.TIME_BUDGET_MS, (
            f"live_update too slow: {median_ms:.1f} ms"
        )

    def test_live_update_does_not_mutate_prices(self, adjuster, live_prices, live_fx):
        """After the context exits, prices must be restored to original length."""
        prices_before_len = len(adjuster.prices)
        with adjuster.live_update(prices=live_prices, fx_prices=live_fx):
            assert len(adjuster.prices) == prices_before_len + 1, (
                "Price row should be added inside live_update"
            )
        assert len(adjuster.prices) == prices_before_len, (
            "Prices must be restored after live_update exits"
        )

    def test_live_update_result_has_extra_row(self, adjuster, live_prices, live_fx):
        """Inside live_update, get_clean_returns should include the new price row."""
        n_before = adjuster.get_clean_returns().shape[0]
        with adjuster.live_update(prices=live_prices, fx_prices=live_fx):
            result = adjuster.get_clean_returns()
        assert result.shape[0] == n_before + 1, (
            f"Expected {n_before + 1} return rows inside live_update, got {result.shape[0]}"
        )


# ---------------------------------------------------------------------------
# append_update path
# ---------------------------------------------------------------------------

class TestAppendUpdatePerformance:
    """
    Adjuster.append_update: permanently extend prices and FX data, then
    measure get_clean_returns on the enlarged dataset.
    """

    TIME_BUDGET_MS = 300

    def _make_adjuster(self, prices, instruments, composition, fx_fwd, fx_spot):
        adj = Adjuster(
            prices=prices,
            instruments=instruments,
            is_intraday=False,
            return_type="percentage",
        )
        rng = np.random.default_rng(45)
        ters = {isin: rng.uniform(0.0001, 0.005) for isin in instruments}
        adj.add(TerComponent(ters))
        adj.add(FxSpotComponent(fx_composition=composition, fx_prices=fx_spot))
        adj.add(FxForwardCarryComponent(fwd_composition=composition, fx_forward_points=fx_fwd, tenor="1M",
                                        fx_spot_prices=fx_spot))
        # Warm up component caches before appending new data
        adj.get_clean_returns()
        return adj

    @pytest.fixture(scope="module")
    def new_prices(self, instruments):
        rng = np.random.default_rng(52)
        return pd.Series(
            rng.uniform(50, 200, size=N_INSTRUMENTS),
            index=list(instruments.keys()),
        )

    @pytest.fixture(scope="module")
    def new_fx(self):
        rng = np.random.default_rng(53)
        return pd.Series(rng.uniform(0.8, 1.5, size=len(CURRENCIES)), index=CURRENCIES)

    def test_append_update_under_budget(
        self, prices, instruments, composition, fx_fwd, fx_spot, new_prices, new_fx
    ):
        """append_update + get_clean_returns on N_DATES+1 prices under budget."""
        timings = []
        result = None
        for _ in range(3):
            adj = self._make_adjuster(prices, instruments, composition, fx_fwd, fx_spot)
            t0 = time.perf_counter()
            adj.append_update(prices=new_prices, fx_prices=new_fx)
            result = adj.get_clean_returns()
            timings.append((time.perf_counter() - t0) * 1e3)

        median_ms = sorted(timings)[len(timings) // 2]
        print(
            f"\n  append_update + get_clean_returns [{N_DATES+1}d×{N_INSTRUMENTS}i]: "
            f"{median_ms:.1f} ms  (budget {self.TIME_BUDGET_MS} ms)"
        )
        assert result.shape[1] == N_INSTRUMENTS
        assert median_ms < self.TIME_BUDGET_MS, (
            f"append_update too slow: {median_ms:.1f} ms"
        )

    def test_append_update_persists_price_row(
        self, prices, instruments, composition, fx_fwd, fx_spot, new_prices, new_fx
    ):
        """Price history must grow by exactly 1 row after append_update."""
        adj = self._make_adjuster(prices, instruments, composition, fx_fwd, fx_spot)
        before = len(adj.prices)
        adj.append_update(prices=new_prices, fx_prices=new_fx)
        assert len(adj.prices) == before + 1, (
            f"Expected {before + 1} price rows after append, got {len(adj.prices)}"
        )

    def test_append_update_returns_shape(
        self, prices, instruments, composition, fx_fwd, fx_spot, new_prices, new_fx
    ):
        """get_clean_returns after append_update must cover N_DATES return rows."""
        adj = self._make_adjuster(prices, instruments, composition, fx_fwd, fx_spot)
        adj.append_update(prices=new_prices, fx_prices=new_fx)
        result = adj.get_clean_returns()
        # ReturnCalculator keeps the first row → N_DATES+1 prices → N_DATES+1 return rows
        assert result.shape == (N_DATES + 1, N_INSTRUMENTS), (
            f"Unexpected shape after append_update: {result.shape}"
        )
