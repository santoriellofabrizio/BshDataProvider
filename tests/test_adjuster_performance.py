"""
Performance tests for TerComponent and FxForwardCarryComponent.

Verifies that the daily path of calculate_adjustment runs within
acceptable time budgets at production scale (800 instruments × 11 dates).
"""
import time
import numpy as np
import pandas as pd
import pytest
from datetime import datetime

from sfm_data_provider.analytics.adjustments.ter import TerComponent
from sfm_data_provider.analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from sfm_data_provider.analytics.adjustments.adjuster import Adjuster
from sfm_data_provider.analytics.adjustments.return_calculations import ReturnCalculator
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.instruments.instruments import EtfInstrument


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_instruments(n: int) -> dict[str, EtfInstrument]:
    """Create n EtfInstrument objects with EUR currency."""
    instruments: dict[str, EtfInstrument] = {}
    for i in range(n):
        isin = f"IE{i:010d}"
        inst = EtfInstrument(id=isin, currency="EUR")
        instruments[isin] = inst
    return instruments


def _make_daily_dates(n_dates: int) -> list[datetime]:
    """Return n_dates consecutive business-day midnights as datetime."""
    idx = pd.bdate_range("2026-03-01", periods=n_dates, freq="B")
    return [d.to_pydatetime() for d in idx]


CURRENCIES = ["USD", "GBP", "CHF", "AUD", "DKK", "HKD", "NOK",
              "PLN", "SEK", "CNY", "JPY", "CNH", "CAD", "INR", "BRL"]


def _make_fx_spot_prices(dates: list[datetime]) -> pd.DataFrame:
    """Random FX spot prices for CURRENCIES, indexed by dates."""
    rng = np.random.default_rng(42)
    data = rng.uniform(0.8, 1.5, size=(len(dates), len(CURRENCIES)))
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=CURRENCIES)


def _make_fx_forward_prices(dates: list[datetime]) -> pd.DataFrame:
    """Random FX forward points in basis points for CURRENCIES."""
    rng = np.random.default_rng(43)
    data = rng.uniform(-50, 50, size=(len(dates), len(CURRENCIES)))
    return pd.DataFrame(data, index=pd.DatetimeIndex(dates), columns=CURRENCIES)


def _make_composition(instruments: dict[str, EtfInstrument]) -> pd.DataFrame:
    """Random FX composition matrix (instruments × CURRENCIES), rows sum ≈ 1."""
    rng = np.random.default_rng(44)
    n = len(instruments)
    raw = rng.dirichlet(np.ones(len(CURRENCIES)), size=n)
    return pd.DataFrame(raw, index=list(instruments.keys()), columns=CURRENCIES)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

N_INSTRUMENTS = 800
N_DATES = 11


@pytest.fixture
def instruments():
    return _make_instruments(N_INSTRUMENTS)


@pytest.fixture
def daily_dates():
    return _make_daily_dates(N_DATES)


@pytest.fixture
def prices(daily_dates, instruments):
    """Random price DataFrame (n_dates × n_instruments)."""
    rng = np.random.default_rng(41)
    data = rng.uniform(50, 200, size=(len(daily_dates), len(instruments)))
    return pd.DataFrame(data, index=pd.DatetimeIndex(daily_dates),
                        columns=list(instruments.keys()))


@pytest.fixture
def return_calculator():
    return ReturnCalculator("percentage")


# ---------------------------------------------------------------------------
# TerComponent
# ---------------------------------------------------------------------------

class TestTerComponentPerformance:
    """TerComponent.calculate_adjustment daily path must stay under budget."""

    TIME_BUDGET_MS = 100  # generous ceiling; target <10 ms

    @pytest.fixture
    def ter_component(self, instruments, daily_dates, return_calculator):
        ters = {isin: np.random.default_rng(45).uniform(0.0001, 0.005)
                for isin in instruments}
        comp = TerComponent(ters)
        comp.set_return_calculator(return_calculator)
        return comp

    def test_daily_under_budget(self, ter_component, instruments, daily_dates):
        """calculate_adjustment must complete within TIME_BUDGET_MS."""
        # Warm up the @cached_property
        ter_component.calculate_adjustment(instruments=instruments, dates=daily_dates)

        # Measure
        timings = []
        for _ in range(5):
            t0 = time.perf_counter()
            result = ter_component.calculate_adjustment(
                instruments=instruments, dates=daily_dates
            )
            elapsed_ms = (time.perf_counter() - t0) * 1e3
            timings.append(elapsed_ms)

        median_ms = sorted(timings)[len(timings) // 2]
        print(f"\n  TerComponent median: {median_ms:.1f} ms "
              f"(budget {self.TIME_BUDGET_MS} ms, runs: "
              f"{', '.join(f'{t:.1f}' for t in timings)})")
        assert result.shape == (N_DATES, N_INSTRUMENTS), (
            f"Wrong shape: {result.shape}"
        )
        assert median_ms < self.TIME_BUDGET_MS, (
            f"TerComponent too slow: {median_ms:.1f} ms (budget {self.TIME_BUDGET_MS} ms)"
        )

    def test_values_non_zero(self, ter_component, instruments, daily_dates):
        """Adjustments should contain non-zero values for applicable instruments."""
        result = ter_component.calculate_adjustment(
            instruments=instruments, dates=daily_dates
        )
        # First date has year_fraction = 0, so skip it
        non_zero_count = (result.iloc[1:] != 0).sum().sum()
        assert non_zero_count > 0, "All adjustments are zero — values lost"

    def test_values_negative(self, ter_component, instruments, daily_dates):
        """TER adjustments must be non-positive (cost deducted)."""
        result = ter_component.calculate_adjustment(
            instruments=instruments, dates=daily_dates
        )
        assert (result <= 1e-12).all().all(), "TER adjustments should be <= 0"


# ---------------------------------------------------------------------------
# FxForwardCarryComponent
# ---------------------------------------------------------------------------

class TestFxForwardCarryPerformance:
    """FxForwardCarryComponent.calculate_adjustment daily path must stay under budget."""

    TIME_BUDGET_MS = 100  # generous ceiling; target <10 ms

    @pytest.fixture
    def carry_component(self, instruments, daily_dates, return_calculator):
        composition = _make_composition(instruments)
        fx_fwd = _make_fx_forward_prices(daily_dates)
        fx_spot = _make_fx_spot_prices(daily_dates)
        comp = FxForwardCarryComponent(
            fwd_composition=composition,
            fx_forward_prices=fx_fwd,
            tenor="1M",
            fx_spot_prices=fx_spot,
        )
        comp.set_return_calculator(return_calculator)
        return comp

    def test_daily_under_budget(self, carry_component, instruments, daily_dates):
        """calculate_adjustment must complete within TIME_BUDGET_MS."""
        # Warm up
        carry_component.calculate_adjustment(instruments=instruments, dates=daily_dates)

        timings = []
        for _ in range(5):
            t0 = time.perf_counter()
            result = carry_component.calculate_adjustment(
                instruments=instruments, dates=daily_dates
            )
            elapsed_ms = (time.perf_counter() - t0) * 1e3
            timings.append(elapsed_ms)

        median_ms = sorted(timings)[len(timings) // 2]
        print(f"\n  FxForwardCarryComponent median: {median_ms:.1f} ms "
              f"(budget {self.TIME_BUDGET_MS} ms, runs: "
              f"{', '.join(f'{t:.1f}' for t in timings)})")
        assert result.shape == (N_DATES, N_INSTRUMENTS), (
            f"Wrong shape: {result.shape}"
        )
        assert median_ms < self.TIME_BUDGET_MS, (
            f"FxForwardCarryComponent too slow: {median_ms:.1f} ms "
            f"(budget {self.TIME_BUDGET_MS} ms)"
        )

    def test_values_non_zero(self, carry_component, instruments, daily_dates):
        """Carry adjustments should contain non-zero values."""
        result = carry_component.calculate_adjustment(
            instruments=instruments, dates=daily_dates
        )
        non_zero_count = (result != 0).sum().sum()
        assert non_zero_count > 0, "All carry adjustments are zero — values lost"


# ---------------------------------------------------------------------------
# End-to-end Adjuster daily path
# ---------------------------------------------------------------------------

class TestAdjusterDailyPerformance:
    """Full Adjuster pipeline for the daily path must stay under budget."""

    TIME_BUDGET_MS = 200

    @pytest.fixture
    def adjuster(self, prices, instruments):
        adj = Adjuster(
            prices=prices,
            instruments=instruments,
            is_intraday=False,
            return_type="percentage",
        )

        # TER
        ters = {isin: np.random.default_rng(45).uniform(0.0001, 0.005)
                for isin in instruments}
        adj.add(TerComponent(ters))

        # FxForwardCarry
        composition = _make_composition(instruments)
        fx_fwd = _make_fx_forward_prices(prices.index.tolist())
        fx_spot = _make_fx_spot_prices(prices.index.tolist())
        adj.add(FxForwardCarryComponent(
            fwd_composition=composition,
            fx_forward_prices=fx_fwd,
            tenor="1M",
            fx_spot_prices=fx_spot,
        ))

        return adj

    def test_calculate_adjustment_under_budget(self, adjuster):
        """Full calculate_adjustment across all components under budget."""
        # Warm up
        adjuster.calculate_adjustment()

        timings = []
        for _ in range(5):
            t0 = time.perf_counter()
            result = adjuster.calculate_adjustment()
            elapsed_ms = (time.perf_counter() - t0) * 1e3
            timings.append(elapsed_ms)

        median_ms = sorted(timings)[len(timings) // 2]
        print(f"\n  Adjuster.calculate_adjustment median: {median_ms:.1f} ms "
              f"(budget {self.TIME_BUDGET_MS} ms, runs: "
              f"{', '.join(f'{t:.1f}' for t in timings)})")
        assert result.shape == (N_DATES, N_INSTRUMENTS)
        assert median_ms < self.TIME_BUDGET_MS, (
            f"Adjuster.calculate_adjustment too slow: {median_ms:.1f} ms "
            f"(budget {self.TIME_BUDGET_MS} ms)"
        )

    def test_get_clean_returns_under_budget(self, adjuster):
        """Full get_clean_returns pipeline under budget."""
        # Warm up
        adjuster.get_clean_returns()

        timings = []
        for _ in range(5):
            t0 = time.perf_counter()
            result = adjuster.get_clean_returns()
            elapsed_ms = (time.perf_counter() - t0) * 1e3
            timings.append(elapsed_ms)

        median_ms = sorted(timings)[len(timings) // 2]
        print(f"\n  Adjuster.get_clean_returns median: {median_ms:.1f} ms "
              f"(budget {self.TIME_BUDGET_MS} ms, runs: "
              f"{', '.join(f'{t:.1f}' for t in timings)})")
        assert result.shape[1] == N_INSTRUMENTS
        assert median_ms < self.TIME_BUDGET_MS, (
            f"Adjuster.get_clean_returns too slow: {median_ms:.1f} ms "
            f"(budget {self.TIME_BUDGET_MS} ms)"
        )
