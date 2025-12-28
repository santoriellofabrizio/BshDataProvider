import logging
from contextlib import contextmanager
from typing import Optional

import pandas as pd
from tqdm import tqdm

from core.holidays.holiday_manager import HolidayManager

logger = logging.getLogger(__name__)


class BaseFetcher:
    """Fetcher base con metodi comuni."""

    def __init__(self, show_progress=True):
        self.show_progress = show_progress
        self.holidays = HolidayManager()

        # ============================================================
        # PROGRESS BAR SUPPORT
        # ============================================================

    @contextmanager
    def progress(self, desc: str, total: Optional[int] = None):
        """
        Context manager for tqdm progress bars.

        Args:
            desc (str): Description for the progress bar.
            total (int, optional): Total iterations count.

        Example:
            with self.progress("Fetching EUREX", total=len(days)) as pbar:
                for d in days:
                    ...
                    pbar.update(1)
        """
        if not self.show_progress or not total or total <= 1:
            # Dummy bar if disabled or trivial
            class DummyBar:
                def update(self, *_): pass

                def close(self): pass

            yield DummyBar()
            return

        pbar = tqdm(
            total=total,
            desc=desc,
            dynamic_ncols=True,
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )
        try:
            yield pbar
        finally:
            pbar.close()

    # ------------------------------------------------------------
    # Utility comuni
    # ------------------------------------------------------------
    @staticmethod
    def _normalize_date(value):
        return value.date() if hasattr(value, "date") else value

    @staticmethod
    def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            "datetime_sampled": "timestamp",
            "datetime": "timestamp",
            "currency_pair": "isin",
            "bid_px_lev_0": "bid",
            "ask_px_lev_0": "ask",
            "mid_price": "mid",
            "bid_price": "bid",
            "ask_price": "ask",
        }
        df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
        if "mid" not in df.columns and {"bid", "ask"} <= set(df.columns):
            df["mid"] = (df["bid"] + df["ask"]) / 2
        if "spread" not in df.columns and {"bid", "ask"} <= set(df.columns):
            df["spread"] = (df["ask"] - df["bid"]) / 2
        if "spread_pct" not in df.columns and {"bid", "ask"} <= set(df.columns):
            df["spread_pct"] = (df["ask"] - df["bid"]) / (df["ask"] + df["bid"]).replace(0, pd.NA)
        if "timestamp" in df.columns and "date" not in df.columns:
            df["date"] = pd.to_datetime(df["timestamp"]).dt.date

        return df

    @staticmethod
    def _frequency_to_seconds(freq: str) -> int:
        mapping = {"tick": 1, "1m": 60, "5m": 300, "15m": 900}
        return mapping.get(freq, 60)

