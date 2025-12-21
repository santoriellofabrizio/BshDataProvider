# future_classifier.py
import re

import pandas as pd
from core.utils.memory_provider import cache_bsh_data
from .base_classifier import BaseClassifier

FUTURE_MONTHS = {"03", "06", "09", "12"}
BBG_MONTHS = {"H", "M", "U", "Z"}  # bloomberg quarterly codes


class FutureClassifier(BaseClassifier):

    @cache_bsh_data
    def _load(self):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("FutureClassifier: manca QueryOracle")
            self._df = pd.DataFrame(self.oracle.get_futures_data())
        return self._df

    # ------------------------------------------------------------
    def matches(self, identifier: str) -> bool:
        df = self._load()
        idu = identifier.upper()

        cols = ["ISIN", "CONTRACT", "TICKER",
                "ACTIVE_ISIN", "ACTIVE_CONTRACT", "EXCH_SYMBOL"]

        # Direct match
        for _, row in df.iterrows():
            if any(idu == str(row.get(c, "")).upper() for c in cols):
                return True

        # EXCH_SYMBOL trimmed of numbers
        clean = idu
        for y in range(2020, 2035):
            clean = clean.replace(str(y), "")
        for m in range(1, 13):
            clean = clean.replace(str(m), "")

        return clean in df["EXCH_SYMBOL"].astype(str).str.upper().values

    @staticmethod
    def is_contract(identifier: str) -> bool:
        s = identifier.upper()

        # 1) Check YYYYMM (with quarterly month)
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", s)
        if m:
            year = m.group(1)
            month = m.group(2)
            if month in FUTURE_MONTHS:
                return True

        # 2) Check Bloomberg short code (H25, M27, U30, Z24)
        if re.search(r"[HMUZ]\d{2}", s):
            return True

        return False



    # ------------------------------------------------------------
    def get_metadata(self, identifier: str):
        df = self._load()
        up = identifier.upper()

        clean = up
        for y in range(2020, 2035):
            clean = clean.replace(str(y), "")
        for m in range(1, 13):
            clean = clean.replace(str(m), "")

        cols = ["ACTIVE_ISIN", "ISIN", "CONTRACT",
                "ACTIVE_CONTRACT", "TICKER", "EXCH_SYMBOL"]

        row = df[df[cols].apply(lambda r: r.astype(str).str.upper().eq(clean).any(), axis=1)]
        if row.empty:
            return {}

        r = row.iloc[0]
        return {
            "root": r.get("TICKER_ROOT") or r.get("TICKER"),
            "future_underlying": r.get("UNDERLYING_TYPE"),
            "suffix": r.get("BBG_TYPE"),
            "is_active_form": up in str(r.get("ACTIVE_ISIN")) or up in str(r.get("ACTIVE_CONTRACT")),
            "timescale_root": r.get("EXCH_SYMBOL"),
            "future_currency": r.get("CURRENCY"),
        }

    def get_ccy(self, isin, market):
        raise NotImplementedError
