# future_classifier.py
import re

import pandas as pd
from sfm_data_provider.core.utils.memory_provider import cache_bsh_data
from .base_classifier import BaseClassifier

FUTURE_MONTHS = {"03", "06", "09", "12"}
BBG_MONTHS = {"H", "M", "U", "Z"}
BBG_SUFFIXES = [" INDEX", " COMDTY"]

# Pattern per codice contratto Bloomberg alla fine: H6, M25, U2026, Z9
_RE_BBG_CONTRACT = re.compile(r"[HMUZ]\d{1,2}$")
# Pattern per YYYYMM alla fine
_RE_YYYYMM = re.compile(r"20\d{2}(?:0[1-9]|1[0-2])$")


def _strip_bbg_suffix(s: str) -> str:
    """Rimuove suffissi Bloomberg tipo ' Index', ' Comdty' (case-insensitive)."""
    out = s.strip().upper()
    for sfx in BBG_SUFFIXES:
        if out.endswith(sfx):
            out = out[: -len(sfx)].strip()
    return out


def _extract_root(identifier: str) -> str:
    """
    Da un identifier Bloomberg estrae il root del future.
    Es: TUH6 COMDTY -> TU, TUA COMDTY -> TUA,
        ESM25 INDEX -> ES, ES202506 INDEX -> ES
    """
    clean = _strip_bbg_suffix(identifier)

    # Prova a rimuovere codice contratto BBG (H6, M25, ...)
    root = _RE_BBG_CONTRACT.sub("", clean)
    if root and root != clean:
        return root

    # Prova a rimuovere YYYYMM
    root = _RE_YYYYMM.sub("", clean)
    if root and root != clean:
        return root

    # Prova a rimuovere active suffix (singola lettera BBG alla fine, es. TUA -> TU)
    if len(clean) > 1 and clean[-1] in BBG_MONTHS:
        return clean[:-1]

    return clean


class FutureClassifier(BaseClassifier):

    def _load(self):
        if self._df is None:
            if not self.oracle:
                raise RuntimeError("FutureClassifier: manca QueryOracle")
            self._df = pd.DataFrame(self.oracle.get_futures_data())
        return self._df

    # ------------------------------------------------------------
    def matches(self, identifier: str) -> bool:
        df = self._load()
        idu = identifier.strip().upper()

        cols = [
            "ISIN", "CONTRACT", "TICKER",
            "ACTIVE_ISIN", "ACTIVE_CONTRACT", "EXCH_SYMBOL",
        ]

        # 1) Direct match su tutte le colonne
        for c in cols:
            if c in df.columns and idu in df[c].astype(str).str.upper().values:
                return True

        # 2) Match dopo aver rimosso suffisso Bloomberg (" INDEX", " COMDTY")
        stripped = _strip_bbg_suffix(idu)
        if stripped != idu:
            for c in cols:
                if c in df.columns and stripped in df[c].astype(str).str.upper().values:
                    return True

        # 3) Match sul root estratto vs EXCH_SYMBOL
        root = _extract_root(idu)
        if root:
            exch = df["EXCH_SYMBOL"].astype(str).str.upper().values
            if root in exch:
                return True

        return False

    # ------------------------------------------------------------
    @staticmethod
    def is_contract(identifier: str) -> bool:
        """
        True se l'identifier contiene un codice contratto specifico
        (non active form).
        Es: TUH6 COMDTY -> True, ESM25 INDEX -> True,
            ES202506 -> True, TUA COMDTY -> False
        """
        s = _strip_bbg_suffix(identifier)

        # 1) YYYYMM con mese trimestrale
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", s)
        if m:
            month = m.group(2)
            if month in FUTURE_MONTHS:
                return True

        # 2) Bloomberg short code: H6, M25, U9, Z24 (1-2 cifre)
        if _RE_BBG_CONTRACT.search(s):
            return True

        return False

    # ------------------------------------------------------------
    def get_metadata(self, identifier: str):
        df = self._load()
        up = identifier.strip().upper()
        root = _extract_root(up)
        stripped = _strip_bbg_suffix(up)

        cols = [
            "ACTIVE_ISIN", "ISIN", "CONTRACT",
            "ACTIVE_CONTRACT", "TICKER", "EXCH_SYMBOL",
        ]

        # Cerca prima match esatto (stripped), poi per root
        row = pd.DataFrame()
        for candidate in (stripped, root):
            if not candidate:
                continue
            mask = df[cols].apply(
                lambda r: r.astype(str).str.upper().eq(candidate).any(), axis=1
            )
            row = df[mask]
            if not row.empty:
                break

        if row.empty:
            return {}

        r = row.iloc[0]

        # Determina se è active form: singola lettera BBG alla fine (TUA, ESZ)
        is_active = (
            not self.is_contract(up)
            and len(stripped) > 1
            and stripped[-1] in BBG_MONTHS
        )

        return {
            "root": r.get("TICKER_ROOT") or r.get("TICKER"),
            "future_underlying": r.get("UNDERLYING_TYPE"),
            "suffix": r.get("BBG_TYPE"),
            "is_active_form": is_active,
            "timescale_root": r.get("EXCH_SYMBOL"),
            "future_currency": r.get("CURRENCY"),
        }

    # ------------------------------------------------------------
    def get_ccy(self, isin, market):
        raise NotImplementedError