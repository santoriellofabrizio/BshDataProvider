
import re
from datetime import date as _date, datetime as _datetime

import numpy as np
import pandas as pd

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def _flatten_excel_arg(val):
    """
    Converte input Excel (cella singola, riga, colonna, area 2D)
    in lista 1D di stringhe pulite.
    """
    if val is None:
        return []
    if isinstance(val, (list, tuple)):
        out = []
        for row in val:
            if isinstance(row, (list, tuple)):
                for v in row:
                    if v not in (None, ""):
                        out.append(str(v).strip())
            else:
                if row not in (None, ""):
                    out.append(str(row).strip())
        return out
    if val in (None, ""):
        return []
    return [str(val).strip()]


def _split_ids_isin_ticker(ids_or_tickers):
    """
    Prende uno o più identificativi (ISIN o ticker) e li separa.
    Ritorna:
      - values: lista originale pulita
      - isins: lista con ISIN o None
      - tickers: lista con ticker o None
    """
    values = _flatten_excel_arg(ids_or_tickers)
    isins = []
    tickers = []
    for v in values:
        vu = v.upper()
        if ISIN_RE.match(vu):
            isins.append(vu)
            tickers.append(None)
        else:
            isins.append(None)
            tickers.append(v)
    return values, isins, tickers


def _parse_options(options):
    """
    Converte:
      - stringa: "key1=val1;key2=val2"
      - range: ogni cella "key=val"
    in dict con tipi base (bool, int, float, date, str).
    """
    if options is None:
        return {}

    if isinstance(options, (list, tuple)):
        parts = []
        for row in options:
            if isinstance(row, (list, tuple)):
                parts.extend(str(v).strip() for v in row if v not in (None, ""))
            elif row not in (None, ""):
                parts.append(str(row).strip())
        text = ";".join(parts)
    else:
        text = str(options).strip()

    if not text:
        return {}

    out = {}
    for chunk in re.split(r"[;,\n]", text):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue

        k, v = chunk.split("=", 1)
        key = k.strip()
        val = v.strip()
        if not key:
            continue

        low = val.lower()

        # bool
        if low in ("true", "false"):
            out[key] = (low == "true")
            continue

        # int
        try:
            out[key] = int(val)
            continue
        except ValueError:
            pass

        # float
        try:
            out[key] = float(val.replace(",", "."))
            continue
        except ValueError:
            pass

        # date ISO YYYY-MM-DD
        try:
            if len(val) == 10 and val[4] == "-" and val[7] == "-":
                out[key] = _date.fromisoformat(val)
                continue
        except Exception:
            pass

        # fallback string
        out[key] = val

    return out


def _is_time_index(idx):
    if not len(idx):
        return False
    if pd.api.types.is_datetime64_any_dtype(idx):
        return True
    first = idx[0]
    return isinstance(first, (_date, _datetime))


def _format_result_for_excel(result):
    """
    Regole:
    - DF 1x1 o Series len 1 -> scalare
    - DF 1xN -> valori orizzontali
    - DF Nx1 -> colonna verticale
    - Series con index temporale -> [data, valore] in verticale
    - Series generica -> valori orizzontali
    - dict semplice -> valori orizzontali
    - list/ndarray -> orizzontale (o DF se list of dict)
    - DF multi-col con index temporale -> tabella così com'è
    - DF generico -> tabella così com'è
    """
    if result is None:
        return pd.DataFrame()

    # DataFrame
    if isinstance(result, pd.DataFrame):
        if result.empty:
            return pd.DataFrame()

        rows, cols = result.shape

        # 1x1
        if rows == 1 and cols == 1:
            return result.iat[0, 0]

        # 1xN -> orizzontale
        if rows == 1 and cols > 1:
            return result

        # Nx1 -> verticale
        if cols == 1:
            s = result.iloc[:, 0]
            if len(s) == 1:
                return s.iloc[0]
            if _is_time_index(result.index):
                return [[idx, val] for idx, val in s.items()]
            return [[v] for v in s.tolist()]

        # multi-col
        if _is_time_index(result.index):
            return result
        return result

    # Series
    if isinstance(result, pd.Series):
        if result.empty:
            return None
        if len(result) == 1:
            return result.iloc[0]
        if _is_time_index(result.index):
            return [[idx, val] for idx, val in result.items()]
        return [result.tolist()]

    # dict
    if isinstance(result, dict):
        if not result:
            return None
        if any(isinstance(v, dict) for v in result.values()):
            return result
        return [list(result.values())]

    # list / array
    if isinstance(result, (list, tuple, np.ndarray)):
        if not result:
            return None
        if isinstance(result[0], dict):
            return pd.DataFrame(result)
        return [list(result)]

    # scalar
    return result