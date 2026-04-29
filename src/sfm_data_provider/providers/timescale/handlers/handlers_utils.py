import logging

import pandas as pd


def _freq_to_seconds(freq: str) -> int:
    # es: "5m" → 300
    freq = freq.lower()
    if freq.endswith("m"):
        return int(freq[:-1]) * 60
    if freq.endswith("s"):
        return int(freq[:-1])
    if freq.endswith("h"):
        return int(freq[:-1]) * 60 * 60
    raise ValueError(f"Unsupported frequency: {freq}")


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

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

    # Rename campi
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    cols = set(df.columns)

    # Ricostruzione mid
    if "mid" not in cols and {"bid", "ask"} <= cols:
        df["mid"] = (df["bid"] + df["ask"]) / 2

    # spread = (ask - bid) / 2   ← questa è la TUA formula originale
    if "spread" not in cols and {"bid", "ask"} <= cols:
        df["spread"] = (df["ask"] - df["bid"]) / 2

    # FIX: calcolo denominatore separato per evitare problemi di operator precedence
    # spread_pct = (ask - bid) / (ask + bid)
    if "spread_pct" not in cols and {"bid", "ask"} <= cols:
        denom = (df["ask"] + df["bid"]).replace(0, pd.NA)
        df["spread_pct"] = (df["ask"] - df["bid"]) / denom

    # Costruzione date da timestamp
    if "timestamp" in df.columns and "date" not in df.columns:
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date

    return df


def _build_results(
        df: pd.DataFrame,
        requests: list,
        fields: list[str],
        is_daily: bool,
        business_days,
        fstart,
        fend,
) -> dict:
    """
    Ricostruisce il dizionario dei risultati coerenti per ogni strumento.

    Output finale:
    {
        "IE00B4L5Y983": {
            "PX_LAST": {date1: val1, date2: val2, ...},
            "PX_OPEN": {date1: val1, ...},
        },
        ...
    }
    """

    df = _ensure_columns_are_upper(df)
    results: dict[str, dict[str, dict]] = {}

    # ref_index SOLO per daily (per intraday non serve)
    ref_index = business_days if is_daily else (df["TIMESTAMP"] if not df.empty else [fstart, fend])
    ref_index = pd.DatetimeIndex(ref_index)

    # Cicla ogni request (instrument)
    for req in requests:
        isin = req.instrument.isin or req.instrument.id

        if not df.empty:
            # FIX: loop su ref_index con variabile `d`, rimosso il pre-loop `subs` che
            # usava `first` (undefined) e veniva comunque sovrascritto qui dentro
            if callable(req.subscription):
                subs = [req.subscription(d) for d in ref_index]
            else:
                subs = [req.subscription]

            sub_df = df[df["ISIN"].isin(subs)]

            if sub_df.empty:
                # Se non ci sono dati per quell'ISIN
                if is_daily:
                    # Daily: serie di None per tutte le business days
                    results[req.instrument.id] = {
                        f: pd.Series([None] * len(ref_index), index=ref_index).to_dict()
                        for f in fields
                    }
                else:
                    # Intraday: dict vuoto (non ci sono dati)
                    results[req.instrument.id] = {f: {} for f in fields}
                continue

            # Costruisce l'indice temporale come DatetimeIndex
            idx = pd.to_datetime(sub_df["DATE"] if is_daily else sub_df["TIMESTAMP"])

            # Crea dizionario field → {timestamp: valore}
            if is_daily:
                # Daily: reindex su business_days per garantire date complete
                results[req.instrument.id] = {
                    f: (
                        pd.Series(sub_df[f].values, index=idx)
                        .groupby(level=0)
                        .mean()
                        .reindex(ref_index)
                        .to_dict()
                    )
                    for f in fields
                    if f in sub_df.columns
                }
            else:
                # Intraday: usa SOLO i timestamp effettivi dai dati (NO reindex!)
                results[req.instrument.id] = {
                    f: (
                        pd.Series(sub_df[f].values, index=idx.sort_values())
                        .loc[fstart:fend]
                        .groupby(level=0)
                        .mean()
                        .to_dict()
                    )
                    for f in fields
                    if f in sub_df.columns
                }

    return results


def _ensure_columns_are_upper(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.columns = df.columns.str.upper()
    except Exception as e:
        logging.warning(f"Failed to convert columns to uppercase: {e}")
    finally:
        return df