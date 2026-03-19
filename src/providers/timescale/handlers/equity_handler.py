from datetime import time
import pandas as pd
from providers.timescale.handlers.base_handlers import Handler
from core.enums.markets import Market
from providers.timescale.handlers.handlers_utils import _build_results, _freq_to_seconds, _normalize_dataframe


class EquityHandler(Handler):

    def can_handle(self, req):
        t = req.instrument.type.upper()
        return t in ("ETP", "STOCK")

    # ------------------------------------------------------------------
    # PROCESS IDENTICO AL VECCHIO FETCHER
    # ------------------------------------------------------------------
    def process(self, requests, query):
        if not requests:
            return {}

        first = requests[0]
        fields = first.fields if isinstance(first.fields, list) else [first.fields]
        is_daily = "d" in str(first.frequency).lower()

        subs = [r.subscription or r.instrument.id for r in requests]
        req_by_sub = {sub: r for sub, r in zip(subs, requests)}

        # Market / segment
        market = first.market
        currency = getattr(first.currency, "value", first.currency)
        segment = None
        if market and market.upper() in Market.get_timescale_segments():
            segment = market
            market = Market.get_timescale_segments()[market.upper()]

        snapshot_time = getattr(first, "snapshot_time", None) or time(17)
        business_days = self.holiday_manager.get_business_days(first.start, first.end, market)
        # --------------------------------------------------------------
        # DAILY
        # --------------------------------------------------------------
        if is_daily:
            all_rows = []

            # Ciclo su tutte le date richieste
            for dt in business_days:
                df_dt = query.fairvalue_array_isin_currency(
                    date=dt,
                    market=market,
                    currency=currency,
                    array_isin=sorted(subs),
                    fairvalue_time=snapshot_time,
                    segment=segment,
                )

                if df_dt is not None and not df_dt.empty:
                    df_dt["date"] = dt
                    all_rows.append(df_dt)

            # Concateno tutti i risultati
            if not all_rows:
                return {
                    r.instrument.id: {f: {b: None for b in business_days} for f in fields}
                    for r in requests
                }

            df = pd.concat(all_rows, ignore_index=True)

            # Normalizzazione completa
            df = _normalize_dataframe(df)

            # Ricostruzione output
            return _build_results(
                df=df,
                requests=requests,
                fields=fields,
                is_daily=True,
                business_days=business_days,
                fstart=business_days[0],
                fend=business_days[-1],
            )

        # --------------------------------------------------------------
        # INTRADAY
        # --------------------------------------------------------------
        sec = _freq_to_seconds(first.frequency)

        rows = []
        for current_day in business_days:
            for sub in subs:
                r = req_by_sub[sub]

                df = query.best_sampled_isin_currency(
                    date=current_day.date(),
                    market=market,
                    currency=currency,
                    isin=sub,
                    seconds_sampling=sec,
                    segment=segment,
                )

                df = _normalize_dataframe(df)

                if df is not None and not df.empty:
                    df["isin"] = sub
                    df["date"] = current_day.date()  # traccia giorno intraday
                    rows.append(df)

        df_all = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

        return _build_results(
            df=df_all,
            requests=requests,
            fields=fields,
            is_daily=is_daily,
            business_days=None,
            fstart=first.start,
            fend=first.end,
        )

