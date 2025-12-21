from datetime import time
import pandas as pd
from providers.timescale.handlers.base_handlers import Handler

from providers.timescale.handlers.handlers_utils import _freq_to_seconds, _build_results, _normalize_dataframe
from providers.timescale.query_timescale import QueryTimeScale


class FXHandler(Handler):

    def can_handle(self, req):
        return req.instrument.type.upper() == "CURRENCYPAIR"


    def process(self, requests, query: QueryTimeScale):
        first = requests[0]
        is_daily = "d" in str(first.frequency).lower()

        snapshot_time = getattr(first, "snapshot_time", None) or time(17)

        pairs = [r.subscription or r.instrument.id for r in requests]
        req_by_pair = {p: r for p, r in zip(pairs, requests)}

        fields = first.fields if isinstance(first.fields, list) else [first.fields]

        # ======================================================================
        # DAILY → ciclo sulle date, concat, normalize, build_results
        # ======================================================================
        if is_daily:
            days = self.holiday_manager.get_business_days(first.start, first.end, "FX")
            rows = []

            for dt in days:
                df = query.daily_mid_array_currency(
                    date=dt.date(),
                    array_currency=pairs,
                    fairvalue_time=snapshot_time,
                )

                df = _normalize_dataframe(df)

                if df is not None and not df.empty:
                    df["date"] = dt.date()   # per coerenza con altri handler
                    rows.append(df)

            df_all = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

            business_days = sorted(
                pd.unique(pd.to_datetime(df_all["date"]).dt.date)
            ) if not df_all.empty else []

            fstart = business_days[0] if business_days else first.start
            fend = business_days[-1] if business_days else first.end

            return _build_results(
                df=df_all,
                requests=requests,
                fields=fields,
                is_daily=True,
                business_days=business_days,
                fstart=fstart,
                fend=fend,
            )

        # ======================================================================
        # INTRADAY → ciclo sulle date, concat, normalize, build_results
        # ======================================================================
        sec = _freq_to_seconds(first.frequency)
        days = pd.date_range(first.start, first.end, freq="D")
        rows = []

        for dt in days:
            for p in pairs:
                df = query.best_sampled_currency(
                    date=dt.date(),
                    currency_pair=p,
                    seconds_sampling=sec,
                )

                df = _normalize_dataframe(df)

                if df is not None and not df.empty:
                    df["isin"] = p
                    df["date"] = dt.date()
                    rows.append(df)

        df_all = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

        return _build_results(
            df=df_all,
            requests=requests,
            fields=fields,
            is_daily=False,
            business_days=None,
            fstart=first.start,
            fend=first.end,
        )
