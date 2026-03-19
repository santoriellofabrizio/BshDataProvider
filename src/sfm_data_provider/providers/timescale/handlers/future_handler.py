import pandas as pd
from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler
from sfm_data_provider.providers.timescale.handlers.handlers_utils import _freq_to_seconds, _build_results, _normalize_dataframe
from sfm_data_provider.providers.timescale.query_timescale import QueryTimeScale


class FutureHandler(Handler):

    def can_handle(self, req):
        return req.instrument.type.upper() == "FUTURE"

    def process(self, requests, query: QueryTimeScale):
        first = requests[0]
        is_daily = "d" in str(first.frequency).lower()
        market = first.market
        snapshot_time = first.snapshot_time

        if not is_daily:
             sec = _freq_to_seconds(first.frequency)

        subs = [
            r.subscription(first.start) if callable(r.subscription) else r.subscription
            for r in requests
        ]

        req_by_sub = {sub: r for sub, r in zip(subs, requests)}

        # ------------------------------------------------------------------
        # DAILY: ciclo sulle date, concat, normalize, build_results
        # ------------------------------------------------------------------
        if is_daily:
            days = pd.date_range(first.start, first.end, freq="D")
            rows = []

            for dt in days:
                df = query.fairvalue_array_isin(
                    date=dt.date(),
                    market=market,
                    array_isin=subs,
                    fairvalue_time=snapshot_time,
                )

                df = _normalize_dataframe(df)

                if df is not None and not df.empty:
                    df["date"] = dt.date()
                    rows.append(df)

            df_all = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

            business_days = sorted(pd.unique(pd.to_datetime(df_all["date"]).dt.date)) \
                if not df_all.empty else []

            fstart = business_days[0] if business_days else first.start
            fend = business_days[-1] if business_days else first.end

            return _build_results(
                df=df_all,
                requests=requests,
                fields=first.fields,
                is_daily=True,
                business_days=business_days,
                fstart=fstart,
                fend=fend,
            )

        # ------------------------------------------------------------------
        # INTRADAY: ciclo sulle date, concat, normalize, build_results
        # ------------------------------------------------------------------
        days = pd.date_range(first.start, first.end, freq="D")
        rows = []

        for dt in days:
            for sub in subs:
                df = query.best_sampled_isin(
                    date=dt.date(),
                    market=market,
                    isin=sub,
                    seconds_sampling=sec,
                )

                df = _normalize_dataframe(df)

                if df is not None and not df.empty:
                    df["isin"] = sub
                    df["date"] = dt.date()
                    rows.append(df)

        df_all = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

        return _build_results(
            df=df_all,
            requests=requests,
            fields=first.fields,
            is_daily=False,
            business_days=None,
            fstart=first.start,
            fend=first.end,
        )
