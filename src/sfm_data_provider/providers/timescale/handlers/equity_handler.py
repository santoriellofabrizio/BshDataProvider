from datetime import time
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler
from sfm_data_provider.core.enums.markets import Market
from sfm_data_provider.providers.timescale.handlers.handlers_utils import (
    _build_results,
    _freq_to_seconds,
    _normalize_dataframe
)


class EquityHandler(Handler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Calcola workers ottimali in base alla CPU
        cpu_count = os.cpu_count() or 4
        # Max 10 workers per evitare troppi connection al DB
        self.max_workers = min(cpu_count * 2, 10)

    def can_handle(self, req):
        t = req.instrument.type.upper()
        return t in ("ETP", "STOCK") and req.request_type

    # ------------------------------------------------------------------
    # HELPER METHODS PER PARALLELIZZAZIONE
    # ------------------------------------------------------------------

    def _fetch_single_day_daily(self, query, dt, market, currency, subs, snapshot_time, segment):
        """
        Fetch daily data for a single date.

        Args:
            query: Query object
            dt: Date to fetch
            market: Market identifier
            currency: Currency code
            subs: Tuple of subscriptions/ISINs
            snapshot_time: Time for snapshot
            segment: Market segment

        Returns:
            DataFrame with data for the date or None
        """
        try:
            df_dt = query.fairvalue_array_isin_currency(
                date=dt,
                market=market,
                currency=currency,
                array_isin=subs,
                fairvalue_time=snapshot_time,
                segment=segment,
            )

            if df_dt is not None and not df_dt.empty:
                df_dt["date"] = dt
                return df_dt
            return None

        except Exception as e:
            # Log error but continue processing other dates
            print(f"Error fetching data for {dt}: {e}")
            return None

    def _fetch_single_day_intraday(self, query, current_day, market, currency, subs, sec, segment):
        """
        Fetch intraday data for a single date.

        Args:
            query: Query object
            current_day: Date to fetch
            market: Market identifier
            currency: Currency code
            subs: Tuple of subscriptions/ISINs
            sec: Sampling seconds
            segment: Market segment

        Returns:
            DataFrame with data for the date or None
        """
        try:
            df = query.best_sampled_isin_currency(
                date=current_day.date(),
                market=market,
                currency=currency,
                isin=subs,
                seconds_sampling=sec,
                segment=segment,
            )

            df = _normalize_dataframe(df)

            if df is not None and not df.empty:
                df["date"] = current_day.date()
                return df
            return None

        except Exception as e:
            # Log error but continue processing other dates
            print(f"Error fetching intraday data for {current_day}: {e}")
            return None

    # ------------------------------------------------------------------
    # MAIN PROCESS METHOD
    # ------------------------------------------------------------------

    def process(self, requests, query):
        if not requests:
            return {}

        first = requests[0]
        fields = first.fields if isinstance(first.fields, list) else [first.fields]
        is_daily = "d" in str(first.frequency).lower()

        subs = tuple(sorted(r.subscription or r.instrument.id for r in requests))
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

        # Adatta workers al numero di giorni da processare
        workers = min(self.max_workers, len(business_days))

        # --------------------------------------------------------------
        # DAILY - PARALLELIZZATO
        # --------------------------------------------------------------
        if is_daily:
            all_rows = []

            with self.progress(
                    f"Fetching daily equity data - {market} - {currency} (parallel: {workers} workers)",
                    len(business_days)
            ) as pbar:

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    # Submit tutte le query in parallelo
                    future_to_date = {
                        executor.submit(
                            self._fetch_single_day_daily,
                            query, dt, market, currency, subs, snapshot_time, segment
                        ): dt
                        for dt in business_days
                    }

                    # Raccogli i risultati man mano che completano
                    for future in as_completed(future_to_date):
                        result = future.result()
                        if result is not None:
                            all_rows.append(result)
                        pbar.update(1)

            # Se non ci sono risultati, ritorna struttura vuota
            if not all_rows:
                return {
                    r.instrument.id: {f: {b: None for b in business_days} for f in fields}
                    for r in requests
                }

            # Concatena tutti i risultati (copy=False per performance)
            df = pd.concat(all_rows, ignore_index=True, copy=False)

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
        # INTRADAY - PARALLELIZZATO
        # --------------------------------------------------------------
        sec = _freq_to_seconds(first.frequency)
        rows = []

        with self.progress(
                f"Fetching intraday equity data - {market} - {currency} (parallel: {workers} workers)",
                len(business_days)
        ) as pbar:

            with ThreadPoolExecutor(max_workers=workers) as executor:
                # Submit tutte le query in parallelo
                future_to_date = {
                    executor.submit(
                        self._fetch_single_day_intraday,
                        query, current_day, market, currency, subs, sec, segment
                    ): current_day
                    for current_day in business_days
                }

                # Raccogli i risultati man mano che completano
                for future in as_completed(future_to_date):
                    result = future.result()
                    if result is not None:
                        rows.append(result)
                    pbar.update(1)

        # Concatena risultati (copy=False per performance)
        df_all = pd.concat(rows, ignore_index=True, copy=False) if rows else pd.DataFrame()

        return _build_results(
            df=df_all,
            requests=requests,
            fields=fields,
            is_daily=is_daily,
            business_days=None,
            fstart=first.start,
            fend=first.end,
        )