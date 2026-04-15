import os
from collections import defaultdict
from typing import Callable

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyparsing import results

from sfm_data_provider.core.requests.requests import BulkRequest
from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler
from sfm_data_provider.providers.timescale.query_timescale import QueryTimeScale


class MarketTradesHandler(Handler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cpu_count = os.cpu_count() or 4
        self.max_workers = min(cpu_count * 2, 10)

    def can_handle(self, req):
        return req.request_type.upper() == 'BULK' and req.fields[0].upper() == 'MARKET_TRADES'

    # ------------------------------------------------------------------
    # HELPER METHOD
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_single_day(query, day, subscriptions, market, segment):
        """
        Fetch market trades for a single date.

        Args:
            query: QueryTimeScale object
            day: Date to fetch
            subscriptions: List of ISINs/subscriptions
            market: Market identifier
            segment: Market segment (optional)

        Returns:
            DataFrame with trades for the date, or None
        """
        try:
            df = query.trades_market_array_isin(
                array_isin=subscriptions,
                market=market,
                segment=segment,
                date=day,
            )

            if df is not None and not df.empty:
                df["date"] = day
                return df
            return None

        except Exception as e:
            print(f"Error fetching market trades for {day}: {e}")
            return None

    # ------------------------------------------------------------------
    # MAIN PROCESS METHOD
    # ------------------------------------------------------------------

    def process(self, requests: list[BulkRequest], query: QueryTimeScale):
        if not requests:
            return {}

        first = requests[0]
        subscriptions = [r.subscription for r in requests]
        ids = [r.instrument.id for r in requests]
        fields = first.fields if isinstance(first.fields, list) else [first.fields]

        segment = first.extra_params.get('segment', None)
        market = first.extra_params.get('market', 'EURONEXT')

        business_days = self.holiday_manager.get_business_days(first.start, first.end, market)

        workers = min(self.max_workers, len(business_days))

        all_rows = []

        with self.progress(
                f"Fetching market trades - {market} (parallel: {workers} workers)",
                len(business_days)
        ) as pbar:

            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_date = {
                    executor.submit(
                        self._fetch_single_day,
                        query, day, [s(day) if isinstance(s, Callable) else s for s in subscriptions], market, segment
                    ): day
                    for day in business_days
                }

                for future in as_completed(future_to_date):
                    result = future.result()
                    if result is not None:
                        all_rows.append(result)
                    pbar.update(1)

        if not all_rows:
            return {}

        df = pd.concat(all_rows, ignore_index=True, copy=False)
        results = defaultdict(dict)

        for instrument_id, isin in zip(ids, subscriptions):
            mask = (df["isin"] == isin)
            results[instrument_id]['MARKET_TRADES'] = df.loc[mask].to_dict('records')

        return results
