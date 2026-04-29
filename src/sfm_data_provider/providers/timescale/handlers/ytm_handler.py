from typing import List, Any, Dict

from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.core.holidays.holiday_manager import HolidayManager
from sfm_data_provider.core.requests.requests import BaseStaticRequest
from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler
from sfm_data_provider.providers.timescale.query_timescale import QueryTimeScale


class YTMHandler(Handler):

    def process(self, requests: List[Any], query: QueryTimeScale) -> Dict[str, Any]:

        group_by_instrument = {}
        for r in requests:
            group_by_instrument[r.instrument.type] = group_by_instrument.get(r.instrument.type, []) + [r]
        out = {}
        for type, reqs in group_by_instrument.items():
            match type:
                case InstrumentType.ETP:
                    out.update(self._process_etp(reqs, query))
                case InstrumentType.FUTURE:
                    raise NotImplementedError("For futures ytm. use api.info.get_future_ytm() [FETCHES FROM BLOOMBERG]")
                #     out.update(self._process_future(reqs, query))
        return out

    def can_handle(self, req: BaseStaticRequest) -> bool:
        return "YTM" in [f.upper() for f in req.fields]

    @staticmethod
    def _process_etp(requests: List[BaseStaticRequest], query: QueryTimeScale) -> Dict[str, Any]:
        # --- Extract metadata ---
        first = requests[0]
        isins = [r.subscription for r in requests]
        ids = [r.instrument.id for r in requests]

        coverage_threshold = first.extra_params.get("coverage_threshold", 0.8)

        hm = HolidayManager()
        dates = [d.date() for d in hm.get_business_days(first.start, first.end)]
        df = query.get_etf_ytm(isins, dates, coverage_threshold)
        df = df.reindex(isins,axis=1)
        df.columns = ids
        result = {
            col: {"YTM": df[col].to_dict()}
            for col in df.columns
        }

        return result

    @staticmethod
    def _process_future(requests: List[BaseStaticRequest], query: QueryTimeScale) -> Dict[str, Any]:

        first = requests[0]
        subs = [r.subscription for r in requests]  # callable(date) !
        ids = [r.instrument.id for r in requests]

        coverage_threshold = first.extra_params.get("coverage_threshold", 0.8)

        hm = HolidayManager()
        dates = [d.date() for d in hm.get_business_days(first.start, first.end)]

        # --- Compute the actual timeseries subscription codes ---
        # Now subscriptions_string is a string
        subscriptions_string = [sub(dates[-1]) if callable(sub) else sub for sub in subs]

        df = query.get_etf_ytm(subscriptions_string, dates, coverage_threshold)
        df = df[subscriptions_string]
        df.columns = ids

        return {
            col: {"YTM": df[col].to_dict()}
            for col in df.columns
        }



