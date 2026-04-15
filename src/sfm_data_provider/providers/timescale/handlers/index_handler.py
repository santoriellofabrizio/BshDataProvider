from sfm_data_provider.providers.timescale.handlers.base_handlers import Handler
from sfm_data_provider.core.enums.instrument_types import InstrumentType
from sfm_data_provider.providers.timescale.query_timescale import QueryTimeScale


class IndexHandler(Handler):

    def can_handle(self, req):
        return req.instrument.type == InstrumentType.INDEX

    def process(self, requests, query: QueryTimeScale):
        first = requests[0]
        subscriptions = [r.subscription for r in requests]
        ids = [r.instrument.id for r in requests]
        fields = first.fields if isinstance(first.fields, list) else [first.fields]
        results_df = query.overnight_financing_rate(
                start_date=first.start, end_date=first.end, rates_list=subscriptions)

        return {
                    inst_id: {
                        "overnight_rate": (
                            results_df[sub].to_dict()
                            if sub in results_df.columns
                            else {}        # oppure: None, [], {}, come preferisci
                        )
                    }
                    for inst_id, sub in zip(ids, subscriptions)
                }

