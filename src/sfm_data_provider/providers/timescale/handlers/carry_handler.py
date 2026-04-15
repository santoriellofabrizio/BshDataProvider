from typing import List, Any, Dict

from core.requests.requests import  BaseStaticRequest
from providers.timescale.handlers.base_handlers import Handler
from providers.timescale.query_timescale import QueryTimeScale


class CarryHandler(Handler):

    def process(self, requests: List[Any], query: QueryTimeScale) -> Dict[str, Any]:

        query.overnight_financing_rate_for_currency()

    def can_handle(self, req: BaseStaticRequest) -> bool:
        return "CARRY" in [f.upper() for f in req.fields]

