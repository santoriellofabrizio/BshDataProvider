
from typing import List, Any, Dict

from sfm_data_provider.core.requests.requests import BaseStaticRequest
from sfm_data_provider.providers.timescale.query_timescale import QueryTimeScale


class GeneralInfoHandler:

    @staticmethod
    def handle(requests: List[Any], query: QueryTimeScale) -> Dict[str, Any]:

        req = requests[0]
        field = req.fields
        if len(field) > 1:
            raise ValueError("can only handle one general field at a time")
        field = field[0]

        classe = req.extra_params.get("classe", None)
        cache_provenienza = req.extra_params.get("cache_provenienza")

        if field.upper() == "BOND_ISINS":
            return {"GENERAL":
                        {"BOND_ISINS": query.get_bond_isin(classe=classe,
                                                          cache_provenienza=cache_provenienza)["isin"].to_list()}}

        return {}

    def can_handle(self, req: BaseStaticRequest) -> bool:
        return req.request_type == 'general'

