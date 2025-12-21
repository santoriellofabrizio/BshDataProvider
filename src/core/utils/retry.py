from typing import List, Callable, Dict, Any, Set

from core.requests.requests import BaseRequest
from core.response_tracking import RequestStatus
from core.response_tracking.request_tracker import RequestTracker


class RetryManager:
    def retry(
            self,
            requests: List[BaseRequest],
            tracker: RequestTracker,
            fallbacks: List[Dict[str, Any]],
            dispatch: Callable[[List[BaseRequest]], Dict]
    ) -> Dict:
        """
        Ritenta richieste incomplete/failed con fallback sources.
        Richiede SOLO i field e le date mancanti.
        """
        working_results = {}

        for fallback in fallbacks:
            # 1. Identifica requests che necessitano retry
            retry_requests = self._build_retry_requests(
                requests,
                tracker,
                fallback
            )

            if not retry_requests:
                break  # Tutto completo!

            # 2. Invia retry
            retry_results = dispatch(retry_requests)

            # 3. Merge intelligente: sovrascrive solo i None
            working_results = self._merge_results(
                working_results,
                retry_results,
                tracker
            )

        return working_results

    def _build_retry_requests(
            self,
            requests: List[BaseRequest],
            tracker: RequestTracker,
            fallback: Dict[str, Any]
    ) -> List[BaseRequest]:
        """
        Costruisce requests di retry con SOLO i field mancanti/incomplete.
        """
        retry_requests = []

        for req in requests:
            status = tracker.get(req.request_id)

            if not status or not status.state.should_retry:
                continue  # Skip - già completo

            # Identifica field che servono
            missing_fields = self._get_missing_fields(status)

            if not missing_fields:
                continue

            # Crea nuova request con solo missing fields
            retry_req = req.with_updates(
                fields=list(missing_fields),
                source=fallback["source"]
            )

            retry_requests.append(retry_req)

        return retry_requests

    def _get_missing_fields(self, status: RequestStatus) -> Set[str]:
        """
        Identifica quali field necessitano retry.

        Returns:
            - Field completamente mancanti (status.missing_fields)
            - Field con timeseries incomplete (hanno metadata)
        """
        needs_retry = set(status.missing_fields)  # Field mai ricevuti

        # Aggiungi field con timeseries incomplete
        for field in status.fields_received:
            metadata_key = f"timeseries_{field}_incomplete"
            if metadata_key in status.metadata:
                # Timeseries con buchi -> richiedi di nuovo
                needs_retry.add(field)

        return needs_retry

    def _merge_results(
            self,
            base: Dict,
            new: Dict,
            tracker: RequestTracker
    ) -> Dict:
        """
        Merge intelligente che sovrascrive solo i None.

        Per field scalari:
        - Se base ha None -> prendi new
        - Se base ha valore -> mantieni base

        Per timeseries:
        - Merge date per date
        - Sovrascrivi solo date con None
        """
        merged = dict(base)

        for instrument_id, instrument_data in new.items():
            if instrument_id not in merged:
                merged[instrument_id] = instrument_data
                continue

            for field, value in instrument_data.items():
                if field not in merged[instrument_id]:
                    # Field nuovo
                    merged[instrument_id][field] = value
                else:
                    # Field già presente - merge
                    base_value = merged[instrument_id][field]
                    merged[instrument_id][field] = self._merge_field_values(
                        base_value,
                        value
                    )

        return merged

    def _merge_field_values(self, base_value: Any, new_value: Any) -> Any:
        """
        Merge di un singolo field.

        Scalare: prendi new se base è None
        Timeseries: merge date per date
        """
        # Se base è None, prendi il nuovo
        if base_value is None:
            return new_value

        # Se è timeseries (dict), merge date per date
        if isinstance(base_value, dict) and isinstance(new_value, dict):
            merged_ts = dict(base_value)
            for date, val in new_value.items():
                # Sovrascrivi solo se base ha None
                if date not in merged_ts or self._is_none_or_nan(merged_ts[date]):
                    merged_ts[date] = val
            return merged_ts

        # Altrimenti mantieni il base (già valido)
        return base_value