"""
request_tracker.py - Core logic per il tracking delle richieste (SEMPLIFICATO).

Gestisce il ciclo di vita delle richieste in modo thread-safe,
permettendo di tracciare, aggiornare e interrogare lo stato di un batch.
"""

import threading
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

from sfm_data_provider.core.enums.request_state import RequestState
from sfm_data_provider.core.requests.requests import BaseRequest
from sfm_data_provider.core.response_tracking import RequestStatus
from sfm_data_provider.core.response_tracking.request_status import create_sent_status

logger = logging.getLogger(__name__)


class RequestTracker:
    """Tracker thread-safe per monitorare lo stato delle richieste."""

    def __init__(self, batch_id: Optional[str] = None):
        """
        Inizializza il tracker.

        Args:
            batch_id: Identificativo opzionale del batch
        """
        self._statuses: Dict[str, RequestStatus] = {}
        self._lock = threading.RLock()
        self._batch_id = batch_id or datetime.now().strftime("%Y%m%d_%H%M%S")

    def track(
        self,
        request: BaseRequest,
        provider: Optional[str] = None
    ) -> RequestStatus:
        """Registra una richiesta da tracciare."""
        with self._lock:
            status = create_sent_status(request, provider)
            self._statuses[request.request_id] = status
            logger.debug(f"Tracked request: {request.request_id}")
            return status

    def track_many(
        self,
        requests: List[BaseRequest],
        provider: Optional[str] = None
    ) -> List[RequestStatus]:
        """Registra multiple richieste."""
        with self._lock:
            return [self.track(req, provider) for req in requests]

    def update_with_result(
        self,
        request_id: str,
        result_data: Dict[str, Any],
        error: Optional[Exception] = None
    ) -> Optional[RequestStatus]:
        """Aggiorna lo stato di una richiesta con i risultati ricevuti."""
        with self._lock:
            if request_id not in self._statuses:
                logger.warning(f"Request not found for update: {request_id}")
                return None

            old_status = self._statuses[request_id]
            new_status = old_status.with_results(result_data, error)
            self._statuses[request_id] = new_status

            logger.debug(
                f"Updated {request_id}: {old_status.state.name} -> {new_status.state.name} "
                f"({new_status.completion_rate:.0%})"
            )
            return new_status

    def mark_failed(
        self,
        request_id: str,
        error: Optional[Exception] = None,
        preserve_received: bool = True
    ) -> Optional[RequestStatus]:
        """
        Marca una richiesta come fallita.
        
        Args:
            request_id: ID della richiesta
            error: Eccezione opzionale da registrare
            preserve_received: Se True, preserva i field già ricevuti (default).
                             Se False, azzera fields_received.
        
        Returns:
            Nuovo RequestStatus in stato FAILED, o None se request_id non trovato
        """
        with self._lock:
            if request_id not in self._statuses:
                return None

            status = self._statuses[request_id]
            new_status = RequestStatus(
                request=status.request,
                state=RequestState.FAILED,
                fields_requested=status.fields_requested,
                fields_received=status.fields_received if preserve_received else set(),
                error=error,
                provider=status.provider,
                metadata=status.metadata,
            )
            self._statuses[request_id] = new_status
            return new_status

    def get(self, request_id: str) -> Optional[RequestStatus]:
        """Ottiene lo status di una richiesta specifica."""
        with self._lock:
            return self._statuses.get(request_id)

    def get_all(self) -> List[RequestStatus]:
        """Ritorna tutti gli status tracciati."""
        with self._lock:
            return list(self._statuses.values())

    def get_failed(self) -> List[RequestStatus]:
        """Ritorna tutte le richieste fallite."""
        with self._lock:
            return [s for s in self._statuses.values() if s.state == RequestState.FAILED]

    def get_incomplete(self) -> List[RequestStatus]:
        """Ritorna tutte le richieste incomplete (PARTIAL, EMPTY, TIMEOUT)."""
        with self._lock:
            incomplete_states = {RequestState.PARTIAL, RequestState.EMPTY, RequestState.TIMEOUT}
            return [s for s in self._statuses.values() if s.state in incomplete_states]

    # ============================================================
    # Aggregations
    # ============================================================

    @property
    def partial_count(self) -> int:
        """Numero di richieste parziali."""
        with self._lock:
            return sum(1 for s in self._statuses.values() if s.state == RequestState.PARTIAL)

    @property
    def total(self) -> int:
        """Numero totale di richieste tracciate."""
        with self._lock:
            return len(self._statuses)

    @property
    def complete_count(self) -> int:
        """Numero di richieste completate."""
        with self._lock:
            return sum(1 for s in self._statuses.values() if s.state == RequestState.COMPLETE)

    @property
    def failed_count(self) -> int:
        """Numero di richieste fallite."""
        with self._lock:
            return sum(1 for s in self._statuses.values() if s.state == RequestState.FAILED)

    @property
    def success_rate(self) -> float:
        """Percentuale di successo (COMPLETE + PARTIAL)."""
        if self.total == 0:
            return 0.0
        return (self.complete_count + self.partial_count) / self.total

    def clear(self) -> None:
        """Rimuove tutti gli status tracciati."""
        with self._lock:
            self._statuses.clear()
            logger.debug("Tracker cleared")

    def reset(self) -> None:
        """Reset completo del tracker con nuovo batch_id."""
        with self._lock:
            self._statuses.clear()
            self._batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __str__(self) -> str:
        """Riepilogo testuale dello stato del tracker."""
        lines = [
            f"RequestTracker[{self._batch_id}] (Total: {self.total})",
            f"  Complete: {self.complete_count}",
            f"  Partial: {self.partial_count}",
            f"  Failed: {self.failed_count}",
            f"  Success rate: {self.success_rate:.1%}",
        ]
        return "\n".join(lines)

    def __len__(self) -> int:
        return self.total

    def __contains__(self, request_id: str) -> bool:
        with self._lock:
            return request_id in self._statuses