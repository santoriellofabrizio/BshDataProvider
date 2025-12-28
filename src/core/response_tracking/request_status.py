"""
request_status.py - Value Object per lo stato di una richiesta (SEMPLIFICATO).

Rappresenta lo stato immutabile di una singola richiesta in un momento specifico.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Set, Dict, Any

from core.enums.request_state import (
    RequestState,
    infer_state_from_result,
    evaluate_result_quality
)
from core.requests.requests import BaseRequest


@dataclass(frozen=True)
class RequestStatus:
    """
    Value Object immutabile che rappresenta lo stato di una richiesta.

    Attributes:
        request: Richiesta originale
        state: Stato corrente (RequestState enum: SENT/COMPLETE/INCOMPLETE/FAILED)
        fields_requested: Set di field richiesti
        fields_received: Set di field ricevuti con dati validi (non None/NaN)
        timestamp: Momento di creazione dello status
        error: Eventuale eccezione catturata
        provider: Nome del provider che ha gestito la richiesta
        metadata: Dizionario per dati aggiuntivi (es. timeseries_info)

    Example:
        >>> status = RequestStatus(
        ...     request=my_request,
        ...     state=RequestState.COMPLETE,
        ...     fields_requested={"TER", "DESCRIPTION"},
        ...     fields_received={"TER", "DESCRIPTION"}
        ... )
        >>> status.is_complete
        True
        >>> status.completion_rate
        1.0
    """

    request: BaseRequest
    state: RequestState
    fields_requested: Set[str]
    fields_received: Set[str]
    timestamp: datetime = field(default_factory=datetime.now)
    error: Optional[Exception] = None
    provider: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validazioni post-inizializzazione."""
        # Converti a set se necessario
        if not isinstance(self.fields_requested, set):
            object.__setattr__(self, 'fields_requested', set(self.fields_requested))
        if not isinstance(self.fields_received, set):
            object.__setattr__(self, 'fields_received', set(self.fields_received))

        # Validazione: fields_received deve essere subset di fields_requested
        if not self.fields_received.issubset(self.fields_requested):
            extra = self.fields_received - self.fields_requested
            raise ValueError(
                f"Received fields contain unexpected items: {extra}. "
                f"Expected only: {self.fields_requested}"
            )

    # ============================================================
    # Properties di Comodo
    # ============================================================

    @property
    def instrument_id(self) -> str:
        """ID dello strumento (ISIN, ticker, etc.)."""
        return self.request.instrument.id if self.request.instrument else "UNKNOWN"

    @property
    def request_id(self) -> str:
        """ID univoco della richiesta."""
        return self.request.request_id

    @property
    def completion_rate(self) -> float:
        """
        Percentuale di completamento (0.0 - 1.0).

        Returns:
            Rapporto tra field ricevuti e field richiesti
        """
        if not self.fields_requested:
            return 1.0
        return len(self.fields_received) / len(self.fields_requested)

    # ============================================================
    # Metodi Essenziali
    # ============================================================

    def with_results(
            self,
            result_data: Dict[str, Any],
            error: Optional[Exception] = None
    ) -> 'RequestStatus':
        """
        Crea un nuovo RequestStatus aggiornato con i risultati ricevuti.

        Riconosce timeseries (dict di {date: value}) e conta come ricevuto
        anche se ha alcuni None, ma registra i metadata sulla completezza.
        
        Tutte le chiavi vengono normalizzate a UPPERCASE per consistenza.

        Args:
            result_data: Dizionario con i dati ricevuti (formato: {field: value})
                        Per timeseries: {field: {date: value, ...}}
            error: Eventuale errore da registrare

        Returns:
            Nuovo RequestStatus con state e fields_received aggiornati

        Example:
            >>> # Field scalari
            >>> result = {"TER": 0.005, "DESCRIPTION": None}
            >>> new_status = status.with_results(result)
            >>> new_status.fields_received
            {'TER'}  # DESCRIPTION è None quindi non conta

            >>> # Timeseries con alcuni None
            >>> result = {"MID": {"2024-01-01": 100, "2024-01-02": None}}
            >>> new_status = status.with_results(result)
            >>> new_status.fields_received
            {'MID'}  # Ricevuto perché ha almeno un dato valido
            >>> new_status.metadata["timeseries_MID_incomplete"]
            {'total': 2, 'missing': 1, 'completion_rate': 0.5}
        """
        # Normalizza result_data a UPPERCASE per consistenza
        result_data_upper = {k.upper(): v for k, v in result_data.items()}
        
        # Valuta la qualità di ogni field (già uppercase da evaluate_result_quality)
        quality = evaluate_result_quality(result_data_upper)

        # Estrai field con dati validi
        fields_received = set()
        updated_metadata = dict(self.metadata)

        for field_name, has_valid_data in quality.items():
            # field_name è già UPPERCASE
            if has_valid_data:
                fields_received.add(field_name)

                # Controlla se è timeseries con valori mancanti
                field_value = result_data_upper.get(field_name)
                if isinstance(field_value, dict):
                    # È un dizionario, potrebbe essere timeseries
                    total_entries = len(field_value)
                    # Conta anche NaN come missing
                    missing_entries = sum(
                        1 for v in field_value.values() 
                        if v is None or (isinstance(v, float) and v != v)  # NaN check: v != v
                    )
                    
                    if missing_entries > 0:
                        # Timeseries incompleta - registra metadata
                        updated_metadata[f"timeseries_{field_name}_incomplete"] = {
                            "total": total_entries,
                            "missing": missing_entries,
                            "completion_rate": (total_entries - missing_entries) / total_entries if total_entries > 0 else 0.0
                        }

        # Inferisci nuovo stato (passa result_data_upper per consistenza)
        new_state = infer_state_from_result(
            fields_requested=self.fields_requested,
            fields_received=fields_received,
            result_data=result_data_upper,
            has_error=(error is not None)
        )

        return RequestStatus(
            request=self.request,
            state=new_state,
            fields_requested=self.fields_requested,
            fields_received=fields_received,
            timestamp=datetime.now(),
            error=error,
            provider=self.provider,
            metadata=updated_metadata,
        )

    # ============================================================
    # Metodi Magic
    # ============================================================

    def __str__(self) -> str:
        """Human-readable representation."""
        return (
            f"RequestStatus({self.instrument_id}, "
            f"{self.state.display_name}, "
            f"{len(self.fields_received)}/{len(self.fields_requested)} fields)"
        )


# ============================================================
# Factory Functions
# ============================================================

def create_sent_status(
        request: BaseRequest,
        provider: Optional[str] = None
) -> RequestStatus:
    """
    Factory per creare uno status SENT per una nuova richiesta.

    Args:
        request: Richiesta da tracciare
        provider: Nome del provider che gestirà la richiesta

    Returns:
        RequestStatus in stato SENT

    Example:
        >>> status = create_sent_status(my_request, provider="oracle")
        >>> status.state
        RequestState.SENT
    """
    fields = request.fields if isinstance(request.fields, list) else [request.fields]

    return RequestStatus(
        request=request,
        state=RequestState.SENT,
        fields_requested=set(f.upper() for f in fields),
        fields_received=set(),
        provider=provider,
    )


# Alias per backward compatibility
def create_pending_status(
        request: BaseRequest,
        provider: Optional[str] = None
) -> RequestStatus:
    """Alias di create_sent_status per backward compatibility."""
    return create_sent_status(request, provider)
