"""
request_state.py - Stati possibili per il tracking delle richieste.

Questo enum definisce tutti gli stati attraverso cui una richiesta può passare
durante il suo ciclo di vita nel sistema BSH Data Provider.
"""

from enum import Enum
from typing import Set, Any, Dict
import math


class RequestState(Enum):
    """
    Stati del ciclo di vita di una richiesta.

    Stati:
        PENDING: Richiesta creata ma non ancora inviata al provider
        SENT: Richiesta inviata al provider, in attesa di risposta
        COMPLETE: Richiesta completata con successo, tutti i field ricevuti con dati validi
        PARTIAL: Richiesta completata parzialmente, alcuni field mancanti o vuoti
        EMPTY: Richiesta completata ma tutti i field sono vuoti/None
        FAILED: Richiesta fallita completamente per errore tecnico
        TIMEOUT: Richiesta scaduta per timeout

    Transizioni tipiche:
        PENDING -> SENT -> COMPLETE (successo totale)
        PENDING -> SENT -> PARTIAL (successo parziale)
        PENDING -> SENT -> EMPTY (no dati disponibili)
        PENDING -> SENT -> FAILED (errore tecnico)
        PENDING -> SENT -> TIMEOUT (scadenza)

    Example:
        >>> state = RequestState.COMPLETE
        >>> state.is_successful
        True
        >>> state.should_retry
        False
    """

    PENDING = "pending"
    SENT = "sent"
    COMPLETE = "complete"
    PARTIAL = "partial"
    EMPTY = "empty"
    FAILED = "failed"
    TIMEOUT = "timeout"

    @property
    def is_successful(self) -> bool:
        """
        Indica se lo stato rappresenta un successo (completo o parziale).

        Returns:
            True se COMPLETE o PARTIAL, False altrimenti
        """
        return self in (RequestState.COMPLETE, RequestState.PARTIAL)

    @property
    def is_error(self) -> bool:
        """
        Indica se lo stato rappresenta un errore tecnico.

        Returns:
            True se FAILED o TIMEOUT, False altrimenti
        """
        return self in (RequestState.FAILED, RequestState.TIMEOUT)

    @property
    def is_terminal(self) -> bool:
        """
        Indica se lo stato è terminale (non può più cambiare senza retry).

        Returns:
            True se COMPLETE, PARTIAL, EMPTY, FAILED o TIMEOUT
        """
        return self in (
            RequestState.COMPLETE,
            RequestState.PARTIAL,
            RequestState.EMPTY,
            RequestState.FAILED,
            RequestState.TIMEOUT
        )

    @property
    def should_retry(self) -> bool:
        """
        Indica se lo stato suggerisce un retry.

        Returns:
            True se FAILED, TIMEOUT, EMPTY o PARTIAL
        """
        return self in (
            RequestState.FAILED,
            RequestState.TIMEOUT,
            RequestState.EMPTY,
            RequestState.PARTIAL
        )

    @property
    def display_name(self) -> str:
        """
        Nome human-readable dello stato.

        Returns:
            Descrizione user-friendly dello stato
        """
        display_names = {
            RequestState.PENDING: "In attesa",
            RequestState.SENT: "Inviata",
            RequestState.COMPLETE: "Completata",
            RequestState.PARTIAL: "Parziale",
            RequestState.EMPTY: "Vuota",
            RequestState.FAILED: "Fallita",
            RequestState.TIMEOUT: "Scaduta",
        }
        return display_names.get(self, self.value)


    def can_transition_to(self, new_state: 'RequestState') -> bool:
        """
        Verifica se è possibile transitare a un nuovo stato.

        Args:
            new_state: Stato target della transizione

        Returns:
            True se la transizione è valida, False altrimenti

        Example:
            >>> RequestState.PENDING.can_transition_to(RequestState.SENT)
            True
            >>> RequestState.COMPLETE.can_transition_to(RequestState.FAILED)
            False
        """
        # Stati terminali non possono transitare (eccetto per retry)
        if self.is_terminal:
            return False

        # Transizioni valide
        valid_transitions = {
            RequestState.PENDING: {RequestState.SENT, RequestState.FAILED},
            RequestState.SENT: {
                RequestState.COMPLETE,
                RequestState.PARTIAL,
                RequestState.EMPTY,
                RequestState.FAILED,
                RequestState.TIMEOUT
            },
        }

        return new_state in valid_transitions.get(self, set())

    def __str__(self) -> str:
        """String representation usando display_name."""
        return self.display_name

    def __repr__(self) -> str:
        """Representation for debugging."""
        return f"RequestState.{self.name}"


# ============================================================
# Data Quality Evaluation
# ============================================================

def is_none_or_nan(value: Any) -> bool:
    """
    Check if a value is None or NaN.

    Args:
        value: Value to check

    Returns:
        True if value is None or NaN, False otherwise

    Examples:
        >>> is_none_or_nan(None)
        True
        >>> is_none_or_nan(float('nan'))
        True
        >>> is_none_or_nan(0)
        False
        >>> is_none_or_nan("test")
        False
    """
    if value is None:
        return True

    # Check for NaN (works for both float('nan') and numpy/pandas nan)
    try:
        return math.isnan(value)
    except (TypeError, ValueError):
        # Not a numeric type, so can't be NaN
        return False


def is_value_empty(value: Any) -> bool:
    """
    Determina se un valore è considerato "vuoto" (no dati utili).

    Vuoto significa:
        - None or NaN
        - Lista/dict vuoti
        - Lista/dict con solo valori None/NaN
        - Time series con tutti i valori None/NaN

    Args:
        value: Valore da valutare

    Returns:
        True se il valore è vuoto, False se contiene dati utili

    Examples:
        >>> is_value_empty(None)
        True
        >>> is_value_empty(float('nan'))
        True
        >>> is_value_empty(0.005)
        False
        >>> is_value_empty([])
        True
        >>> is_value_empty([{"date": "2024-01-01", "value": None}])
        True
        >>> is_value_empty([{"date": "2024-01-01", "value": 100.5}])
        False
    """
    if is_none_or_nan(value):
        return True

    if isinstance(value, (list, tuple)):
        if len(value) == 0:
            return True
        # Controlla se è una time series (lista di dict con 'value')
        if all(isinstance(item, dict) for item in value):
            # Time series: controlla se tutti i 'value' sono None/NaN
            values = [item.get('value') for item in value if 'value' in item]
            if values and all(is_none_or_nan(v) for v in values):
                return True
            # Se nessun item ha 'value', controlla ricorsivamente
            if not values:
                return all(is_value_empty(item) for item in value)
        else:
            # Lista normale: tutti None/NaN?
            return all(is_value_empty(item) for item in value)

    if isinstance(value, dict):
        if len(value) == 0:
            return True
        # Dict: tutti i valori sono vuoti?
        return all(is_value_empty(v) for v in value.values())

    # Valori scalari non-None sono considerati validi
    # Include 0, False, stringhe vuote "" (potrebbero essere dati validi)
    return False


def evaluate_result_quality(result_data: Dict[str, Any]) -> Dict[str, bool]:
    """
    Valuta la qualità di ogni field in un risultato.

    Le chiavi vengono normalizzate a UPPERCASE per consistenza.

    Args:
        result_data: Dizionario {field_name: value}

    Returns:
        Dizionario {FIELD_NAME: has_valid_data} (chiavi uppercase)

    Example:
        >>> evaluate_result_quality({"ter": 0.005, "NAV": None, "Prices": []})
        {"TER": True, "NAV": False, "PRICES": False}
    """
    return {
        field.upper(): not is_value_empty(value)
        for field, value in result_data.items()
    }


# ============================================================
# State Inference
# ============================================================

def infer_state_from_result(
    fields_requested: Set[str],
    fields_received: Set[str],
    has_error: bool = False,
    result_data: Dict[str, Any] = None
) -> RequestState:
    """
    Inferisce lo stato di una richiesta basandosi sui field ricevuti e la qualità dei dati.

    Args:
        fields_requested: Set di field richiesti
        fields_received: Set di field presenti nella risposta (chiavi del dict)
        has_error: Se True, indica che c'è stato un errore tecnico
        result_data: Dizionario con i dati effettivi per valutare la qualità

    Returns:
        RequestState appropriato

    Examples:
        >>> infer_state_from_result({"TER", "NAV"}, {"TER", "NAV"}, result_data={"TER": 0.005, "NAV": 100})
        RequestState.COMPLETE

        >>> infer_state_from_result({"TER", "NAV"}, {"TER", "NAV"}, result_data={"TER": 0.005, "NAV": None})
        RequestState.PARTIAL

        >>> infer_state_from_result({"TER", "NAV"}, {"TER", "NAV"}, result_data={"TER": None, "NAV": None})
        RequestState.EMPTY

        >>> infer_state_from_result({"TER", "NAV"}, set())
        RequestState.FAILED
    """
    if has_error:
        return RequestState.FAILED

    if not fields_requested:
        return RequestState.FAILED

    if not fields_received:
        return RequestState.FAILED

    # Se abbiamo result_data, valutiamo la qualità
    if result_data is not None:
        quality = evaluate_result_quality(result_data)

        # Conta field con dati validi (non vuoti)
        fields_with_data = {
            field for field in fields_requested
            if field in quality and quality[field]
        }

        if not fields_with_data:
            # Tutti i field sono vuoti/None
            return RequestState.EMPTY

        if fields_with_data == fields_requested:
            # Tutti i field hanno dati validi
            return RequestState.COMPLETE

        # Alcuni field hanno dati, altri no
        return RequestState.PARTIAL

    # Fallback senza result_data: basato solo su presenza field
    if fields_received >= fields_requested:
        return RequestState.COMPLETE

    if fields_received & fields_requested:
        # Almeno un field richiesto è presente
        return RequestState.PARTIAL

    return RequestState.FAILED
