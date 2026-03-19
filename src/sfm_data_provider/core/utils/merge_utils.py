"""
merge_utils.py - Merge intelligente di risultati con fallback.

Aggiorna SOLO le date/valori che erano NaN, preserva il resto.
"""

import math
import pandas as pd
from typing import Dict, Any, Set, Optional, List
from pandas import Timestamp
import datetime as dt


def is_none_or_nan(value: Any) -> bool:
    """Controlla se un valore è None o NaN."""
    if value is None:
        return True
    try:
        return math.isnan(value)
    except (TypeError, ValueError):
        return False


def _normalize_date_key(key):
    """Normalizza le chiavi di date a Timestamp."""
    if isinstance(key, Timestamp):
        return key
    elif isinstance(key, dt.date):
        return Timestamp(key)
    else:
        return Timestamp(key)


def _merge_timeseries(original: Dict, retry: Dict) -> Dict:
    """
    Merge intelligente di timeseries.
    Sostituisce SOLO le date che avevano NaN nell'originale.
    Preserva tutto il resto identico.
    Normalizza gli indici (Timestamp).
    """
    # Normalizza indici originali
    merged = {}
    for date_key, value in original.items():
        norm_key = _normalize_date_key(date_key)
        merged[norm_key] = value
    
    # Merge con retry
    for date_key, retry_value in retry.items():
        norm_key = _normalize_date_key(date_key)
        original_value = merged.get(norm_key)
        
        # Sostituisci SOLO se nell'originale era NaN/None e nel retry è valido
        if is_none_or_nan(original_value) and not is_none_or_nan(retry_value):
            merged[norm_key] = retry_value
    
    return merged


def merge_incomplete_results(
    original_results: Dict[str, Dict[str, Any]],
    retry_results: Dict[str, Dict[str, Any]],
    incomplete_statuses: List[Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Merge intelligente dei risultati di retry.
    
    Aggiorna SOLO:
    - Le date con NaN/None nella timeseries (rimpiazza con valore dal retry)
    - I field completamente mancanti (None) sostituisce con valore dal retry
    
    Preserva:
    - Tutte le date con valori validi
    - Tutto quello che non era None/NaN
    """
    merged_result = dict(original_results) if original_results else {}
    
    # Mappa instrumentId -> set di field incompleti
    incomplete_fields_map: Dict[str, Set[str]] = {}
    for status in incomplete_statuses:
        instr_id = status.instrument_id
        
        # Field richiesti ma non ricevuti
        missing_fields = status.fields_requested - status.fields_received
        
        # Field ricevuti ma incompleti (hanno NaN interni)
        incomplete_ts_fields = set()
        for field_name in status.fields_received:
            for meta_key in status.metadata.keys():
                if meta_key.startswith(f"timeseries_{field_name.upper()}_incomplete"):
                    incomplete_ts_fields.add(field_name.upper())
        
        incomplete_fields_map[instr_id] = missing_fields | incomplete_ts_fields
    
    # Merge risultati
    for instrument_id, retry_fields in retry_results.items():
        if instrument_id not in merged_result:
            # Nuovo strumento dal retry - normalizza anche qui
            normalized_fields = {}
            for field_name, field_value in retry_fields.items():
                if isinstance(field_value, dict):
                    normalized_fields[field_name] = {_normalize_date_key(k): v for k, v in field_value.items()}
                else:
                    normalized_fields[field_name] = field_value
            merged_result[instrument_id] = normalized_fields
            continue
        
        original_fields = merged_result[instrument_id]
        incomplete_fields = incomplete_fields_map.get(instrument_id, set())
        
        for field_name, retry_value in retry_fields.items():
            field_upper = field_name.upper()
            
            # Se questo field era incompleto -> aggiorna intelligentemente
            if field_upper in incomplete_fields:
                original_value = original_fields.get(field_name)
                
                # Se entrambi sono dict (timeseries) -> merge intelligente
                if isinstance(original_value, dict) and isinstance(retry_value, dict):
                    merged_result[instrument_id][field_name] = _merge_timeseries(
                        original_value, retry_value
                    )
                elif is_none_or_nan(original_value) and not is_none_or_nan(retry_value):
                    # Scalare: sostituisci SOLO se era None/NaN
                    merged_result[instrument_id][field_name] = retry_value
    
    return merged_result
