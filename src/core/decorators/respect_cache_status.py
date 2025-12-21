import functools
import logging
from core.utils.memory_provider import ENABLE_CACHE, enable_cache, disable_cache

logger = logging.getLogger(__name__)

def respect_cache_kwarg(func):
    """
    Decoratore che controlla il kwarg 'cache':
    - Se cache=True → forza la cache attiva
    - Se cache=False → la disattiva
    - Se non specificato → lascia lo stato invariato
    Dopo l'esecuzione, ripristina lo stato iniziale.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        # Salva stato corrente
        prev_state = ENABLE_CACHE
        new_state = kwargs.pop("cache", None)

        try:
            # Aggiorna solo se l’utente ha specificato cache=
            if new_state is not None:
                if new_state:
                    enable_cache()
                else:
                    disable_cache()
                logger.debug(f"[CACHE TEMPORARY {'ENABLED' if new_state else 'DISABLED'}] for {func.__qualname__}")

            # Esegui funzione
            return func(*args, **kwargs)

        finally:
            # Ripristina lo stato iniziale
            if new_state is not None and ENABLE_CACHE != prev_state:
                if prev_state:
                    enable_cache()
                else:
                    disable_cache()
                logger.debug(f"[CACHE RESTORED] {func.__qualname__} → {'ENABLED' if prev_state else 'DISABLED'}")

    return wrapper
