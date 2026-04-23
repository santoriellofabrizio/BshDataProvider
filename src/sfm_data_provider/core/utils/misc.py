import threading


def _animate_progress(pbar, max_increment: int, stop_event: threading.Event,
                      base_delay: float = 0.05, max_delay: float = 1.5) -> None:
    """Riempie incrementalmente la barra durante un dispatch sincrono.

    Rallenta in modo esponenziale: non raggiunge mai max_increment da sola,
    in modo che il chiamante possa scattare al valore reale al termine.
    """
    animated = 0
    delay = base_delay
    while animated < max_increment - 1:
        if stop_event.wait(delay):
            return
        pbar.update(1)
        animated += 1
        delay = min(delay * 1.08, max_delay)
