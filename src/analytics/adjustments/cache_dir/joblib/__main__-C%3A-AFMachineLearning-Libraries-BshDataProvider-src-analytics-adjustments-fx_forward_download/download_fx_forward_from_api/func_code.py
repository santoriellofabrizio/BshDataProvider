# first line: 11
@memory.cache
def download_fx_forward_from_api():
    API_KEY = "b56648746ca2be5b2df43964d50f9e03"
    BASE_URL = "https://api.forexrateapi.com/v1"

    # Cambiamo l'endpoint in quello documentato
    # Nota: Verifica nel tuo piano se l'endpoint è 'forward' o 'fluctuation'
    endpoint = "/forward"

    params = {
        "api_key": API_KEY,
        "base": "EUR",
        "symbols": "USD",
        # Alcune API richiedono i giorni (days) invece dei tenors testuali
    }

    resp = requests.get(f"{BASE_URL}{endpoint}", params=params)

    if resp.status_code == 404:
        print("L'endpoint '/forward' non è stato trovato. Verificare il piano API.")
        # Fallback o gestione errore
        return pd.DataFrame()

    resp.raise_for_status()
    # ... resto del parsing
    for tenor, fwd_rate in data["forward_rates"]["USD"].items():
        # Calcolo dell'hedging cost annualizzato
        # (Fwd / Spot - 1) * frequenza_annua
        annualized_cost = (fwd_rate / spot_rate - 1) * tenor_to_year.get(tenor, 1)

        records.append({
            "tenor": tenor,
            "spot": spot_rate,
            "fx_forward": fwd_rate,
            "annualized_hedging_cost": annualized_cost
        })

    return pd.DataFrame(records).set_index("tenor")
