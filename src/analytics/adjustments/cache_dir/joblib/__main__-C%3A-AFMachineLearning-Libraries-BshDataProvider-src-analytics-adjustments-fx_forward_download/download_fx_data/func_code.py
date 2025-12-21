# first line: 11
@memory.cache
def download_fx_data():
    API_KEY = "b56648746ca2be5b2df43964d50f9e03"
    BASE_URL = "https://api.forexrateapi.com/v1"

    # 1. Recuperiamo lo SPOT (per calcolare il differenziale)
    spot_resp = requests.get(f"{BASE_URL}/latest", params={"api_key": API_KEY, "base": "EUR", "symbols": "USD"})
    spot_rate = spot_resp.json()["rates"]["USD"]

    # 2. Recuperiamo i FORWARD
    params = {
        "api_key": API_KEY,
        "base": "EUR",
        "symbols": "USD",
        "tenors": "1M,3M,6M,1Y"
    }
    resp = requests.get(f"{BASE_URL}/forward-rates", params=params)
    resp.raise_for_status()
    data = resp.json()

    records = []
    # Mappa per annualizzare i rendimenti
    tenor_to_year = {"1M": 12, "3M": 4, "6M": 2, "1Y": 1}

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
