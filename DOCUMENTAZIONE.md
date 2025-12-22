# BshDataProvider – Documentazione operativa

## Panoramica
- **BshDataProvider** offre un client unificato per dati di mercato (storici, intraday, snapshot) e dati statici/anagrafici provenienti da Timescale, Oracle e Bloomberg.
- L’entrypoint consigliato è la **facade `BshData`** (`src/interface/bshdata.py`), che inizializza logging, cache e le tre API principali (`market`, `info`, `general`).
- Il **client sottostante** (`src/client.py`) gestisce il dispatch verso i provider attivi, il tracciamento delle richieste e le politiche di fallback.
- La libreria include anche il motore di **adjustment dei rendimenti** (`src/analytics/adjustments`) per applicare TER, FX, dividend e altre correzioni su serie di prezzi.

## Configurazione
- Il file di default è `config/bshdata_config.yaml`:
  - Sezione `client`: abilita/disabilita i provider (`activate_timescale`, `activate_oracle`, `activate_bloomberg`).
  - Sezione `api`: imposta logging (`log_level`, `log_file`, `log_level_file`), cache (`cache`, `cache_path`) e autocomplete/warmup.
- Le credenziali possono essere lette da YAML, variabili d’ambiente (`BSH_*`) o file `.env` grazie al `ConfigManager` (`src/core/utils/config_manager.py`).
- Le subscription specifiche per alcuni strumenti (es. Bloomberg) stanno in `config/subscriptions.yaml` e vengono passate automaticamente ai request builder.

## Avvio rapido
1. **Installazione**: `pip install -e .` (richiede Python 3.11+ e le dipendenze in `pyproject.toml`).
2. **Config locale**: duplica `config/bshdata_config.yaml` inserendo credenziali e scegli i provider da attivare.
3. **Istanziazione**:
   ```python
   from interface.bshdata import BshData

   bsh = BshData(
       config_path="config/bshdata_config.yaml",
       cache=True,
       log_level="INFO",
   )
   ```
4. **Esempi rapidi**:
   ```python
   # Dati di mercato
   etf = bsh.market.get_daily_etf(start="2024-01-01", end="2024-01-31", isin="IE00B4L5Y983")
   fx_intraday = bsh.market.get_intraday_fx(date="2024-03-01", id="EURUSD", frequency="5m")

   # Dati statici / anagrafici
   ter = bsh.info.get_ter(isin="IE00B4L5Y983", source="oracle")
   fx_comp = bsh.info.get_fx_composition(isin="IE00B4L5Y983", source="oracle")

   # Lookup generali
   etp_isins = bsh.general.get_etp_isins(segments=["EQUITY"])
   bsh.general.provider_healthcheck()
   ```

## API di mercato (`MarketDataAPI`)
- File: `src/interface/api/market_api.py` – gestisce serie storiche, intraday e snapshot.
- Metodi principali:
  - `get_daily_*`: ETF (`get_daily_etf`), azioni (`get_daily_stock`), FX (`get_daily_currency`), futures (`get_daily_future`), swap (`get_daily_swap`), CDX (`get_daily_cdx`).
  - `get_intraday_*`: ETF (`get_intraday_etf`), futures (`get_intraday_future`), FX (`get_intraday_fx`), swap (`get_intraday_swap`), CDX (`get_intraday_cdx`).
  - `get_day_snapshot_*`: snapshot giornalieri per ETF e futures con `snapshot_time` configurabile.
- Parametri ricorrenti:
  - `source`: provider primario (es. `timescale`, `bloomberg`, `oracle`).
  - `market` e `currency` per normalizzare richieste (default EUREX per futures su Timescale, ETFP per ETF).
  - `frequency`, `start_time`, `end_time` per l’intraday; `snapshot_time` per i daily aggregati.
- Tutte le richieste passano dal request builder centralizzato e usano il tracker del client per loggare errori e incompletezza; i fallback vengono applicati quando configurati.

## API statiche/anagrafiche (`InfoDataAPI`)
- File: `src/interface/api/info_data_api.py` – orchestrazione di richieste Reference/Bulk/Historical.
- Wrapper disponibili:
  - `get_ter`, `get_dividends`, `get_fx_composition`, `get_pcf_composition`, `get_nav` per dati tipici di ETF/strumenti.
  - `get_future_fields`, `get_stock_fields`, `get_stock_markets`, `get_etp_fields` per metadati specifici.
- Supporta fallback multipli: se una richiesta è incompleta/failed, può ripartire con sorgenti o mercati alternativi mantenendo il tracciamento dello stato.

## API generali (`GeneralDataAPI`)
- File: `src/interface/api/general_data_api.py` – lookup trasversali senza specificare strumenti.
- Uso comune:
  - `get_etp_isins`, `get_etf_markets`, `get_futures_identifiers` per liste e mapping.
  - `get_available_info_fields` / `get_available_market_fields` per scoprire i campi supportati.
  - `provider_healthcheck()` per verificare la disponibilità dei provider attivi.

## Motore di adjustment
- File principale: `src/analytics/adjustments/adjuster.py` – orchestration delle componenti di adjustment su serie di prezzi e FX.
- Caratteristiche:
  - Supporto **intraday** opzionale (`intraday=True`) con normalizzazione automatica delle date solo in modalità daily.
  - Normalizzazione FX (`EURUSD` → `USD`, inversione per `USDEUR`, gestione `NaN` su divisioni per zero).
  - Pipeline estendibile: `Adjuster.add(Component)` permette di concatenare componenti come TER, YTM, FX carry, repo e dividendi.
- Esempio sintetico:
  ```python
  from analytics.adjustments.adjuster import Adjuster
  from analytics.adjustments.ter import TerComponent

  adjuster = Adjuster(prices=df_prices, fx_prices=df_fx, intraday=False)
  adjuster.add(TerComponent(ter_values))
  adjustments = adjuster.calculate()
  clean_returns = adjuster.clean_returns(df_prices.pct_change())
  ```

## Logging, cache e tracking
- `BshData` configura il logging globale una sola volta; puoi regolare `log_level` e `log_file` via costruttore o YAML.
- Cache globale basata su `joblib`: abilita/disabilita con `BshData.enable_cache()` / `BshData.disable_cache()` o tramite parametro `cache` nel costruttore.
- Lo stato delle richieste è disponibile tramite `bsh.client.tracker`, utile per diagnosticare errori/fallback.

## Test e diagnostica
- La suite include test per gli adjuster e la validazione FX (es. `test_adjuster_improvements.py`, `test_fx_validation.py`).
- Esegui tutti i test con `python -m pytest` dalla root del repository per verificare l’integrità dopo modifiche.

