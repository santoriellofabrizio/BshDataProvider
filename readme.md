# BshDataProvider

BshDataProvider è una libreria Python che fornisce un’API unificata per dati di mercato e dati statici provenienti da:

- **TimescaleDB**
- **Oracle**
- **Bloomberg** (opzionale)
- **Mock provider** (per test offline)

Offre un’interfaccia coerente, caching integrato, normalizzazione automatica di ISIN/Ticker, e integrazione con Excel tramite xlwings.

---

## EntryPoint del progetto

bshdata->BshData, facade per le tre api:

  - general
  - info
  - market

per info sui campi disponibili chiama BshData().general.available_fields()
o cercare file fields.yaml

## Template del file di configurazione `bshdata_config.yaml`

```yaml
timescale_connection:
  db_name: aidb
  port: 5432
  user: aivwr
  password: 

oracle_connection:
  environment: PROD
  user: AF_QUANTLIB
  tns_name: ORABOH
  password: 
  schema: AF_DATAMART_DBA

client:
  activate_oracle: True
  activate_timescale: True
  activate_bloomberg: True

api:
  log_level: INFO
  log_file: logs/bshapi.log
  log_level_file: INFO
  autocomplete: True
  cache: True
  cache_path: ../cache_bsh

```

# How to use ? 

```python
# 1) import and instantiate api as:
from bshDataProvider import BshData

api = BshData(config_path = "<example_path>.yaml", **kwrgs)

# 2) use .market .info depending on the type of data required

api.market.get_daily_etf(ticker="IHYG", start="2025-10-10", **kwargs)

```