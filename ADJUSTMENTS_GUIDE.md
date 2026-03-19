# Return Adjustments - Complete Guide

## Overview

La libreria `BshDataProvider` fornisce un sistema completo per il calcolo di aggiustamenti ai ritorni finanziari. Gli aggiustamenti compensano costi, carry e altri effetti che impattano i ritorni ma non sono catturati dalle variazioni di prezzo.

---

## Table of Contents

1. [Architettura](#architettura)
2. [Tipi di Aggiustamenti](#tipi-di-aggiustamenti)
3. [Formule Dettagliate](#formule-dettagliate)
4. [Componenti](#componenti)
5. [Modalità Operative](#modalità-operative)
6. [Live Data Support](#live-data-support)
7. [Best Practices](#best-practices)

---

## Architettura

### Principio Base

```
Raw Return = (Price_t2 - Price_t1) / Price_t1

Adjusted Return = Raw Return + Adjustments
```

### Filosofia

**Gli aggiustamenti correggono i ritorni per costi e benefici che:**
1. **Non appaiono nei prezzi** (es. TER è un costo interno all'ETF)
2. **Impattano la performance reale** dell'investitore
3. **Devono essere sottratti/aggiunti** per confrontare strumenti su base equa

---

## Tipi di Aggiustamenti

### 1. **Costi** (Sottratti dai ritorni - negativi)
- TER (Total Expense Ratio)
- YTM (Yield to Maturity) per Fixed Income
- FX Forward Carry (hedging cost)

### 2. **Benefici** (Aggiunti ai ritorni - positivi)
- Dividendi
- Repo Rate (financing benefit per futures)
- CDX Carry

### 3. **Correzioni** (possono essere + o -)
- FX Spot Correction (esposizione valutaria)

---

## Formule Dettagliate

### 1. TER (Total Expense Ratio)

**Applicabile a:** ETF

**Formula:**
```
adjustment = -TER × year_fraction_shifted
```

**Componenti:**
- `TER`: Ratio annuale in decimale (0.0020 = 0.20% = 20 bps)
- `year_fraction_shifted`: Frazione dell'anno con shift per settlement (T+2)

**Logica:**
- Il TER è un costo **interno all'ETF** che riduce il NAV
- Non appare nei prezzi di mercato (già incorporato nel NAV)
- **Perché sottrarlo?** Per isolare la performance dell'underlying al netto dei costi

**Esempio:**
```python
# ETF con TER = 0.20% annuo
# Periodo: 1 giorno (year_fraction ≈ 1/365 = 0.00274)
# Settlement shift: T+2

TER = 0.0020
year_fraction_shifted = 0.00274

adjustment = -0.0020 × 0.00274 = -0.00000548 = -0.548 bps al giorno
```

**Interpretation:** L'ETF "costa" circa 0.55 bps al giorno in fee di gestione.

---

### 2. YTM (Yield to Maturity)

**Applicabile a:** ETF Fixed Income, Future Fixed Income, Index Fixed Income

**Formula:**
```
adjustment = -YTM × year_fraction_shifted
```

**Componenti:**
- `YTM`: Yield to maturity in decimale (0.045 = 4.5%)
- `year_fraction_shifted`: Frazione dell'anno con shift per settlement

**Logica:**
- Il YTM rappresenta il **rendimento atteso** dell'obbligazione
- Nei prezzi bond, il YTM si manifesta come **carry negativo** (il prezzo sale man mano che ci avvicina alla maturity)
- **Perché sottrarlo?** Per isolare il movimento di prezzo al netto del carry implicito

**Esempio:**
```python
# Bond ETF con YTM = 4.5%
# Periodo: 1 giorno

YTM = 0.045
year_fraction_shifted = 0.00274

adjustment = -0.045 × 0.00274 = -0.0001233 = -12.33 bps al giorno
```

**Interpretation:** Il bond "guadagna" 12.33 bps al giorno di carry, che va sottratto per vedere il movimento puro di spread/tassi.

---

### 3. Dividend

**Applicabile a:** Stock, ETF DIST/INC

**Formula:**
```
dividend_normalized = (dividend_amount × fx_fund) / (price × fx_trading)

adjustment = +dividend_normalized
```

**Componenti:**
- `dividend_amount`: Importo del dividendo nella valuta del fondo
- `fx_fund`: Tasso di cambio EUR/fund_currency
- `price`: Prezzo dello strumento prima dell'ex-dividend
- `fx_trading`: Tasso di cambio EUR/trading_currency

**Logica:**
- I dividendi causano un **drop di prezzo** il giorno ex-dividend
- Il drop è un effetto **meccanico**, non di mercato
- **Perché aggiungerlo?** Per compensare il drop e mantenere continuità nel ritorno

**Esempio:**
```python
# Stock: $1.50 dividend, price = $150
# USD/EUR = 1.10

dividend_amount = 1.50
fx_fund = 1.10  # USD is fund currency
price = 150
fx_trading = 1.10  # USD is trading currency

dividend_normalized = (1.50 × 1.10) / (150 × 1.10) = 1.65 / 165 = 0.01 = 1.0%

adjustment = +0.01 = +1.0%
```

**Interpretation:** Il dividendo del 1% compensa esattamente il drop di prezzo del 1%.

#### **Intraday Dividend Logic**

Per dati intraday, i dividendi sono applicati quando la **data cambia**:

```python
# Timestamps
14-01 16:00  # Last timestamp on 14th
15-01 09:00  # First timestamp on 15th (DATE CHANGED)
15-01 14:00  # Same date (no adjustment)

# Dividend on 15-01:
# - Applied to period 14-01 16:00 → 15-01 09:00 (crosses midnight)
# - NOT applied to period 15-01 09:00 → 15-01 14:00 (same date)
```

**Rationale:** I dividendi sono trattati come eventi a **mezzanotte**.

---

### 4. FX Spot Correction

**Applicabile a:** ETF non-hedged, Stock

**Formula:**
```
fx_correction = Σ(fx_return[ccy] × weight[ccy]) - fx_return[trading_ccy]
```

**Componenti:**
- `fx_return[ccy]`: Ritorno FX EUR/CCY per ogni valuta nell'underlying
- `weight[ccy]`: Peso della valuta nel portafoglio (0.65 = 65%)
- `fx_return[trading_ccy]`: Ritorno FX della valuta di trading

**Logica:**
- Un ETF ha **esposizione multi-valuta** nell'underlying
- Ma **quota in una valuta** (es. EUR)
- Se USD si apprezza vs EUR, e l'ETF ha 65% USD, il ritorno **EUR-based** include il movimento FX
- **Perché correggerlo?** Per isolare il ritorno dell'underlying al netto dell'FX

**Esempio:**
```python
# IWDA LN: 65% USD, 10% GBP, 25% EUR
# Trading currency: EUR
# USD +1%, GBP -0.5%, EUR 0%

fx_return_USD = 0.01
fx_return_GBP = -0.005
fx_return_EUR = 0.0

weighted_fx = (0.01 × 0.65) + (-0.005 × 0.10) + (0.0 × 0.25) 
            = 0.0065 - 0.0005 + 0.0
            = 0.0060

trading_fx = 0.0  # Trading in EUR

fx_correction = 0.0060 - 0.0 = +0.0060 = +60 bps
```

**Interpretation:** L'apprezzamento del USD ha contribuito +60 bps al ritorno EUR-based dell'ETF.

---

### 5. FX Forward Carry

**Applicabile a:** ETF currency-hedged

**Formula:**
```
Step 1: Annualize forward prices
fx_fwd_annualized = fx_fwd_price × tenor_factor
  (e.g., 1M forward: tenor_factor = 12)

Step 2: Convert to decimal
fx_fwd_diff = fx_fwd_annualized / unit_divisor
  (e.g., bp: divisor = 10000)

Step 3: Get rate differential
rate_diff = fx_fwd_diff / fx_spot
  (This gives: r_EUR - r_ccy)

Step 4: Apply weights
fx_fwd_cost = Σ(rate_diff[ccy] × weight_fwd[ccy]) × year_fraction_shifted

adjustment = -fx_fwd_cost
```

**Componenti:**
- `fx_fwd_price`: Prezzo forward (in bp o pct)
- `tenor_factor`: Moltiplicatore per annualizzare (1M = 12, 3M = 4)
- `unit_divisor`: Divisore per convertire unità (bp = 10000)
- `fx_spot`: Tasso spot EUR/CCY
- `weight_fwd`: Peso della valuta hedged

**Logica:**
- Gli ETF currency-hedged usano **forward FX** per hedgare l'esposizione valutaria
- I forward hanno un **costo** basato sul differenziale tassi d'interesse
- Se r_EUR < r_USD, hedgare USD costa (negative carry)
- **Perché sottrarlo?** È un costo reale che riduce il ritorno

**Esempio:**
```python
# ETF hedged 100% USD
# FX Forward 1M: +15 bp (EUR rates higher → pay premium to hedge USD)
# FX Spot: 1.10

fx_fwd_price = 15.0  # bp
tenor_factor = 12  # 1M
unit_divisor = 10000  # bp

fx_fwd_annualized = 15.0 × 12 = 180.0
fx_fwd_diff = 180.0 / 10000 = 0.018 = 1.8%
rate_diff = 0.018 / 1.10 = 0.0164 = 1.64%

fx_fwd_cost = 0.0164 × 1.0 × 0.00274 = 0.0000449 = 4.49 bps al giorno

adjustment = -0.0000449 = -4.49 bps
```

**Interpretation:** Hedgare USD costa 4.49 bps al giorno (perché EUR rates > USD rates).

---

### 6. Repo Rate

**Applicabile a:** Future

**Formula:**
```
adjustment = +repo_rate × year_fraction_shifted
```

**Componenti:**
- `repo_rate`: Tasso repo annuale per la valuta del future
- `year_fraction_shifted`: Frazione dell'anno con shift per settlement

**Logica:**
- Chi compra un **future** non paga il full notional (solo margin)
- **Evita il costo di finanziamento** (repo) che pagherebbe comprando il sottostante
- Questo è un **beneficio** rispetto al cash
- **Perché aggiungerlo?** Per riflettere il saving di financing

**Esempio:**
```python
# Future equity, currency EUR
# Repo rate EUR: 2.0%

repo_rate = 0.020
year_fraction_shifted = 0.00274

adjustment = +0.020 × 0.00274 = +0.0000548 = +5.48 bps al giorno
```

**Interpretation:** Il future "guadagna" 5.48 bps al giorno di financing benefit rispetto al cash.

---

### 7. CDX Carry

**Applicabile a:** CDX (Credit Default Swap Index)

**Formula:**
```
time_to_maturity_days = days from today to maturity
  (Maturity = last_roll_date + 5 years)

carry_rate = (spread / 10000) × (1 / time_to_maturity_days) × 365

adjustment = +carry_rate × year_fraction
```

**Componenti:**
- `spread`: Spread CDX in basis points
- `time_to_maturity_days`: Giorni alla maturity (5Y rolling)
- `year_fraction`: Frazione dell'anno (standard, no shift)

**Roll dates:** 20 Marzo, 20 Settembre

**Logica:**
- Il CDX ha uno **spread** che rappresenta il costo annuale di protezione
- Man mano che passa il tempo, lo spread **accrua** come carry
- Il carry è **positivo** per chi vende protezione (riceve lo spread)
- **Perché aggiungerlo?** Riflette l'accrual dello spread nel tempo

**Esempio:**
```python
# CDX spread: 120 bp
# Time to maturity: 1800 days (≈ 5 years)
# Date: 10 Gennaio 2024 (last roll: 20 Settembre 2023)

spread = 120.0
time_to_maturity_days = 1800

carry_rate = (120.0 / 10000) × (1 / 1800) × 365
           = 0.012 × 0.0005556 × 365
           = 0.00243 = 24.3 bps annui

year_fraction = 0.00274  # 1 day
adjustment = +0.00243 × 0.00274 = +0.00000666 = +0.666 bps al giorno
```

**Interpretation:** Il CDX accrua 0.666 bps al giorno di carry positivo.

---

## Componenti

### TerComponent
```python
ters = {'IWDA LN': 0.0020, 'VWRL LN': 0.0022}
adjuster.add(TerComponent(ters, settlement_days=2))
```

### YtmComponent
```python
ytm = pd.DataFrame({
    'AGGH LN': [0.045, 0.046],  # ETF
    'FUTURE_ISIN': [0.042, 0.043],  # Future
    'INDEX_ISIN': [0.038, 0.039],  # Index
}, index=dates)
adjuster.add(YtmComponent(ytm, settlement_days=2))
```

### DividendComponent
```python
dividends = pd.DataFrame({
    'AAPL US': [0, 0, 0.50, 0],  # $0.50 on date 3
}, index=dates)
adjuster.add(DividendComponent(dividends))
```

### FxSpotComponent
```python
fx_composition = pd.DataFrame({
    'USD': [0.65, 0.60],
    'GBP': [0.10, 0.15],
}, index=['IWDA LN', 'VWRL LN'])
adjuster.add(FxSpotComponent(fx_composition))
```

### FxForwardComponent
```python
fx_fwd_composition = pd.DataFrame({
    'USD': [1.0, 1.0],  # 100% hedged
}, index=['HEDGED_ETF_1', 'HEDGED_ETF_2'])

fx_fwd_prices = pd.DataFrame({
    'USD': [15.0, 16.0],  # bp
}, index=dates)

adjuster.add(FxForwardComponent(
    fx_fwd_composition, 
    fx_fwd_prices,
    tenor='1M',
    unit='bp'
))
```

### RepoComponent
```python
# Mode 1: Per currency
future_currencies = pd.Series({
    'FUTURE_1': 'EUR',
    'FUTURE_2': 'USD',
})
repo_rates = pd.DataFrame({
    'EUR': [0.020, 0.021],
    'USD': [0.025, 0.026],
}, index=dates)
adjuster.add(RepoComponent(future_currencies, repo_rates))

# Mode 2: Per instrument
repo_data = pd.DataFrame({
    'FUTURE_1': [0.020, 0.021],
    'FUTURE_2': [0.025, 0.026],
}, index=dates)
adjuster.add(RepoComponent(repo_data=repo_data))
```

### CdxComponent
```python
cdx_spreads = pd.DataFrame({
    'CDX_ISIN_1': [120.5, 122.0],  # bp
    'CDX_ISIN_2': [85.2, 86.1],
}, index=dates)
adjuster.add(CdxComponent(cdx_spreads, tenor='5Y'))
```

---

## Modalità Operative

### 1. Daily Mode (default)
```python
adj = Adjuster(prices, fx_prices, intraday=False)
# Timestamps normalized to dates
```

### 2. Intraday Mode
```python
adj = Adjuster(prices, fx_prices, intraday=True)
# Timestamps preserved (10:30, 14:00, etc.)
```

### 3. Settlement Type Support
```python
# Same for all (T+2)
adj = Adjuster(prices, fx_prices, settlement_days=2)

# Per-instrument
settlement = pd.Series({
    'ETF_1': 2,  # T+2
    'FUTURE_1': 1,  # T+1
    'BOND_1': 3,  # T+3
})
adj = Adjuster(prices, fx_prices, settlement_days=settlement)

# Or as dict
settlement_dict = {'ETF_1': 2, 'FUTURE_1': 1}
adj = Adjuster(prices, fx_prices, settlement_days=settlement_dict)
```

### 4. Period Returns vs Cumulative Returns

**Period Returns:**
```python
raw_returns = prices.pct_change()
adjustments = adjuster.calculate()
clean_returns = raw_returns + adjustments
```

**Cumulative Returns:**
```python
raw_cumulative = (1 + raw_returns).cumprod() - 1
cumulative_adj = adjuster.get_adjustments_cumulative()
clean_cumulative = raw_cumulative + cumulative_adj
```

---

## Live Data Support

### Problem
Quando ricevi **prezzi live**, non vuoi ricalcolare:
- ✅ TER (statico)
- ✅ YTM (statico)
- ✅ Dividendi (statici)

Ma DEVI ricalcolare:
- ❌ FX Spot correction (dipende da fx_prices live)
- ❌ FX Forward carry (dipende da fx_fwd_prices live)

### Solution
```python
# Initial setup (9:00 AM)
adj = Adjuster(prices, fx_prices, intraday=True)
adj.add(TerComponent(ters))       # Static
adj.add(YtmComponent(ytms))       # Static
adj.add(FxSpotComponent(fx_comp)) # Dynamic (FX-dependent)

# First calculation (calculates everything)
adjustments_9am = adj.calculate()
# TER: -0.0002, YTM: -0.0015, FX: +0.0005
# TOTAL: -0.0012

# --- LIVE FX UPDATE (10:30 AM) ---
new_fx = pd.Series({'USD': 1.12, 'GBP': 0.86})  # From 1.10 to 1.12
adj.update_fx_prices(new_fx)

# Recalculation (ONLY FX components recalculated)
adjustments_10_30 = adj.calculate()
# TER: -0.0002 (CACHED - unchanged)
# YTM: -0.0015 (CACHED - unchanged)
# FX: +0.0008 (RECALCULATED with new FX)
# TOTAL: -0.0009
```

### Benefits
- ⚡ **Performance**: Evita ricalcoli inutili
- ✅ **Accuracy**: Solo FX components sono FX-dependent
- 🔄 **Flexibility**: Update FX live senza reinizializzare tutto

---

## Best Practices

### 1. Component Order
L'ordine NON importa (gli adjustment sono sommati), ma per chiarezza:
```python
adj.add(TerComponent(...))           # Costs first
adj.add(YtmComponent(...))
adj.add(FxForwardComponent(...))
adj.add(DividendComponent(...))      # Benefits
adj.add(RepoComponent(...))
adj.add(FxSpotComponent(...))        # Corrections last
```

### 2. Data Quality
- **TER**: Verifica che sia in decimale (0.0020), non percentuale (0.20)
- **YTM**: Deve essere coerente con il periodo (annuale)
- **FX**: Normalizzato correttamente (EURUSD vs USDEUR)
- **Dividends**: In fund currency, non trading currency

### 3. Testing
```python
# Sanity check: sum of adjustments
total_adj = adjuster.calculate()
print(total_adj.sum(axis=1))  # Should be reasonable magnitude

# Breakdown by component
breakdown = adjuster.get_breakdown()
for comp_name, comp_adj in breakdown.items():
    print(f"{comp_name}: {comp_adj.sum().sum():.6f}")
```

### 4. Live Data
- Use `update_fx_prices()` per FX live
- Non ricalcolare TER/YTM/dividendi ad ogni tick
- Cache static components quando possibile

---

## Summary Table

| Component | Instrument Types | Static/Dynamic | Sign | Formula |
|-----------|-----------------|----------------|------|---------|
| **TER** | ETF | Static | Negative | `-TER × yf_shifted` |
| **YTM** | ETF, Future, Index | Static | Negative | `-YTM × yf_shifted` |
| **Dividend** | Stock, ETF | Static | Positive | `+(div × fx_fund) / (price × fx_trading)` |
| **FX Spot** | ETF, Stock | **Dynamic** | ± | `Σ(fx_ret × weight) - fx_ret_trading` |
| **FX Forward** | ETF hedged | **Dynamic** | Negative | `-Σ(rate_diff × weight_fwd) × yf` |
| **Repo** | Future | Static | Positive | `+repo × yf_shifted` |
| **CDX Carry** | CDX | Static | Positive | `+(spread/10000) × (1/ttm) × 365 × yf` |

**Legend:**
- `yf` = year_fraction
- `yf_shifted` = year_fraction with settlement shift
- `ttm` = time to maturity

---

## References

- **Settlement conventions**: T+1, T+2, T+3 (days between trade and settlement)
- **Year fractions**: ACT/365 (actual days / 365)
- **FX conventions**: EUR base (EURUSD = EUR/USD)
- **CDX roll dates**: March 20, September 20

---

**Date**: December 21, 2025  
**Library**: BshDataProvider  
**Version**: 2.0
