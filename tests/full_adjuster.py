import numpy as np
import pandas as pd
from datetime import time, date
from matplotlib import pyplot as plt

from sfm_data_provider.analytics.adjustments import Adjuster, TerComponent, FxSpotComponent, FxForwardCarryComponent, \
    DividendComponent
# Import dai tuoi moduli specifici
from sfm_data_provider.interface.bshdata import BshData


def full_test():
    # --- 1. SETUP INIZIALE ---
    # IUSA (S&P 500 UCITS ETF USD) vs IUSE (S&P 500 EUR Hedged)
    ticker = ['IUSA', 'IUSE']
    currencies = ['USD']
    start_date = '2026-02-10'
    end_date = '2026-04-20'

    # Inizializzazione API con config locale
    config_path = r'C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml'
    api = BshData(config_path)

    # --- 2. RECUPERO DATI ANAGRAFICI E COSTI ---
    ter = api.info.get_ter(ticker=ticker) / 100

    fx_composition = api.info.get_fx_composition(
        ticker=ticker, fx_fxfwrd='fx'
    )
    fx_forward_info = api.info.get_fx_composition(
        ticker=ticker, fx_fxfwrd="fxfwrd"
    )

    # --- 3. RECUPERO DATI DI MERCATO (DAILY) ---
    # Usiamo lo stesso snapshot_time per ETF e Cambi per minimizzare il rumore
    snap = time(16)

    prices = api.market.get_daily_etf(
        ticker=ticker, start=start_date, end=end_date, snapshot_time=snap
    )

    fx_prices = api.market.get_daily_currency(
        ticker=[f"EUR{c}" for c in currencies], start=start_date, end=end_date, snapshot_time=snap
    ).to_frame(name="EURUSD")

    fx_forward_prices = api.market.get_daily_fx_forward(
        base_currency='EUR',
        quoted_currency=fx_forward_info.columns.tolist(),
        start=start_date,
        end=end_date
    )

    dividends = api.info.get_dividends(ticker=ticker, start=start_date)

    # --- 4. COSTRUZIONE DELL'ADJUSTER ---
    # Applichiamo le componenti per "pulire" i rendimenti dai fattori esterni
    adjuster = (
        Adjuster(prices)
        .add(TerComponent(ter))
        .add(FxSpotComponent(fx_composition, fx_prices))
        .add(FxForwardCarryComponent(fx_forward_info, fx_forward_prices, "1M", fx_prices))
        .add(DividendComponent(dividends, prices, fx_prices=fx_prices))
    )


    # --- 5. ANALISI DEI RENDIMENTI (ALPHA & BETA) ---
    clean_returns = adjuster.get_clean_returns().dropna() * 10000
    debug = adjuster.get_breakdown()

    for name, val in debug.items():
        val.plot(kind='bar', title=name)
        plt.show()

    # X = IUSA (Benchmark), Y = IUSE (Test)
    x = clean_returns['IUSA'].values
    y = clean_returns['IUSE'].values

    # Calcolo manuale con NumPy (Minimi Quadrati)
    # Beta = Cov(x,y) / Var(x)
    matrix = np.cov(x, y)
    beta = matrix[0, 1] / matrix[0, 0]

    # Alpha = Media(y) - Beta * Media(x)
    alpha_daily = np.mean(y) - beta * np.mean(x)
    alpha_annualized = alpha_daily * 252

    # R-squared (coefficiente di determinazione)
    correlation_matrix = np.corrcoef(x, y)
    r_squared = correlation_matrix[0, 1] ** 2

    # --- 6. OUTPUT RISULTATI ---
    print("\n" + "=" * 40)
    print("TEST RISULTATI: IUSA vs IUSE (Adjusted)")
    print("=" * 40)
    print(f"BETA:         {beta:.4f}  (Aspettativa: ~1.0)")
    print(f"ALPHA (Ann.): {alpha_annualized:.6f}  (Aspettativa: ~0.0)")
    print(f"R-SQUARED:    {r_squared:.4f}  (Aspettativa: >0.98)")
    print("=" * 40)

    # --- 7. VISUALIZZAZIONE ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # Plot 1: Performance Cumulata
    cumulative_ret = (1 + clean_returns).cumprod()
    cumulative_ret.plot(ax=ax1, title='Confronto Performance Cumulata Rettificata')
    ax1.set_ylabel('Valore Rebased (1.0)')
    ax1.grid(True, linestyle='--', alpha=0.7)

    # Plot 2: Scatter Plot della Regressione
    ax2.scatter(x, y, alpha=0.5, color='blue', label='Rendimenti Giornalieri')
    # Linea di regressione: y = a + bx
    regression_line = alpha_daily + beta * x
    ax2.plot(x, regression_line, color='red', lw=2, label=f'Fit: Beta={beta:.3f}')

    ax2.set_title('Analisi di Regressione: IUSA vs IUSE')
    ax2.set_xlabel('Rendimenti IUSA (Clean)')
    ax2.set_ylabel('Rendimenti IUSE (Clean)')
    ax2.legend()
    ax2.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    full_test()