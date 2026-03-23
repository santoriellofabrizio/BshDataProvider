from datetime import time, date

from matplotlib import pyplot as plt

from sfm_data_provider.analytics.adjustments.dividend import DividendComponent

from sfm_data_provider.analytics.adjustments.fx_forward_carry import FxForwardCarryComponent

from sfm_data_provider.analytics.adjustments.fx_spot import FxSpotComponent

from sfm_data_provider.analytics.adjustments.ter import TerComponent

from sfm_data_provider.analytics.adjustments.adjuster import Adjuster
from sfm_data_provider.interface.bshdata import BshData


def full_test():
    # 1. Setup Iniziale
    ticker = ['IUSA', 'IUSE']  # Usiamo una lista pulita
    currencies = ['USD']
    start_date = '2026-01-10'
    end_date = '2026-03-01'
    ref_date = date(2026, 3, 2)

    api = BshData(r'C:\AFMachineLearning\Libraries\SFMDataProvider\config\bshdata_config.yaml')

    # 2. Recupero Dati (Info & Static)
    # Completamento della riga 'ter = g' interrotta
    ter = api.info.get_ter(ticker=ticker) / 100

    fx_composition = api.info.get_fx_composition(
        ticker=ticker, fx_fxfwrd='fx', reference_date=ref_date
    )
    fx_forward_info = api.info.get_fx_composition(
        ticker=ticker, fx_fxfwrd="fxfwrd", reference_date=ref_date
    )

    # 3. Recupero Dati di Mercato (Daily)
    prices = api.market.get_daily_etf(
        ticker=ticker, start=start_date, end=end_date, snapshot_time=time(16)
    )
    fx_prices = api.market.get_daily_currency(
        ticker=[f"EUR{c}" for c in currencies], start=start_date, end=end_date, snapshot_time=time(16)
    )
    fx_forward_prices = api.market.get_daily_fx_forward(base_currency='EUR',
                                                        quoted_currency=fx_forward_info.columns.tolist(),
                                                        start=start_date, end=end_date
                                                        )
    dividends = api.info.get_dividends(ticker=ticker, start=start_date)

    # 4. Inizializzazione Adjuster (Logica della classe)
    # Questa parte applica le componenti di costo/cambio ai prezzi base
    adjuster = (
        Adjuster(prices)
        .add(TerComponent(ter))
        .add(FxSpotComponent(fx_composition, fx_prices))
        .add(FxForwardCarryComponent(fx_forward_info, fx_forward_prices, "1M", fx_prices))
        .add(DividendComponent(dividends, prices, fx_prices=fx_prices))
    )

    # 5. Output dei risultati
    adjusted_returns_BP = adjuster.get_clean_returns()
    adjusted_returns_BP.plot(kind='bar', title='clean_returns')
    (1 + adjusted_returns_BP).cumprod(axis=1).plot(title='rebased_prices')

    plt.show()

    print("Prezzi Originali (Prime righe):")
    print(prices.head())
    print("\nPrezzi Rettificati (TER + FX + Dividendi):")
    print(adjusted_returns_BP.head())
    # 5. Calcolo dei Rendimenti Puliti
    # get_clean_returns() dovrebbe restituire i rendimenti logaritmici o percentuali
    clean_returns = adjuster.get_clean_returns().dropna()

    # Assumiamo di confrontare IUSA (Ticker A) vs IUSE (Ticker B)
    y = clean_returns['IUSE']
    x = clean_returns['IUSA']

    # 6. Regressione Lineare per Alpha e Beta
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    # Alpha annualizzato (assumendo rendimenti giornalieri e 252 giorni lavorativi)
    annualized_alpha = intercept * 252

    print("-" * 30)
    print(f"RISULTATI ANALISI REGRESSIONE:")
    print(f"Beta (Slope): {slope:.4f}  <-- Target: ~1.0")
    print(f"Alpha (Intercept): {intercept:.6f}")
    print(f"Alpha Annualizzato: {annualized_alpha:.4f} <-- Target: ~0.0")
    print(f"R-squared: {r_value ** 2:.4f}")
    print("-" * 30)

    # 7. Visualizzazione Comparativa
    fig, ax = plt.subplots(2, 1, figsize=(10, 10))

    # Plot 1: Rendimenti Cumulati Sovrapposti (Performance Relativa)
    (1 + clean_returns).cumprod().plot(ax=ax[0], title='Performance Cumulata Pulita (Base 1)')
    ax[0].set_ylabel('Rendimento Cumulato')
    ax[0].grid(True)

    # Plot 2: Scatter Plot con Linea di Regressione
    ax[1].scatter(x, y, alpha=0.5, label='Rendimenti Giornalieri')
    ax[1].plot(x, intercept + slope * x, 'r', label=f'Regressione (Beta={slope:.2f})')
    ax[1].set_title('Scatter Plot: IUSA vs IUSE')
    ax[1].set_xlabel('Rendimenti IUSA')
    ax[1].set_ylabel('Rendimenti IUSE')
    ax[1].legend()
    ax[1].grid(True)

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    full_test()