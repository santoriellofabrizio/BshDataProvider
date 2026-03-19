"""
Test End-to-End del sistema Analytics per ritorni aggiustati.
Questo test verifica che tutti i componenti funzionino insieme.
"""
import pandas as pd
import datetime as dt





def test_analytics():
    print("=" * 60)
    print("TEST END-TO-END: Sistema Analytics Ritorni Aggiustati")
    print("=" * 60)

    # ============================================================================
    # STEP 1: Setup dati di test
    # ============================================================================
    print("\n[STEP 1] Setup dati di test...")

    isin_list = ['US0378331005', 'US5949181045', 'US88160R1014']  # AAPL, MSFT, TSLA
    bbg_codes = {isin: isin for isin in isin_list}
    trading_currency = pd.Series(['USD'] * len(isin_list), index=isin_list)
    settlement_type = pd.Series(['T+2'] * len(isin_list), index=isin_list)

    # Generate business days for last week
    end_date = dt.date(2024, 12, 6)
    start_date = dt.date(2024, 12, 2)
    relevant_dates = generate_date_range(start_date, end_date, business_days_only=True)

    print(f"✓ ISIN list: {isin_list}")
    print(f"✓ Date range: {start_date} to {end_date} ({len(relevant_dates)} business days)")

    # ============================================================================
    # STEP 2: Crea logger
    # ============================================================================
    print("\n[STEP 2] Creazione logger...")
    logger = AnalyticsLogger(log_terminal=True, log_terminal_level='INFO')
    logger.info("Logger inizializzato correttamente")
    print("✓ Logger creato")

    # ============================================================================
    # STEP 3: Crea ProviderAdapter con dati mock
    # ============================================================================
    print("\n[STEP 3] Creazione ProviderAdapter con dati mock...")

    class MockBshClient:
        """Mock del client BshData per testing."""
        pass

    # Crea adapter personalizzato con dividendi mock
    class TestProviderAdapter(ProviderAdapter):
        def download_dividends(self, isin_list, relevant_dates, bbg_codes_dict):
            """Override con dividendi mock per test."""
            df = pd.DataFrame(0., index=relevant_dates, columns=isin_list)
            # Simula dividendi
            df.loc[dt.date(2024, 12, 3), 'US0378331005'] = 0.25  # AAPL $0.25
            df.loc[dt.date(2024, 12, 5), 'US5949181045'] = 0.75  # MSFT $0.75
            df.loc[dt.date(2024, 12, 4), 'US88160R1014'] = 0.00  # TSLA no dividend
            return df

    adapter = TestProviderAdapter(MockBshClient())
    print("✓ ProviderAdapter creato con dividendi mock")

    # ============================================================================
    # STEP 4: Crea InstrumentFactory e Stock
    # ============================================================================
    print("\n[STEP 4] Creazione strumenti tramite Factory...")

    factory = InstrumentFactory()
    print(f"✓ Factory creata con tipi: {factory.get_registered_types()}")

    # Crea strumento Stock
    stock = factory.create_instrument(
        instrument_list=isin_list,
        bbg_codes_dict=bbg_codes,
        instrument_type='EQUITY',
        trading_currency=trading_currency,
        data_downloader=adapter,
        relevant_dates=relevant_dates,
        settlement_type=settlement_type,
        logger=logger
    )

    print(f"✓ Strumento creato: {type(stock).__name__}")

    # ============================================================================
    # STEP 5: Download dati (dividendi)
    # ============================================================================
    print("\n[STEP 5] Download dati...")
    stock.download_data()
    print("✓ Download completato")

    # ============================================================================
    # STEP 6: Calcola aggiustamenti
    # ============================================================================
    print("\n[STEP 6] Calcolo aggiustamenti...")
    adjustments = stock.get_adjustments()
    print("✓ Aggiustamenti calcolati")
    print(f"\nAggiustamenti (dividendi):")
    print(adjustments)

    # Verifica dividendi
    total_dividends = adjustments.sum().sum()
    print(f"\n✓ Totale dividendi: ${total_dividends:.2f}")

    # ============================================================================
    # STEP 7: Simula ritorni e applica correzioni
    # ============================================================================
    print("\n[STEP 7] Simulazione ritorni e applicazione correzioni...")

    # Crea ritorni fittizi
    returns = pd.DataFrame({
        'US0378331005': [0.01, -0.005, 0.02, 0.015, -0.01],
        'US5949181045': [0.015, 0.01, -0.008, 0.012, 0.02],
        'US88160R1014': [-0.02, 0.03, 0.01, -0.015, 0.025]
    }, index=relevant_dates)

    print("Ritorni originali (primi 3 giorni):")
    print(returns.head(3))

    # Applica correzioni
    cleaned_returns = stock.clean_returns(returns, cumulative=False)
    print("\nRitorni corretti (primi 3 giorni):")
    print(cleaned_returns.head(3))

    # Confronto
    print("\nDifferenza (correzioni applicate):")
    difference = cleaned_returns - returns
    print(difference)

    # ============================================================================
    # STEP 8: Verifica year fractions
    # ============================================================================
    print("\n[STEP 8] Verifica year fractions...")
    print("Year fractions standard (primi 3 giorni):")
    print(stock._standard_year_fractions.head(3))

    print("\nYear fractions shifted T+2 (primi 3 giorni):")
    print(stock._shifted_year_fractions.head(3))

    # ============================================================================
    # STEP 9: Test utility functions
    # ============================================================================
    print("\n[STEP 9] Test utility functions...")

    # Test get_contiguous_date
    today = dt.date(2024, 12, 6)  # venerdì
    next_business = get_contiguous_date(today, 1)
    prev_business = get_contiguous_date(today, -1)
    print(f"✓ Oggi: {today} ({today.strftime('%A')})")
    print(f"✓ Prossimo giorno lavorativo: {next_business} ({next_business.strftime('%A')})")
    print(f"✓ Precedente giorno lavorativo: {prev_business} ({prev_business.strftime('%A')})")

    # ============================================================================
    # RIEPILOGO FINALE
    # ============================================================================
    print("\n" + "=" * 60)
    print("✅ TEST END-TO-END COMPLETATO CON SUCCESSO!")
    print("=" * 60)
    print(f"\nComponenti testati:")
    print(f"  ✓ Logger (con colori)")
    print(f"  ✓ ProviderAdapter (con mock)")
    print(f"  ✓ InstrumentFactory")
    print(f"  ✓ Stock (calcolo dividendi)")
    print(f"  ✓ Aggiustamenti ritorni")
    print(f"  ✓ Year fractions (standard e shifted)")
    print(f"  ✓ Utility functions")
    print(f"\nIl sistema è pronto per essere integrato con i tuoi provider reali!")
    print("=" * 60)


def test_etfs():
    from client import BSHDataClient
    from core.instruments import InstrumentFactory
    import datetime as dt

    client = BSHDataClient("config/bshdata_config.yaml")
    factory = InstrumentFactory(client)

    instruments = [
        factory.create(type='ETP', ticker='IHYG', currency='EUR', autocomplete=True),
        factory.create(type='ETP', ticker='IHYU', currency='EUR', autocomplete=True),
    ]

    adjuster = ReturnAdjuster(
        instruments=instruments,
        window_days=10,
        last_day=dt.date(2025, 11, 5)
    )

    adjuster.set_data_downloader(client)
    adjuster.download_data()

    adjustments = adjuster.get_adjustments()
    print(adjustments)

def test_end_to_end_stock():
    """
    Test ReturnAdjuster con ETF obbligazionari IHYG e IHYU
    """
    from client import BSHDataClient
    from core.instruments import InstrumentFactory
    import datetime as dt

    client = BSHDataClient("config/bshdata_config.yaml")
    factory = InstrumentFactory(client)

    instruments = [
        factory.create(type='STOCK', ticker='UCG', currency='EUR', autocomplete=True),
        factory.create(type='STOCK', ticker='RACE', currency='EUR', autocomplete=True),
    ]

    adjuster = ReturnAdjuster(
        instruments=instruments,
        window_days=10,
        last_day=dt.date(2024, 12, 5)
    )

    adjuster.set_data_downloader(client)
    adjuster.download_data()

    adjustments = adjuster.get_adjustments()
    print(adjustments)


def test_future():
    from client import BSHDataClient
    from core.instruments import InstrumentFactory
    import datetime as dt

    client = BSHDataClient("config/bshdata_config.yaml")
    factory = InstrumentFactory(client)

    instruments = [
        factory.create(id="FBTS", autocomplete=True),
        factory.create(id='FBTP', autocomplete=True),
    ]

    adjuster = ReturnAdjuster(
        instruments=instruments,
        window_days=10,
        last_day=dt.date(2025, 10, 5)
    )

    adjuster.set_data_downloader(client)
    adjuster.download_data()

    adjustments = adjuster.get_adjustments()
    print(adjustments)

def test_cdx():

    from client import BSHDataClient
    from core.instruments import InstrumentFactory
    import datetime as dt

    client = BSHDataClient("config/bshdata_config.yaml")
    factory = InstrumentFactory(client)

    instruments = [
        factory.create(id="CDXHY5", type=InstrumentType.CDXINDEX, autocomplete=True),
        factory.create(id='CDXEM5', type=InstrumentType.CDXINDEX, autocomplete=True),
    ]

    adjuster = ReturnAdjuster(
        instruments=instruments,
        window_days=10,
        last_day=dt.date(2025, 10, 5)
    )

    adjuster.set_data_downloader(client)
    adjuster.download_data()

    adjustments = adjuster.get_adjustments()
    print(adjustments)


def test_index():

    from client import BSHDataClient
    from core.instruments import InstrumentFactory
    import datetime as dt

    client = BSHDataClient("config/bshdata_config.yaml")
    factory = InstrumentFactory(client)

    instruments = [
        factory.create(id="ESTRON", type=InstrumentType.INDEX, autocomplete=True),
        factory.create(id='SOFR', type=InstrumentType.INDEX, autocomplete=True),
    ]

    adjuster = ReturnAdjuster(
        instruments=instruments,
        window_days=10,
        last_day=dt.date(2025, 10, 5)
    )

    adjuster.set_data_downloader(client)
    adjuster.download_data()

    adjustments = adjuster.get_adjustments()
    print(adjustments)


