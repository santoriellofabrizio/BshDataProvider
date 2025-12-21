# first line: 1047
    @cache_bsh_data
    def get_etps_data(self) -> list[dict]:
        """
        Carica tutti gli ETP da af_datamart_dba.etps_instruments.
        """
        data, cols = self.conn.execute_query("""
               SELECT
                   ID,
                   H2O_ID,
                   CBONDS_ID,
                   ISIN,
                   DESCRIPTION,
                   CFI_CODE,
                   INSTRUMENT_TYPE,
                   ISSUER_ID,
                   INSTRUMENT_ID,
                   TICKER,
                   UNDERLYING_TYPE,
                   UNDERLYING_CATEGORY,
                   UNDERLYING_ID,
                   LEVERAGE,
                   ETP_TYPE,
                   CURRENCY_HEDGING,
                   STATUS,
                   MERGED_ETF_ID,
                   FUND_CURRENCY,
                   PAYMENT_POLICY,
                   ISSUE_DATE,
                   PRIMARY_EXCHANGE_CODE,
                   PRIMARY_EXCHANGE_ID,
                   UNDERLYING
               FROM af_datamart_dba.etps_instruments
               WHERE STATUS = 'ACTIVE'
           """)
        return [dict(zip(cols, row)) for row in data]
