# first line: 1044
    @cache_bsh_data
    def get_etps_data(self) -> list[dict]:
        """
        Carica tutti gli ETP da af_datamart_dba.etps_instruments.
        """
        data, cols = self.conn.execute_query("""
                     SELECT
                        e.ID,
                        e.H2O_ID,
                        e.CBONDS_ID,
                        e.ISIN,
                        e.DESCRIPTION,
                        e.CFI_CODE,
                        e.INSTRUMENT_TYPE,
                        i.SHORT_NAME,
                        e.INSTRUMENT_ID,
                        e.TICKER,
                        e.UNDERLYING_TYPE,
                        e.UNDERLYING_CATEGORY,
                        e.UNDERLYING_ID,
                        e.LEVERAGE,
                        e.ETP_TYPE,
                        e.CURRENCY_HEDGING,
                        e.STATUS,
                        e.MERGED_ETF_ID,
                        e.FUND_CURRENCY,
                        e.PAYMENT_POLICY,
                        e.ISSUE_DATE,
                        e.PRIMARY_EXCHANGE_CODE,
                        e.PRIMARY_EXCHANGE_ID,
                        e.UNDERLYING,
                        e.ISSUER_ID
                    FROM af_datamart_dba.etps_instruments e
                    JOIN ISSUERS i
                        ON e.ISSUER_ID = i.ISSUER_ID
                    WHERE e.STATUS = 'ACTIVE'

           """)
        return [dict(zip(cols, row)) for row in data]
