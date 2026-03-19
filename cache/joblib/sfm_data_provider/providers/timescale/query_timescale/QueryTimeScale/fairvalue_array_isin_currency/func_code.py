# first line: 182
    @cache_bsh_data
    def fairvalue_array_isin_currency(
        self,
        date: dt.date,
        market: str,
        array_isin: Tuple[str],
        currency: str,
        fairvalue_time: dt.time,
        max_spread_percent: float = 1.,
        segment: Optional[str] = None
    ):
        isins_list = "'" + "', '".join(array_isin) + "'"
        datetime_from = f'{date:%Y-%m-%d} 00:00'
        datetime_to = f'{date:%Y-%m-%d} {fairvalue_time:%H:%M:%S}'

        query = f"""
            SELECT 
                tab.isin,
                tab."desc",
                b.datetime,
                b.bid_px_lev_0 AS bid,
                b.ask_px_lev_0 AS ask,
                (b.bid_px_lev_0 + b.ask_px_lev_0) / 2 AS mid
            FROM books b
            INNER JOIN (
                SELECT 
                    a.isin,
                    a."desc",
                    a.id_strumento,
                    MAX(b.datetime) AS datetime
                FROM books b
                INNER JOIN anatit a 
                    ON b.id_strumento = a.id_strumento
                WHERE 
                    a.isin IN ({isins_list})
                    AND a.cache_provenienza = '{market}'
                    AND a.divisa = '{currency}'
                    {f"AND a.segmento = '{segment}'" if segment else ""}
                    AND b.datetime > '{datetime_from}'
                    AND b.datetime <= '{datetime_to}'
                    AND b.bid_px_lev_0 > 0
                    AND b.ask_px_lev_0 > b.bid_px_lev_0
                    AND b.ask_px_lev_0 / b.bid_px_lev_0 - 1 <= {max_spread_percent}
                GROUP BY a.isin, a."desc", a.id_strumento
            ) tab 
                ON tab.id_strumento = b.id_strumento 
                AND b.datetime = tab.datetime
            WHERE 
                b.datetime > '{datetime_from}' 
                AND b.datetime <= '{datetime_to}'
        """
        return self._get_results(query, market=market, date_=date)
