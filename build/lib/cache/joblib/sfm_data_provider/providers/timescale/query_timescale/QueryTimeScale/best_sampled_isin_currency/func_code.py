# first line: 271
    @cache_bsh_data
    def best_sampled_isin_currency(
            self,
            date: dt.date,
            market: str,
            isin: Union[str, Tuple[str, ...]],  # Usa Tuple invece di List
            seconds_sampling: int,
            currency: str = 'EUR',
            segment: Optional[str] = None
    ):
        # Converti tuple in lista per l'elaborazione
        isins = [isin] if isinstance(isin, str) else list(isin)
        isin_list = ','.join(f"'{i}'" for i in isins)
        isin_filter = f"a.isin IN ({isin_list})"
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        query = f'''
            select isin, "desc", tick + '{seconds_sampling} second' as datetime_sampled, bid_price, bid_qty, ask_price, ask_qty
            from (
                select a.isin as isin, a."desc",
                    time_bucket_gapfill('{seconds_sampling} second', b.datetime) as tick,
                    locf(last(b.bid_px_lev_0, b.datetime)) as bid_price,
                    locf(last(b.bid_qty_lev_0, b.datetime)) as bid_qty,
                    locf(last(b.ask_px_lev_0, b.datetime)) as ask_price,
                    locf(last(b.ask_qty_lev_0, b.datetime)) as ask_qty
                FROM books b, "anatit" a
                where b.datetime >= '{date_start}' 
                  and b.datetime < '{date_end}'
                  and b.id_strumento = a.id_strumento
                  and {isin_filter}
                  and a.cache_provenienza = '{market}'
                  and a.divisa = '{currency}'
                  {f"AND a.segmento = '{segment}'" if segment else ""}
                group by tick, a.isin, a."desc"
                order by a.isin, tick
            ) res
            where res.tick::time + '{seconds_sampling} second'>= '09:00'
              and res.tick::time + '{seconds_sampling} second' <= '17:30'
            order by isin, datetime_sampled
        '''
        return self._get_results(query, date)
