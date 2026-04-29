import datetime as dt
from typing import List, Optional, Union, Tuple
import numpy as np
import pandas as pd
from sfm_datalibrary.connections.db_connections import PostgreSQLConnection

from sfm_data_provider.core.utils.memory_provider import cache_bsh_data


class QueryTimeScale:
    """
    Classe di interfaccia per interrogare Timescale/PostgreSQL con query predefinite
    su libri, trade, fair value, FX, ecc.
    Supporta opzionalmente il filtro per 'segmento' in anatit/anafx.
    """

    def __init__(self,host: str, port: int, db_name: str, user: str, password: str):
        self._port = port
        self._db_name = db_name
        self._host = host
        self._user = user
        self._password = password

    def create_connection(self) -> PostgreSQLConnection:
        conn = PostgreSQLConnection(self._host, self._port, self._db_name, self._user, self._password)
        return conn

    def _get_results(self, query: str, date_: dt.date | None = None, market: str | None = None) -> Optional[pd.DataFrame]:
        conn = self.create_connection()
        results = conn.execute_query(query)
        conn.close()

        if results is None:
            return None

        data, columns = np.array(results[0]), results[1]
        if data is None or len(data) == 0:
            return pd.DataFrame(columns=columns)

        if not date_:
            return pd.DataFrame(data, columns=columns).rename({"ref_date":"date"},axis=1)

        num_extra_columns = 2 if market is None else 3
        result = np.concatenate((np.empty((len(data), num_extra_columns)), data), axis=1)
        col_idx = 0
        if market is not None:
            result[:, col_idx] = market
            col_idx += 1
        if not date_:
            return pd.DataFrame(result, columns=columns)
        else:
            result[:, col_idx] = date_.strftime("%Y%m")
            col_idx += 1
            result[:, col_idx] = date_

            names = ["AAAAMM", "Data"] + columns if market is None else ["Mercato", "AAAAMM", "Data"] + columns
            return pd.DataFrame(result, columns=names)

    # === METODI PRE-ESISTENTI ESTESI ===
    @cache_bsh_data
    def book_all_isin(
        self,
        date: dt.date,
        market: str,
        isin: str,
        segment: Optional[str] = None
    ) -> pd.DataFrame:
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        query = f'''select a.isin, a."desc", b.*
                    from books b, anatit a
                    where b.id_strumento = a.id_strumento
                      and a.isin = '{isin}'
                      and a.cache_provenienza = '{market}'
                      {f"AND a.segmento = '{segment}'" if segment else ""}
                      and b.datetime > '{date_start}' 
                      and b.datetime < '{date_end}'
                    order by b.datetime, b.row_id_cache'''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def depth_sampled_isin(
        self,
        date: dt.date,
        market: str,
        isin: str,
        start_time: dt.time,
        end_time: dt.time,
        seconds_sampling: int,
        segment: Optional[str] = None
    ):
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        start_ti = start_time.strftime("%H:%M:%S")
        end_ti = end_time.strftime("%H:%M:%S")

        query = f'''select isin, "desc", tick + '{seconds_sampling} second' as datetime_sampled, 
                        bid_price_0, bid_qty_0, ask_price_0, ask_qty_0, 
                        bid_price_1, bid_qty_1, ask_price_1, ask_qty_1,
                        bid_price_2, bid_qty_2, ask_price_2, ask_qty_2,
                        bid_price_3, bid_qty_3, ask_price_3, ask_qty_3,
                        bid_price_4, bid_qty_4, ask_price_4, ask_qty_4
                    from (select a.isin, a."desc", time_bucket_gapfill('{seconds_sampling} second', b.datetime) as tick, 
                            locf(last(b.bid_px_lev_0, b.datetime)) as bid_price_0, 
                            locf(last(b.bid_qty_lev_0, b.datetime)) as bid_qty_0,
                            locf(last(b.ask_px_lev_0, b.datetime)) as ask_price_0, 
                            locf(last(b.ask_qty_lev_0, b.datetime)) as ask_qty_0,
                            locf(last(b.bid_px_lev_1, b.datetime)) as bid_price_1, 
                            locf(last(b.bid_qty_lev_1, b.datetime)) as bid_qty_1,
                            locf(last(b.ask_px_lev_1, b.datetime)) as ask_price_1, 
                            locf(last(b.ask_qty_lev_1, b.datetime)) as ask_qty_1,
                            locf(last(b.bid_px_lev_2, b.datetime)) as bid_price_2, 
                            locf(last(b.bid_qty_lev_2, b.datetime)) as bid_qty_2,
                            locf(last(b.ask_px_lev_2, b.datetime)) as ask_price_2, 
                            locf(last(b.ask_qty_lev_2, b.datetime)) as ask_qty_2,
                            locf(last(b.bid_px_lev_3, b.datetime)) as bid_price_3, 
                            locf(last(b.bid_qty_lev_3, b.datetime)) as bid_qty_3,
                            locf(last(b.ask_px_lev_3, b.datetime)) as ask_price_3, 
                            locf(last(b.ask_qty_lev_3, b.datetime)) as ask_qty_3,
                            locf(last(b.bid_px_lev_4, b.datetime)) as bid_price_4, 
                            locf(last(b.bid_qty_lev_4, b.datetime)) as bid_qty_4,
                            locf(last(b.ask_px_lev_4, b.datetime)) as ask_price_4, 
                            locf(last(b.ask_qty_lev_4, b.datetime)) as ask_qty_4
                    FROM books b, "anatit" a
                    where b.datetime >= '{date_start}' 
                      and b.datetime < '{date_end}' 
                      and b.id_strumento = a.id_strumento
                      and a.isin = '{isin}'
                      and a.cache_provenienza = '{market}'
                      {f"AND a.segmento = '{segment}'" if segment else ""}
                    group by tick, a.isin, a."desc"
                    order by a.isin, tick) res
                where res.tick::time + '{seconds_sampling} second'>= '{start_ti}' 
                  and res.tick::time + '{seconds_sampling} second' <= '{end_ti}' 
                  and res.tick::date in (select * from utilities.trading_days)
                order by isin, datetime_sampled'''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def fairvalue_array_isin(
        self,
        date: dt.date,
        market: str,
        array_isin: List[str],
        fairvalue_time: dt.time,
        max_spread_percent: float = 0.01,
        segment: Optional[str] = None
    ):
        isins_list = "', '".join(array_isin)
        datetime_from = f"{date:%Y-%m-%d} 00:00"
        datetime_to = f"{date:%Y-%m-%d} {fairvalue_time:%H:%M:%S}"

        query = f"""
            SELECT 
                a.isin,
                a."desc",
                b.datetime,
                (b.bid_px_lev_0 + b.ask_px_lev_0)/2 AS mid_price,
                b.bid_px_lev_0 AS bid_price,
                b.ask_px_lev_0 AS ask_price
            FROM anatit a
            JOIN LATERAL (
                SELECT datetime, bid_px_lev_0, ask_px_lev_0
                FROM books b
                WHERE b.id_strumento = a.id_strumento
                  AND b.datetime > '{datetime_from}' 
                  AND b.datetime <= '{datetime_to}'
                  AND b.bid_px_lev_0 > 0 
                  AND b.ask_px_lev_0 > b.bid_px_lev_0
                  AND (b.ask_px_lev_0 / b.bid_px_lev_0 - 1) <= {max_spread_percent}
                ORDER BY b.datetime DESC
                LIMIT 1
            ) b ON TRUE
            
                WHERE a.isin IN ('{isins_list}')
                AND a.cache_provenienza = '{market}'
                {f"AND a.segmento = '{segment}'" if segment else ""}
                ORDER BY a.isin;
        """
        return self._get_results(query, market=market, date_=date)

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

    @cache_bsh_data
    def best_sampled_isin(
        self,
        date: dt.date,
        market: str,
        isin: str,
        seconds_sampling: int,
        segment: Optional[str] = None
    ):
        if market is None:
            raise ValueError("market cannot be None")
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
                  and a.isin = '{isin}'
                  and a.cache_provenienza = '{market}'
                  {f"AND a.segmento = '{segment}'" if segment else ""}
                group by tick, a.isin, a."desc"
                order by a.isin, tick
            ) res
            where res.tick::time + '{seconds_sampling} second'>= '09:00'
              and res.tick::time + '{seconds_sampling} second' <= '17:30'
            order by isin, datetime_sampled
        '''
        return self._get_results(query, date)

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


    @cache_bsh_data
    def best_sampled_currency(
        self,
        date: dt.date,
        currency_pair: str,
        seconds_sampling: int,
        segment: Optional[str] = None
    ):
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        query = f'''select currency_pair, tick + '{seconds_sampling} second' as datetime_sampled, bid_price, ask_price
                    from (
                        select a.currency_pair as currency_pair, 
                            time_bucket_gapfill('{seconds_sampling} second', fr.datetime) as tick,
                            locf(last(fr.bid, fr.datetime)) as bid_price,
                            locf(last(fr.ask, fr.datetime)) as ask_price
                        FROM fx_rates fr join anafx a on fr.id_currency_pair = a.id_currency_pair 
                        where fr.datetime >= '{date_start}' 
                          and fr.datetime < '{date_end}' 
                          and a.currency_pair = '{currency_pair}'
                          {f"AND a.segmento = '{segment}'" if segment else ""}
                        group by tick, a.currency_pair
                        order by a.currency_pair, tick
                    ) res
                    where res.tick::time + '{seconds_sampling} second'>= '09:00'
                      and res.tick::time + '{seconds_sampling} second' <= '17:30'
                    order by currency_pair, datetime_sampled'''
        return self._get_results(query, date)

    @cache_bsh_data
    def daily_mid_array_currency(
        self,
        date: dt.date,
        array_currency: List[str],
        fairvalue_time: dt.time,
        segment: Optional[str] = None
    ):
        currency_list = "'" + "', '".join(array_currency) + "'"
        datetime_from = f'{date:%Y-%m-%d} 00:00'
        datetime_to = f'{date:%Y-%m-%d} {fairvalue_time:%H:%M:%S}'
        query = f'''
            select tab.currency_pair, r.datetime, avg((r.bid + r.ask) / 2) as Mid
            from fx_rates r
            inner join (
                select a.currency_pair, a.id_currency_pair, max(r.datetime) as datetime
                from fx_rates r
                inner join anafx a on r.id_currency_pair = a.id_currency_pair
                where a.currency_pair in ({currency_list})
                  and r.datetime > '{datetime_from}' 
                  and r.datetime <= '{datetime_to}'
                  {f"AND a.segmento = '{segment}'" if segment else ""}
                group by a.currency_pair, a.id_currency_pair
            ) tab 
            on tab.id_currency_pair = r.id_currency_pair and r.datetime = tab.datetime
            where r.datetime > '{datetime_from}' and r.datetime <= '{datetime_to}' 
            group by tab.currency_pair, tab.id_currency_pair, r.datetime
        '''
        return self._get_results(query, date_=date)

    @cache_bsh_data
    def trades_market_ohlc_candles(
        self,
        date: dt.date,
        market: str,
        isin: str,
        seconds_sampling: int,
        start_time: Optional[dt.time] = None,
        end_time: Optional[dt.time] = None,
        segment: Optional[str] = None
    ):
        date_str = date.strftime("%Y-%m-%d")
        start_time_str = start_time.strftime("%H:%M:%S") if start_time is not None else "08:30"
        end_time_str = end_time.strftime("%H:%M:%S") if end_time is not None else "18:30"
        query = f'''select a.isin as "ISIN",
                    time_bucket_gapfill('{seconds_sampling} second', m.datetime) as "TIMESTAMP",
                    first(m.price, m.datetime) as "OPEN", last(m.price, m.datetime) as "CLOSE",
                    max(m.price) as "HIGH", min(m.price) as "LOW"
                    FROM market_trades m, "anatit" a
                    where m.datetime >= '{date_str} {start_time_str}'
                      and m.datetime < '{date_str} {end_time_str}'
                      and m.id_strumento = a.id_strumento
                      and a.isin = '{isin}'
                      and a.cache_provenienza = '{market}'
                      f"AND a.segmento = '{segment}'" if segment else ""
                    group by "TIMESTAMP", a.isin
                    order by "TIMESTAMP"'''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def trades_market_vwap_volume_count(
        self,
        date: dt.date,
        market: str,
        isin: str,
        seconds_sampling: int,
        start_time: Optional[dt.time] = None,
        end_time: Optional[dt.time] = None,
        segment: Optional[str] = None
    ):
        date_str = date.strftime("%Y-%m-%d")
        start_time_str = start_time.strftime("%H:%M:%S") if start_time is not None else "08:30"
        end_time_str = end_time.strftime("%H:%M:%S") if end_time is not None else "18:30"
        query = f'''select a.isin as "ISIN",
                    time_bucket_gapfill('{seconds_sampling} second', m.datetime) as "TIMESTAMP",
                    CASE
                        WHEN sum(m.qty) > 0 THEN sum(m.price * m.qty)/sum(m.qty)
                        ELSE 0
                    END AS "VWAP",
                    coalesce(sum(m.qty), 0) as "VOLUME",
                    coalesce(count(*), 0) as "COUNT"
                    FROM market_trades m, "anatit" a
                    where m.datetime >= '{date_str} {start_time_str}'
                      and m.datetime < '{date_str} {end_time_str}'
                      and m.id_strumento = a.id_strumento
                      and a.isin = '{isin}'
                      and a.cache_provenienza = '{market}'
                      f"AND a.segmento = '{segment}'" if segment else ""
                    group by "TIMESTAMP", a.isin
                    order by "TIMESTAMP"'''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def trades_market_array_isin(
        self,
        date: dt.date,
        market: str,
        array_isin: List[str],
        segment: Optional[str] = None
    ):
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        isins_list = "'" + "', '".join(array_isin) + "'"
        segment_clause = f"AND a.segmento = '{segment}'" if segment else ""

        query = f'''SELECT a.isin, a."desc", a.classe, a.mercato_desc, a.segmento, a.divisa, a.moltiplicatore, 
                        sum(abs(m.qty)) as Qty, sum(abs(m.qty) * m.price * a.moltiplicatore) as Ctv, 
                        count(m.id_strumento) as Nop
                    FROM anatit a, market_trades m
                    WHERE a.id_strumento = m.id_strumento 
                      AND a.isin in ({isins_list}) 
                      AND a.cache_provenienza = '{market}'
                      {segment_clause}
                      AND m.datetime >= '{date_start}' 
                      AND m.datetime < '{date_end}'
                    GROUP BY a.isin, a."desc", a.classe, a.mercato_desc, a.segmento, a.divisa, a.moltiplicatore'''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def trades_all_market_array_isin(
        self,
        date: dt.date,
        market: str,
        array_isin: List[str],
        segment: Optional[str] = None
    ):
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        isins_list = "'" + "', '".join(array_isin) + "'"
        query = f'''select m.datetime, a.isin, a."desc", a.classe, a.mercato_desc, a.segmento, a.divisa, a.moltiplicatore, 
                        m.qty as Qty, abs(m.qty) * m.price * a.moltiplicatore as Ctv
                    from anatit a, market_trades m
                    where a.id_strumento = m.id_strumento 
                      and a.isin in ({isins_list}) 
                      and a.cache_provenienza = '{market}'
                      f"AND a.segmento = '{segment}'" if segment else ""
                      and m.datetime >= '{date_start}' 
                      and m.datetime < '{date_end}' '''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def market_phase_market_array_isin(
        self,
        date: dt.date,
        market: str,
        array_isin: List[str],
        segment: Optional[str] = None
    ):
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        isins_list = "'" + "', '".join(array_isin) + "'"
        query = f'''select m.datetime, a.isin, a."desc", m.Fase
                    from anatit a, market_phase m
                    where m.id_strumento in (
                          select id_strumento from anatit 
                          where isin in ({isins_list}) 
                          and cache_provenienza = '{market}'
                          f"AND a.segmento = '{segment}'" if segment else ""
                      ) 
                      and a.id_strumento = m.id_strumento
                      and m.datetime > '{date_start}' 
                      and m.datetime < '{date_end}'
                    order by m.datetime, m.row_id_cache'''
        return self._get_results(query, date_=date, market=market)

    @cache_bsh_data
    def trades_market_array_classe(
        self,
        date: dt.date,
        market: str,
        array_classe: List[str],
        segment: Optional[str] = None
    ):
        date_start = date.strftime("%Y-%m-%d")
        date_end = (date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        classes_list = "'" + "', '".join(array_classe) + "'"
        query = f'''select a.isin, a."desc", a.classe, a.mercato_desc, a.segmento, a.divisa, a.moltiplicatore, 
                        sum(abs(m.qty::bigint)) as Qty, sum(abs(m.qty::bigint) * m.price * a.moltiplicatore) as Ctv, 
                        count(m.id_strumento) as Nop
                    from anatit a, market_trades m
                    where a.id_strumento = m.id_strumento 
                      and a.classe in ({classes_list}) 
                      and a.cache_provenienza = '{market}'
                      f"AND a.segmento = '{segment}'" if segment else ""
                      and m.datetime >= '{date_start}' 
                      and m.datetime < '{date_end}'
                    group by a.isin, a."desc", a.classe, a.mercato_desc, a.segmento, a.divisa, a.moltiplicatore'''

        return self._get_results(query, date_=date, market=market)
    @cache_bsh_data
    def overnight_financing_rate(
            self,
            start_date: dt.date,
            end_date: dt.date,
            rates_list: List[str],
            segment: Optional[str] = None
    ) -> pd.DataFrame:

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        isin_str = "', '".join(rates_list)
        segment_filter = f"AND a.segmento = '{segment}'" if segment else ""

        # ---------------------------------------------------------
        # 1. Estraggo TUTTI i fairvalue nell'intervallo [start_date, end_date]
        # ---------------------------------------------------------
        query = f'''
            SELECT 
                a.isin,
                CAST(f.datetime AS DATE) AS date,
                f.fairvalue
            FROM fairvalues f
            INNER JOIN anatit a 
                ON a.id_strumento = f.id_market_instrument
            WHERE a."isin" IN ('{isin_str}')
              {segment_filter}
              AND CAST(f.datetime AS DATE) >= '{start_str}'
              AND CAST(f.datetime AS DATE) <= '{end_str}'
            ORDER BY a.isin, date
        '''

        return self._get_results(query).pivot(index="date", columns="isin", values="fairvalue")

    @cache_bsh_data
    def get_etf_ytm(self, isin_list: List[str], dates: List[dt.date], coverage_threshold):
        etf_str = "', '".join(isin_list)

        query = f'''SELECT e.ytm, e.coverage, a.isin, e.ref_date
        FROM public.etf_fi_statistics e
        INNER JOIN public.anatit a ON e.id_strumento = a.id_strumento
        WHERE a.isin in ('{etf_str}') AND e.coverage > {coverage_threshold} ORDER BY a.isin'''
        ytm = self._get_results(query)
        etf_ytm_sparse = ytm.pivot(index='date', columns='isin', values='ytm')
        new_index = etf_ytm_sparse.index.union(dates)
        etf_ytm_sparse = etf_ytm_sparse.reindex(new_index, fill_value=np.nan)

        etf_ytm_sparse = etf_ytm_sparse.ffill()
        etf_ytm_sparse = etf_ytm_sparse.bfill()
        return etf_ytm_sparse.loc[dates]

    from typing import Union, List, Tuple
    import pandas as pd

    @cache_bsh_data
    def get_bond_isin(
            self,
            classe: Union[int, List[int], Tuple[int, ...]] = (9, 10),
            cache_provenienza: str = "EURONEXT",
    ) -> pd.DataFrame:
        """
        Estrae gli ISIN univoci filtrando per classe e data, con query pre-formattata.
        """

        # Trasformiamo l'input in una stringa compatibile con SQL: (9, 10) o (9,)
        if isinstance(classe, (list, tuple)):
            # Gestisce il caso di lista/tupla (es: [9, 10] -> "9, 10")
            classe_filter = ", ".join(map(str, classe))
        else:
            # Gestisce il caso di singolo intero (es: 9 -> "9")
            classe_filter = str(classe)

        # Costruzione della query completa (senza parametri esterni)
        query = f'''
            SELECT 
                a.isin
            FROM anatit a
            JOIN books b ON a.id_strumento = b.id_strumento
            WHERE a.classe IN ({classe_filter}) 
              AND a.cache_provenienza = '{cache_provenienza}'
              AND b.datetime >= (
                  SELECT max(d)
                  FROM generate_series(
                      current_date - interval '4 day',
                      current_date - interval '1 day',
                      interval '1 day'
                  ) d
                  WHERE extract(isodow from d) < 6
              )
            GROUP BY a.isin
        '''

        return self._get_results(query)
