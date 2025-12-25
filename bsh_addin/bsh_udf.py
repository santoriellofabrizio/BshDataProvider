import re
from datetime import date as _date, datetime as _datetime, timedelta
from functools import lru_cache

import pandas as pd
import xlwings as xw
from dateutil.utils import today

try:
    from utilsExcel import _parse_options, _split_ids_isin_ticker, _format_result_for_excel, \
        _flatten_excel_arg
except ImportError:
    from bsh_addin.utilsExcel import _parse_options, _split_ids_isin_ticker, _format_result_for_excel, \
        _flatten_excel_arg
from interface.bshdata import BshData

# ============================================================
# API singleton
# ============================================================

_api = None


def get_api():
    global _api
    if _api is None:
        _api = BshData(
            config_path=r"C:\AFMachineLearning\Libraries\BshDataProvider\config\bshdata_config.yaml",
        )
    return _api


# ============================================================
# UDF: InfoData
# ============================================================

@xw.func(category="InfoData", async_mode='threading', description="Get NAV time series using ISIN or ticker.")
@xw.ret(expand="table")
def get_nav(id_code,
            start_date,
            end_date=None,
            source="oracle",
            options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame().fillna("").astype(str).values.tolist()

        start = _date.fromisoformat(start_date)
        if isinstance(start, _datetime):
            start = start.date()

        if end_date:
            end_date = _date.fromisoformat(end_date)
            if isinstance(end_date, _datetime):
                end_date = end_date.date()

        res = api.info.get_nav(
            isin=isins,
            ticker=tickers,
            source=source,
            subscriptions=None,
            start=start,
            end=end_date,
            **extra,
        )
        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="InfoData", description="Get historical dividends2.csv using ISIN or ticker.")
@xw.ret(expand="table")
def get_dividends(id_code,
                  start_date=None,
                  end_date=None,
                  source="oracle",
                  options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        if "start" in extra:
            start = extra.pop("start")
        else:
            start = _date.fromisoformat(start_date) if start_date else today() - timedelta(days=360)

        if "end" in extra:
            end = extra.pop("end")
        else:
            end = _date.fromisoformat(end_date) if end_date else today()

        res = api.info.get(**extra)
        return _format_result_for_excel(res)
    except:
        return "ERROR"

@lru_cache
@xw.func(category="InfoData", description="Get FX composition using ISIN or ticker.")
@xw.ret(expand="table")
def get_fx_composition(id_code,
                       reference_date=None,
                       crncy=None,
                       fx_fxfwrd="both",
                       source="oracle",
                       options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return None

        if "fx_fxfwrd" in extra:
            fx_fxfwrd = extra.pop("fx_fxfwrd")
        if "reference_date" in extra:
            reference_date = extra.pop("reference_date")

        res = api.info.get_fx_composition(isin=isins, ticker=tickers,
                                          reference_date=reference_date,
                                          fx_fxfwrd=fx_fxfwrd, source=source, **extra)
        res = res.get(crncy) if crncy else res
        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="InfoData", async_mode='threading', description="Get PCF composition using ISIN or ticker.")
@xw.ret(expand="table")
def get_pcf_composition(id_code,
                        reference_date=None,
                        include_cash=False,
                        comp_field="WEIGHT_NAV",
                        source="oracle",
                        options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        if "reference_date" in extra:
            reference_date = extra.pop("reference_date")
        if "include_cash" in extra:
            include_cash = extra.pop("include_cash")
        if "comp_field" in extra:
            comp_field = extra.pop("comp_field")

        raw = api.info.get(**extra)

        if comp_field.upper() == "RAW":
            return _format_result_for_excel(raw)

        if not isinstance(raw, pd.DataFrame):
            return _format_result_for_excel(raw)

        field = comp_field.upper()
        if field not in raw.columns:
            return _format_result_for_excel(raw)

        pivot = raw.pivot_table(
            index="BSH_ID_ETF",
            columns="BSH_ID_COMP",
            values=field,
            aggfunc="first",
        )

        return _format_result_for_excel(pivot)
    except:
        return "ERROR"


@xw.func(category="InfoData", description="Get generic ETP fields using ISIN or ticker")
@xw.ret(expand="table")
def get_etp_fields(id_code,
                   fields,
                   source="oracle",
                   options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields)

        res = api.info.get_etp_fields(
            type="ETP",
            isin=isins,
            ticker=tickers,
            source=source,
            fields=field_list,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="InfoData", description="Get generic STOCK fields using ISIN or TICKER")
@xw.ret(expand="table")
def get_stock_fields(id_code,
                     fields,
                     source="oracle",
                     options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields)
        res = api.info.get_stock_fields(
            isin=isins,
            ticker=tickers,
            source=source,
            fields=field_list,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="InfoData", description="Get generic STOCK markets")
@xw.ret(expand="table", index=False)
def get_stock_markets(id_code,
                      source="oracle",
                      options=None):
    try:
        api = get_api()
        extra = _parse_options(options)

        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        res = api.info.get_stock_markets(isin=isins, ticker=tickers, source=source, **extra)
        return _format_result_for_excel(res)
    except:
        return "ERROR"


# ============================================================
# MarketData - Daily
# ============================================================

@xw.func(category="MarketData", async_mode="threading")
@xw.ret(expand="table")
def get_daily_etf(id_code,
                  start_date,
                  end_date=None,
                  fields="MID",
                  source="timescale",
                  market="ETFP",
                  currency_et_etp="EUR",
                  snapshot_time="17:00:00",
                  options=None):
    try:
        api = get_api()
        extra = _parse_options(options)
        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields) or ["MID"]

        res = api.market.get_daily_etf(
            start=start_date,
            end=end_date or today(),
            isin=isins,
            ticker=tickers,
            fields=field_list,
            source=source,
            market=market,
            currency=currency_et_etp,
            snapshot_time=snapshot_time,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="MarketData", async_mode="threading")
@xw.ret(expand="table")
def get_daily_future(id_code,
                     start_date,
                     end_date=None,
                     fields="MID",
                     source="timescale",
                     snapshot_time="17:00:00",
                     suffix=None,
                     options=None):
    try:
        api = get_api()
        extra = _parse_options(options)
        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields) or ["MID"]

        res = api.market.get_daily_future(
            start=start_date,
            end=end_date or today(),
            isin=isins,
            ticker=tickers,
            fields=field_list,
            source=source,
            snapshot_time=snapshot_time,
            suffix=suffix,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="MarketData", async_mode="threading")
@xw.ret(expand="table")
def get_daily_currency(id_code,
                       start_date,
                       end_date=None,
                       fields="MID",
                       snapshot_time="17:00:00",
                       source="timescale",
                       options=None):
    try:
        api = get_api()
        extra = _parse_options(options)
        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields) or ["MID"]

        res = api.market.get_daily_currency(
            start=start_date,
            end=end_date or today(),
            isin=isins,
            ticker=tickers,
            snapshot_time=snapshot_time,
            fields=field_list,
            source=source,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


# ============================================================
# MarketData - Intraday
# ============================================================

@xw.func(category="MarketData", volatile=False, call_in_wizard=False)
def get_intraday_etf(id_code,
                     day,
                     start_time="09:00:00",
                     end_time="17:00:00",
                     fields="MID",
                     frequency="15m",
                     curr_of_etp="EUR",
                     source="timescale",
                     market="ETFP",
                     options=None):
    try:
        api = get_api()
        extra = _parse_options(options)
        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame().fillna("").astype(str).values.tolist()

        field_list = _flatten_excel_arg(fields) or ["MID"]
        res = api.market.get_intraday_etf(
            date=day,
            isin=isins,
            ticker=tickers,
            frequency=frequency,
            fields=field_list,
            start_time=start_time,
            end_time=end_time,
            source=source,
            market=market,
            currency=curr_of_etp,
            **extra,
        )
        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="MarketData", async_mode="threading")
@xw.ret(expand="table")
def get_intraday_future(id_code,
                        day,
                        start_time="09:00:00",
                        end_time="17:00:00",
                        fields="MID",
                        frequency="1m",
                        source="timescale",
                        options=None):
    try:
        api = get_api()
        extra = _parse_options(options)
        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields) or ["MID"]

        res = api.market.get_intraday_future(
            date=day,
            isin=isins,
            ticker=tickers,
            frequency=frequency,
            fields=field_list,
            start_time=start_time,
            end_time=end_time,
            source=source,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


@xw.func(category="MarketData", async_mode="threading")
@xw.ret(expand="table")
def get_intraday_currency(id_code,
                          day,
                          start_time="09:00:00",
                          end_time="17:00:00",
                          fields="MID",
                          frequency="1m",
                          source="timescale",
                          options=None):
    try:
        api = get_api()
        extra = _parse_options(options)
        values, isins, tickers = _split_ids_isin_ticker(id_code)
        if not values:
            return pd.DataFrame()

        field_list = _flatten_excel_arg(fields) or ["MID"]

        res = api.market.get_intraday_fx(
            date=day,
            isin=isins,
            ticker=tickers,
            start_time=start_time,
            end_time=end_time,
            frequency=frequency,
            fields=field_list,
            source=source,
            **extra,
        )

        return _format_result_for_excel(res)
    except:
        return "ERROR"


# ============================================================
# System
# ============================================================

@xw.func(category="System", description="Show current sys.path for debug.")
@xw.ret(expand="vertical")
def show_sys_path():
    try:
        import sys
        return sys.path
    except:
        return "ERROR"
