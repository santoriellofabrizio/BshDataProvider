# first line: 20
@memory.cache
def load_data():
    tickers_list = ["CSSPX.MI", "IUSA.MI", "IUSE.MI"]
    tickers_obj = yf.Tickers(tickers_list)

    data = tickers_obj.history(period="1y", auto_adjust=False)
    fx = yf.download('EURUSD=X', period="1y")

    ter = pd.Series({ticker: tickers_obj.tickers[ticker].info.get("netExpenseRatio", 0)
                     for ticker in tickers_list})
    infos = pd.Series({ticker: tickers_obj.tickers[ticker].info
                       for ticker in tickers_list})

    return data, fx, ter, infos
