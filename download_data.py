import yfinance as yf


tickers = ["CSSPX", "IUSA","IUSE"]

subs = [f"{t}.MI" for t in tickers]


data = yf.Tickers(subs)
data = data.download(interval="15m", start='2025-11-01', auto_adjust=False)
# data.Dividends.to_csv("DividendsData.csv")
close = data["Close"]
close.columns = tickers
close.to_csv("prices.csv")

fx = yf.Ticker('EURUSD=X').history(interval="15m", start='2025-11-01')



a = 0




