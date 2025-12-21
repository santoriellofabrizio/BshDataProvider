# first line: 38
@memory.cache
def load_sp500(adj=True):
    sp500 = yf.download("^GSPC", period="1y", auto_adjust=adj)
    return sp500
