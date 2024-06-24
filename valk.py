import requests
import pandas as pd
from datetime import datetime

import json


def deep_search(node, kv):
    if isinstance(node, list):
        for i in node:
            for x in deep_search(i, kv):
                yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in deep_search(j, kv):
                yield x


def get_sector_code(api_key, ticker):
    url = f"https://api.tradier.com/beta/markets/fundamentals/company"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }
    params = {
        'symbols': ticker
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    try:
        data = data[0]
    except:
        pass

    try:
        x = deep_search(json.loads(json.dumps(data)), 'morningstar_sector_code')
        data = list(data)
        data = [y for y in data if y is not None]
        return data
    except:
        return 0


def get_dividends(api_key, ticker):
    url = f"https://api.tradier.com/beta/markets/fundamentals/dividends"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }
    params = {
        'symbols': ticker
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    try:
        data = data[0]
    except:
        pass

    try:
        data = deep_search(json.loads(json.dumps(data)), 'cash_dividends')
        data = list(data)
        data = [y for y in data if y is not None]
        return data
    except:
        return 0


def div2df(data):
    if len(data) == 1:
        for item in data:
            if type(item) is list:
                return pd.DataFrame(item)
            else:
                return pd.DataFrame(data)
    else:
        print(data)
        print('dividends are bs')


def splits2df(data):
    tmp = []
    for item in data:
       for key in item.keys():

            tmp.append(item[key])

    return pd.DataFrame(tmp)


def get_splits(api_key, ticker):
    url = f"https://api.tradier.com/beta/markets/fundamentals/corporate_actions"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }
    params = {
        'symbols': ticker
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    try:
        data = data[0]
    except:
        pass

    try:
        data = deep_search(json.loads(json.dumps(data)), 'stock_splits')
        data = list(data)
        data = [y for y in data if y is not None]
        return data
    except:
        return 0


def mergedata(data, splits, dividends):

    data['splits'] = 0
    data['dividends'] = 0
    data.set_index('date', inplace=True)

    for i, row in splits.iterrows():
        for j, jrow in data.iterrows():
            t = str(row['ex_date'])
            if str(row['ex_date']) == str(j):
                c = row['adjustment_factor']
                data.at[j, 'splits'] = c

    for i, row in dividends.iterrows():
        for j, jrow in data.iterrows():
            t = str(row['ex_date'])
            if str(row['ex_date']) == str(j):
                c = row['cash_amount']
                data.at[j, 'dividends'] = c

    return data


def get_tradier_data(api_key, start_date, end_date, tickers):
    # Define the endpoint and headers
    url = "https://api.tradier.com/v1/markets/history"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }

    all_data = []

    for ticker in tickers:
        print(ticker)
        # Get the sector code for the ticker
        # sector_code = get_sector_code(api_key, ticker)
        splits = get_splits(api_key, ticker)
        dividends = get_dividends(api_key, ticker)
        splits = splits2df(splits)
        dividends = div2df(dividends)


        # Set the query parameters for historical data
        params = {
            'symbol': ticker,
            'start': start_date,
            'end': end_date
        }

        # Make the request
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        try:
            if 'history' in data and 'day' in data['history']:
                df = pd.DataFrame(data['history']['day'])
                df['symbol'] = ticker
                df = mergedata(df, splits, dividends)
                # df['sector_code'] = sector_code
                all_data.append(df)
        except:
            pass
    # Combine all data into a single DataFrame
    if all_data:
        result = pd.concat(all_data)
        #result['date'] = pd.to_datetime(result['date'])
        #result.set_index('date', inplace=True)
        return result
    else:
        return pd.DataFrame()


# Usage example
api_key = ''  # Replace with your Tradier API key
start_date = '2019-01-01'
end_date = '2024-06-23'
tickers = list(
    {"AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "FB", "BRK.B", "JNJ", "JPM", "V", "PG", "NVDA", "DIS", "HD", "PYPL",
     "NFLX", "XOM", "KO", "PEP", "CSCO", "MRK", "PFE", "INTC", "T", "VZ", "CRM", "WMT", "MCD", "CVX", "IBM", "GS", "BA",
     "CAT", "MMM", "GE", "ADBE", "CMCSA", "ABT", "NKE", "TMO", "ORCL", "AVGO", "LLY", "DHR", "ACN", "COST", "TXN",
     "MDT", "HON", "UNH", "BMY", "LMT", "NEE", "QCOM", "LOW", "PM", "SBUX", "MO", "AMGN", "IBM", "BLK", "BKNG", "ISRG",
     "C", "DE", "DUK", "MMM", "SO", "AXP", "LRCX", "CB", "SCHW", "BA", "SPGI", "GILD", "ZTS", "SYK", "USB", "TJX",
     "TGT", "FISV", "CAT", "CL", "CCI", "GD", "NSC", "ITW", "WM", "CI", "ADI", "EW", "ECL", "VRTX", "ICE", "MET",
     "MNST", "KMB", "ETN", "AON", "MMC", "EMR", "APD", "AIG", "ADI", "CME", "MCO", "F", "AEP", "ORLY", "DG", "SRE",
     "PGR", "HUM", "HCA", "ADP", "MCHP", "KMI", "TEL", "FCX", "OXY", "GM", "SLB", "ROST", "PSA", "PRU", "TRV", "WBA",
     "WEC", "DFS", "ROK", "AMP", "PXD", "EOG", "PPG", "STT", "RMD", "AFL", "MSI", "ALL", "ES", "D", "HES", "HIG", "SYY",
     "FIS", "SWK", "LEN", "VTRS", "KHC", "BBY", "XEL", "BAX", "PEAK", "VLO", "CTL", "KEYS", "DRI", "STZ", "ANET", "FTV",
     "CDW", "IQV", "WMB", "ZBH", "ALGN", "CLX", "KDP", "MAA", "AVB", "TSN", "NTRS", "PPL", "AEE", "DTE", "CMS", "NUE",
     "CTLT", "ATO", "LYB", "CF", "IRM", "MKC", "DOV", "ODFL", "ETSY", "HOLX", "MPWR", "CBRE", "EXPD", "RSG", "PWR",
     "CTAS", "NDSN", "WRB", "NVR", "TFX", "PKI", "TYL", "XYL", "MKTX", "BRO", "EXR", "NDAQ", "CNP", "FMC", "JKHY",
     "LDOS", "TECH", "TSCO", "WST", "CINF", "AMCR", "BIO", "TRMB", "WAB", "CAG", "AKAM", "VMC", "IP", "RJF", "MAS",
     "PKG", "LUV", "WAT", "HII", "DGX", "BKR", "CE", "NWSA", "NWS", "BF.B", "JBHT", "CPRT", "WDC", "WYNN", "W", "WU",
     "ZION", "WRK", "WHR", "VFC", "URI", "TPR", "TXT", "TROW", "TER", "SIVB", "SEDG", "SJM", "SEE", "SBAC", "RHI",
     "PNW", "PKI", "PENN", "OKE", "NTAP", "NLOK", "NI", "NLSN", "NRG", "NTES", "NDAQ", "NDSN", "NOV", "NRZ", "NWL",
     "NYT", "ODFL", "OKE", "OLN", "OMC", "ON", "ORLY", "OTIS", "OXY", "PAYX", "PCAR", "PEAK", "PENN", "PEP", "PFE",
     "PG", "PGR", "PH", "PINS", "PKG", "PLD", "PLTR", "PM", "PNC", "PNW", "PPL", "PRU", "PSA", "PSX", "PTC", "PVH",
     "PWR", "PXD", "PYPL", "QRVO", "QSR", "RCL", "RE", "REG", "RF", "RHI", "RJF", "RL", "RMD", "ROK", "ROL", "ROP",
     "ROST", "RSG", "RTX", "SBAC", "SBUX", "SCHW", "SEE", "SHW", "SIVB", "SJM", "SLB", "SNA", "SNPS", "SO", "SPGI",
     "SRE", "STE", "STT", "STX", "STZ", "SWK", "SWKS", "SYK", "SYY", "TAP", "T", "TDG", "TDY", "TEL", "TER", "TFC",
     "TFX", "TGNA", "TGT", "TJX", "TMO", "TMUS", "TPR", "TRMB", "TROW", "TRV", "TSCO", "TSN", "TT", "TTWO", "TWTR",
     "TXN", "TXT", "TYL", "UA", "UAL"})

print(len(tickers))
data = get_tradier_data(api_key, start_date, end_date, tickers)
data.to_csv('data.csv')
print(data.head())
