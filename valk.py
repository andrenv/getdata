import requests
import pandas as pd
from datetime import datetime

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
        return data[0]['results'][2]['tables']['asset_classification']['morningstar_sector_code']
    except:
        return 0

    return 0

def get_tradier_data(api_key, start_date, end_date, tickers):
    # Define the endpoint and headers
    url = "https://api.tradier.com/v1/markets/history"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }
    
    all_data = []
    
    for ticker in tickers:
        # Get the sector code for the ticker
        sector_code = get_sector_code(api_key, ticker)
        
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
                df['sector_code'] = sector_code
                all_data.append(df)
        except:
            pass
    # Combine all data into a single DataFrame
    if all_data:
        result = pd.concat(all_data)
        result['date'] = pd.to_datetime(result['date'])
        result.set_index('date', inplace=True)
        return result
    else:
        return pd.DataFrame()

# Usage example
api_key = 'YOUR_TRADIER_API_KEY'  # Replace with your Tradier API key
start_date = '2023-01-01'
end_date = '2023-12-31'
tickers = ["AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "FB", "BRK.B", "JNJ", "JPM", "V", 
    "PG", "NVDA", "DIS", "HD", "PYPL", "NFLX", "XOM", "KO", "PEP", "CSCO", 
    "MRK", "PFE", "INTC", "T", "VZ", "CRM", "WMT", "MCD", "CVX", "IBM", "GS", 
    "BA", "CAT", "MMM", "GE", "ADBE", "CMCSA", "ABT", "NKE", "TMO", "ORCL", 
    "AVGO", "LLY", "DHR", "ACN", "COST", "TXN", "MDT", "HON", "UNH", "BMY", 
    "LMT", "NEE", "QCOM", "LOW", "PM", "SBUX", "MO", "AMGN", "IBM", "BLK", 
    "BKNG", "ISRG", "C", "DE", "DUK", "MMM", "SO", "AXP", "LRCX", "CB", 
    "SCHW", "BA", "SPGI", "GILD", "ZTS", "SYK", "USB", "TJX", "TGT", "FISV", 
    "CAT", "CL", "CCI", "GD", "NSC", "ITW", "WM", "CI", "ADI", "EW", "ECL", 
    "VRTX", "ICE", "MET", "MNST", "KMB", "ETN", "AON", "MMC", "EMR", "APD", 
    "AIG", "ADI", "CME", "MCO", "F", "AEP", "ORLY", "DG", "SRE", "PGR", 
    "HUM", "HCA", "ADP", "MCHP", "KMI", "TEL", "FCX", "OXY", "GM", "SLB", 
    "ROST", "PSA", "PRU", "TRV", "WBA", "WEC", "DFS", "ROK", "AMP", "PXD", 
    "EOG", "PPG", "STT", "RMD", "AFL", "MSI", "ALL", "ES", "D", "HES", 
    "HIG", "SYY", "FIS", "SWK", "LEN", "VTRS", "KHC", "BBY", "XEL", "BAX", 
    "PEAK", "VLO", "CTL", "KEYS", "DRI", "STZ", "ANET", "FTV", "CDW", "IQV", 
    "WMB", "ZBH", "ALGN", "CLX", "KDP", "MAA", "AVB", "TSN", "NTRS", "PPL", 
    "AEE", "DTE", "CMS", "NUE", "CTLT", "ATO", "LYB", "CF", "IRM", "MKC", 
    "DOV", "ODFL", "ETSY", "HOLX", "MPWR", "CBRE", "EXPD", "RSG", "PWR", 
    "CTAS", "NDSN", "WRB", "NVR", "TFX", "PKI", "TYL", "XYL", "MKTX", 
    "BRO", "EXR", "NDAQ", "CNP", "FMC", "JKHY", "LDOS", "TECH", "TSCO", 
    "WST", "CINF", "AMCR", "BIO", "TRMB", "WAB", "CAG", "AKAM", "VMC", 
    "IP", "RJF", "MAS", "PKG", "LUV", "WAT", "HII", "DGX", "BKR", "CE", 
    "NWSA", "NWS", "BF.B", "JBHT", "CPRT", "WDC", "WYNN", "W", "WU", "ZION", 
    "WRK", "WHR", "VFC", "URI", "TPR", "TXT", "TROW", "TER", "SIVB", "SEDG", 
    "SJM", "SEE", "SBAC", "RHI", "PNW", "PKI", "PENN", "OKE", "NTAP", 
    "NLOK", "NI", "NLSN", "NRG", "NTES", "NDAQ", "NDSN", "NOV", "NRZ", 
    "NWL", "NYT", "ODFL", "OKE", "OLN", "OMC", "ON", "ORLY", "OTIS", 
    "OXY", "PAYX", "PCAR", "PEAK", "PENN", "PEP", "PFE", "PG", "PGR", 
    "PH", "PINS", "PKG", "PLD", "PLTR", "PM", "PNC", "PNW", "PPL", "PRU", 
    "PSA", "PSX", "PTC", "PVH", "PWR", "PXD", "PYPL", "QRVO", "QSR", 
    "RCL", "RE", "REG", "RF", "RHI", "RJF", "RL", "RMD", "ROK", "ROL", 
    "ROP", "ROST", "RSG", "RTX", "SBAC", "SBUX", "SCHW", "SEE", "SHW", 
    "SIVB", "SJM", "SLB", "SNA", "SNPS", "SO", "SPGI", "SRE", "STE", 
    "STT", "STX", "STZ", "SWK", "SWKS", "SYK", "SYY", "TAP", "T", "TDG", 
    "TDY", "TEL", "TER", "TFC", "TFX", "TGNA", "TGT", "TJX", "TMO", 
    "TMUS", "TPR", "TRMB", "TROW", "TRV", "TSCO", "TSN", "TT", "TTWO", 
    "TWTR", "TXN", "TXT", "TYL", "UA", "UAL"]

data = get_tradier_data(api_key, start_date, end_date, tickers)
data.to_csv('data.csv')
print(data.head())
