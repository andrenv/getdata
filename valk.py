import requests
import pandas as pd
from datetime import datetime

def get_sector_code(api_key, ticker):
    url = f"https://api.tradier.com/v1/markets/lookup"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }
    params = {
        'q': ticker
    }
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    # Assuming the API returns a sector code in the response
    if 'securities' in data and 'security' in data['securities']:
        return data['securities']['security'][0].get('sector_code', 'Unknown')
    return 'Unknown'

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
tickers = ["AAPL", "MSFT", "GOOGL"]

data = get_tradier_data(api_key, start_date, end_date, tickers)
data.to_csv('data.csv')
print(data.head())
