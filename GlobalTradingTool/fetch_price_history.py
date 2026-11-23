from schwab.auth import client_from_token_file
from schwab.client import Client
import asyncio
import datetime
import pytz
import numpy as np
import pandas as pd

api_key = 'w'
app_secret = 'U'
callback_url = 'https://127.0.0.1:8182/'
token_path = './token.json'

client = client_from_token_file(token_path=token_path,api_key=api_key,app_secret=app_secret, asyncio=False)

# Define the symbol and timeframe
symbol = 'AAPL'
timeframe = '1d'

data = client.get_price_history(symbol=symbol, 
                                period_type=Client.PriceHistory.PeriodType.DAY, 
                                period=Client.PriceHistory.Period.ONE_DAY,
                                frequency_type=Client.PriceHistory.FrequencyType.MINUTE, 
                                frequency=Client.PriceHistory.Frequency.EVERY_MINUTE).json()['candles']

#print(data)
df = pd.DataFrame(data)
df['datetime'] = df['datetime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
df.to_csv(f'{symbol}_{timeframe}_historical_data.csv', index=False)
