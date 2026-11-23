from schwab.auth import client_from_token_file
from schwab.client import Client
import asyncio
import datetime
import pytz
import numpy as np
import pandas as pd
'''
class PriceHistory:
    class PeriodType(Enum):
        DAY = 'day'
        MONTH = 'month'
        YEAR = 'year'
        YEAR_TO_DATE = 'ytd'

    class Period(Enum):
        # Daily
        ONE_DAY = 1
        TWO_DAYS = 2
        THREE_DAYS = 3
        FOUR_DAYS = 4
        FIVE_DAYS = 5
        TEN_DAYS = 10

        # Monthly
        ONE_MONTH = 1
        TWO_MONTHS = 2
        THREE_MONTHS = 3
        SIX_MONTHS = 6

        # Year
        ONE_YEAR = 1
        TWO_YEARS = 2
        THREE_YEARS = 3
        FIVE_YEARS = 5
        TEN_YEARS = 10
        FIFTEEN_YEARS = 15
        TWENTY_YEARS = 20

        # Year to date
        YEAR_TO_DATE = 1

    class FrequencyType(Enum):
        MINUTE = 'minute'
        DAILY = 'daily'
        WEEKLY = 'weekly'
        MONTHLY = 'monthly'

    class Frequency(Enum):
        # Minute
        EVERY_MINUTE = 1               UPTO 40 days
        EVERY_FIVE_MINUTES = 5         UPTO 9 months
        EVERY_TEN_MINUTES = 10         UPTO 9 months
        EVERY_FIFTEEN_MINUTES = 15     UPTO 9 months
        EVERY_THIRTY_MINUTES = 30      UPTO 9 months

        # Other frequencies
        DAILY = 1
        WEEKLY = 1
        MONTHLY = 1
'''
api_key = 'I'
app_secret = 'U'
callback_url = 'https://127.0.0.1:8182/'
token_path = './token.json'

client = client_from_token_file(token_path=token_path,api_key=api_key,app_secret=app_secret, asyncio=False)


symbol_list = ['SPY','QQQ']
timeframe = 'minute'  # for filename only

'''
data = client.get_price_history(symbol=symbol, 
                                period_type=Client.PriceHistory.PeriodType.DAY, 
                                period=Client.PriceHistory.Period.TEN_DAYS,
                                frequency_type=Client.PriceHistory.FrequencyType.MINUTE, 
                                frequency=Client.PriceHistory.Frequency.EVERY_FIVE_MINUTES).json()['candles']
'''
end_datetime = datetime.datetime.now()
start_datetime = end_datetime - datetime.timedelta(days=40)

z=0
merged_df=None
for symbol in symbol_list:
    data = client.get_price_history(symbol=symbol, 
                                start_datetime = start_datetime,end_datetime = end_datetime,
                                frequency_type=Client.PriceHistory.FrequencyType.MINUTE, 
                                frequency=Client.PriceHistory.Frequency.EVERY_MINUTE).json()['candles']
    #print(data)
    df = pd.DataFrame(data)
    df['datetime'] = df['datetime'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
    ####est = pytz.timezone('US/Eastern')
    #df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize('Asia/Kolkata'))
    #df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize('Asia/Singapore'))
    #df['datetime'] = df['datetime'].apply(lambda x: x.tz_convert('US/Eastern'))
    #df.to_csv(f'{symbol}_{timeframe}_historical_data.csv', index=False)
    df = df[['datetime','close']]
    df.rename(columns={'close':symbol}, inplace=True)
    if z==0:
        merged_df = df
    else:
        merged_df = pd.merge(merged_df, df, on='datetime', how='outer')
    z+=1
merged_df.to_csv('minute_historical_data.csv', index=False)
merged_df.set_index('datetime', inplace=True)
merged_df.ffill(inplace=True)
merged_df2 = merged_df.resample('2min').last()
merged_df2 = merged_df2.dropna(how='all')
merged_df2.to_csv('twominute_historical_data.csv', index=True)


