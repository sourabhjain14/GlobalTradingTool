import sys
from schwab.auth import easy_client, client_from_token_file
from schwab.client import Client
from schwab.streaming import StreamClient
import asyncio
import json
import threading
import datetime
import pandas as pd

from schwab.orders.equities import equity_buy_limit
from schwab.orders.common import Duration, Session

import globalvar

def get_token_age(client):
    token_age=client.token_age()
    token_age_remaining = 7*24*3600-token_age
    token_age_remaining_hr = token_age_remaining/3600
    print("token_age_remaining:",token_age_remaining_hr//24, " Days ",token_age_remaining_hr%24, "Hour")

def get_df(candles_dict):
    df_data = {}
    # Loop through the original dictionary and populate df_data
    for stock, entries in candles_dict.items():
        for entry in entries:
            datetime_val = entry['datetime'].strftime("%Y %M %D %H %M")
            close_val = entry['close']
            if datetime_val not in df_data:
                df_data[datetime_val] = {}
            df_data[datetime_val][stock] = close_val
    
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame.from_dict(df_data, orient='index')
    display(df)


def place_order_using_signal(signal, candles_dict, accountHash):
    for k,v in signal.items():
        if v==True:
            price = str(0.05*(round(candles_dict[k][-1]['close'],2)//0.05))
            print(k,price)
            #print(accountHash,type(accountNumber))
            order_id = client.place_order(accountHash, 
                                          equity_buy_limit(k, 1, price).set_duration(Duration.GOOD_TILL_CANCEL).set_session(Session.NORMAL).build())
            print(order_id)
            return order_id
            #break
#place_order_using_signal()

def start_streamer(client,accountNumber):
    #def streamer(symbol_list,accountNumber):
    stream_client = StreamClient(client, account_id=accountNumber)
    
    def run_async(coroutine): 
        loop = asyncio.new_event_loop() 
        asyncio.set_event_loop(loop) 
        loop.run_until_complete(coroutine)
    
    async def read_stream():
        await stream_client.login()
        global counter
        counter+=1
        candles_dict=globalvar.candles_dict
        def print_message(message):
            #print(type(message))
            #print(message)
            #adict  = json.dumps(message, indent=4)
            #print(adict)
            #print(type(adict))
            alist = message['content']
            print(alist)
            timenow = datetime.datetime.now()
            for a in alist:
                symbol = a['key']
                try:
                    price = a['LAST_PRICE']
                except:
                    continue
                if symbol not in candles_dict.keys():
                    candles_dict[symbol] = []
                    candles = candles_dict[symbol]
                    candle = {'datetime':timenow,'close':price}
                    candles.append(candle)
                else:
                    candles = candles_dict[symbol]
                    candle = candles[-1]
                    timemn = 60*timenow.hour+timenow.minute
                    candlemn = 60*candle['datetime'].hour+candle['datetime'].minute
                    if timemn>candlemn:
                        new_candle = {'datetime':timenow,'close':price}
                        candles.append(new_candle)
                    else:
                        candle['close'] = price
                #candles_dict[symbol] = price
                #print(counter, symbol, price)
    
        # Always add handlers before subscribing because many streams start sending
        # data immediately after success, and messages with no handlers are dropped.
        #stream_client.add_nasdaq_book_handler(print_message)
        #await stream_client.nasdaq_book_subs(symbol_list)
    
        stream_client.add_level_one_equity_handler(print_message)
        await stream_client.level_one_equity_subs(symbol_list)#, fields='LAST_PRICE')
    
        while True:
            await stream_client.handle_message()
    
    #asyncio.run(read_stream())
    thread = threading.Thread(target=run_async, args=(read_stream(),)) 
    thread.start() 
    #thread.join()