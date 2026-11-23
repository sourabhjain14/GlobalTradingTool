import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import time


import globalvar



# Alpaca API credentials

# paper
API_KEY = "P"
API_SECRET = "D"


#sourabh paper
#API_KEY = "L"
#API_SECRET = "Q"



BASE_URL = "https://paper-api.alpaca.markets"#"https://paper-api.alpaca.markets"  # Use paper trading for testing

# Initialize Alpaca REST API
def create_client():
    api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")
    api.get_account()
    return api



async def place_order(api, symbol, side, qty=1):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,  # "buy" for long, "sell" for short
            type="market",
            time_in_force="gtc"  # Good till canceled
        )
        return order
        '''
        print("Order Submitted Successfully:")
        print(f"  Order ID: {order.id}")
        print(f"  Client Order ID: {order.client_order_id}")
        print(f"  Symbol: {order.symbol}")
        print(f"  Side: {order.side}")
        print(f"  Quantity: {order.qty}")
        print(f"  Type: {order.order_type}")
        print(f"  Time in Force: {order.time_in_force}")
        print(f"  Status: {order.status}")
        print(f"  Created At: {order.created_at}")
        print(f"  Filled Quantity: {order.filled_qty}")
        print(f"  Filled Avg Price: {order.filled_avg_price}")
        print(f"Placed {side} order for {qty} shares of {symbol}")
        '''
    except Exception as e:
        print(f"Error placing {side} order: {e}")

async def place_limit_order(api, symbol, side, price, qty=1):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,  # "buy" for long, "sell" for short
            type="limit",
            limit_price=str(price),
            time_in_force="gtc"  # Good till canceled
        )
        return order
        '''
        print("Order Submitted Successfully:")
        print(f"  Order ID: {order.id}")
        print(f"  Client Order ID: {order.client_order_id}")
        print(f"  Symbol: {order.symbol}")
        print(f"  Side: {order.side}")
        print(f"  Quantity: {order.qty}")
        print(f"  Type: {order.order_type}")
        print(f"  Time in Force: {order.time_in_force}")
        print(f"  Status: {order.status}")
        print(f"  Created At: {order.created_at}")
        print(f"  Filled Quantity: {order.filled_qty}")
        print(f"  Filled Avg Price: {order.filled_avg_price}")
        print(f"Placed {side} order for {qty} shares of {symbol}")
        '''
    except Exception as e:
        print(f"Error placing {side} order: {e}")





trades = []
candle_data = []

# Function to process trade data and create 10-second candles
def process_trades():
    #global trades, candle_data
    #if not trades:
    #    return
    trades = globalvar.alpaca_trades
    candle_data = globalvar.alpaca_candles
    
    # Convert trades to DataFrame
    df = pd.DataFrame(trades)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # Set the start time for the first candle
    start_time = df["timestamp"].min().floor("10S")
    
    # Group trades into 10-second intervals
    df.set_index("timestamp", inplace=True)
    ohlc = df["price"].resample("10S").ohlc()
    volume = df["size"].resample("10S").sum()
    
    # Combine OHLC and volume
    candle = ohlc.join(volume.rename("volume")).reset_index()
    
    # Filter candles starting from the first trade
    candle = candle[candle["timestamp"] >= start_time]
    
    # Append to candle_data
    for _, row in candle.iterrows():
        if pd.notna(row["open"]):  # Ensure valid candle
            candle_data.append({
                "timestamp": row["timestamp"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            })
    
    # Clear trades after processing
    trades = trades[-10:]  # Keep last few trades

# Function to generate trading signal and place orders
def generate_signal_and_trade(symbol="AAPL"):
    if len(candle_data) < 2:
        return  # Need at least 2 candles for comparison

    # Get the latest and previous candle
    latest_candle = candle_data[-1]
    previous_candle = candle_data[-2]
    
    # Simple momentum signal: compare close prices
    if latest_candle["close"] > previous_candle["close"]:
        # Bullish signal: place long order
        print(f"Bullish signal detected at {latest_candle['timestamp']}: Close {latest_candle['close']} > Previous {previous_candle['close']}")
        place_order(symbol, "buy")
    elif latest_candle["close"] < previous_candle["close"]:
        # Bearish signal: place short order
        print(f"Bearish signal detected at {latest_candle['timestamp']}: Close {latest_candle['close']} < Previous {previous_candle['close']}")
        place_order(symbol, "sell")


# Callback for handling trade updates from the stream
async def trade_callback(t):
    globalvar.alpaca_trades.append({
        "timestamp": datetime.utcnow(),
        "price": float(t.price),
        "size": int(t.size)
    })

    #print("see")
    #print(t)
    # Process trades every 10 seconds
    if len(globalvar.alpaca_trades) >= 1 and (datetime.utcnow().second % 10 == 0):
        process_trades()
        #if candle_data:
        #    print(candle_data[-1])
        #    #generate_signal_and_trade(symbol="AAPL")  # Check for trading signal


async def handle_trade(trade):
    print(f"Trade: {trade}")

async def handle_quote(quote):
    print(f"Quote: {quote}")


async def start_stream(symbols):
    # Initialize Alpaca stream
    stream = Stream(API_KEY, API_SECRET, base_url=BASE_URL, data_feed="iex")
    
    # Subscribe to trades for a specific symbol
    #symbol = "AAPL"
    for symbol in symbols:
        #stream.subscribe_trades(handle_trade, symbol)
        stream.subscribe_quotes(handle_quote, symbol)
    #stream.subscribe_trades(trade_callback, symbol)
    
    # Start the stream
    await stream._run_forever()









# Define the time range for the current trading day
today = datetime.now().date()
start_time = datetime.combine(today, datetime.min.time()).isoformat() + 'Z'  # Start of today (UTC)
end_time = datetime.combine(today, datetime.max.time()).isoformat() + 'Z'    # End of today (UTC)

# Helper function to calculate P&L for the day
def calculate_pnl_for_day(api=None):
    try:
        # Get account information
        account = api.get_account()
        equity_start = float(account.equity)  # Current equity
        # To estimate P&L, we can use portfolio history or account equity changes
        # Alpaca doesn't provide direct daily P&L, so we approximate using account equity
        # For more precision, calculate based on closed positions
        closed_positions = api.list_positions()  # Get current positions
        unrealized_pnl = sum(float(pos.unrealized_pl) for pos in closed_positions)
        # Alternatively, fetch portfolio history (limited granularity in alpaca_trade_api)
        clock = api.get_clock()
        if clock.is_open:
            # If market is open, use current equity minus initial equity
            # For simplicity, assume P&L is unrealized P&L from positions
            return round(unrealized_pnl, 2)
        else:
            return 0.0  # If market is closed, no intraday P&L without historical data
    except Exception as e:
        print(f"Error calculating P&L: {e}")
        return 0.0

# Helper function to calculate total charges (commissions/fees)
def calculate_total_charges(orders):
    total_charges = 0.0
    for order in orders:
        if order.status == 'filled':
            # Alpaca equity trading is commission-free, but crypto may have fees
            # Fees are not directly exposed in alpaca_trade_api order objects
            # Approximate fees for crypto trades if applicable (check documentation)
            if order.asset_class == 'crypto' and order.filled_avg_price and order.filled_qty:
                # Example: Assume 0.2% fee for crypto trades (adjust based on Alpaca's fee schedule)
                fee = float(order.filled_avg_price) * float(order.filled_qty) * 0.002
                total_charges += fee
    return round(total_charges, 2)

# Fetch orders with filters
def fetch_orders(status='all', api=None):
    try:
        orders = api.list_orders(
            status=status,  # 'open', 'closed', or 'all'
            after=start_time,
            until=end_time,
            direction='asc',  # Sort by time ascending
            limit=100  # Adjust limit as needed
        )
        return orders
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return []

# Fetch session information
def get_session_info(api=None):
    # Fetch all orders for the day
    all_orders = fetch_orders(status='all', api=api)
    
    # Filter orders by status
    submitted_orders = [o for o in all_orders if o.status in ['new', 'accepted', 'pending_new']]
    filled_orders = [o for o in all_orders if o.status == 'filled']
    canceled_orders = [o for o in all_orders if o.status == 'canceled']
    
    # Calculate P&L
    daily_pnl = calculate_pnl_for_day(api=api)
    
    # Calculate total charges (commissions/fees)
    total_charges = calculate_total_charges(all_orders)

    '''
    # Print results
    print("\nSession Information for Today:")
    print(f"Date: {today}")
    print("\nOrders Submitted:")
    for order in submitted_orders:
        print(f"  - Order ID: {order.id}, Symbol: {order.symbol}, Side: {order.side}, "
              f"Qty: {order.qty or order.notional}, Status: {order.status}, "
              f"Submitted At: {order.submitted_at}")
    
    print("\nOrders Filled:")
    for order in filled_orders:
        print(f"  - Order ID: {order.id}, Symbol: {order.symbol}, Side: {order.side}, "
              f"Filled Qty: {order.filled_qty}, Avg Price: {order.filled_avg_price}, "
              f"Filled At: {order.filled_at}")
    
    print("\nOrders Canceled:")
    for order in canceled_orders:
        print(f"  - Order ID: {order.id}, Symbol: {order.symbol}, Side: {order.side}, "
              f"Qty: {order.qty or order.notional}, Canceled At: {order.canceled_at}")
    
    print(f"\nP&L for the Day: ${daily_pnl}")
    print(f"Total Charges Paid for the Day: ${total_charges}")
    '''
    
    # Return as dictionary for further processing
    return {
        "submitted_orders": len(submitted_orders),
        "filled_orders": len(filled_orders),
        "canceled_orders": len(canceled_orders),
        "daily_pnl": daily_pnl,
        "total_charges": total_charges
    }

# Execute the function
#if __name__ == "__main__":
#    session_info = get_session_info()
