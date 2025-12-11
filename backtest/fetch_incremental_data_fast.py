import ccxt
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time
import pytz
import concurrent.futures
import threading

# Thread-safe counters
successful = 0
failed = 0
lock = threading.Lock()

def fetch_symbol_data(symbol, end_time, data_dir):
    global successful, failed
    exchange = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    
    try:
        filename = data_dir / f"{symbol.replace('/', '').replace(':', '')}.csv"
        timeframe = '1m'
        
        # Determine start time
        if filename.exists():
            try:
                df_existing = pd.read_csv(filename, parse_dates=['timestamp'])
                if len(df_existing) > 0:
                    last_timestamp = df_existing['timestamp'].max()
                    start_time = last_timestamp + timedelta(minutes=1)
                    if start_time.tzinfo is None:
                        start_time = pytz.UTC.localize(start_time)
                else:
                    start_time = end_time - timedelta(days=365)
            except:
                start_time = end_time - timedelta(days=365)
        else:
            start_time = end_time - timedelta(days=365)
        
        if start_time >= end_time:
            with lock:
                successful += 1
                # print(f"✓ {symbol} up to date")
            return

        all_data = []
        current_start = start_time
        
        # Fetch loop
        while current_start < end_time:
            since = int(current_start.timestamp() * 1000)
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                break
                
            if not ohlcv:
                break
                
            chunk_data = []
            for candle in ohlcv:
                ts = candle[0]
                if start_time.timestamp() * 1000 <= ts <= end_time.timestamp() * 1000:
                    chunk_data.append(candle)
            
            if not chunk_data:
                if ohlcv[0][0] > end_time.timestamp() * 1000:
                    break
            
            all_data.extend(chunk_data)
            last_timestamp = ohlcv[-1][0]
            current_start = datetime.fromtimestamp(last_timestamp / 1000, tz=pytz.UTC) + timedelta(minutes=1)
            
            time.sleep(0.1) # Gentle rate limit per thread

        if not all_data:
            with lock:
                successful += 1
            return

        # Save
        df_new = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_new['timestamp'] = pd.to_datetime(df_new['timestamp'], unit='ms')
        
        if filename.exists():
            df_existing = pd.read_csv(filename, parse_dates=['timestamp'])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
        
        df_combined = df_combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        df_combined.to_csv(filename, index=False)
        
        with lock:
            successful += 1
            print(f"[{successful}/?] {symbol}: +{len(df_new)} candles")

    except Exception as e:
        with lock:
            failed += 1
            print(f"✗ {symbol} failed: {e}")

def fetch_incremental_data_fast():
    # End Time: 2025-12-11 13:30 Beijing = 05:30 UTC
    end_time = datetime(2025, 12, 11, 5, 30, 0, tzinfo=pytz.UTC)
    data_dir = Path('/Users/muce/1m_data/new_backtest_data_1year_1m')
    
    # Get Markets
    exchange = ccxt.binance({'options': {'defaultType': 'future'}})
    markets = exchange.load_markets()
    usdt_perpetuals = [s for s in markets.keys() if '/USDT:USDT' in s]
    
    print(f"Starting parallel fetch for {len(usdt_perpetuals)} symbols...")
    print(f"Target End: {end_time} UTC")
    
    # Run with thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_symbol_data, symbol, end_time, data_dir) for symbol in usdt_perpetuals]
        concurrent.futures.wait(futures)
        
    print("Done.")

if __name__ == "__main__":
    fetch_incremental_data_fast()
