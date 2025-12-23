
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import time

# Add path for BinanceClient
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data.binance_client import BinanceClient

def load_config():
    # Reuse existing config or mock it if generic
    config_path = Path(__file__).parent / "config/fetch_config.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        # Fallback config
        return {"data_dir": "/Users/muce/1m_data/new_backtest_data_1year_1m"}

def get_last_timestamp(file_path):
    """Read last line of CSV to get last timestamp"""
    try:
        with open(file_path, 'rb') as f:
            try:
                f.seek(-1024, os.SEEK_END)
            except OSError:
                f.seek(0)
            lines = f.readlines()
            if not lines:
                return None
            
            last_line = lines[-1].decode('utf-8').strip()
            if not last_line:
                last_line = lines[-2].decode('utf-8').strip()
                
            ts_str = last_line.split(',')[0]
            try:
                # Assuming index is UTC-aware or will be handled
                ts = pd.to_datetime(ts_str)
                return ts
            except:
                return None
    except Exception:
        return None

def fetch_incremental(symbol, start_ts_ms, end_ts_ms, client):
    """Fetch range via API"""
    all_ohlcv = []
    current_since = start_ts_ms
    limit = 1500
    
    while current_since < end_ts_ms:
        try:
            ohlcv = client.exchange.fetch_ohlcv(symbol, '1m', since=current_since, limit=limit)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            last_ts = ohlcv[-1][0]
            current_since = last_ts + 60000
            
            time.sleep(0.2) # Aggressive fetches
            
            if last_ts >= end_ts_ms:
                break
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            time.sleep(2)
            break
            
    if not all_ohlcv:
        return None
        
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_numeric(df['timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms') # Naive UTCC
    return df

def main():
    print("🚀 Starting Targeted Data Fetch (To Dec 26 22:00 CST)...")
    config = load_config()
    data_dir = Path(config['data_dir'])
    
    # Target: Current Time (Dynamic)
    target_end_dt = datetime.now(timezone.utc)
    target_end_ms = int(target_end_dt.timestamp() * 1000)
    
    print(f"Target End Time: {target_end_dt} (UTC)")
    
    files = list(data_dir.glob("*.csv"))
    print(f"Checking {len(files)} symbols...")
    
    client = BinanceClient(load_markets=False)
    
    updates_count = 0
    
    for i, file_path in enumerate(files):
        symbol = file_path.stem
        # Filter generic
        if '-' in symbol: continue 
        
        last_ts = get_last_timestamp(file_path)
        
        if last_ts is None:
            continue
            
        if last_ts.tzinfo is None:
             last_ts = last_ts.replace(tzinfo=timezone.utc)
             
        # Check if we need updates
        # Allow 5 mins gap? No, we want EXACTLY up to target if possible.
        # But if last_ts >= target - 1min, we are good.
        
        if last_ts >= target_end_dt - timedelta(minutes=1):
            continue
            
        start_fetch_dt = last_ts + timedelta(minutes=1)
        start_fetch_ms = int(start_fetch_dt.timestamp() * 1000)
        
        if start_fetch_ms >= target_end_ms:
            continue
            
        print(f"[{i}/{len(files)}] 🔄 Updating {symbol}: {last_ts} -> {target_end_dt} ...")
        
        new_df = fetch_incremental(symbol, start_fetch_ms, target_end_ms, client)
        
        if new_df is not None and not new_df.empty:
            new_df.set_index('timestamp', inplace=True)
            new_df.to_csv(file_path, mode='a', header=False)
            updates_count += 1
            print(f"✅ {symbol}: Appended {len(new_df)} rows.")
        else:
            print(f"⚠️ {symbol}: No data found.")
            
    print(f"\nCompleted. Updated {updates_count} symbols.")

if __name__ == "__main__":
    main()
