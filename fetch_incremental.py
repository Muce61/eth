
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
    config_path = Path(__file__).parent / "config/fetch_config.json"
    with open(config_path, 'r') as f:
        return json.load(f)

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
            # Handle potential empty lines at end
            if not last_line:
                # Try second to last
                last_line = lines[-2].decode('utf-8').strip()
                
            ts_str = last_line.split(',')[0]
            # Try parsing
            try:
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
    
    # print(f"DEBUG: {symbol} Fetching {current_since} -> {end_ts_ms}")
    
    while current_since < end_ts_ms:
        try:
            ohlcv = client.exchange.fetch_ohlcv(symbol, '1m', since=current_since, limit=limit)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            last_ts = ohlcv[-1][0]
            current_since = last_ts + 60000
            
            time.sleep(0.5) # Rate limit safety
            
            if last_ts >= end_ts_ms:
                break
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            time.sleep(5) # Backoff
            break
            
    if not all_ohlcv:
        return None
        
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_numeric(df['timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def main():
    print("🚀 Starting Incremental Data Fetch...")
    config = load_config()
    data_dir = Path(config['data_dir'])
    
    # Target End Time: Yesterday 23:59:59 UTC
    # Why Yesterday? To ensure the daily candle is closed and 1m data is finalized.
    # Actually, for 1m data, we can fetch up to "Now - 1 min".
    # But user asked for "Previous Day's Increment".
    # Let's assume this runs at 9AM BJ (1AM UTC).
    # Previous day = (Now - 1 day). 
    # Example: Runs on Dec 27 01:00 UTC. Previous day is Dec 26.
    # Target End: Dec 26 23:59:59.
    
    now_utc = datetime.now(timezone.utc)
    # Floor to previous day midnight?
    # No, user wants data up to end of yesterday.
    
    # If run at 9AM BJ (01:00 UTC), "Yesterday" is indeed the full previous UTC day.
    yesterday = now_utc - timedelta(days=1)
    target_end_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999000)
    target_end_ms = int(target_end_dt.timestamp() * 1000)
    
    print(f"Target End Time: {target_end_dt} (UTC)")
    
    files = list(data_dir.glob("*.csv"))
    print(f"Checking {len(files)} symbols...")
    
    client = BinanceClient(load_markets=False)
    
    updates_count = 0
    
    for file_path in files:
        symbol = file_path.stem
        last_ts = get_last_timestamp(file_path)
        
        if last_ts is None:
            # print(f"⚠️ {symbol}: Could not read timestamp")
            continue
            
        # Check if local matches target
        # Local should be <= Target End
        # If Local < Target End - 5 minutes, we update
        
        # Ensure last_ts is timezone aware if target is
        if last_ts.tzinfo is None:
             last_ts = last_ts.replace(tzinfo=timezone.utc)
             
        if last_ts >= target_end_dt - timedelta(minutes=5):
            # Already up to date
            continue
            
        # Verify gap?
        # If gap > 2 days, warn?
        # Just fetch gap.
        
        start_fetch_dt = last_ts + timedelta(minutes=1)
        start_fetch_ms = int(start_fetch_dt.timestamp() * 1000)
        
        if start_fetch_ms >= target_end_ms:
            continue
            
        print(f"🔄 Updating {symbol}: {last_ts} -> {target_end_dt} ...")
        
        new_df = fetch_incremental(symbol, start_fetch_ms, target_end_ms, client)
        
        if new_df is not None and not new_df.empty:
            # Append to file
            # Use 'a' mode, header=False
            new_df.set_index('timestamp', inplace=True)
            new_df.to_csv(file_path, mode='a', header=False)
            updates_count += 1
            print(f"✅ {symbol}: Appended {len(new_df)} rows.")
        else:
            print(f"⚠️ {symbol}: No data found for incremental range.")
            
    print(f"\nCompleted. Updated {updates_count} symbols.")

if __name__ == "__main__":
    main()
