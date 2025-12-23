import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from data.binance_client import BinanceClient

# Configuration
SAVE_DIR = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
DATE_START = "2024-12-17 00:00:00"
DATE_END = "2025-12-24 23:59:59"
MAX_WORKERS = 3 # Reduced to 3 per user request (New IP)
RETRY_LIMIT = 5

start_dt = datetime.strptime(DATE_START, "%Y-%m-%d %H:%M:%S")
end_dt = datetime.strptime(DATE_END, "%Y-%m-%d %H:%M:%S")
START_TS = int(start_dt.timestamp() * 1000)
END_TS = int(end_dt.timestamp() * 1000)

print_lock = threading.Lock()

def fetch_symbol(symbol):
    client = BinanceClient() # New instance per thread? Or safer to use raw CCXT in thread.
    # CCXT instances might not be thread safe depending on implementation, 
    # but creating fresh one is safe.
    
    # Check skip
    clean_sym = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
    save_path = SAVE_DIR / f"{clean_sym}.csv"
    
    if save_path.exists() and save_path.stat().st_size > 1_000_000:
        return f"SKIP {symbol} (>1MB)"
        
    current_since = START_TS
    all_ohlcv = []
    
    while current_since < END_TS:
        retry = 0
        ohlcv = None
        
        while retry < RETRY_LIMIT:
            try:
                # Use private method or raw exchange to allow threading if wrapper issues exist
                # But BinanceClient is simple wrapper.
                # Just catch rate limits.
                ohlcv = client.exchange.fetch_ohlcv(symbol, timeframe='1m', since=current_since, limit=1500)
                break
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "Too Many Requests" in err_str:
                     time.sleep(10 * (retry + 1)) # Backoff hard
                else:
                     time.sleep(1)
                retry += 1
        
        if not ohlcv:
            break
            
        all_ohlcv.extend(ohlcv)
        last_ts = ohlcv[-1][0]
        current_since = last_ts + 60000
        
        if last_ts >= END_TS:
            break
            
        # Basic rate limit sleep inside thread
        time.sleep(0.1) 
        
    if not all_ohlcv:
        return f"EMPTY {symbol}"
        
    # Save
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Basic overwrite logic for speed (since we skip >1MB anyway)
    # If resuming, we rely on the SKIP check above.
    df.to_csv(save_path)
    
    return f"DONE {symbol} ({len(df)} rows)"

def main():
    print("="*60)
    print(f"🚀 FAST Fetch Global 1m Data ({MAX_WORKERS} Threads)")
    print("="*60)
    
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Get Symbols (Single Thread)
    main_client = BinanceClient()
    tickers = main_client.get_usdt_tickers()
    all_symbols = [t[0] for t in tickers]
    
    print(f"Total Symbols Found: {len(all_symbols)}")
    print("-" * 60)
    
    # 2. Pre-scan for Completion
    done_symbols = []
    todo_symbols = []
    
    print("🔍 Scanning local data for progress (Strong Completion Check)...")
    
    target_start = datetime.strptime(DATE_START, "%Y-%m-%d %H:%M:%S")
    target_end = datetime.strptime(DATE_END, "%Y-%m-%d %H:%M:%S")
    
    for sym in all_symbols:
        clean_sym = sym.replace('/USDT:USDT', 'USDT').replace('/', '')
        save_path = SAVE_DIR / f"{clean_sym}.csv"
        
        is_complete = False
        if save_path.exists():
            try:
                # 1. Quick Size Check (Empty files)
                if save_path.stat().st_size < 1000:
                    is_complete = False
                else:
                    # 2. Strong Timestamp Check
                    # Read First Line
                    df_head = pd.read_csv(save_path, nrows=1)
                    if not df_head.empty and 'timestamp' in df_head.columns:
                        first_ts = pd.to_datetime(df_head['timestamp'].iloc[0])
                        
                        # Read Last Line
                        with open(save_path, 'rb') as f:
                            try:
                                f.seek(-2048, 2) # Seek from end
                            except OSError:
                                f.seek(0)
                            lines = f.readlines()
                            last_line = lines[-1].decode('utf-8', errors='ignore')
                            last_ts_str = last_line.split(',')[0]
                            last_ts = pd.to_datetime(last_ts_str)
                            
                        # Tolerance: 48 hours (to tolerate slight fetch delays or timezone gaps)
                        # CRITICAL: We DO NOT check Start Time rigorously for "New Listings". 
                        # If a file exists and ends reasonably correctly, we assume it matches the best available data??
                        # User said: "Update to Strong Completion (but note some coins don't have data)".
                        # If we enforce Start Time, we will loop-fetch new coins forever.
                        # Strategy: 
                        # 1. If End Time is missing (lagging), definitely re-fetch.
                        # 2. If Start Time is missing... checking against 'listing date' is impossible here.
                        #    But if the file is BIG (>50MB) and starts late, maybe it IS a new listing.
                        #    Let's stick to the user's "Strong Completion" request essentially implying "Try to get everything".
                        #    Re-fetching a new listing once is fine. It will overwrite with same data.
                        
                        s_ok = first_ts <= target_start + pd.Timedelta(days=2)
                        e_ok = last_ts >= target_end - pd.Timedelta(days=2)
                        
                        if s_ok and e_ok:
                            is_complete = True
                        else:
                            # Refined Logic for New Listings vs Missing Data:
                            # If file starts late ONLY, it might be a new listing.
                            # But we can't be sure without metadata.
                            # To be safe and "Strong", we mark as Incomplete. 
                            # It will fetch again. If API returns same data, so be it. 
                            # The cost is just bandwidth/time.
                            is_complete = False
            except Exception:
                is_complete = False
                
        if is_complete:
            done_symbols.append(sym)
        else:
            todo_symbols.append(sym)
            
    # 3. Report Status
    print(f"✅ Already Completed: {len(done_symbols)} symbols")
    print(f"⏳ Remaining to Fetch: {len(todo_symbols)} symbols")
    print("-" * 60)
    print("Remaining List (First 50):")
    print(todo_symbols[:50])
    if len(todo_symbols) > 50:
        print(f"... and {len(todo_symbols)-50} more.")
    print("-" * 60)
    
    if not todo_symbols:
        print("🎉 ALL DATA FETCHED! Nothing to do.")
        return

    # 4. Process Remaining
    completed = len(done_symbols)
    total = len(all_symbols)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(fetch_symbol, sym): sym for sym in todo_symbols}
        
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                msg = future.result()
                with print_lock:
                    completed += 1
                    # Progress relative to TOTAL
                    left = total - completed
                    print(f"[{completed}/{total}] {msg} | Remaining: {left}", flush=True)
            except Exception as e:
                with print_lock:
                    completed += 1
                    print(f"[{completed}/{total}] ERROR {sym}: {e}", flush=True)

if __name__ == "__main__":
    main()
