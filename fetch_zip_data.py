
import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import concurrent.futures
import threading
import sys
import os
import time

# Add project root needed for data.binance_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data.binance_client import BinanceClient

# Config
DATA_DIR = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
DATE_START = "2024-12-17"
DATE_END = "2025-12-26"

# Binance Vision Base URLs (FUTURES UM)
BASE_MONTHLY = "https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/1m/{symbol}-1m-{year}-{month}.zip"
BASE_DAILY = "https://data.binance.vision/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{year}-{month}-{day}.zip"

print_lock = threading.Lock()

def get_target_months_and_days():
    # Dynamic Date Generation based on Config
    start = datetime.strptime(DATE_START, "%Y-%m-%d")
    end = datetime.strptime(DATE_END, "%Y-%m-%d")
    
    months = set()
    days = set()
    
    current = start
    while current <= end:
        # Collect Months (Year, Month)
        months.add((current.strftime("%Y"), current.strftime("%m")))
        
        # Collect Days (Year, Month, Day)
        # Note: Vision usually releases Monthly zips after month ends.
        # But we can try to download all daily zips as fallback or for current month.
        # Strategy: 
        # 1. Try Monthly ZIP first (covers whole month)
        # 2. Try Daily ZIPs for ALL days in range (simpler than checking if month is over)
        # Actually, downloading 365 daily zips is slow.
        # Better: Only download Daily ZIPs for the Last Month or Current Month?
        # For simplicity/robustness in Phase 1, let's just create the list for ALL days.
        # The downloader can decide priority, but here we return lists.
        # Wait, downloading 365 zips is 365 requests.
        # Monthly zips are 12 requests.
        # Let's return ALL months in range, and ALL days in range.
        # The downloader (attempt_zip_download) tries monthlies first, then dailies.
        # So if monthly succeeds, we have the data.
        # But we need to handle overlaps? 
        # attempt_zip_download concatenates everything.
        # If we download Monthly Jan AND Daily Jan 1, we duplicate data.
        # But we have drop_duplicates at the end. So it's safe.
        days.add((current.strftime("%Y"), current.strftime("%m"), current.strftime("%d")))
        current += timedelta(days=1)
        
    # Sort
    sorted_months = sorted(list(months))
    sorted_days = sorted(list(days))
    
    # Optimization: If we are in the middle of a month, we need dailies for that month.
    # If a month is fully past, we prefer Monthly zip.
    # But checking "Fully Past" is hard without knowing today's date vs release schedule.
    # Safe bet: Try both. De-dup handles it.
    
    return sorted_months, sorted_days

# Global Session for connection pooling
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=10, pool_maxsize=10)
session.mount('http://', adapter)
session.mount('https://', adapter)

def download_and_extract(url):
    retries = 3
    for attempt in range(retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            # Use session for connection pooling
            r = session.get(url, headers=headers, timeout=20) 
            
            if r.status_code != 200:
                if r.status_code != 404:
                    # Generic error, might retry? No, status codes are usually final unless 5xx
                    if r.status_code >= 500:
                        time.sleep(1)
                        continue
                    # print(f"DEBUG: {url} -> Status {r.status_code}")
                return None
                
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                csv_name = z.namelist()[0]
                with z.open(csv_name) as f:
                    # Some Vision files have headers, some don't.
                    # Detect header by reading first line logic or just checking content.
                    df = pd.read_csv(f, header=None)
                    
                    # Check for header row
                    first_cell = str(df.iloc[0, 0])
                    if "open_time" in first_cell.lower():
                        # Drop header row
                        df = df.iloc[1:]
                        
                    df = df.iloc[:, :6]
                    df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    
                    # Fix FutureWarning: Explicitly cast to numeric first
                    df['timestamp'] = pd.to_numeric(df['timestamp'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    return df
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1)) # Backoff 2s, 4s, 6s
                continue
            print(f"DEBUG: Failed downloading {url} after {retries} attempts: {e}")
            return None
    return None

def attempt_zip_download(symbol):
    """
    Phase 1: Try to get data ONLY from ZIPs.
    Returns: DataFrame or None
    """
    sym_upper = symbol.upper()
    all_dfs = []
    
    months, days_list = get_target_months_and_days()
    
    # Monthlies
    for year, month in months:
        url = BASE_MONTHLY.format(symbol=sym_upper, year=year, month=month)
        df = download_and_extract(url)
        if df is not None:
            all_dfs.append(df)
            
    # Dailies
    for year, month, day in days_list:
        url = BASE_DAILY.format(symbol=sym_upper, year=year, month=month, day=day)
        df = download_and_extract(url)
        if df is not None:
            all_dfs.append(df)
            
    if not all_dfs:
        return None
        
    full_df = pd.concat(all_dfs)
    full_df.sort_values('timestamp', inplace=True)
    full_df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
    return full_df

def fetch_via_api_safe(symbol, start_ts, end_ts):
    """
    Phase 2: Use API safely (Single Threaded usage intended).
    """
    try:
        # Load markets False to save weight
        client = BinanceClient(load_markets=False)
        all_ohlcv = []
        current_since = start_ts
        limit = 1500 
        
        print(f"📡 API Fetching: {symbol}...")
        
        while current_since < end_ts:
            try:
                ohlcv = client.exchange.fetch_ohlcv(symbol, timeframe='1m', since=current_since, limit=limit)
                if not ohlcv:
                    break
                
                all_ohlcv.extend(ohlcv)
                last_ts = ohlcv[-1][0]
                current_since = last_ts + 60000 
                
                # Safe Sleep per request
                time.sleep(0.2) 
                
                if last_ts >= end_ts:
                    break
            except Exception as e:
                if "Too many requests" in str(e) or "418" in str(e) or "429" in str(e):
                    print(f"🚨 Rate Limited on {symbol}. Sleeping 60s...")
                    time.sleep(60)
                else:
                    print(f"API Error {symbol}: {e}")
                    break
                    
        if not all_ohlcv:
            return None
            
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
        
    except Exception as e:
        print(f"Client Init Error {symbol}: {e}")
        return None

def save_df(symbol, df, method):
    save_path = DATA_DIR / f"{symbol}.csv"
    df.set_index('timestamp', inplace=True)
    df.to_csv(save_path)
    with print_lock:
        print(f"✅ {symbol}: Saved {len(df)} rows via {method}")

def main():
    print("🚀 Starting Safe 2-Phase Fetcher...")
    
    
    # ---------------------------------------------------------
    # Get All Symbols from API
    # ---------------------------------------------------------
    print("📡 Fetching ALL USDT-Margined Symbols from Binance API...")
    try:
        # Load markets to discover symbols
        client = BinanceClient(load_markets=True)
        markets = client.exchange.markets
        symbols = []
        for s, m in markets.items():
            if m.get('swap') and m.get('linear') and m.get('quote') == 'USDT':
                # Convert 'BTC/USDT:USDT' -> 'BTCUSDT' if needed, or just use 'id'
                # Binance 'id' is usually 'BTCUSDT'
                symbols.append(m['id'])
                
        # Remove duplicates and sort
        symbols = sorted(list(set(symbols)))
        print(f"✅ Found {len(symbols)} contract pairs.")
    except Exception as e:
        print(f"❌ Failed to fetch symbols: {e}")
        # Fallback to local
        files = list(DATA_DIR.glob("*.csv"))
        symbols = [f.stem for f in files]
        print(f"⚠️ Falling back to {len(symbols)} local files.")

    print(f"Total Symbols to Check: {len(symbols)}")
    
    # ---------------------------------------------------------
    
    # ---------------------------------------------------------
    # Filter Existing (Resume Capability - Smart Check)
    # ---------------------------------------------------------
    print("🔍 Checking for existing files (Resume Mode)...")
    pending_symbols = []
    skipped_count = 0
    target_end_dt = datetime.strptime(DATE_END, "%Y-%m-%d")
    
    for sym in symbols:
        csv_path = DATA_DIR / f"{sym}.csv"
        is_complete = False
        
        if csv_path.exists() and csv_path.stat().st_size > 1024:
            try:
                # Read last line to check timestamp
                with open(csv_path, 'rb') as f:
                    try:
                        f.seek(-1024, os.SEEK_END)
                    except OSError:
                        f.seek(0)
                        
                    lines = f.readlines()
                    if lines:
                        last_line = lines[-1].decode('utf-8', errors='ignore').strip()
                        if not last_line:
                            last_line = lines[-2].decode('utf-8', errors='ignore').strip() if len(lines) > 1 else ""
                            
                        if last_line:
                            parts = last_line.split(',')
                            ts_str = parts[0]
                            try:
                                last_ts = pd.to_datetime(ts_str)
                                # Check if last_ts is close to DATE_END (within 2 days tolerance)
                                # Vision data might lag by 1 day or month end.
                                # User wants "Fully Fetched".
                                # If last_ts >= 2025-12-25 (assuming DATE_END is 26), we are good?
                                # Let's say if within 24h of DATE_END, we skip.
                                if last_ts >= target_end_dt - timedelta(days=2):
                                    is_complete = True
                                    # print(f"DEBUG: {sym} complete (Last: {last_ts})")
                            except:
                                pass
            except Exception as e:
                print(f"Warning checking {sym}: {e}")
                
        if is_complete:
             skipped_count += 1
        else:
            pending_symbols.append(sym)
            
    print(f"⏩ Skipped {skipped_count} complete files.")
    print(f"📋 Remaining to fetch: {len(pending_symbols)}")
    
    symbols = pending_symbols
    
    if not symbols:
        print("🎉 ALL data is already fully fetched!")
        return
    
    # ---------------------------------------------------------
    # Phase 1: ZIP Download (Multi-Threaded, High Concurrency ok for CDN)
    # ---------------------------------------------------------
    print("\n📦 [Phase 1] Attempting High-Speed ZIP Download...")
    missing_symbols = []
    
    completed_zip = 0
    
    def process_zip_task(sym):
        df = attempt_zip_download(sym)
        if df is not None and not df.empty:
            save_df(sym, df, "ZIP")
            return True
        else:
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_sym = {executor.submit(process_zip_task, sym): sym for sym in symbols}
        
        for future in concurrent.futures.as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                success = future.result()
                if not success:
                    missing_symbols.append(sym)
                    with print_lock:
                        print(f"⚠️ {sym}: Missing in Vision -> Queueing for Phase 2")
                else:
                    completed_zip += 1
            except Exception as e:
                missing_symbols.append(sym)
                print(f"Error ZIP {sym}: {e}")

    print("-" * 60)
    print(f"Phase 1 Summary: {completed_zip} Succcess, {len(missing_symbols)} Failed/Missing.")
    print("-" * 60)
    
    if not missing_symbols:
        print("🎉 All data fetched via ZIP!")
        return

    # ---------------------------------------------------------
    # Phase 2: API Fallback (Single-Threaded, Safe)
    # ---------------------------------------------------------
    print("\n🐢 [Phase 2] Starting Slow & Safe API Fallback (Single Thread)...")
    print(f"Targeting {len(missing_symbols)} missing symbols.")
    
    # Define Time Range
    start_dt = datetime.strptime(DATE_START, "%Y-%m-%d")
    end_dt = datetime.strptime(DATE_END, "%Y-%m-%d") + timedelta(days=1)
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    
    count = 0
    # Re-init client to be sure (or reuse)
    # Note: Phase 2 function creates its own client, which is fine but slower.
    # Optimization: Pass client if possible? 
    # fetch_via_api_safe initializes its own. That's fine.
    
    for sym in missing_symbols:
        count += 1
        print(f"[{count}/{len(missing_symbols)}] Processing {sym}...")
        
        df = fetch_via_api_safe(sym, start_ts, end_ts)
        if df is not None and not df.empty:
            save_df(sym, df, "API (Safe)")
        else:
            print(f"❌ {sym}: Completely Failed (No data on API either)")
        
        # Extra safety sleep between coins
        time.sleep(1.0) 
 

if __name__ == "__main__":
    main()
