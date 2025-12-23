#!/usr/bin/env python3
"""
Bulk Fetch 1s Data from AggTrades (Optimized / Multiprocessing)
===============================================================
1. Fetches all USDT-M Futures symbols.
2. Uses **Multiprocessing** (4 workers) to process symbols in parallel.
3. For each symbol:
    a. Downloads Daily AggTrades Zip (Binance Vision).
    b. Parses trades -> 1s OHLCV.
    c. **Forward Fills** empty seconds to guarantee 86,400 rows/day.
    d. Saves normalized CSV to `klines_data_usdm_1s_agg`.
"""

import os
import sys
import csv
import json
import time
import zipfile
import io
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuration
LOOKBACK_DAYS = 370
OUTPUT_BASE = Path("klines_data_usdm_1s_agg")
VISION_BASE = "https://data.binance.vision"
MAX_WORKERS = 4  # Number of parallel processes

def get_usdt_symbols():
    try:
        print("Fetching exchange info...")
        r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        r.raise_for_status()
        data = r.json()
        symbols = [s['symbol'] for s in data['symbols'] 
                   if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL']
        symbols.sort()
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

def download_zip(session, url):
    try:
        r = session.get(url, timeout=60)
        if r.status_code == 200: return r.content
        return None
    except Exception: return None

def process_aggtrades_to_1s(symbol, day_date, zip_content):
    """Parse AggTrades and build perfect 1s candles."""
    trades = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            for name in zf.namelist():
                if name.endswith('.csv'):
                    with zf.open(name) as f:
                        for line in io.TextIOWrapper(f, encoding="utf-8"):
                            parts = line.strip().split(',')
                            if len(parts) < 6: continue
                            try:
                                p = float(parts[1])
                                q = float(parts[2])
                                t = int(parts[5])
                                trades.append((t, p, q))
                            except ValueError: continue
                    break
    except Exception as e:
        return None

    if not trades: return None

    # Build DataFrame
    df = pd.DataFrame(trades, columns=['time', 'price', 'qty'])
    df['ts_sec'] = df['time'] // 1000 * 1000
    
    # Resample to 1s OHLCV
    ohlc = df.groupby('ts_sec').agg({
        'price': ['first', 'max', 'min', 'last'],
        'qty': 'sum'
    })
    ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
    
    # Reindex to 86,400 seconds (Forward Fill)
    day_start = datetime(day_date.year, day_date.month, day_date.day, tzinfo=timezone.utc)
    start_ts = int(day_start.timestamp() * 1000)
    end_ts = start_ts + (86400 * 1000)
    
    full_idx = range(start_ts, end_ts, 1000)
    ohlc = ohlc.reindex(full_idx)
    
    # Fill Missing Data
    ohlc['close'] = ohlc['close'].ffill().bfill()
    ohlc['open'] = ohlc['open'].fillna(ohlc['close'])
    ohlc['high'] = ohlc['high'].fillna(ohlc['close'])
    ohlc['low'] = ohlc['low'].fillna(ohlc['close'])
    ohlc['volume'] = ohlc['volume'].fillna(0)
    
    return ohlc.reset_index().rename(columns={'index': 'open_time'})

def process_symbol_year(symbol):
    """Worker function to process a single symbol for the entire year."""
    # Each process gets its own session
    session = requests.Session()
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    
    sym_dir = OUTPUT_BASE / f"{symbol}_1s_agg"
    sym_dir.mkdir(exist_ok=True, parents=True)
    
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    
    processed_count = 0
    curr = start_date
    
    # We iterate internally to minimize process spawning overhead
    while curr <= end_date:
        ymd = curr.strftime("%Y-%m-%d")
        ymd_short = curr.strftime("%Y%m%d")
        out_file = sym_dir / f"{symbol}_1s_{ymd_short}.csv"
        
        # Skip if valid exists
        if out_file.exists() and out_file.stat().st_size > 1024 * 50:
            curr += timedelta(days=1)
            continue
            
        url = f"{VISION_BASE}/data/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{ymd}.zip"
        zip_data = download_zip(session, url)
        
        if zip_data:
            df_1s = process_aggtrades_to_1s(symbol, curr, zip_data)
            if df_1s is not None:
                df_1s.to_csv(out_file, index=False)
                processed_count += 1
                # print(f"  {symbol} {ymd}: Saved") # Too noisy for parallel output
        
        curr += timedelta(days=1)
    
    return f"{symbol}: Processed {processed_count} days"

def main():
    symbols = get_usdt_symbols()
    if not symbols: return
    
    OUTPUT_BASE.mkdir(exist_ok=True)
    
    print(f"Starting AggTrades Build (Parallel x{MAX_WORKERS}) for {len(symbols)} symbols")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print("-" * 50)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol_year, sym): sym for sym in symbols}
        
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]
            try:
                msg = future.result()
                print(f"[{i}/{len(symbols)}] {msg}")
            except Exception as e:
                print(f"[{i}/{len(symbols)}] {sym} Failed: {e}")

if __name__ == "__main__":
    main()
