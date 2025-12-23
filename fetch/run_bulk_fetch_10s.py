#!/usr/bin/env python3
"""
Bulk Fetch & Resample 10s Data (1 Year)
=======================================
1. Fetches all USDT-M Futures symbols.
2. For each symbol:
    a. Downloads 1-year of Mark Price 1s data (Binance Vision).
    b. Resamples to 10s resolution.
    c. DELETES the 1s source files to save disk space.
"""

import os
import sys
import json
import csv
import time
import shutil
import requests
import subprocess
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Configuration
LOOKBACK_DAYS = 370
OUTPUT_BASE_1S = Path("klines_data_usdm")
OUTPUT_BASE_10S = Path("klines_data_usdm_10s")
PYTHON_EXEC = sys.executable
SCRIPT_VISION_FETCH = Path(__file__).parent / "fetch_binance_vision_usdm_mark_1s.py"

def get_usdt_symbols():
    """Fetch all trading USDT futures pairs."""
    try:
        print("Fetching exchange info...")
        r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        r.raise_for_status()
        data = r.json()
        
        symbols = []
        for s in data['symbols']:
            if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL':
                symbols.append(s['symbol'])
        
        symbols.sort()
        print(f"Found {len(symbols)} USDT Trading Pairs.")
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

def resample_symbol_1s_to_10s(symbol):
    """Resample all 1s CSVs for a symbol to a single (or daily) 10s CSV."""
    # Source Dir
    src_dir = OUTPUT_BASE_1S / f"{symbol}_1s_mark"
    if not src_dir.exists():
        print(f"  [Warn] No source data for {symbol}")
        return False
        
    # Target Dir
    tgt_dir = OUTPUT_BASE_10S / f"{symbol}_10s"
    tgt_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"  Resampling {symbol} 1s -> 10s...")
    
    # Process file by file to keep memory low
    csv_files = sorted(list(src_dir.glob("*.csv")))
    if not csv_files:
        return False
        
    count = 0
    for f in csv_files:
        try:
            # Read 1s
            df = pd.read_csv(f)
            
            # Identify Time Column
            cols = [c for c in df.columns if 'time' in c.lower() and 'open' in c.lower()]
            if not cols: continue
            ts_col = cols[0]
            
            df[ts_col] = pd.to_datetime(df[ts_col], unit='ms')
            df.set_index(ts_col, inplace=True)
            
            # Resample 10s
            agg = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            }
            # Only include columns that exist
            agg = {k:v for k,v in agg.items() if k in df.columns}
            
            df_10s = df.resample('10s').agg(agg).dropna()
            
            # Output Filename
            out_name = f.name.replace("1s", "10s")
            out_path = tgt_dir / out_name
            
            df_10s.to_csv(out_path)
            count += 1
            
        except Exception as e:
            print(f"    Error processing {f.name}: {e}")
            
    print(f"  Converted {count} files.")
    return True

def cleanup_1s_data(symbol):
    """Delete the 1s data folder for the symbol."""
    src_dir = OUTPUT_BASE_1S / f"{symbol}_1s_mark"
    if src_dir.exists():
        print(f"  Cleaning up 1s source: {src_dir}")
        shutil.rmtree(src_dir)

def main():
    if not SCRIPT_VISION_FETCH.exists():
        print(f"Error: Fetch script not found at {SCRIPT_VISION_FETCH}")
        return

    symbols = get_usdt_symbols()
    if not symbols: return
    
    # Optional: Filter for testing
    # symbols = symbols[:1] 
    
    print(f"Starting Bulk Fetch for {len(symbols)} symbols...")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print(f"Output: {OUTPUT_BASE_10S}")
    print("-" * 50)
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] Processing {symbol}...")
        
        # 1. Fetch 1s Data (Binance Vision)
        cmd = [
            PYTHON_EXEC, str(SCRIPT_VISION_FETCH),
            "--symbol", symbol,
            "--lookback-days", str(LOOKBACK_DAYS),
            "--out-base", str(OUTPUT_BASE_1S),
            "--fallback-index",
            "--fallback-aggtrades"
        ]
        
        try:
            # Run fetch script, capture valid output only
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  Fetch Failed: {result.stderr[:200]}...")
                continue
                
            # Check if any files were downloaded/exist
            src_dir = OUTPUT_BASE_1S / f"{symbol}_1s_mark"
            if not src_dir.exists() or not list(src_dir.glob("*.csv")):
                print("  No data fetched (Symbol listing might be new). Skipping.")
                continue
                
            # 2. Resample to 10s
            if resample_symbol_1s_to_10s(symbol):
                # 3. Cleanup 1s if successful
                cleanup_1s_data(symbol)
            else:
                print("  Resample failed or no data.")
                
        except KeyboardInterrupt:
            print("\nAborted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"  Critical check error: {e}")

if __name__ == "__main__":
    main()
