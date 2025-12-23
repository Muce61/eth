#!/usr/bin/env python3
"""
Bulk Fetch 1s Data (1 Year) - Raw Storage
=========================================
1. Fetches all USDT-M Futures symbols.
2. For each symbol:
    a. Downloads 1-year of 1s Klines (Binance Vision).
    b. Fallback to AggTrades if Mark Price missing.
    c. KEEPS the raw 1s CSV files (No resampling).
"""

import os
import sys
import subprocess
import requests
from pathlib import Path

# Configuration
LOOKBACK_DAYS = 370
OUTPUT_BASE_1S = Path("klines_data_usdm_1s")
PYTHON_EXEC = sys.executable
SCRIPT_VISION_FETCH = Path(__file__).parent / "fetch_binance_vision_usdm_mark_1s.py"
SCRIPT_REPAIR = Path(__file__).parent / "repair_usdm_1s_mark.py"

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

def main():
    if not SCRIPT_VISION_FETCH.exists():
        print(f"Error: Fetch script not found at {SCRIPT_VISION_FETCH}")
        return
    if not SCRIPT_REPAIR.exists():
        print(f"Error: Repair script not found at {SCRIPT_REPAIR}")
        return

    symbols = get_usdt_symbols()
    if not symbols: return
    
    print(f"Starting Bulk Fetch & Repair (86,400 rows) for {len(symbols)} symbols...")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print(f"Output: {OUTPUT_BASE_1S}")
    print("-" * 50)
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] 1. Fetching {symbol}...")
        
        # 1. Fetch 1s Data
        cmd_fetch = [
            PYTHON_EXEC, str(SCRIPT_VISION_FETCH),
            "--symbol", symbol,
            "--lookback-days", str(LOOKBACK_DAYS),
            "--out-base", str(OUTPUT_BASE_1S),
            "--fallback-index",
            "--fallback-aggtrades"
        ]
        
        try:
            # Run fetch script
            result_fetch = subprocess.run(cmd_fetch, text=True)
            if result_fetch.returncode != 0:
                print(f"  Fetch Failed with code {result_fetch.returncode}")
                continue
                
            # 2. Repair (Check & Fill to 86,400 rows)
            print(f"[{i}/{len(symbols)}] 2. Repairing {symbol} (Aligning to 86,400 rows)...")
            cmd_repair = [
                PYTHON_EXEC, str(SCRIPT_REPAIR),
                "--symbol", symbol,
                "--root", str(OUTPUT_BASE_1S),
                "--fallback-index",
                "--fallback-aggtrades"
            ]
            
            result_repair = subprocess.run(cmd_repair, text=True)
            if result_repair.returncode != 0:
                print(f"  Repair Failed with code {result_repair.returncode}")
                
        except KeyboardInterrupt:
            print("\nAborted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"  Critical execution error: {e}")

if __name__ == "__main__":
    main()
