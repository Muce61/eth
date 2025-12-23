#!/usr/bin/env python3
"""
Fetch Missing 1s Data
=====================
Reads missing_symbols.txt and fetches 1s data for them.
"""

import os
import sys
import subprocess
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
MISSING_FILE = PROJECT_ROOT / "missing_symbols.txt"
OUTPUT_BASE = Path("/Users/muce/1m_data/klines_data_usdm_1s_agg")
PYTHON_EXEC = sys.executable

# Scripts
SCRIPT_VISION_FETCH = Path(__file__).parent / "fetch_binance_vision_usdm_mark_1s.py"
SCRIPT_REPAIR = Path(__file__).parent / "repair_usdm_1s_mark.py"

# Config
LOOKBACK_DAYS = 370

def main():
    if not MISSING_FILE.exists():
        print(f"❌ Missing file not found: {MISSING_FILE}")
        return

    print(f"Reading missing symbols from {MISSING_FILE}...")
    with open(MISSING_FILE, 'r') as f:
        symbols = [line.strip() for line in f if line.strip()]

    # Filter out weird symbols (e.g. containing Chinese characters or dates)
    # Basic validation: alphanumeric and sensible length
    valid_symbols = []
    for s in symbols:
        if s.isalnum() and len(s) < 20:
             valid_symbols.append(s)
        else:
             print(f"⚠️  Skipping invalid symbol: {s}")

    print(f"Found {len(valid_symbols)} valid missing symbols.")
    print(f"Output Directory: {OUTPUT_BASE}")
    
    # Ensure output dir exists
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    for i, symbol in enumerate(valid_symbols, 1):
        print(f"\n[{i}/{len(valid_symbols)}] Processing {symbol}...")
        
        # 1. Fetch
        cmd_fetch = [
            PYTHON_EXEC, str(SCRIPT_VISION_FETCH),
            "--symbol", symbol,
            "--lookback-days", str(LOOKBACK_DAYS),
            "--out-base", str(OUTPUT_BASE),
            "--fallback-index",
            "--fallback-aggtrades"
        ]
        
        try:
            res = subprocess.run(cmd_fetch, text=True)
            if res.returncode != 0:
                print(f"  ❌ Fetch Failed for {symbol}")
                continue
                
            # 2. Repair
            print(f"  Repairing {symbol}...")
            cmd_repair = [
                PYTHON_EXEC, str(SCRIPT_REPAIR),
                "--symbol", symbol,
                "--root", str(OUTPUT_BASE),
                "--fallback-index", # Consistent args
                "--fallback-aggtrades"
            ]
            subprocess.run(cmd_repair, text=True)
            
        except KeyboardInterrupt:
            print("\nAborted.")
            sys.exit(0)
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    main()
