import pandas as pd
from pathlib import Path
import os
import sys

def clean_and_verify():
    data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
    
    if not data_dir.exists():
        print(f"Directory not found: {data_dir}")
        return

    print(f"🧹 Starting cleanup in {data_dir}...")
    
    files = list(data_dir.glob("*.csv"))
    print(f"Initial file count: {len(files)}")
    
    deleted_usdtusdt = 0
    deleted_corrupt = 0
    valid_files = 0
    
    # 1. Delete USDTUSDT files
    usdt_usdt_files = [f for f in files if "USDTUSDT" in f.name]
    for f in usdt_usdt_files:
        try:
            os.remove(f)
            deleted_usdtusdt += 1
            # print(f"Deleted duplicate: {f.name}")
        except Exception as e:
            print(f"Error deleting {f.name}: {e}")
            
    print(f"🗑️ Deleted {deleted_usdtusdt} 'USDTUSDT' duplicates.")
    
    # 2. Check for Corruption in Remaining Files
    remaining_files = list(data_dir.glob("*.csv"))
    print(f"Checking {len(remaining_files)} remaining files for corruption...")
    
    for i, f in enumerate(remaining_files):
        try:
            # Try to read and parse timestamps
            # Using 'coerce' will turn bad strings into NaT. 
            # If a file has mixed format lines from bad merges, verify we can read it.
            
            # Simple check: timestamp column exists and head/tail readable
            df = pd.read_csv(f)
            
            if 'timestamp' not in df.columns:
                raise ValueError("No timestamp column")
            
            # Strict parse check on sample to catch "unconverted data" error
            # If read_csv worked, it processed the file structure.
            # But the 'timestamp' col might contain unparseable strings if we don't convert.
            
            pd.to_datetime(df['timestamp'], errors='raise') # Will raise if format is completely broken/mixed in a way pandas hates
            
            valid_files += 1
            
        except Exception as e:
            try:
                os.remove(f)
                deleted_corrupt += 1
                # print(f"Deleted corrupt {f.name}: {e}")
            except OSError as os_err:
                 print(f"Failed to delete {f.name}: {os_err}")
        
        if i % 100 == 0:
            print(f"Processed {i}...", end='\r')
            
    print(f"\n🗑️ Deleted {deleted_corrupt} corrupt/unreadable files.")
    print(f"✅ Valid files remaining: {valid_files}")
    
    # 3. Final Verification of Dec 24 Coverage (Optional but requested)
    print("\n🔍 Verifying Dec 24 Coverage for survivors...")
    
    final_files = list(data_dir.glob("*.csv"))
    start_target = pd.Timestamp("2025-12-24 00:00:00", tz='UTC')
    end_target = pd.Timestamp("2025-12-24 23:59:00", tz='UTC')
    
    fully_covered = 0
    
    for f in final_files:
        try:
            df = pd.read_csv(f)
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])
            
            if df.empty: continue
            
            # TZ Normalize
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
            else:
                 df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
                 
            min_ts = df.iloc[0]['timestamp']
            max_ts = df.iloc[-1]['timestamp']
            
            if min_ts <= start_target and max_ts >= end_target:
                fully_covered += 1
                
        except:
            pass
            
    print(f"🌟 Files with FULL Dec 24 coverage: {fully_covered}")

if __name__ == "__main__":
    clean_and_verify()
