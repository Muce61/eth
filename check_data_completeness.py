
import pandas as pd
from pathlib import Path
from datetime import datetime
import glob
import os

DATA_DIR = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
TARGET_START = datetime(2024, 12, 17)
TARGET_END = datetime(2025, 12, 24)

if not DATA_DIR.exists():
    print(f"Directory {DATA_DIR} does not exist.")
    exit(1)

files = list(DATA_DIR.glob("*.csv"))
print(f"Found {len(files)} files in {DATA_DIR}")

incomplete = []
missing_start = []
missing_end = []
valid_count = 0
error_count = 0

print("Checking coverage...")

for file_path in files:
    try:
        # Read first and last row efficiently?
        # Pandas read_csv can be slow for many files.
        # But we only need timestamps.
        # Let's try reading just header and tail?
        # Actually, for 1 year of 1m data (500k rows), it is ~30-50MB.
        # Reading all might take a while for 600 files (18GB total).
        # Better to check file modification time? No, need content.
        
        # Read valid start:
        df_start = pd.read_csv(file_path, nrows=5)
        if df_start.empty or 'timestamp' not in df_start.columns:
            error_count += 1
            print(f"EMPTY/INVALID: {file_path.name}")
            continue
            
        start_ts_str = df_start['timestamp'].iloc[0]
        try:
             start_ts = pd.to_datetime(start_ts_str)
        except:
             start_ts = pd.to_datetime(start_ts_str, unit='ms')

        # Read valid end:
        # Use python seek to read end?
        # Or Just use pandas but skip?
        # pd.read_csv with chunksize? No.
        
        # Efficient tail reading in Python
        with open(file_path, 'rb') as f:
            try:
                f.seek(-1024, os.SEEK_END)
            except OSError:
                f.seek(0)
            last_chunk = f.readlines()
            last_line = last_chunk[-1].decode('utf-8')
            
            # Parse last line
            # Default CSV format: timestamp,open,high,low,close,volume
            # 2024-12-17 00:00:00, ...
            try:
                last_ts_str = last_line.split(',')[0]
                last_ts = pd.to_datetime(last_ts_str)
            except:
                 # Try parsing assuming index is first
                 pass
        
        # Verify
        s_ok = True
        e_ok = True
        
        # Allow some buffer (e.g. data starts at 00:01)
        if start_ts > TARGET_START + pd.Timedelta(hours=24):
             missing_start.append(f"{file_path.stem}: Starts {start_ts}")
             s_ok = False
             
        if last_ts < TARGET_END - pd.Timedelta(hours=24):
             missing_end.append(f"{file_path.stem}: Ends {last_ts}")
             e_ok = False
             
        if s_ok and e_ok:
            valid_count += 1
        else:
            incomplete.append(file_path.stem)
            
    except Exception as e:
        error_count += 1
        # print(f"Error checking {file_path.name}: {e}")

print(f"\nChecked {len(files)} files.")
print(f"✅ Valid Coverage: {valid_count}")
print(f"❌ Incomplete: {len(incomplete)}")
print(f"⚠️ Errors: {error_count}")

if incomplete:
    print("\nSample Incomplete:")
    for m in (missing_start[:10] + missing_end[:10]):
        print(m)
