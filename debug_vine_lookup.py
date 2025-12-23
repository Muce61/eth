
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Mocking RealEngine Logic
symbol_1m = "VINEUSDT" 
data_dir_1s = Path("/Users/muce/1m_data/klines_data_usdm_1s_agg")

path_1s_dir = data_dir_1s / f"{symbol_1m}_1s_agg"

current_time = datetime(2025, 5, 2, 12, 45, 0)
date_str = current_time.strftime("%Y%m%d")

print(f"Checking Dir: {path_1s_dir}")
print(f"Exists? {path_1s_dir.exists()}")

# Construct file path
symbol_1s_name = path_1s_dir.stem.replace('_1s_agg', '')
file_name = f"{symbol_1s_name}_1s_{date_str}.parquet"
file_path = path_1s_dir / file_name

print(f"Looking for file: {file_path}")
print(f"Exists? {file_path.exists()}")

if file_path.exists():
    print("Loading Parquet...")
    df = pd.read_parquet(file_path)
    print(f"Loaded Rows: {len(df)}")
    
    # Check slice
    # Logic in RealEngine: 
    # start_slice = current_time
    # end_slice = current_time + timedelta(minutes=14, seconds=59)
    # But wait, RealEngine iterates 1s slices? 
    # NO. RealEngine.py: 
    # slice_1s = df_1s_day.loc[start_slice:end_slice]
    # It takes the WHOLE 15m block.
    
    # Filter 12:45:00 - 12:59:59
    start_slice = current_time
    end_slice = datetime(2025, 5, 2, 12, 59, 59)
    
    # Adjust TS in DF to match
    if 'timestamp' in df.columns:
         # Simplified conversion
         if df['timestamp'].iloc[0] > 20000000000:
              df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
         else:
              df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
         df.set_index('timestamp', inplace=True)
    
    # Naive vs Aware?
    # current_time is Naive above (no tzinfo). DF might be naive or aware.
    # Let's force naive
    if df.index.tz is not None:
         df.index = df.index.tz_convert('UTC').tz_localize(None)
         
    slice_1s = df.loc[start_slice:end_slice]
    print(f"Slice 15m Size: {len(slice_1s)}")
    
    max_h = slice_1s['high'].max()
    print(f"Max High in 15m window: {max_h}")
    # 0.0560 ?
