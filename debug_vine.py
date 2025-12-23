
import pandas as pd
from datetime import datetime, timezone

file_path = "/Users/muce/1m_data/klines_data_usdm_1s_agg/VINEUSDT_1s_agg/VINEUSDT_1s_20250502.parquet"

try:
    df = pd.read_parquet(file_path)
    if 'timestamp' in df.columns:
        if df['timestamp'].iloc[0] > 20000000000:
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        else:
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        if df['timestamp'].dt.tz is not None:
             df['timestamp'] = df['timestamp'].dt.tz_convert('UTC').dt.tz_localize(None)
        df.set_index('timestamp', inplace=True)
        
    start = datetime(2025, 5, 2, 12, 45, 0)
    end = datetime(2025, 5, 2, 12, 46, 0)
    
    slice_data = df.loc[start:end]
    print("VINEUSDT Data 12:45:00 - 12:46:00:")
    print(slice_data.head(10))
    print("...")
    
    # Check Max High
    max_h = slice_data['high'].max()
    print(f"Max High: {max_h}")
    
except Exception as e:
    print(f"Error: {e}")
