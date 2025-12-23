
import pandas as pd
from datetime import datetime, timezone

file_path = "/Users/muce/1m_data/klines_data_usdm_1s_agg/TNSRUSDT_1s_agg/TNSRUSDT_1s_20251123.parquet"

try:
    df = pd.read_parquet(file_path)
    
    if 'timestamp' in df.columns:
        if df['timestamp'].iloc[0] > 20000000000: # ms
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        else:
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC').dt.tz_localize(None)
            
        df.set_index('timestamp', inplace=True)
    elif isinstance(df.index, pd.DatetimeIndex):
         if df.index.tz is not None:
             df.index = df.index.tz_convert('UTC').tz_localize(None)

    # Filter for 10:30:00 - 10:30:59
    target_start = datetime.strptime("2025-11-23 10:30:00", "%Y-%m-%d %H:%M:%S")
    target_end = datetime.strptime("2025-11-23 10:31:00", "%Y-%m-%d %H:%M:%S")
    
    slice_data = df.loc[target_start:target_end]
    
    print("CHECKING MAX HIGH IN FULL MINUTE:")
    max_h = slice_data['high'].max()
    print(f"Max High: {max_h}")
    
    # Find row with max high or >= 0.1397
    spike_row = slice_data[slice_data['high'] >= 0.1397]
    if not spike_row.empty:
        print("\nFOUND SPIKE >= 0.1397:")
        print(spike_row)
    else:
        print("\nNO PRICE >= 0.1397 found in this minute.")
        idxmax = slice_data['high'].idxmax()
        print(f"Peak was {max_h} at {idxmax}")

except Exception as e:
    print(f"Error: {e}")
