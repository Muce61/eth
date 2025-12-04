import pandas as pd
from pathlib import Path

file_path = "/Users/muce/1m_data/processed_15m_data/RENUSDTUSDT.csv"
df = pd.read_csv(file_path)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index('timestamp').sort_index()

nov_data = df.loc['2025-11-01':'2025-11-30']
print(f"Total rows: {len(df)}")
print(f"Nov 2025 rows: {len(nov_data)}")

if not nov_data.empty:
    print("First 5 rows of Nov:")
    print(nov_data.head())
    print("Last 5 rows of Nov:")
    print(nov_data.tail())
else:
    print("No data for Nov 2025!")
    # Check nearest data
    print("Last data before Nov:")
    print(df.loc[:'2025-11-01'].tail())
