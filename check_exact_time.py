
import pandas as pd
from pathlib import Path
import os

DATA_DIR = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
samples = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"]

print(f"Checking end times for samples in {DATA_DIR}...")

for sym in samples:
    path = DATA_DIR / f"{sym}.csv"
    if path.exists():
        with open(path, 'rb') as f:
            try:
                f.seek(-200, os.SEEK_END)
            except OSError:
                f.seek(0)
            lines = f.readlines()
            if lines:
                last_line = lines[-1].decode('utf-8').strip()
                # csv format: timestamp,open,high,low,close,volume
                ts_str = last_line.split(',')[0]
                ts = pd.to_datetime(ts_str)
                print(f"{sym}: {ts} (UTC)")
            else:
                print(f"{sym}: EMPTY")
    else:
        print(f"{sym}: NOT FOUND")
