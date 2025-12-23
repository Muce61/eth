
import pandas as pd
from pathlib import Path
import os
import concurrent.futures

DATA_DIR = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
# We want data up to Dec 24 2025.
# Let's say anything ending before Dec 20 2025 is "Lagging".
CRITICAL_END = pd.Timestamp("2025-12-20")

files = list(DATA_DIR.glob("*.csv"))
print(f"Checking {len(files)} files for End Date lag...")

def check_end(f):
    try:
        with open(f, 'rb') as fh:
            try:
                fh.seek(-1024, os.SEEK_END)
            except OSError:
                fh.seek(0)
            lines = fh.readlines()
            if not lines:
                return f.stem, "EMPTY"
            last = lines[-1].decode('utf-8', errors='ignore')
            ts_str = last.split(',')[0]
            ts = pd.to_datetime(ts_str)
            if ts < CRITICAL_END:
                return f.stem, ts
    except Exception:
        pass
    return None

lagging = []
with concurrent.futures.ThreadPoolExecutor() as exe:
    futures = [exe.submit(check_end, f) for f in files]
    for fut in concurrent.futures.as_completed(futures):
        res = fut.result()
        if res:
            lagging.append(res)

if not lagging:
    print("✅ All files end after 2025-12-20! Data is up to date.")
else:
    print(f"❌ Found {len(lagging)} lagging files:")
    for sym, ts in lagging:
        print(f"{sym}: Ends {ts}")
