import pandas as pd
from pathlib import Path
from datetime import datetime
import pytz

def verify_data():
    data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
    
    # Target Range: Dec 24 00:00 to Dec 24 23:59 UTC
    start_target = pd.Timestamp("2025-12-24 00:00:00", tz='UTC')
    end_target = pd.Timestamp("2025-12-24 23:59:00", tz='UTC')
    
    files = list(data_dir.glob("*.csv"))
    print(f"🔍 Scanning {len(files)} files in {data_dir}...")
    
    complete_count = 0
    incomplete_count = 0
    missing_data = [] # List of (symbol, reason)
    
    for i, f in enumerate(files):
        try:
            # optimize: read first and last few rows only if possible? 
            # But we need to ensure no gaps inside? 
            # For "completeness" check, start and end is good proxy for now.
            # Reading full file is slow for 600 files.
            
            # Read first line and last line? 
            # Actually, files are small enough for 1 year (100MB max usually, 1m is smaller for few days).
            # These backfill files are likely small.
            
            df = pd.read_csv(f)
            if 'timestamp' not in df.columns:
                missing_data.append((f.stem, "No timestamp col"))
                incomplete_count += 1
                continue
                
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Normalize TZ
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
            else:
                df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
                
            min_ts = df['timestamp'].min()
            max_ts = df['timestamp'].max()
            
            # Check coverage
            starts_ok = min_ts <= start_target
            ends_ok = max_ts >= end_target
            
            if starts_ok and ends_ok:
                complete_count += 1
            else:
                reason = []
                if not starts_ok: reason.append(f"Starts Late ({min_ts})")
                if not ends_ok: reason.append(f"Ends Early ({max_ts})")
                missing_data.append((f.stem, ", ".join(reason)))
                incomplete_count += 1
                
        except Exception as e:
            missing_data.append((f.stem, f"Read Error: {e}"))
            incomplete_count += 1
            
        if i % 50 == 0:
            print(f"Processed {i}/{len(files)}...", end='\r')
            
    print("\n" + "="*50)
    print(f"✅ Complete Symbols: {complete_count}")
    print(f"❌ Incomplete Symbols: {incomplete_count}")
    print("="*50)
    
    if missing_data:
        print("\nincomplete Details (Top 20):")
        for sym, reason in missing_data[:20]:
            print(f"{sym}: {reason}")
            
    if incomplete_count == 0:
        print("\n🌟 All data files fully cover Dec 24!")

if __name__ == "__main__":
    verify_data()
