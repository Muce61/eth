import sys
import os
from pathlib import Path
from datetime import datetime
import pytz
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from backtest.real_engine import RealBacktestEngine

class Last3DaysBacktestEngine1m(RealBacktestEngine):
    def load_data(self):
        """
        Load data from the 1-year 1m dataset (updated incrementally).
        NO RESAMPLING - Uses raw 1m data.
        """
        data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
        self.data_feed = {}
        
        # Target Period: 
        start_date = pd.Timestamp("2025-12-08 05:30:00", tz='UTC')
        end_date = pd.Timestamp("2025-12-11 05:30:00", tz='UTC')
        
        print(f"Loading 1m data (Raw) for period: {start_date} to {end_date}...")
        
        files = list(data_dir.glob("*.csv"))
        
        count = 0
        for file_path in files:
            try:
                symbol = file_path.stem 
                # Parse Dates
                df_1m = pd.read_csv(file_path, parse_dates=['timestamp'], index_col='timestamp')
                
                # Check timezone
                if df_1m.index.tz is None:
                    df_1m.index = df_1m.index.tz_localize('UTC')
                
                # Filter by Date Range
                mask = (df_1m.index >= start_date) & (df_1m.index <= end_date)
                df_subset = df_1m.loc[mask].copy()
                
                if not df_subset.empty:
                    # NO RESAMPLING
                    df_subset.columns = [c.lower() for c in df_subset.columns]
                    self.data_feed[symbol] = df_subset
                    count += 1
                    
            except Exception as e:
                print(f"Error loading {file_path.name}: {e}")
                
        print(f"Loaded {count} symbols with 1m data in range.")

def main():
    start_date = "2025-12-08 05:30:00"
    end_date = "2025-12-11 05:30:00"
    
    print(f"Starting 3-Day Backtest (1m Timeframe): {start_date} -> {end_date} (UTC)")
    
    # Run Engine
    engine = Last3DaysBacktestEngine1m(initial_balance=1000)
    # Hack config to use 1m
    engine.config.TIMEFRAME = '1m' 
    engine.run(start_date=start_date, end_date=end_date, days=None)

if __name__ == "__main__":
    main()
