import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import pytz

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from backtest.real_engine import RealBacktestEngine

class Last3MonthsBacktestEngine1m(RealBacktestEngine):
    def load_data(self):
        """
        Load data from the 1-year 1m dataset (updated incrementally).
        NO RESAMPLING - Uses raw 1m data.
        """
        data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
        self.data_feed = {}
        
        # Target Period: 3 Months Back from 2025-12-11
        # Start: Sep 11, 2025 (Approx)
        end_date = pd.Timestamp("2025-12-11 05:30:00", tz='UTC')
        start_date = end_date - pd.Timedelta(days=90)
        
        print(f"Loading 1m data (Raw) for period: {start_date} to {end_date}...")
        
        if not data_dir.exists():
             print(f"Error: Data directory {data_dir} not found.")
             return

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
                # Optimization: Check if file even overlaps with range before heavy operations
                if df_1m.index[-1] < start_date or df_1m.index[0] > end_date:
                    continue

                mask = (df_1m.index >= start_date) & (df_1m.index <= end_date)
                df_subset = df_1m.loc[mask].copy()
                
                if not df_subset.empty:
                    # NO RESAMPLING
                    # Ensure column names are lower case
                    df_subset.columns = [c.lower() for c in df_subset.columns]
                    self.data_feed[symbol] = df_subset
                    count += 1
                    
            except Exception as e:
                print(f"Error loading {file_path.name}: {e}")
                
        print(f"Loaded {count} symbols with 1m data in range.")

def main():
    end_date_str = "2025-12-11 05:30:00"
    # 90 days prior = 2025-09-12
    start_date_str = "2025-09-12 05:30:00"
    
    print(f"Starting 3-Month Backtest (1m Timeframe): {start_date_str} -> {end_date_str} (UTC)")
    print("This might take a while due to large dataset processing...")
    
    # Run Engine
    engine = Last3MonthsBacktestEngine1m(initial_balance=1000)
    
    # Hack config to use 15m (Enable Resampling for Signal)
    engine.config.TIMEFRAME = '15m'
    
    # Force alignment parameters just in case
    engine.config.TOP_GAINER_COUNT = 50 # Match 'Strict Logic' update
    
    engine.run(start_date=start_date_str, end_date=end_date_str, days=None)

if __name__ == "__main__":
    main()
