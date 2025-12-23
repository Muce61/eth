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
        
        # Initialize 1s data map
        self.data_dir_1s = Path("/Users/muce/1m_data/klines_data_usdm_1s_agg")
        self.symbol_map_1s = {} 
        
        # Target Period: 3 Months
        # Load slightly earlier for warmup if needed, though engine handles warmup lookback.
        # But load_data needs to return the full range + warmup.
        # Config.WARMUP_DAYS = 11.
        end_date = pd.Timestamp("2026-01-01 00:00:00", tz='UTC') # Future buffer
        # Backtest last 3 months: Sep 26 - Dec 27
        start_date = pd.Timestamp("2025-09-01 00:00:00", tz='UTC') # Start earlier for warmup
        
        print(f"Loading 1m data (Raw) for period: {start_date} to {end_date}...")
        
        if not data_dir.exists():
             print(f"Error: Data directory {data_dir} not found.")
             return

        files = list(data_dir.glob("*.csv"))
        
        count = 0
        for file_path in files:
            try:
                symbol_1m = file_path.stem 
                
                # 1. Filter Delivery/Quarterly Contracts
                if '-' in symbol_1m:
                    continue
                    
                # 2. Map to 1s Symbol
                if symbol_1m.endswith("USDTUSDT"):
                     symbol_1s = symbol_1m.replace("USDTUSDT", "USDT")
                else:
                     symbol_1s = symbol_1m
                
                # Check 1s existence
                path_1s = self.data_dir_1s / f"{symbol_1s}_1s_agg"
                if not path_1s.exists():
                     path_1s_mark = self.data_dir_1s / f"{symbol_1s}_1s_mark"
                     if not path_1s_mark.exists():
                          # print(f"Skipping {symbol_1m}: No 1s data found")
                          continue
                     else:
                          self.symbol_map_1s[symbol_1m] = path_1s_mark
                else:
                     self.symbol_map_1s[symbol_1m] = path_1s

                # Parse Dates Robustly
                try:
                    df_1m = pd.read_csv(file_path, on_bad_lines='skip')
                    if 'timestamp' not in df_1m.columns:
                        continue
                        
                    df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'], errors='coerce')
                    df_1m = df_1m.dropna(subset=['timestamp'])
                    df_1m.set_index('timestamp', inplace=True)
                except Exception as e:
                    print(f"CSV Read Error {file_path.name}: {e}")
                    continue
                
                # Check timezone
                if df_1m.index.tz is None:
                    df_1m.index = df_1m.index.tz_localize('UTC')
                else:
                    df_1m.index = df_1m.index.tz_convert('UTC')
                
                # Filter by Date Range
                # Optimization: Check if file even overlaps with range before heavy operations
                if df_1m.index[-1] < start_date or df_1m.index[0] > end_date:
                    continue

                mask = (df_1m.index >= start_date) & (df_1m.index <= end_date)
                df_subset = df_1m.loc[mask].copy()
                
                if not df_subset.empty:
                    # FIX: Resample 1m to 15m to match Live Bot TIMEFRAME
                    agg_dict = {
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }
                    # Handle capitalized columns if needed (engine standardizes to lower, but raw might be different)
                    final_agg = {}
                    available_cols = df_subset.columns.tolist()
                    for col, func in agg_dict.items():
                        if col in available_cols:
                            final_agg[col] = func
                        elif col.capitalize() in available_cols:
                            final_agg[col.capitalize()] = func
                            
                    df_15m = df_subset.resample('15min').agg(final_agg).dropna()
                    df_15m.columns = [c.lower() for c in df_15m.columns]
                    
                    self.data_feed[symbol_1m] = df_15m
                    count += 1
                    
            except Exception as e:
                print(f"Error loading {file_path.name}: {e}")
                
        print(f"Loaded {count} symbols with 1m data in range.")

def main():
    # Target: Last 7 Days (Dec 21 - Dec 28) for Signal Verification
    end_date_str = "2025-12-28 14:00:00" 
    start_date_str = "2025-12-21 00:00:00"
    
    print(f"Starting Backtest (1m Timeframe): {start_date_str} -> {end_date_str} (UTC)")
    print("This might take a while due to large dataset processing...")
    
    # Run Engine
    engine = Last3MonthsBacktestEngine1m(initial_balance=100)
    
    # Hack config to use 15m (Enable Resampling for Signal)
    engine.config.TIMEFRAME = '15m'
    
    # Force alignment parameters just in case
    engine.config.TOP_GAINER_COUNT = 50 # Match 'Strict Logic' update
    
    engine.run(start_date=start_date_str, end_date=end_date_str, days=None)

if __name__ == "__main__":
    main()
