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

class FullYearBacktestEngine1m(RealBacktestEngine):
    def load_data(self):
        """
        Load data from the 1-year 1m dataset.
        Uses raw 1m data for universe and resamples to 15m for strategy.
        """
        data_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
        self.data_feed = {}
        
        # Initialize 1s data map
        self.data_dir_1s = Path("/Users/muce/1m_data/klines_data_usdm_1s_agg")
        self.symbol_map_1s = {} 
        
        # Target Period: 1 Year Back from 2025-12-22
        end_date = pd.Timestamp("2025-12-22 20:00:00", tz='UTC')
        start_date = end_date - pd.Timedelta(days=365)
        
        print(f"Loading 1m data (Raw) for 1-Year period: {start_date} to {end_date}...")
        
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
                          continue
                     else:
                          self.symbol_map_1s[symbol_1m] = path_1s_mark
                else:
                     self.symbol_map_1s[symbol_1m] = path_1s

                # Parse CSV
                try:
                    df_1m = pd.read_csv(file_path, on_bad_lines='skip')
                    if 'timestamp' not in df_1m.columns:
                        continue
                        
                    df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'], errors='coerce')
                    df_1m = df_1m.dropna(subset=['timestamp'])
                    df_1m.set_index('timestamp', inplace=True)
                except Exception as e:
                    continue
                
                # Check timezone
                if df_1m.index.tz is None:
                    df_1m.index = df_1m.index.tz_localize('UTC')
                else:
                    df_1m.index = df_1m.index.tz_convert('UTC')
                
                # Date Filter
                if df_1m.index[-1] < start_date or df_1m.index[0] > end_date:
                    continue

                mask = (df_1m.index >= start_date) & (df_1m.index <= end_date)
                df_subset = df_1m.loc[mask].copy()
                
                if not df_subset.empty:
                    agg_dict = {
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }
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
                
        print(f"Loaded {count} symbols for 1-Year backtest.")

def main():
    end_date_str = "2025-12-22 20:00:00"
    start_date_str = "2024-12-22 20:00:00"
    
    print(f"Starting 1-Year Backtest: {start_date_str} -> {end_date_str} (UTC)")
    
    # Run Engine
    engine = FullYearBacktestEngine1m(initial_balance=1000)
    engine.config.TIMEFRAME = '15m'
    engine.config.TOP_GAINER_COUNT = 50 
    
    # Note: This will generate trades in backtest_trades.csv
    # We might want to rename it afterward to avoid overwriting 3-month results
    engine.run(start_date=start_date_str, end_date=end_date_str)

if __name__ == "__main__":
    main()
