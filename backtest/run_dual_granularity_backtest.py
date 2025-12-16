"""
Dual Granularity Backtest Runner
================================
Uses 15m data for signal generation and 1m data for exit checks.

Architecture:
- data_feed: 15m preprocessed data (for strategy signals)
- data_feed_1m: 1m raw data (for granular exit checks)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Import base engine
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from backtest.real_engine import RealBacktestEngine


class DualGranularityBacktestEngine(RealBacktestEngine):
    """
    Enhanced backtest engine with dual data source architecture.
    """
    
    def __init__(self, initial_balance=1000):
        super().__init__(initial_balance)
        self.data_feed_1m = {}  # 1m data for exit checks
        
    def load_data(self, start_date=None, end_date=None):
        """
        Load both 15m (preprocessed) and 1m (raw) data.
        """
        # Parse date range
        if start_date:
            start_ts = pd.Timestamp(start_date)
            if start_ts.tzinfo is None:
                start_ts = start_ts.tz_localize('UTC')
        else:
            start_ts = None
            
        if end_date:
            end_ts = pd.Timestamp(end_date)
            if end_ts.tzinfo is None:
                end_ts = end_ts.tz_localize('UTC')
        else:
            end_ts = None
            
        # === LOAD 15m PREPROCESSED DATA (for signals) ===
        data_dir_15m = Path("e:/dikong/eth/data/15m_preprocessed")
        self.data_feed = {}
        count_15m = 0
        
        print(f"Loading 15m preprocessed data from {data_dir_15m}...")
        
        for file_path in data_dir_15m.glob("*.csv"):
            symbol = file_path.stem
            try:
                df = pd.read_csv(file_path, parse_dates=['timestamp'], index_col='timestamp')
                
                # Normalize timezone: If naive, localize to UTC; if already tz-aware, convert to UTC
                if df.index.tz is None:
                    df.index = df.index.tz_localize('UTC')
                else:
                    df.index = df.index.tz_convert('UTC')
                
                # Filter by date range (both now guaranteed to be tz-aware UTC)
                if start_ts is not None:
                    df = df[df.index >= start_ts]
                if end_ts is not None:
                    df = df[df.index <= end_ts]
                    
                if len(df) > 0:
                    df.columns = [c.lower() for c in df.columns]
                    self.data_feed[symbol] = df
                    count_15m += 1
                    
            except Exception as e:
                print(f"Error loading 15m {file_path.name}: {e}")
                
        print(f"Loaded {count_15m} symbols with 15m data.")
        
        # === LOAD 1m RAW DATA (for exit checks) ===
        data_dir_1m = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
        self.data_feed_1m = {}
        count_1m = 0
        
        print(f"Loading 1m raw data from {data_dir_1m}...")
        
        # Only load 1m data for symbols we have 15m data for
        symbols_to_load = set(self.data_feed.keys())
        
        for file_path in data_dir_1m.glob("*.csv"):
            symbol = file_path.stem
            if symbol not in symbols_to_load:
                continue
                
            try:
                df = pd.read_csv(file_path, parse_dates=['timestamp'], index_col='timestamp')
                
                # Filter by date range
                if start_ts is not None:
                    df = df[df.index >= start_ts]
                if end_ts is not None:
                    df = df[df.index <= end_ts]
                    
                if len(df) > 0:
                    df.columns = [c.lower() for c in df.columns]
                    self.data_feed_1m[symbol] = df
                    count_1m += 1
                    
            except Exception as e:
                print(f"Error loading 1m {file_path.name}: {e}")
                
        print(f"Loaded {count_1m} symbols with 1m data for exit checks.")
        
    def _manage_single_position(self, symbol, current_time, is_paper=False):
        """
        Override: Use 1m data for granular exit checks.
        """
        if is_paper:
            positions = self.paper_positions
        else:
            positions = self.positions
            
        if symbol not in positions:
            return
            
        pos = positions[symbol]
        
        # Get 1m data for this symbol
        df_1m = self.data_feed_1m.get(symbol)
        if df_1m is None or df_1m.empty:
            # Fallback to 15m data if 1m not available
            df_1m = self.data_feed.get(symbol)
            if df_1m is None:
                return
                
        # Ensure current_time is comparable with DataFrame index
        # Handle timezone mismatch
        if df_1m.index.tz is None and current_time.tzinfo is not None:
            current_time_naive = current_time.replace(tzinfo=None)
        else:
            current_time_naive = current_time
                
        # Find all 1m candles since last check (within current 15m window)
        # current_time is on 15m boundary (e.g., 12:15)
        # We want to check 1m candles from 12:00 to 12:14
        window_start = current_time_naive - timedelta(minutes=15)
        window_end = current_time_naive - timedelta(minutes=1)  # Exclude current minute
        
        # Get 1m candles in this window
        mask = (df_1m.index > window_start) & (df_1m.index <= window_end)
        candles_in_window = df_1m[mask]
        
        if candles_in_window.empty:
            # No 1m data in window, use 15m fallback
            df_15m = self.data_feed.get(symbol)
            if df_15m is None or current_time_naive not in df_15m.index:
                return
            row_loc = df_15m.index.get_loc(current_time_naive)
            if row_loc < 1:
                return
            candle = df_15m.iloc[row_loc - 1]  # Previous closed candle
            high_price = candle['high']
            low_price = candle['low']
            
            # Update trailing stop with High
            if high_price > pos.get('highest_price', pos['entry_price']):
                pos['highest_price'] = high_price
                
            # Check stop loss
            if low_price <= pos['stop_loss']:
                self._close_position(symbol, pos['stop_loss'], current_time, 'Stop Loss')
                return
                
            # Check smart exit
            should_exit, reason, exit_price = self.smart_exit.check_exit(pos, low_price, current_time)
            if should_exit:
                self._close_position(symbol, exit_price, current_time, reason)
            return
            
        # Process each 1m candle sequentially for granular exit
        for ts, candle in candles_in_window.iterrows():
            high_price = candle['high']
            low_price = candle['low']
            
            # 1. Update trailing stop with High (optimistic: price went high first)
            if high_price > pos.get('highest_price', pos['entry_price']):
                pos['highest_price'] = high_price
                
            # 2. Check stop loss with Low (pessimistic: then crashed)
            if low_price <= pos['stop_loss']:
                self._close_position(symbol, pos['stop_loss'], ts, 'Stop Loss')
                return
                
            # 3. Check smart exit (trailing stop, break-even, etc.)
            should_exit, reason, exit_price = self.smart_exit.check_exit(pos, low_price, ts)
            if should_exit:
                self._close_position(symbol, exit_price, ts, reason)
                return


def main():
    end_date_str = "2025-12-12 20:00:00"
    # 90 days prior = 2025-09-13
    start_date_str = "2025-09-13 05:30:00"
    
    print(f"Starting Dual-Granularity Backtest: {start_date_str} -> {end_date_str} (UTC)")
    print("Signal Layer: 15m preprocessed data")
    print("Execution Layer: 1m raw data for exit checks")
    print("=" * 60)
    
    # Run Engine
    engine = DualGranularityBacktestEngine(initial_balance=1000)
    
    # Set config to 15m (for signal generation)
    engine.config.TIMEFRAME = '15m'
    engine.config.TOP_GAINER_COUNT = 50
    
    # Load both data sources
    engine.load_data(start_date=start_date_str, end_date=end_date_str)
    
    # Run backtest
    engine.run(start_date=start_date_str, end_date=end_date_str, days=None)


if __name__ == "__main__":
    main()
