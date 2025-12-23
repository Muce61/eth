
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
from backtest.real_engine import RealBacktestEngine

# Config
DATA_DIR = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
START_DATE = "2025-12-20"
END_DATE = "2025-12-21"

def run_test():
    logging.basicConfig(level=logging.ERROR)
    
    # 1. Load Data (Just one symbol for speed)
    data_feed = {}
    target_symbol = "ETHUSDT" # File is ETHUSDT.csv? Or ETHUSDTUSDT.csv? 
    # Usually in new_backtest_data, it matches file naming.
    # The snippet said symbol matching logic: check files ending in USDTUSDT.
    # Let's try to find the file.
    
    file_path = DATA_DIR / f"{target_symbol}.csv"
    if not file_path.exists():
         file_path = DATA_DIR / f"{target_symbol}USDT.csv"
    
    if not file_path.exists():
        print(f"Error: {file_path} not found")
        # Try finding any file
        files = list(DATA_DIR.glob("*.csv"))
        if files:
            file_path = files[0]
            target_symbol = file_path.stem
        else:
            return

    print(f"Loading {target_symbol} from {file_path}...")
    df = pd.read_csv(file_path)
    df.columns = [c.strip().lower() for c in df.columns]
    
    # Convert timestamps
    if 'open_time' in df.columns:
        df['timestamp'] = pd.to_datetime(df['open_time'])
    elif 'timestamp' in df.columns:
         try:
             df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
         except:
             df['timestamp'] = pd.to_datetime(df['timestamp'])
             
    df.set_index('timestamp', inplace=True)
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
        
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
        
    # RESAMPLE TO 15m (Simulate RealBacktestEngine load_data)
    agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    df_15m = df.resample('15min').agg(agg_dict).dropna()
    
    data_feed[target_symbol] = df_15m
    print(f"Loaded {len(df_15m)} 15m candles.")

    # 2. Init Engine
    engine = RealBacktestEngine(initial_balance=1000)
    engine.data_feed = data_feed
    engine.symbol_map_1s = {target_symbol: target_symbol} # Dummy map, assuming no 1s lookups for this test if possible, or mapping to self if name matches
    engine.active_1s_cache = {}

    # 3. Run
    # Use UTC explicitly
    start_dt = pd.Timestamp(START_DATE, tz='UTC')
    end_dt = pd.Timestamp(END_DATE, tz='UTC')
    
    print(f"Running backtest from {start_dt} to {end_dt}...")
    engine.run(start_date=start_dt, end_date=end_dt)
    print("Test Complete")

if __name__ == "__main__":
    run_test()
