import sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import time

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from data.binance_client import BinanceClient

def main():
    print("="*60)
    print("Fetching Global 1m Data (Dec 23 - Dec 25) for Backtest")
    print("="*60)
    
    client = BinanceClient()
    
    # Target Data Directory (Main Backtest Dir)
    save_dir = Path("/Users/muce/1m_data/new_backtest_data_1year_1m")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # Fetch ALL USDT Symbols
    print("Fetching ALL USDT Futures symbols...")
    tickers = client.get_usdt_tickers()
    target_symbols = [t[0] for t in tickers]
    
    print(f"Targeting {len(target_symbols)} symbols.")
    
    # Target: 1 Year (2024-12-17 to 2025-12-24)
    # End date includes the full day of Dec 24? Yes, user said "to 20251224".
    # Assuming inclusive.
    date_end_str = "2025-12-24 23:59:59"
    date_start_str = "2024-12-17 00:00:00"
    
    start_dt = datetime.strptime(date_start_str, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(date_end_str, "%Y-%m-%d %H:%M:%S")
    
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    
    success_count = 0
    
    for i, symbol in enumerate(target_symbols):
        try:
            clean_sym = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
            save_path = save_dir / f"{clean_sym}.csv"
            
            # RESUME LOGIC: If file > 1MB, assume it was backfilled in the last run and skip.
            # (Old files were ~200KB for 2-3 days data. Full year is ~30MB)
            if save_path.exists() and save_path.stat().st_size > 1_000_000:
                 print(f"[{i+1}/{len(target_symbols)}] Skipping {symbol} (Already backfilled: {save_path.stat().st_size/1024/1024:.1f}MB)", flush=True)
                 continue
                 
            print(f"[{i+1}/{len(target_symbols)}] Fetching {symbol}...", end=' ', flush=True)
            
            all_ohlcv = []
            current_since = start_ts
            
            while current_since < end_ts:
                # Add retry logic
                retry = 0
                max_retries = 3
                ohlcv = None
                
                while retry < max_retries:
                    try:
                        ohlcv = client.exchange.fetch_ohlcv(symbol, timeframe='1m', since=current_since, limit=1500)
                        break
                    except Exception as e:
                        retry += 1
                        time.sleep(1)
                
                if not ohlcv:
                    break
                
                all_ohlcv.extend(ohlcv)
                
                last_ts = ohlcv[-1][0]
                current_since = last_ts + 60000
                
                if last_ts >= end_ts:
                    break
                    
                time.sleep(0.2) 
            
            if not all_ohlcv:
                continue
                
            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            clean_sym = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
            save_path = save_dir / f"{clean_sym}.csv"
            
            if save_path.exists():
                try:
                    existing_df = pd.read_csv(save_path)
                    # Try flexible parsing
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], errors='coerce')
                    existing_df = existing_df.dropna(subset=['timestamp'])
                    existing_df.set_index('timestamp', inplace=True)
                    
                    # Ensure timezone-naive for consistency (or convert all to UTC)
                    # Input 'df' from CCXT is naive (from unit='ms')
                    # If existing has tz info, convert to naive UTC or matching
                    if existing_df.index.tz is not None:
                        existing_df.index = existing_df.index.tz_convert(None) # Convert to naive UTC
                        
                    # Combine
                    combined = pd.concat([existing_df, df])
                    # Remove duplicates on index
                    combined = combined[~combined.index.duplicated(keep='last')]
                    combined.sort_index(inplace=True)
                    combined.to_csv(save_path)
                except Exception as e:
                    print(f"Merge Error {symbol}: {e}. Overwriting file.")
                    df.to_csv(save_path)
            else:
                df.to_csv(save_path)
            
            success_count += 1
            print(f"Done. ({len(df)} rows)", flush=True)
            time.sleep(0.2) # Increased sleep
            
        except Exception as e:
            print(f"\nError {symbol}: {e}")
            pass
            
    print(f"\n✅ Done. Updated {success_count} symbols in {save_dir}")

if __name__ == "__main__":
    main()
