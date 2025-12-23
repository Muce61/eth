
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
from pathlib import Path
from dotenv import load_dotenv

# Load Environment Variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path, override=True)

API_KEY = os.getenv("BINANCE_API_KEY")
SECRET = os.getenv("BINANCE_SECRET")

if not API_KEY or not SECRET:
    print("❌ Error: BINANCE_API_KEY or BINANCE_SECRET not found in .env")
    sys.exit(1)

def fetch_position_history(days=7):
    """
    Fetches position history by:
    1. Scanning Global Income History to find 'REALIZED_PNL' events (identifies traded symbols).
    2. Fetching Trade History ONLY for those identified symbols (efficient).
    """
    client = Client(API_KEY, SECRET)
    
    end_time_dt = datetime.now()
    start_time_dt = end_time_dt - timedelta(days=days)
    
    start_ts = int(start_time_dt.timestamp() * 1000)
    end_ts = int(end_time_dt.timestamp() * 1000)
    
    print(f"🔍 Scanning Income History from {start_time_dt} to {end_time_dt}...")
    
    # 1. Fetch Global Income History (No Symbol Filter)
    # This allows us to find WHAT we traded without looping 300 symbols.
    income_logs = []
    current_start = start_ts
    
    while True:
        # Fetch mostly chunks
        chunk = client.futures_income_history(startTime=current_start, endTime=end_ts, limit=1000)
        
        if not chunk:
            break
            
        income_logs.extend(chunk)
        
        # Pagination Update
        last_item_time = chunk[-1]['time']
        if last_item_time == current_start:
            # Prevent infinite loop if multiple items have exact same ms timestamp
            current_start += 1 
        else:
            current_start = last_item_time
            
        if current_start >= end_ts:
            break
            
        print(f"   Fetched {len(income_logs)} income records... (Latest: {datetime.fromtimestamp(current_start/1000)})")

    # Filter for REALIZED_PNL to find 'Closed Positions'
    pnl_records = [x for x in income_logs if x['incomeType'] == 'REALIZED_PNL']
    
    if not pnl_records:
        print("⚠️ No Realized PnL records found in this period.")
        return

    # Extract Unique Symbols
    traded_symbols = list(set([x['symbol'] for x in pnl_records]))
    print(f"✅ Found {len(traded_symbols)} Traded Symbols: {traded_symbols}")
    
    # 2. Fetch Detailed Trade History (Targeted)
    all_trades = []
    
    for symbol in traded_symbols:
        print(f"   ⬇️ Downloading Trades for {symbol}...")
        try:
            # We fetch trades for the same period
            # Note: futures_account_trades allows 7 days range max usually? 
            # Actually standard api has 7 day limit per call usually. 
            # python-binance handles filtering but the API might complain if range is too wide.
            # We loop simply if needed, but for 'days=7' it usually fits or we rely on recent.
            # Ideally we use start_ts. 
            
            # The API constraint: "The difference between startTime and endTime can't be longer than 7 days"
            # We already set days=7 default.
            trades = client.futures_account_trades(symbol=symbol, startTime=start_ts, endTime=end_ts)
            all_trades.extend(trades)
        except Exception as e:
            print(f"   ❌ Error fetching {symbol}: {e}")

    if not all_trades:
        print("⚠️ No Trade records found.")
        return

    # 3. Save to CSV
    df = pd.DataFrame(all_trades)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df.sort_values(by='time', inplace=True)
    
    output_file = "logs/historical_trades_smart.csv"
    df.to_csv(output_file, index=False)
    
    print("\n" + "="*50)
    print(f"✅ Successfully exported {len(df)} trades to {output_file}")
    print("="*50)
    print("Sample Data:")
    print(df[['time', 'symbol', 'side', 'price', 'qty', 'realizedPnl', 'commission']].head())

if __name__ == "__main__":
    # Default 7 days, can be changed
    fetch_position_history(days=7)
